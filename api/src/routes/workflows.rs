use axum::{Json, extract::{State, Path}, http::StatusCode};
use serde::{Deserialize, Serialize};
use validator::Validate;
use uuid::Uuid;

use crate::middleware::AuthUser;
use crate::state::AppState;
use common::{AppError, AppResult, ids};
use db::queries;
use domain::JobKind;

#[derive(Debug, Deserialize, Validate)]
pub struct CreateWorkflowReq {
    pub project_id: Uuid,
    #[validate(length(min=1, max=100))]
    pub name: String,
    pub jobs: Vec<WorkflowJob>,
    pub edges: Vec<WorkflowEdge>,
}

#[derive(Debug, Deserialize, Validate)]
pub struct WorkflowJob {
    pub queue_id: Uuid,
    pub payload: serde_json::Value,
    #[validate(range(min=0, max=100))]
    pub priority: Option<i32>,
}

#[derive(Debug, Deserialize)]
pub struct WorkflowEdge {
    pub parent: usize, // index into jobs array
    pub child: usize,
}

#[derive(Debug, Serialize)]
pub struct WorkflowResp {
    pub workflow_id: Uuid,
    pub jobs: Vec<serde_json::Value>,
    pub edges: Vec<serde_json::Value>,
}

pub async fn create(
    auth: AuthUser,
    State(state): State<AppState>,
    Json(req): Json<CreateWorkflowReq>,
) -> AppResult<(StatusCode, Json<WorkflowResp>)> {
    req.validate().map_err(|e| AppError::Validation(e.to_string()))?;
    if req.jobs.is_empty() { return Err(AppError::Validation("workflow must have at least one job".to_string())); }
    // Check project membership
    let proj = queries::get_project(&state.pool, req.project_id).await?
        .ok_or_else(|| AppError::NotFound("project not found".to_string()))?;
    if !queries::user_in_org(&state.pool, auth.user_id, proj.org_id).await? {
        return Err(AppError::Forbidden("forbidden".to_string()));
    }
    // Validate edges
    for e in &req.edges {
        if e.parent >= req.jobs.len() || e.child >= req.jobs.len() {
            return Err(AppError::Validation("edge index out of bounds".to_string()));
        }
        if e.parent == e.child {
            return Err(AppError::Validation("self loop".to_string()));
        }
    }
    // Create workflow
    let wf_id: Uuid = sqlx::query_scalar("INSERT INTO workflows (project_id, name) VALUES ($1, $2) RETURNING id")
        .bind(req.project_id)
        .bind(&req.name)
        .fetch_one(&state.pool)
        .await?;
    // Create jobs
    let mut job_ids = Vec::new();
    let mut job_vals = Vec::new();
    for (idx, wj) in req.jobs.iter().enumerate() {
        let queue = queries::get_queue(&state.pool, wj.queue_id).await?
            .ok_or_else(|| AppError::NotFound(format!("queue {} not found", idx)))?;
        if queue.project_id != req.project_id {
            return Err(AppError::Validation(format!("job {} queue not in project", idx)));
        }
        // Check if this job has any parent (is child)
        let is_child = req.edges.iter().any(|e| e.child == idx);
        let kind = if is_child { JobKind::Immediate } else { JobKind::Immediate };
        let status = if is_child { "WAITING" } else { "QUEUED" };
        let priority = wj.priority.unwrap_or(queue.default_priority);
        let payload = wj.payload.clone();
        let job_id: Uuid = sqlx::query_scalar(
            r#"INSERT INTO jobs (queue_id, workflow_id, type, status, payload, priority, max_attempts, queued_at)
               VALUES ($1, $2, 'immediate', $3::job_status, $4, $5, 3, CASE WHEN $3='QUEUED' THEN NOW() ELSE NULL END)
               RETURNING id"#,
        )
        .bind(wj.queue_id)
        .bind(wf_id)
        .bind(status)
        .bind(&payload)
        .bind(priority)
        .fetch_one(&state.pool)
        .await?;
        // If queued, create outbox
        if !is_child {
            let subject = ids::nats_subject(&proj.org_id, &proj.id, &wj.queue_id, priority);
            let eid = Uuid::new_v4();
            sqlx::query(
                r#"INSERT INTO outbox_events (id, job_id, queue_id, org_id, project_id, subject, payload, priority, nats_msg_id)
                   VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)"#,
            )
            .bind(eid)
            .bind(job_id)
            .bind(wj.queue_id)
            .bind(proj.org_id)
            .bind(proj.id)
            .bind(&subject)
            .bind(&payload)
            .bind(priority)
            .bind(eid.to_string())
            .execute(&state.pool)
            .await?;
        }
        job_ids.push(job_id);
        job_vals.push(serde_json::json!({"id": job_id, "queue_id": wj.queue_id, "status": status, "priority": priority}));
    }
    // Create edges
    let mut edge_vals = Vec::new();
    for e in req.edges {
        let parent_id = job_ids[e.parent];
        let child_id = job_ids[e.child];
        sqlx::query("INSERT INTO workflow_edges (parent_id, child_id) VALUES ($1, $2) ON CONFLICT DO NOTHING")
            .bind(parent_id)
            .bind(child_id)
            .execute(&state.pool)
            .await?;
        edge_vals.push(serde_json::json!({"parent": parent_id, "child": child_id}));
    }
    Ok((StatusCode::CREATED, Json(WorkflowResp{ workflow_id: wf_id, jobs: job_vals, edges: edge_vals })))
}

pub async fn get(
    auth: AuthUser,
    State(state): State<AppState>,
    Path(wf_id): Path<Uuid>,
) -> AppResult<Json<serde_json::Value>> {
    let wf: Option<(Uuid, Uuid, String, String, chrono::DateTime<chrono::Utc>)> = sqlx::query_as(
        "SELECT id, project_id, name, status, created_at FROM workflows WHERE id = $1"
    )
    .bind(wf_id)
    .fetch_optional(&state.pool)
    .await?;
    let (id, proj_id, name, status, created_at) = wf.ok_or_else(|| AppError::NotFound("workflow not found".to_string()))?;
    let proj = queries::get_project(&state.pool, proj_id).await?
        .ok_or_else(|| AppError::NotFound("project not found".to_string()))?;
    if !queries::user_in_org(&state.pool, auth.user_id, proj.org_id).await? {
        return Err(AppError::Forbidden("forbidden".to_string()));
    }
    let jobs: Vec<db::models::Job> = sqlx::query_as("SELECT * FROM jobs WHERE workflow_id = $1 ORDER BY created_at")
        .bind(wf_id)
        .fetch_all(&state.pool)
        .await?;
    let edges: Vec<(Uuid, Uuid)> = sqlx::query_as("SELECT parent_id, child_id FROM workflow_edges WHERE parent_id IN (SELECT id FROM jobs WHERE workflow_id=$1) OR child_id IN (SELECT id FROM jobs WHERE workflow_id=$1)")
        .bind(wf_id)
        .fetch_all(&state.pool)
        .await?;
    Ok(Json(serde_json::json!({
        "id": id, "project_id": proj_id, "name": name, "status": status, "created_at": created_at,
        "jobs": jobs,
        "edges": edges.iter().map(|(p,c)| serde_json::json!({"parent": p, "child": c})).collect::<Vec<_>>()
    })))
}
