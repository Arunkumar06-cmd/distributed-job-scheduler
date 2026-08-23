use axum::{
    extract::{Path, State},
    http::StatusCode,
    Json,
};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use uuid::Uuid;
use validator::Validate;

use crate::middleware::AuthUser;
use crate::routes::validate::validate_payload;
use crate::state::AppState;
use common::{ids, AppError, AppResult};
use db::queries;

#[derive(Debug, Deserialize, Validate)]
pub struct CreateWorkflowReq {
    pub project_id: Uuid,
    #[validate(length(min = 1, max = 100))]
    pub name: String,
    #[validate(nested)]
    pub jobs: Vec<WorkflowJob>,
    pub edges: Vec<WorkflowEdge>,
}

#[derive(Debug, Deserialize, Validate)]
pub struct WorkflowJob {
    pub queue_id: Uuid,
    pub payload: serde_json::Value,
    #[validate(range(min = 0, max = 100))]
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
    req.validate()
        .map_err(|e| AppError::Validation(e.to_string()))?;
    if req.jobs.is_empty() {
        return Err(AppError::Validation(
            "workflow must have at least one job".to_string(),
        ));
    }
    // Check project membership
    let proj = queries::get_project(&state.pool, req.project_id)
        .await?
        .ok_or_else(|| AppError::NotFound("project not found".to_string()))?;
    queries::require_org_writer(&state.pool, auth.user_id, proj.org_id).await?;
    // Validate edges
    for e in &req.edges {
        if e.parent >= req.jobs.len() || e.child >= req.jobs.len() {
            return Err(AppError::Validation("edge index out of bounds".to_string()));
        }
        if e.parent == e.child {
            return Err(AppError::Validation("self loop".to_string()));
        }
    }
    validate_workflow_acyclic(req.jobs.len(), &req.edges)?;
    for (idx, wj) in req.jobs.iter().enumerate() {
        validate_payload(&wj.payload)
            .map_err(|e| AppError::Validation(format!("job {idx}: {e}")))?;
    }
    // Create workflow + jobs + edges in a single transaction
    let mut tx = state.pool.begin().await?;
    let wf_id: Uuid =
        sqlx::query_scalar("INSERT INTO workflows (project_id, name) VALUES ($1, $2) RETURNING id")
            .bind(req.project_id)
            .bind(&req.name)
            .fetch_one(&mut *tx)
            .await?;
    let mut job_ids = Vec::new();
    let mut job_vals = Vec::new();
    for (idx, wj) in req.jobs.iter().enumerate() {
        let queue = queries::get_queue(&state.pool, wj.queue_id)
            .await?
            .ok_or_else(|| AppError::NotFound(format!("queue {} not found", idx)))?;
        if queue.project_id != req.project_id {
            return Err(AppError::Validation(format!(
                "job {} queue not in project",
                idx
            )));
        }
        let is_child = req.edges.iter().any(|e| e.child == idx);
        let status = if is_child { "WAITING" } else { "QUEUED" };
        let priority = wj.priority.unwrap_or(queue.default_priority);
        // Pre-generate the id so shard routing uses the same stable key the
        // job row will carry; hardcoding shard 0 silently strands workflow
        // roots on sharded queues.
        let job_id = ids::new_id();
        let shard_id = ids::shard_for_key(&job_id.to_string(), queue.shard_count);
        let payload = wj.payload.clone();
        sqlx::query(
            r#"INSERT INTO jobs (id, queue_id, workflow_id, shard_id, type, status, payload, priority, max_attempts, queued_at)
               VALUES ($1, $2, $3, $6, 'immediate', $4::job_status, $5, $7, 3, CASE WHEN $4='QUEUED' THEN NOW() ELSE NULL END)"#,
        )
        .bind(job_id)
        .bind(wj.queue_id)
        .bind(wf_id)
        .bind(status)
        .bind(&payload)
        .bind(shard_id)
        .bind(priority)
        .execute(&mut *tx)
        .await?;
        if !is_child {
            let subject = ids::nats_subject_for_shard(
                &proj.org_id,
                &proj.id,
                &wj.queue_id,
                queue.shard_count,
                shard_id,
                priority,
            );
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
            .execute(&mut *tx)
            .await?;
        }
        job_ids.push(job_id);
        job_vals.push(serde_json::json!({"id": job_id, "queue_id": wj.queue_id, "status": status, "priority": priority}));
    }
    let mut edge_vals = Vec::new();
    for e in req.edges {
        let parent_id = job_ids[e.parent];
        let child_id = job_ids[e.child];
        sqlx::query("INSERT INTO workflow_edges (parent_id, child_id) VALUES ($1, $2) ON CONFLICT DO NOTHING")
            .bind(parent_id)
            .bind(child_id)
            .execute(&mut *tx)
            .await?;
        edge_vals.push(serde_json::json!({"parent": parent_id, "child": child_id}));
    }
    tx.commit().await?;
    let _ = state.broadcast.send(format!("workflow.created:{}", wf_id));
    Ok((
        StatusCode::CREATED,
        Json(WorkflowResp {
            workflow_id: wf_id,
            jobs: job_vals,
            edges: edge_vals,
        }),
    ))
}

/// Kahn's algorithm. The DB trigger also rejects cycles, but surfacing that as
/// a 500 makes the API unusable for clients; validate before writing.
fn validate_workflow_acyclic(node_count: usize, edges: &[WorkflowEdge]) -> AppResult<()> {
    let mut indegree = vec![0usize; node_count];
    let mut adj: HashMap<usize, Vec<usize>> = HashMap::new();
    for e in edges {
        adj.entry(e.parent).or_default().push(e.child);
        indegree[e.child] += 1;
    }
    let mut queue: std::collections::VecDeque<usize> =
        (0..node_count).filter(|&n| indegree[n] == 0).collect();
    let mut visited = 0usize;
    while let Some(n) = queue.pop_front() {
        visited += 1;
        if let Some(children) = adj.get(&n) {
            for &c in children {
                indegree[c] -= 1;
                if indegree[c] == 0 {
                    queue.push_back(c);
                }
            }
        }
    }
    if visited != node_count {
        return Err(AppError::Validation(
            "workflow edges contain a cycle".to_string(),
        ));
    }
    Ok(())
}

/// The workflows.status column is a frozen write-time value; derive the live
/// status from member jobs so the dashboard reflects reality.
fn derive_workflow_status(jobs: &[db::models::Job]) -> &'static str {
    use domain::JobStatus;
    if jobs.is_empty() {
        return "RUNNING";
    }
    let terminal = |s: JobStatus| s.is_terminal();
    if jobs.iter().all(|j| j.status == JobStatus::Completed) {
        "COMPLETED"
    } else if jobs.iter().all(|j| terminal(j.status)) {
        // Every job reached a terminal state but not all completed.
        "FAILED"
    } else {
        "RUNNING"
    }
}

pub async fn get(
    auth: AuthUser,
    State(state): State<AppState>,
    Path(wf_id): Path<Uuid>,
) -> AppResult<Json<serde_json::Value>> {
    let wf: Option<(Uuid, Uuid, String, String, chrono::DateTime<chrono::Utc>)> = sqlx::query_as(
        "SELECT id, project_id, name, status, created_at FROM workflows WHERE id = $1",
    )
    .bind(wf_id)
    .fetch_optional(&state.pool)
    .await?;
    let (id, proj_id, name, status, created_at) =
        wf.ok_or_else(|| AppError::NotFound("workflow not found".to_string()))?;
    let proj = queries::get_project(&state.pool, proj_id)
        .await?
        .ok_or_else(|| AppError::NotFound("project not found".to_string()))?;
    if !queries::user_in_org(&state.pool, auth.user_id, proj.org_id).await? {
        return Err(AppError::Forbidden("forbidden".to_string()));
    }
    let jobs: Vec<db::models::Job> =
        sqlx::query_as("SELECT * FROM jobs WHERE workflow_id = $1 ORDER BY created_at")
            .bind(wf_id)
            .fetch_all(&state.pool)
            .await?;
    let edges: Vec<(Uuid, Uuid)> = sqlx::query_as("SELECT parent_id, child_id FROM workflow_edges WHERE parent_id IN (SELECT id FROM jobs WHERE workflow_id=$1) OR child_id IN (SELECT id FROM jobs WHERE workflow_id=$1)")
        .bind(wf_id)
        .fetch_all(&state.pool)
        .await?;
    let live_status = derive_workflow_status(&jobs);
    Ok(Json(serde_json::json!({
        "id": id, "project_id": proj_id, "name": name,
        "status": live_status, "stored_status": status, "created_at": created_at,
        "jobs": jobs,
        "edges": edges.iter().map(|(p,c)| serde_json::json!({"parent": p, "child": c})).collect::<Vec<_>>()
    })))
}
