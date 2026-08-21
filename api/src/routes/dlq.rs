use axum::{
    extract::{Path, Query, State},
    Json,
};
use serde::Deserialize;

use crate::middleware::AuthUser;
use crate::state::AppState;
use common::{ids, AppError, AppResult};
use db::queries;
use uuid::Uuid;

#[derive(Debug, Deserialize)]
pub struct ListDlqQuery {
    pub queue_id: Option<Uuid>,
    pub page: Option<i64>,
    pub page_size: Option<i64>,
}

#[derive(sqlx::FromRow)]
struct FailureSummaryRow {
    queue_id: Uuid,
    summary: String,
    root_cause: Option<String>,
    remediation: Option<String>,
    model: String,
    created_at: chrono::DateTime<chrono::Utc>,
}

pub async fn list(
    auth: AuthUser,
    State(state): State<AppState>,
    Query(q): Query<ListDlqQuery>,
) -> AppResult<Json<serde_json::Value>> {
    if let Some(qid) = q.queue_id {
        let queue = queries::get_queue(&state.pool, qid)
            .await?
            .ok_or_else(|| AppError::NotFound("queue not found".to_string()))?;
        let project = queries::get_project(&state.pool, queue.project_id)
            .await?
            .ok_or_else(|| AppError::NotFound("project not found".to_string()))?;
        if !queries::user_in_org(&state.pool, auth.user_id, project.org_id).await? {
            return Err(AppError::Forbidden("forbidden".to_string()));
        }
    }
    let page = q.page.unwrap_or(1).max(1);
    let page_size = q.page_size.unwrap_or(20).clamp(1, 100);
    let offset = (page - 1) * page_size;
    let entries = queries::list_dlq_entries_for_user(
        &state.pool,
        auth.user_id,
        q.queue_id,
        page_size,
        offset,
    )
    .await?;
    Ok(Json(serde_json::json!({
        "data": entries,
        "page": page,
        "page_size": page_size
    })))
}

pub async fn replay(
    auth: AuthUser,
    State(state): State<AppState>,
    Path(dlq_id): Path<Uuid>,
) -> AppResult<Json<serde_json::Value>> {
    let dlq: db::models::DeadLetterEntry =
        sqlx::query_as("SELECT * FROM dead_letter_entries WHERE id = $1")
            .bind(dlq_id)
            .fetch_optional(&state.pool)
            .await?
            .ok_or_else(|| AppError::NotFound("DLQ entry not found".to_string()))?;
    let queue = queries::get_queue(&state.pool, dlq.queue_id)
        .await?
        .ok_or_else(|| AppError::NotFound("queue not found".to_string()))?;
    let project = queries::get_project(&state.pool, queue.project_id)
        .await?
        .ok_or_else(|| AppError::NotFound("project not found".to_string()))?;
    queries::require_org_writer(&state.pool, auth.user_id, project.org_id).await?;
    let subject = ids::nats_subject(&dlq.org_id, &dlq.project_id, &dlq.queue_id, 5);
    let job =
        queries::replay_dlq_entry(&state.pool, dlq_id, dlq.org_id, dlq.project_id, subject).await?;
    let _ = state.broadcast.send(format!("dlq.replayed:{}", dlq_id));
    Ok(Json(serde_json::json!(job)))
}

pub async fn summary(
    auth: AuthUser,
    State(state): State<AppState>,
    Path(dlq_id): Path<Uuid>,
) -> AppResult<Json<serde_json::Value>> {
    let row: Option<FailureSummaryRow> = sqlx::query_as(
        r#"SELECT d.queue_id, f.summary, f.root_cause, f.remediation, f.model, f.created_at
           FROM failure_summaries f JOIN dead_letter_entries d ON d.id = f.dlq_id
           WHERE f.dlq_id = $1"#,
    )
    .bind(dlq_id)
    .fetch_optional(&state.pool)
    .await?;
    let row = row.ok_or_else(|| AppError::NotFound("failure summary not found".to_string()))?;
    let queue = queries::get_queue(&state.pool, row.queue_id)
        .await?
        .ok_or_else(|| AppError::NotFound("queue not found".to_string()))?;
    let project = queries::get_project(&state.pool, queue.project_id)
        .await?
        .ok_or_else(|| AppError::NotFound("project not found".to_string()))?;
    if !queries::user_in_org(&state.pool, auth.user_id, project.org_id).await? {
        return Err(AppError::Forbidden("forbidden".to_string()));
    }
    Ok(Json(serde_json::json!({
        "dlq_id": dlq_id, "summary": row.summary, "root_cause": row.root_cause,
        "remediation": row.remediation, "model": row.model, "created_at": row.created_at,
    })))
}
