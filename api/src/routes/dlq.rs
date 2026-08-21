use axum::{Json, extract::{State, Path, Query}, http::StatusCode};
use serde::Deserialize;

use crate::middleware::AuthUser;
use crate::state::AppState;
use common::{AppError, AppResult, ids};
use db::queries;
use uuid::Uuid;

#[derive(Debug, Deserialize)]
pub struct ListDlqQuery {
    pub queue_id: Option<Uuid>,
    pub page: Option<i64>,
    pub page_size: Option<i64>,
}

pub async fn list(
    auth: AuthUser,
    State(state): State<AppState>,
    Query(q): Query<ListDlqQuery>,
) -> AppResult<Json<serde_json::Value>> {
    if let Some(qid) = q.queue_id {
        let queue = queries::get_queue(&state.pool, qid).await?.ok_or_else(|| AppError::NotFound("queue not found".to_string()))?;
        let project = queries::get_project(&state.pool, queue.project_id).await?.ok_or_else(|| AppError::NotFound("project not found".to_string()))?;
        if !queries::user_in_org(&state.pool, auth.user_id, project.org_id).await? {
            return Err(AppError::Forbidden("forbidden".to_string()));
        }
    }
    let page = q.page.unwrap_or(1).max(1);
    let page_size = q.page_size.unwrap_or(20).clamp(1, 100);
    let offset = (page-1)*page_size;
    let entries = queries::list_dlq_entries(&state.pool, q.queue_id, page_size, offset).await?;
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
    let dlq: db::models::DeadLetterEntry = sqlx::query_as("SELECT * FROM dead_letter_entries WHERE id = $1")
        .bind(dlq_id).fetch_optional(&state.pool).await?
        .ok_or_else(|| AppError::NotFound("DLQ entry not found".to_string()))?;
    let queue = queries::get_queue(&state.pool, dlq.queue_id).await?.ok_or_else(|| AppError::NotFound("queue not found".to_string()))?;
    let project = queries::get_project(&state.pool, queue.project_id).await?.ok_or_else(|| AppError::NotFound("project not found".to_string()))?;
    if !queries::user_in_org(&state.pool, auth.user_id, project.org_id).await? {
        return Err(AppError::Forbidden("forbidden".to_string()));
    }
    let subject = ids::nats_subject(&dlq.org_id, &dlq.project_id, &dlq.queue_id, 5);
    let job = queries::replay_dlq_entry(&state.pool, dlq_id, dlq.org_id, dlq.project_id, subject).await?;
    let _ = state.broadcast.send(format!("dlq.replayed:{}", dlq_id));
    Ok(Json(serde_json::json!(job)))
}
