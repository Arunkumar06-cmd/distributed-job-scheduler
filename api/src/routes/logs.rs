use axum::{
    extract::{Path, Query, State},
    Json,
};
use serde::Deserialize;

use crate::middleware::AuthUser;
use crate::state::AppState;
use common::{AppError, AppResult};
use db::queries;
use uuid::Uuid;

#[derive(Debug, Deserialize)]
pub struct LogQuery {
    pub limit: Option<i64>,
}

pub async fn list(
    auth: AuthUser,
    State(state): State<AppState>,
    Path(job_id): Path<Uuid>,
    Query(q): Query<LogQuery>,
) -> AppResult<Json<serde_json::Value>> {
    let job = queries::get_job(&state.pool, job_id)
        .await?
        .ok_or_else(|| AppError::NotFound("job not found".to_string()))?;
    let queue = queries::get_queue(&state.pool, job.queue_id)
        .await?
        .ok_or_else(|| AppError::NotFound("queue not found".to_string()))?;
    let project = queries::get_project(&state.pool, queue.project_id)
        .await?
        .ok_or_else(|| AppError::NotFound("project not found".to_string()))?;
    if !queries::user_in_org(&state.pool, auth.user_id, project.org_id).await? {
        return Err(AppError::Forbidden("forbidden".to_string()));
    }
    let logs =
        queries::list_logs(&state.pool, job_id, q.limit.unwrap_or(100).clamp(1, 500)).await?;
    Ok(Json(serde_json::json!(logs)))
}
