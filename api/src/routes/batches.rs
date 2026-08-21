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
pub struct BatchListQuery {
    pub project_id: Uuid,
}

pub async fn list(
    auth: AuthUser,
    State(state): State<AppState>,
    Query(q): Query<BatchListQuery>,
) -> AppResult<Json<serde_json::Value>> {
    let project = queries::get_project(&state.pool, q.project_id)
        .await?
        .ok_or_else(|| AppError::NotFound("project not found".to_string()))?;
    if !queries::user_in_org(&state.pool, auth.user_id, project.org_id).await? {
        return Err(AppError::Forbidden("forbidden".to_string()));
    }
    let batches = queries::list_batches(&state.pool, q.project_id).await?;
    Ok(Json(serde_json::json!(batches)))
}

pub async fn get(
    auth: AuthUser,
    State(state): State<AppState>,
    Path(batch_id): Path<Uuid>,
) -> AppResult<Json<serde_json::Value>> {
    let batch = queries::get_batch(&state.pool, batch_id)
        .await?
        .ok_or_else(|| AppError::NotFound("batch not found".to_string()))?;
    let project = queries::get_project(&state.pool, batch.project_id)
        .await?
        .ok_or_else(|| AppError::NotFound("project not found".to_string()))?;
    if !queries::user_in_org(&state.pool, auth.user_id, project.org_id).await? {
        return Err(AppError::Forbidden("forbidden".to_string()));
    }
    // Also fetch jobs in batch
    let jobs = queries::list_jobs(
        &state.pool,
        Some(batch.queue_id),
        None,
        None,
        None,
        Some(batch_id),
        1000,
        0,
    )
    .await?;
    Ok(Json(serde_json::json!({
        "batch": batch,
        "jobs": jobs,
    })))
}
