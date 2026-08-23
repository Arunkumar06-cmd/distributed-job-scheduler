use axum::{
    extract::{Path, State},
    Json,
};

use crate::middleware::AuthUser;
use crate::state::AppState;
use common::{AppError, AppResult};
use db::queries;
use uuid::Uuid;

pub async fn list(
    _auth: AuthUser,
    State(state): State<AppState>,
) -> AppResult<Json<serde_json::Value>> {
    // Any authenticated user can see workers (could restrict to admin later)
    let hb = state.config.heartbeat_interval_secs as i64;
    let workers = queries::list_workers(&state.pool, hb * 3, hb * 12).await?;
    Ok(Json(serde_json::json!(workers)))
}

pub async fn get(
    _auth: AuthUser,
    State(state): State<AppState>,
    Path(worker_id): Path<Uuid>,
) -> AppResult<Json<serde_json::Value>> {
    let hb = state.config.heartbeat_interval_secs as i64;
    let w = queries::get_worker(&state.pool, worker_id, hb * 3, hb * 12)
        .await?
        .ok_or_else(|| AppError::NotFound("worker not found".to_string()))?;
    Ok(Json(serde_json::json!(w)))
}
