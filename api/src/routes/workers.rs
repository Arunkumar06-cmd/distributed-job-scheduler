use axum::{Json, extract::{State, Path}};

use crate::middleware::AuthUser;
use crate::state::AppState;
use common::{AppError, AppResult};
use db::queries;
use uuid::Uuid;

pub async fn list(
    auth: AuthUser,
    State(state): State<AppState>,
) -> AppResult<Json<serde_json::Value>> {
    // Any authenticated user can see workers (could restrict to admin later)
    let workers = queries::list_workers(&state.pool).await?;
    Ok(Json(serde_json::json!(workers)))
}

pub async fn get(
    auth: AuthUser,
    State(state): State<AppState>,
    Path(worker_id): Path<Uuid>,
) -> AppResult<Json<serde_json::Value>> {
    let w = queries::get_worker(&state.pool, worker_id).await?
        .ok_or_else(|| AppError::NotFound("worker not found".to_string()))?;
    Ok(Json(serde_json::json!(w)))
}
