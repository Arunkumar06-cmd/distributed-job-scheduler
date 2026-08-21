use axum::{
    Json, extract::{State, Path, Query},
    http::StatusCode,
};
use serde::{Deserialize, Serialize};
use validator::Validate;

use crate::middleware::AuthUser;
use crate::state::AppState;
use common::{AppError, AppResult};
use db::queries;
use sqlx;
use uuid::Uuid;

#[derive(Debug, Deserialize, Validate)]
pub struct CreateQueueReq {
    pub project_id: Uuid,
    #[validate(length(min = 1, max = 100))]
    pub name: String,
    pub description: Option<String>,
    #[validate(range(min = 1, max = 1000))]
    pub max_concurrency: Option<i32>,
    #[validate(range(min = 0, max = 100))]
    pub default_priority: Option<i32>,
    pub ack_wait_secs: Option<i32>,
    pub max_receives: Option<i32>,
    pub retry_policy_id: Option<Uuid>,
    #[validate(range(min = 1, max = 10000))]
    pub rate_limit: Option<i32>,
    pub rate_window_secs: Option<i32>,
}

pub async fn create(
    auth: AuthUser,
    State(state): State<AppState>,
    Json(req): Json<CreateQueueReq>,
) -> AppResult<(StatusCode, Json<serde_json::Value>)> {
    req.validate().map_err(|e| AppError::Validation(e.to_string()))?;
    let proj = queries::get_project(&state.pool, req.project_id)
        .await?
        .ok_or_else(|| AppError::NotFound("project not found".to_string()))?;
    if !queries::user_in_org(&state.pool, auth.user_id, proj.org_id).await? {
        return Err(AppError::Forbidden("not authorized".to_string()));
    }
    let mut q = queries::create_queue(
        &state.pool,
        req.project_id,
        &req.name,
        req.description.as_deref().unwrap_or(""),
        req.max_concurrency.unwrap_or(5),
        req.default_priority.unwrap_or(5),
        req.ack_wait_secs.unwrap_or(60),
        req.max_receives.unwrap_or(3),
        req.retry_policy_id,
    )
    .await?;
    if req.rate_limit.is_some() || req.rate_window_secs.is_some() {
        q = sqlx::query_as::<_, db::models::Queue>(
            r#"UPDATE queues SET rate_limit = COALESCE($2, rate_limit), rate_window_secs = COALESCE($3, rate_window_secs) WHERE id = $1 RETURNING *"#,
        )
        .bind(q.id)
        .bind(req.rate_limit)
        .bind(req.rate_window_secs)
        .fetch_one(&state.pool)
        .await?;
    }

    // Ensure NATS stream exists for this queue
    if let Some(nats) = &state.nats {
        let js = async_nats::jetstream::new(nats.clone());
        let stream_name = format!("JOBS_{}_{}_{}", proj.org_id, req.project_id, q.id).replace('-', "_");
        // Use hierarchical subject: org.{org_id}.proj.{project_id}.queue.{queue_id}.*
        let subject = format!("org.{}.proj.{}.queue.{}.*", proj.org_id, req.project_id, q.id);
        let _ = js.create_stream(async_nats::jetstream::stream::Config {
            name: stream_name,
            subjects: vec![subject],
            max_messages: 100_000,
            ..Default::default()
        }).await;
    }

    Ok((StatusCode::CREATED, Json(serde_json::json!(q))))
}

#[derive(Debug, Deserialize)]
pub struct ListQueuesQuery {
    pub project_id: Uuid,
}

pub async fn list(
    auth: AuthUser,
    State(state): State<AppState>,
    Query(q): Query<ListQueuesQuery>,
) -> AppResult<Json<serde_json::Value>> {
    let proj = queries::get_project(&state.pool, q.project_id)
        .await?
        .ok_or_else(|| AppError::NotFound("project not found".to_string()))?;
    if !queries::user_in_org(&state.pool, auth.user_id, proj.org_id).await? {
        return Err(AppError::Forbidden("not authorized".to_string()));
    }
    let queues = queries::list_queues_in_project(&state.pool, q.project_id).await?;
    Ok(Json(serde_json::json!(queues)))
}

pub async fn get(
    auth: AuthUser,
    State(state): State<AppState>,
    Path(queue_id): Path<Uuid>,
) -> AppResult<Json<serde_json::Value>> {
    let q = queries::get_queue(&state.pool, queue_id)
        .await?
        .ok_or_else(|| AppError::NotFound("queue not found".to_string()))?;
    let proj = queries::get_project(&state.pool, q.project_id)
        .await?
        .ok_or_else(|| AppError::NotFound("project not found".to_string()))?;
    if !queries::user_in_org(&state.pool, auth.user_id, proj.org_id).await? {
        return Err(AppError::Forbidden("forbidden".to_string()));
    }
    Ok(Json(serde_json::json!(q)))
}

#[derive(Debug, Deserialize, Validate)]
pub struct UpdateQueueReq {
    pub max_concurrency: Option<i32>,
    pub default_priority: Option<i32>,
    pub ack_wait_secs: Option<i32>,
    pub max_receives: Option<i32>,
    pub description: Option<String>,
}

pub async fn update(
    auth: AuthUser,
    State(state): State<AppState>,
    Path(queue_id): Path<Uuid>,
    Json(req): Json<UpdateQueueReq>,
) -> AppResult<Json<serde_json::Value>> {
    let q = queries::get_queue(&state.pool, queue_id)
        .await?
        .ok_or_else(|| AppError::NotFound("queue not found".to_string()))?;
    let proj = queries::get_project(&state.pool, q.project_id)
        .await?
        .ok_or_else(|| AppError::NotFound("project not found".to_string()))?;
    if !queries::user_in_org(&state.pool, auth.user_id, proj.org_id).await? {
        return Err(AppError::Forbidden("forbidden".to_string()));
    }
    let updated = queries::update_queue_config(
        &state.pool,
        queue_id,
        req.max_concurrency,
        req.default_priority,
        req.ack_wait_secs,
        req.max_receives,
        req.description.as_deref(),
    )
    .await?;
    Ok(Json(serde_json::json!(updated)))
}

pub async fn pause(
    auth: AuthUser,
    State(state): State<AppState>,
    Path(queue_id): Path<Uuid>,
) -> AppResult<Json<serde_json::Value>> {
    let q = queries::get_queue(&state.pool, queue_id)
        .await?
        .ok_or_else(|| AppError::NotFound("queue not found".to_string()))?;
    let proj = queries::get_project(&state.pool, q.project_id)
        .await?
        .ok_or_else(|| AppError::NotFound("project not found".to_string()))?;
    queries::require_org_admin(&state.pool, auth.user_id, proj.org_id).await?;
    let updated = queries::set_queue_paused(&state.pool, queue_id, true).await?;
    let _ = state.broadcast.send(format!("queue.paused:{}", queue_id));
    Ok(Json(serde_json::json!(updated)))
}

pub async fn resume(
    auth: AuthUser,
    State(state): State<AppState>,
    Path(queue_id): Path<Uuid>,
) -> AppResult<Json<serde_json::Value>> {
    let q = queries::get_queue(&state.pool, queue_id)
        .await?
        .ok_or_else(|| AppError::NotFound("queue not found".to_string()))?;
    let proj = queries::get_project(&state.pool, q.project_id)
        .await?
        .ok_or_else(|| AppError::NotFound("project not found".to_string()))?;
    queries::require_org_admin(&state.pool, auth.user_id, proj.org_id).await?;
    let updated = queries::set_queue_paused(&state.pool, queue_id, false).await?;
    let _ = state.broadcast.send(format!("queue.resumed:{}", queue_id));
    Ok(Json(serde_json::json!(updated)))
}

pub async fn stats(
    auth: AuthUser,
    State(state): State<AppState>,
    Path(queue_id): Path<Uuid>,
) -> AppResult<Json<serde_json::Value>> {
    let q = queries::get_queue(&state.pool, queue_id)
        .await?
        .ok_or_else(|| AppError::NotFound("queue not found".to_string()))?;
    let proj = queries::get_project(&state.pool, q.project_id)
        .await?
        .ok_or_else(|| AppError::NotFound("project not found".to_string()))?;
    if !queries::user_in_org(&state.pool, auth.user_id, proj.org_id).await? {
        return Err(AppError::Forbidden("forbidden".to_string()));
    }
    let stats = queries::queue_stats(&state.pool, queue_id).await?;
    Ok(Json(serde_json::json!(stats)))
}
