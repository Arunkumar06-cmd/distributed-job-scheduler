use axum::{
    extract::{Path, Query, State},
    http::{header::HeaderName, HeaderValue, StatusCode},
    Json,
    response::{IntoResponse, Response},
};
use serde::Deserialize;
use validator::Validate;

use crate::middleware::AuthUser;
use crate::state::AppState;
use common::{AppError, AppResult};
use db::queries;
use uuid::Uuid;

#[derive(Debug, Deserialize, Validate)]
pub struct CreateQueueReq {
    pub project_id: Uuid,
    #[validate(length(min = 1, max = 100))]
    pub name: String,
    #[validate(length(max = 500))]
    pub description: Option<String>,
    #[validate(range(min = 1, max = 1000))]
    pub max_concurrency: Option<i32>,
    #[validate(range(min = 0, max = 100))]
    pub default_priority: Option<i32>,
    #[validate(range(min = 1, max = 86400))]
    pub ack_wait_secs: Option<i32>,
    #[validate(range(min = 1, max = 100))]
    pub max_receives: Option<i32>,
    pub retry_policy_id: Option<Uuid>,
    #[validate(range(min = 1, max = 10000))]
    pub rate_limit: Option<i32>,
    #[validate(range(min = 1, max = 86400))]
    pub rate_window_secs: Option<i32>,
    #[validate(range(min = 1, max = 128))]
    pub shard_count: Option<i32>,
}

/// Response decorator for endpoints retained for compatibility but superseded
/// by `PATCH /queues/:id { "is_paused": bool }` (RFC 8594 Sunset).
fn deprecated(mut res: Response) -> Response {
    let h = res.headers_mut();
    h.insert(HeaderName::from_static("deprecation"), HeaderValue::from_static("true"));
    h.insert(
        HeaderName::from_static("sunset"),
        HeaderValue::from_static("Sat, 31 Dec 2027 23:59:59 GMT"),
    );
    h.insert(
        HeaderName::from_static("link"),
        HeaderValue::from_static("</api/v1/queues>; rel=\"successor-version\""),
    );
    res
}

pub async fn create(
    auth: AuthUser,
    State(state): State<AppState>,
    Json(req): Json<CreateQueueReq>,
) -> AppResult<(StatusCode, Json<serde_json::Value>)> {
    req.validate()
        .map_err(|e| AppError::Validation(e.to_string()))?;
    let proj = queries::get_project(&state.pool, req.project_id)
        .await?
        .ok_or_else(|| AppError::NotFound("project not found".to_string()))?;
    queries::require_org_admin(&state.pool, auth.user_id, proj.org_id).await?;
    // Single atomic INSERT: rate/shard config lands with the queue row, and the
    // capacity-token seeding trigger fires exactly once against final values.
    let q = queries::create_queue(
        &state.pool,
        req.project_id,
        &req.name,
        req.description.as_deref().unwrap_or(""),
        req.max_concurrency.unwrap_or(5),
        req.default_priority.unwrap_or(5),
        req.ack_wait_secs.unwrap_or(60),
        req.max_receives.unwrap_or(3),
        req.retry_policy_id,
        req.rate_limit,
        req.rate_window_secs,
        req.shard_count.unwrap_or(1),
    )
    .await?;

    ensure_job_stream(&state, proj.org_id, req.project_id, q.id).await;

    Ok((StatusCode::CREATED, Json(serde_json::json!(q))))
}

/// Best-effort JetStream stream provisioning. Awaited (not fire-and-forget):
/// a publish that races ahead of stream creation would never enter the stream
/// and be invisible to consumers.
pub async fn ensure_job_stream(state: &AppState, org_id: Uuid, project_id: Uuid, queue_id: Uuid) {
    let Some(nats) = &state.nats else { return };
    let js = async_nats::jetstream::new(nats.clone());
    let stream_name = common::ids::nats_stream_name(&org_id, &project_id, &queue_id);
    let subject_filter = format!(
        "org.{org_id}.proj.{project_id}.queue.{queue_id}.>"
    );
    if js.get_stream(&stream_name).await.is_ok() {
        return;
    }
    match js
        .create_stream(async_nats::jetstream::stream::Config {
            name: stream_name.clone(),
            subjects: vec![subject_filter],
            max_messages: 100_000,
            ..Default::default()
        })
        .await
    {
        Ok(_) => tracing::info!(stream = %stream_name, "created job stream"),
        Err(e) => tracing::warn!(stream = %stream_name, error = %e, "could not create job stream"),
    }
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
    // Viewing config is member-level, consistent with list/stats.
    if !queries::user_in_org(&state.pool, auth.user_id, proj.org_id).await? {
        return Err(AppError::Forbidden("not authorized".to_string()));
    }
    Ok(Json(serde_json::json!(q)))
}

#[derive(Debug, Deserialize, Validate)]
pub struct UpdateQueueReq {
    /// Pause/resume via PATCH is the non-deprecated path.
    pub is_paused: Option<bool>,
    pub retry_policy_id: Option<Uuid>,
    #[validate(range(min = 1, max = 1000))]
    pub max_concurrency: Option<i32>,
    #[validate(range(min = 0, max = 100))]
    pub default_priority: Option<i32>,
    #[validate(range(min = 1, max = 86400))]
    pub ack_wait_secs: Option<i32>,
    #[validate(range(min = 1, max = 100))]
    pub max_receives: Option<i32>,
    #[validate(length(max = 500))]
    pub description: Option<String>,
}

pub async fn update(
    auth: AuthUser,
    State(state): State<AppState>,
    Path(queue_id): Path<Uuid>,
    Json(req): Json<UpdateQueueReq>,
) -> AppResult<Json<serde_json::Value>> {
    // Reconfiguration (concurrency, priorities, acks) is an admin operation —
    // same bar as create/pause/resume, not plain membership.
    req.validate()
        .map_err(|e| AppError::Validation(e.to_string()))?;
    let ctx = queries::authorize_queue(&state.pool, auth.user_id, queue_id)
        .await?
        .ok_or_else(|| AppError::NotFound("queue not found".to_string()))?;
    ctx.require_admin()?;
    let updated = queries::update_queue_config(
        &state.pool,
        queue_id,
        req.retry_policy_id,
        req.max_concurrency,
        req.default_priority,
        req.ack_wait_secs,
        req.max_receives,
        req.description.as_deref(),
    )
    .await?;
    let mut updated = updated;
    if let Some(want_paused) = req.is_paused {
        if want_paused != updated.is_paused {
            updated = queries::set_queue_paused(&state.pool, queue_id, want_paused).await?;
        }
    }
    queries::append_audit(
        &state.pool,
        auth.user_id,
        Some(ctx.org_id),
        "queue.update",
        &queue_id.to_string(),
        serde_json::json!({
            "max_concurrency": req.max_concurrency,
            "default_priority": req.default_priority,
            "ack_wait_secs": req.ack_wait_secs,
            "max_receives": req.max_receives,
        }),
    )
    .await?;
    let _ = state.broadcast.send(format!("queue.updated:{}", queue_id));
    Ok(Json(serde_json::json!(updated)))
}

pub async fn pause(
    auth: AuthUser,
    State(state): State<AppState>,
    Path(queue_id): Path<Uuid>,
) -> AppResult<Response> {
    let ctx = queries::authorize_queue(&state.pool, auth.user_id, queue_id)
        .await?
        .ok_or_else(|| AppError::NotFound("queue not found".to_string()))?;
    ctx.require_admin()?;
    let updated = queries::set_queue_paused(&state.pool, queue_id, true).await?;
    queries::append_audit(
        &state.pool,
        auth.user_id,
        Some(ctx.org_id),
        "queue.pause",
        &queue_id.to_string(),
        serde_json::json!({"name": ctx.queue.name}),
    )
    .await?;
    let _ = state.broadcast.send(format!("queue.paused:{}", queue_id));
    Ok(deprecated(Json(serde_json::json!(updated)).into_response()))
}

pub async fn resume(
    auth: AuthUser,
    State(state): State<AppState>,
    Path(queue_id): Path<Uuid>,
) -> AppResult<Response> {
    let ctx = queries::authorize_queue(&state.pool, auth.user_id, queue_id)
        .await?
        .ok_or_else(|| AppError::NotFound("queue not found".to_string()))?;
    ctx.require_admin()?;
    let updated = queries::set_queue_paused(&state.pool, queue_id, false).await?;
    queries::append_audit(
        &state.pool,
        auth.user_id,
        Some(ctx.org_id),
        "queue.resume",
        &queue_id.to_string(),
        serde_json::json!({"name": ctx.queue.name}),
    )
    .await?;
    let _ = state.broadcast.send(format!("queue.resumed:{}", queue_id));
    Ok(deprecated(Json(serde_json::json!(updated)).into_response()))
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

#[derive(Debug, Deserialize)]
pub struct BatchStatsQuery {
    pub ids: String,
}

pub async fn batch_stats(
    auth: AuthUser,
    State(state): State<AppState>,
    Query(q): Query<BatchStatsQuery>,
) -> AppResult<Json<serde_json::Value>> {
    let mut queue_ids: Vec<Uuid> = Vec::new();
    for seg in q.ids.split(',') {
        let id = seg
            .trim()
            .parse::<Uuid>()
            .map_err(|_| AppError::Validation(format!("invalid queue id {seg:?}")))?;
        if !queue_ids.contains(&id) {
            queue_ids.push(id);
        }
    }
    if queue_ids.is_empty() {
        return Ok(Json(serde_json::json!([])));
    }
    if queue_ids.len() > 100 {
        return Err(AppError::Validation(
            "at most 100 queue ids are allowed".to_string(),
        ));
    }
    if !queries::user_can_access_all_queues(&state.pool, auth.user_id, &queue_ids).await? {
        return Err(AppError::Forbidden("forbidden".to_string()));
    }
    let stats = queries::batch_queue_stats(&state.pool, &queue_ids).await?;
    // Preserve the caller's requested order and include zero-activity queues.
    let mut by_id = std::collections::HashMap::new();
    for s in stats {
        by_id.insert(s.queue_id, s);
    }
    let ordered: Vec<db::models::QueueStats> = queue_ids
        .iter()
        .filter_map(|id| by_id.remove(id))
        .collect();
    Ok(Json(serde_json::json!(ordered)))
}
