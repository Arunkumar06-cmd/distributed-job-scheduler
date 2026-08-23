use axum::{
    extract::{Path, Query, State},
    http::{HeaderMap, StatusCode},
    Json,
};
use chrono::{DateTime, Utc};
use crate::extract::ApiJson;
use serde::Deserialize;
use validator::Validate;

use crate::middleware::AuthUser;
use crate::routes::validate::{normalize_idempotency_key, reject_control_chars, validate_payload, validate_retry_config};
use crate::routes::queues;
use crate::state::AppState;
use common::{ids, AppError, AppResult};
use db::queries;
use domain::{JobKind, JobStatus, RetryStrategy};
use uuid::Uuid;

#[derive(Debug, Deserialize, Validate)]
pub struct CreateJobReq {
    pub queue_id: Uuid,
    pub payload: serde_json::Value,
    #[validate(range(min = 0, max = 100))]
    pub priority: Option<i32>,
    pub max_attempts: Option<i32>,
    pub retry_strategy: Option<String>,
    pub base_delay_secs: Option<i64>,
    pub max_delay_secs: Option<i64>,
    pub scheduled_for: Option<DateTime<Utc>>,
    pub idempotency_key: Option<String>,
    #[serde(rename = "type")]
    pub job_type: Option<String>,
    pub kind: Option<String>,
}


fn parse_retry_strategy(s: Option<&str>) -> AppResult<RetryStrategy> {
    match s.unwrap_or("exponential") {
        "fixed" => Ok(RetryStrategy::Fixed),
        "linear" => Ok(RetryStrategy::Linear),
        "exponential" => Ok(RetryStrategy::Exponential),
        other => Err(AppError::Validation(format!(
            "retry_strategy must be fixed|linear|exponential, got {other:?}"
        ))),
    }
}

fn parse_job_kind(s: &str) -> AppResult<JobKind> {
    match s {
        "immediate" => Ok(JobKind::Immediate),
        "delayed" => Ok(JobKind::Delayed),
        "scheduled" => Ok(JobKind::Scheduled),
        "recurring" => Ok(JobKind::Recurring),
        "batch" => Ok(JobKind::Batch),
        other => Err(AppError::Validation(format!(
            "type must be immediate|delayed|scheduled|recurring|batch, got {other:?}"
        ))),
    }
}




pub async fn create(
    auth: AuthUser,
    State(state): State<AppState>,
    headers: HeaderMap,
    ApiJson(req): crate::extract::ApiJson<CreateJobReq>,
) -> AppResult<(StatusCode, Json<serde_json::Value>)> {
    // Authz + tenant context in a single round trip.
    let ctx = queries::authorize_queue(&state.pool, auth.user_id, req.queue_id)
        .await?
        .ok_or_else(|| AppError::NotFound("queue not found".to_string()))?;
    ctx.require_writer()?;
    let queue = &ctx.queue;
    let project_org_id = ctx.org_id;
    let project_id = ctx.project_id;

    // Idempotency key priority: header > body
    let header_key = headers
        .get("Idempotency-Key")
        .and_then(|v| v.to_str().ok())
        .map(|s| s.to_string());
    let idempotency_key = normalize_idempotency_key(header_key, req.idempotency_key)?;

    let kind = parse_job_kind(
        req.kind
            .as_deref()
            .or(req.job_type.as_deref())
            .unwrap_or("immediate"),
    )?;

    let priority = req.priority.unwrap_or(queue.default_priority);
    if !(0..=100).contains(&priority) {
        return Err(AppError::Validation("priority must be 0..100".to_string()));
    }
    validate_payload(&req.payload)?;
    if req.payload.get("type").and_then(|v| v.as_str()).map(|t| reject_control_chars("type", t)).transpose().is_err() {
        return Err(AppError::Validation("payload.type contains control characters".into()));
    }

    // Retry config layering: explicit request > queue's retry policy template
    // > queue columns. This is what keeps retry_policies live configuration.
    let (pol_attempts, pol_strategy, pol_base, pol_max) =
        queries::resolve_retry_defaults(&state.pool, req.queue_id).await?;
    let max_attempts = req.max_attempts.unwrap_or(pol_attempts);
    let base_delay_secs = req.base_delay_secs.unwrap_or(pol_base);
    let max_delay_secs = req.max_delay_secs.unwrap_or(pol_max);
    validate_retry_config(max_attempts, base_delay_secs, max_delay_secs)?;
    let strategy = match req.retry_strategy.as_deref() {
        Some(s) => parse_retry_strategy(Some(s))?,
        None => pol_strategy,
    };
    let routing_key = idempotency_key
        .clone()
        .unwrap_or_else(|| Uuid::new_v4().to_string());
    let shard_id = ids::shard_for_key(&routing_key, queue.shard_count);
    let subject = ids::nats_subject_for_shard(
        &project_org_id,
        &project_id,
        &req.queue_id,
        queue.shard_count,
        shard_id,
        priority,
    );

    let max_attempts = req.max_attempts.unwrap_or(3);

    let params = queries::CreateJobParams {
        queue_id: req.queue_id,
        org_id: project_org_id,
        project_id,
        batch_id: None,
        shard_id,
        kind,
        payload: req.payload,
        priority,
        max_attempts,
        retry_strategy: strategy,
        base_delay_secs,
        max_delay_secs,
        scheduled_for: req.scheduled_for,
        idempotency_key,
        subject,
    };

    let job = queries::create_job_with_outbox(&state.pool, params).await?;

    queues::ensure_job_stream(&state, project_org_id, project_id, req.queue_id).await;

    let _ = state.broadcast.send(format!("job.created:{}", job.id));
    Ok((StatusCode::ACCEPTED, Json(serde_json::json!(job))))
}

#[derive(Debug, Deserialize)]
pub struct ListJobsQuery {
    pub queue_id: Option<Uuid>,
    pub status: Option<String>,
    pub priority_min: Option<i32>,
    pub batch_id: Option<Uuid>,
    pub worker_id: Option<Uuid>,
    pub page: Option<i64>,
    pub page_size: Option<i64>,
}

fn parse_status(s: &str) -> AppResult<Option<JobStatus>> {
    let upper = s.to_uppercase().replace('-', "_");
    match upper.as_str() {
        "SCHEDULED" => Ok(Some(JobStatus::Scheduled)),
        "QUEUED" => Ok(Some(JobStatus::Queued)),
        "CLAIMED" => Ok(Some(JobStatus::Claimed)),
        "RUNNING" => Ok(Some(JobStatus::Running)),
        "RETRY_WAIT" | "RETRYWAIT" => Ok(Some(JobStatus::RetryWait)),
        "COMPLETED" => Ok(Some(JobStatus::Completed)),
        "FAILED" => Ok(Some(JobStatus::Failed)),
        "CANCELLED" => Ok(Some(JobStatus::Cancelled)),
        "WAITING" => Ok(Some(JobStatus::Waiting)),
        "UNKNOWN_EXTERNAL_RESULT" | "UNKNOWN" => Ok(Some(JobStatus::UnknownExternalResult)),
        _ => Err(AppError::Validation(format!(
            "unknown status filter {s:?}"
        ))),
    }
}

pub async fn list(
    auth: AuthUser,
    State(state): State<AppState>,
    Query(q): Query<ListJobsQuery>,
) -> AppResult<Json<serde_json::Value>> {
    // If queue_id provided, verify authz
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
    let status = match q.status.as_deref() {
        None => None,
        Some(s) => parse_status(s)?,
    };
    if let Some(pmin) = q.priority_min {
        if !(0..=100).contains(&pmin) {
            return Err(AppError::Validation(
                "priority_min must be 0..100".to_string(),
            ));
        }
    }
    let page = q.page.unwrap_or(1).max(1);
    let page_size = q.page_size.unwrap_or(20).clamp(1, 100);
    let offset = (page - 1) * page_size;
    let jobs = queries::list_jobs_for_user(
        &state.pool,
        auth.user_id,
        q.queue_id,
        status,
        q.priority_min,
        q.batch_id,
        q.worker_id,
        page_size,
        offset,
    )
    .await?;
    let total =
        queries::count_jobs_for_user(&state.pool, auth.user_id, q.queue_id, status, q.worker_id)
            .await?;
    Ok(Json(serde_json::json!({
        "data": jobs,
        "page": page,
        "page_size": page_size,
        "total": total,
        "total_pages": (total + page_size - 1) / page_size,
    })))
}

pub async fn get(
    auth: AuthUser,
    State(state): State<AppState>,
    Path(job_id): Path<Uuid>,
) -> AppResult<Json<serde_json::Value>> {
    let job = queries::get_job(&state.pool, job_id)
        .await?
        .ok_or_else(|| AppError::NotFound("job not found".to_string()))?;
    // Read access is membership-level (viewers included); writes stay writer+.
    let ctx = queries::authorize_queue(&state.pool, auth.user_id, job.queue_id)
        .await?
        .ok_or_else(|| AppError::Forbidden("not a member of this org".to_string()))?;
    let _ = ctx;
    Ok(Json(serde_json::json!(job)))
}

pub async fn retry(
    auth: AuthUser,
    State(state): State<AppState>,
    Path(job_id): Path<Uuid>,
) -> AppResult<Json<serde_json::Value>> {
    let job = queries::get_job(&state.pool, job_id)
        .await?
        .ok_or_else(|| AppError::NotFound("job not found".to_string()))?;
    let ctx = queries::authorize_queue(&state.pool, auth.user_id, job.queue_id)
        .await?
        .ok_or_else(|| AppError::NotFound("queue not found".to_string()))?;
    ctx.require_writer()?;
    let subject = ids::nats_subject_for_shard(
        &ctx.org_id,
        &ctx.project_id,
        &ctx.queue.id,
        ctx.queue.shard_count,
        job.shard_id,
        job.priority,
    );
    let retried = queries::manual_retry_job(
        &state.pool,
        job_id,
        ctx.org_id,
        ctx.project_id,
        ctx.queue.id,
        subject,
    )
    .await?;
    queries::append_audit(
        &state.pool,
        auth.user_id,
        Some(ctx.org_id),
        "job.retry",
        &job_id.to_string(),
        serde_json::json!({"attempt": retried.attempt}),
    )
    .await?;
    let _ = state.broadcast.send(format!("job.retried:{}", job_id));
    Ok(Json(serde_json::json!(retried)))
}

#[derive(Debug, Deserialize, Validate)]
pub struct BatchCreateReq {
    pub queue_id: Uuid,
    #[validate(length(min = 1, max = 100))]
    pub name: Option<String>,
    pub jobs: Vec<BatchJobItem>,
    #[validate(range(min = 0, max = 100))]
    pub priority: Option<i32>,
    pub max_attempts: Option<i32>,
    pub retry_strategy: Option<String>,
    pub base_delay_secs: Option<i64>,
    pub max_delay_secs: Option<i64>,
}

#[derive(Debug, Deserialize)]
pub struct BatchJobItem {
    pub payload: serde_json::Value,
    pub priority: Option<i32>,
    pub idempotency_key: Option<String>,
}

pub async fn create_batch(
    auth: AuthUser,
    State(state): State<AppState>,
    ApiJson(req): crate::extract::ApiJson<BatchCreateReq>,
) -> AppResult<(StatusCode, Json<serde_json::Value>)> {
    req.validate()
        .map_err(|e| AppError::Validation(e.to_string()))?;
    if req.jobs.is_empty() || req.jobs.len() > 1000 {
        return Err(AppError::Validation(
            "batch must have 1..1000 jobs".to_string(),
        ));
    }
    let strategy = parse_retry_strategy(req.retry_strategy.as_deref())?;
    let max_attempts = req.max_attempts.unwrap_or(3);
    let base_delay_secs = req.base_delay_secs.unwrap_or(5);
    let max_delay_secs = req.max_delay_secs.unwrap_or(3600);
    validate_retry_config(max_attempts, base_delay_secs, max_delay_secs)?;

    // Duplicate idempotency keys inside one request would violate the DB unique
    // constraint mid-transaction; fail fast with a clean 400 instead.
    let mut seen = std::collections::HashSet::with_capacity(req.jobs.len());
    let mut normalized_keys = Vec::with_capacity(req.jobs.len());
    for item in &req.jobs {
        let key = normalize_idempotency_key(None, item.idempotency_key.clone())?;
        if let Some(k) = &key {
            if !seen.insert(k.clone()) {
                return Err(AppError::Validation(format!(
                    "duplicate idempotency_key in batch: {k:?}"
                )));
            }
        }
        normalized_keys.push(key);
    }

    let ctx = queries::authorize_queue(&state.pool, auth.user_id, req.queue_id)
        .await?
        .ok_or_else(|| AppError::NotFound("queue not found".to_string()))?;
    ctx.require_writer()?;
    let queue = &ctx.queue;
    let mut params_list = Vec::with_capacity(req.jobs.len());
    for (item, idempotency_key) in req.jobs.into_iter().zip(normalized_keys) {
        validate_payload(&item.payload)?;
        let priority = item
            .priority
            .or(req.priority)
            .unwrap_or(queue.default_priority);
        if !(0..=100).contains(&priority) {
            return Err(AppError::Validation(
                "priority must be 0..100".to_string(),
            ));
        }
        let shard_id = ids::shard_for_key(
            &idempotency_key.clone().unwrap_or_else(|| Uuid::new_v4().to_string()),
            queue.shard_count,
        );
        let subject = ids::nats_subject_for_shard(
            &ctx.org_id,
            &ctx.project_id,
            &req.queue_id,
            queue.shard_count,
            shard_id,
            priority,
        );
        let params = queries::CreateJobParams {
            queue_id: req.queue_id,
            org_id: ctx.org_id,
            project_id: ctx.project_id,
            batch_id: None,
            shard_id,
            kind: JobKind::Batch,
            payload: item.payload,
            priority,
            max_attempts,
            retry_strategy: strategy,
            base_delay_secs,
            max_delay_secs,
            scheduled_for: None,
            idempotency_key,
            subject,
        };
        params_list.push(params);
    }
    let (batch, created) = queries::create_batch_with_jobs(
        &state.pool,
        ctx.project_id,
        req.queue_id,
        req.name.as_deref().unwrap_or("batch"),
        params_list,
    )
    .await?;
    Ok((
        StatusCode::ACCEPTED,
        Json(serde_json::json!({
            "batch": batch,
            "jobs": created,
        })),
    ))
}
