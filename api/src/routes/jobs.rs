use axum::{
    extract::{Path, Query, State},
    http::{HeaderMap, StatusCode},
    Json,
};
use chrono::{DateTime, Utc};
use serde::Deserialize;
use validator::Validate;

use crate::middleware::AuthUser;
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

fn parse_retry_strategy(s: Option<&str>) -> RetryStrategy {
    match s.unwrap_or("exponential") {
        "fixed" => RetryStrategy::Fixed,
        "linear" => RetryStrategy::Linear,
        _ => RetryStrategy::Exponential,
    }
}

pub async fn create(
    auth: AuthUser,
    State(state): State<AppState>,
    headers: HeaderMap,
    Json(req): Json<CreateJobReq>,
) -> AppResult<(StatusCode, Json<serde_json::Value>)> {
    // Authz: queue -> project -> org membership
    let queue = queries::get_queue(&state.pool, req.queue_id)
        .await?
        .ok_or_else(|| AppError::NotFound("queue not found".to_string()))?;
    let project = queries::get_project(&state.pool, queue.project_id)
        .await?
        .ok_or_else(|| AppError::NotFound("project not found".to_string()))?;
    if !queries::user_in_org(&state.pool, auth.user_id, project.org_id).await? {
        return Err(AppError::Forbidden("not authorized".to_string()));
    }

    // Idempotency key priority: header > body
    let header_key = headers
        .get("Idempotency-Key")
        .or_else(|| headers.get("idempotency-key"))
        .and_then(|v| v.to_str().ok())
        .map(|s| s.to_string());
    let idempotency_key = header_key.or(req.idempotency_key);

    let kind = match req
        .kind
        .as_deref()
        .or(req.job_type.as_deref())
        .unwrap_or("immediate")
    {
        "delayed" => JobKind::Delayed,
        "scheduled" => JobKind::Scheduled,
        "recurring" => JobKind::Recurring,
        "batch" => JobKind::Batch,
        _ => JobKind::Immediate,
    };

    let priority = req.priority.unwrap_or(queue.default_priority);
    if !(0..=100).contains(&priority) {
        return Err(AppError::Validation("priority must be 0..100".to_string()));
    }
    let subject = ids::nats_subject(&project.org_id, &project.id, &queue.id, priority);

    let max_attempts = req.max_attempts.unwrap_or(3);
    let strategy = parse_retry_strategy(req.retry_strategy.as_deref());

    let params = queries::CreateJobParams {
        queue_id: req.queue_id,
        org_id: project.org_id,
        project_id: project.id,
        batch_id: None,
        kind,
        payload: req.payload,
        priority,
        max_attempts,
        retry_strategy: strategy,
        base_delay_secs: req.base_delay_secs.unwrap_or(5),
        max_delay_secs: req.max_delay_secs.unwrap_or(3600),
        scheduled_for: req.scheduled_for,
        idempotency_key,
        subject,
    };

    let job = queries::create_job_with_outbox(&state.pool, params).await?;

    // Ensure stream exists (best-effort)
    if let Some(nats) = &state.nats {
        let js = async_nats::jetstream::new(nats.clone());
        let stream_name =
            format!("JOBS_{}_{}_{}", project.org_id, project.id, queue.id).replace('-', "_");
        let subject_filter = format!(
            "org.{}.proj.{}.queue.{}.*",
            project.org_id, project.id, queue.id
        );
        let _ = js.get_stream(&stream_name).await.map_err(|_| {
            let js2 = js.clone();
            let sn = stream_name.clone();
            let sf = subject_filter.clone();
            // We can't await here easily without blocking; spawn
            tokio::spawn(async move {
                let _ = js2
                    .create_stream(async_nats::jetstream::stream::Config {
                        name: sn,
                        subjects: vec![sf],
                        max_messages: 100_000,
                        ..Default::default()
                    })
                    .await;
            });
            async_nats::error::Error::from(std::io::Error::other("creating stream async"))
        });
    }

    let _ = state.broadcast.send(format!("job.created:{}", job.id));
    Ok((StatusCode::ACCEPTED, Json(serde_json::json!(job))))
}

#[derive(Debug, Deserialize)]
pub struct ListJobsQuery {
    pub queue_id: Option<Uuid>,
    pub status: Option<String>,
    pub priority_min: Option<i32>,
    pub batch_id: Option<Uuid>,
    pub page: Option<i64>,
    pub page_size: Option<i64>,
}

fn parse_status(s: &str) -> Option<JobStatus> {
    let upper = s.to_uppercase().replace('-', "_");
    match upper.as_str() {
        "SCHEDULED" => Some(JobStatus::Scheduled),
        "QUEUED" => Some(JobStatus::Queued),
        "CLAIMED" => Some(JobStatus::Claimed),
        "RUNNING" => Some(JobStatus::Running),
        "RETRY_WAIT" | "RETRYWAIT" => Some(JobStatus::RetryWait),
        "COMPLETED" => Some(JobStatus::Completed),
        "FAILED" => Some(JobStatus::Failed),
        "CANCELLED" => Some(JobStatus::Cancelled),
        "WAITING" => Some(JobStatus::Waiting),
        "UNKNOWN_EXTERNAL_RESULT" | "UNKNOWN" => Some(JobStatus::UnknownExternalResult),
        _ => None,
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
    let status = q.status.as_deref().and_then(parse_status);
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
        page_size,
        offset,
    )
    .await?;
    let total = queries::count_jobs_for_user(&state.pool, auth.user_id, q.queue_id, status).await?;
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
    let queue = queries::get_queue(&state.pool, job.queue_id)
        .await?
        .ok_or_else(|| AppError::NotFound("queue not found".to_string()))?;
    let project = queries::get_project(&state.pool, queue.project_id)
        .await?
        .ok_or_else(|| AppError::NotFound("project not found".to_string()))?;
    if !queries::user_in_org(&state.pool, auth.user_id, project.org_id).await? {
        return Err(AppError::Forbidden("forbidden".to_string()));
    }
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
    let queue = queries::get_queue(&state.pool, job.queue_id)
        .await?
        .ok_or_else(|| AppError::NotFound("queue not found".to_string()))?;
    let project = queries::get_project(&state.pool, queue.project_id)
        .await?
        .ok_or_else(|| AppError::NotFound("project not found".to_string()))?;
    if !queries::user_in_org(&state.pool, auth.user_id, project.org_id).await? {
        return Err(AppError::Forbidden("forbidden".to_string()));
    }
    let subject = ids::nats_subject(&project.org_id, &project.id, &queue.id, job.priority);
    let retried = queries::manual_retry_job(
        &state.pool,
        job_id,
        project.org_id,
        project.id,
        queue.id,
        subject,
    )
    .await?;
    let _ = state.broadcast.send(format!("job.retried:{}", job_id));
    Ok(Json(serde_json::json!(retried)))
}

#[derive(Debug, Deserialize, Validate)]
pub struct BatchCreateReq {
    pub queue_id: Uuid,
    pub name: Option<String>,
    pub jobs: Vec<BatchJobItem>,
    #[validate(range(min = 0, max = 100))]
    pub priority: Option<i32>,
    pub max_attempts: Option<i32>,
    pub retry_strategy: Option<String>,
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
    Json(req): Json<BatchCreateReq>,
) -> AppResult<(StatusCode, Json<serde_json::Value>)> {
    req.validate()
        .map_err(|e| AppError::Validation(e.to_string()))?;
    if req.jobs.is_empty() || req.jobs.len() > 1000 {
        return Err(AppError::Validation(
            "batch must have 1..1000 jobs".to_string(),
        ));
    }
    let queue = queries::get_queue(&state.pool, req.queue_id)
        .await?
        .ok_or_else(|| AppError::NotFound("queue not found".to_string()))?;
    let project = queries::get_project(&state.pool, queue.project_id)
        .await?
        .ok_or_else(|| AppError::NotFound("project not found".to_string()))?;
    if !queries::user_in_org(&state.pool, auth.user_id, project.org_id).await? {
        return Err(AppError::Forbidden("forbidden".to_string()));
    }
    let mut params_list = Vec::with_capacity(req.jobs.len());
    for item in req.jobs {
        let priority = item
            .priority
            .or(req.priority)
            .unwrap_or(queue.default_priority);
        let subject = ids::nats_subject(&project.org_id, &project.id, &queue.id, priority);
        let params = queries::CreateJobParams {
            queue_id: req.queue_id,
            org_id: project.org_id,
            project_id: project.id,
            batch_id: None,
            kind: JobKind::Batch,
            payload: item.payload,
            priority,
            max_attempts: req.max_attempts.unwrap_or(3),
            retry_strategy: parse_retry_strategy(req.retry_strategy.as_deref()),
            base_delay_secs: 5,
            max_delay_secs: 3600,
            scheduled_for: None,
            idempotency_key: item.idempotency_key,
            subject,
        };
        params_list.push(params);
    }
    let (batch, created) = queries::create_batch_with_jobs(
        &state.pool,
        project.id,
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
