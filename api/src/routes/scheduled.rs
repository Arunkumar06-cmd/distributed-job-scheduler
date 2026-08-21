use axum::{
    extract::{Path, Query, State},
    http::StatusCode,
    Json,
};
use chrono::{DateTime, Utc};
use serde::Deserialize;
use validator::Validate;

use crate::middleware::AuthUser;
use crate::state::AppState;
use common::{AppError, AppResult};
use db::queries;
use domain::schedule;
use uuid::Uuid;

#[derive(Debug, Deserialize, Validate)]
pub struct CreateScheduledReq {
    pub queue_id: Uuid,
    #[validate(length(min = 1, max = 100))]
    pub name: String,
    pub job_type: Option<String>,
    pub payload: serde_json::Value,
    #[validate(range(min = 0, max = 100))]
    pub priority: Option<i32>,
    pub cron_expr: Option<String>,
    pub timezone: Option<String>,
    pub run_once_at: Option<DateTime<Utc>>,
}

pub async fn create(
    auth: AuthUser,
    State(state): State<AppState>,
    Json(req): Json<CreateScheduledReq>,
) -> AppResult<(StatusCode, Json<serde_json::Value>)> {
    req.validate()
        .map_err(|e| AppError::Validation(e.to_string()))?;
    if req.cron_expr.is_none() && req.run_once_at.is_none() {
        return Err(AppError::Validation(
            "either cron_expr or run_once_at is required".to_string(),
        ));
    }
    let queue = queries::get_queue(&state.pool, req.queue_id)
        .await?
        .ok_or_else(|| AppError::NotFound("queue not found".to_string()))?;
    let project = queries::get_project(&state.pool, queue.project_id)
        .await?
        .ok_or_else(|| AppError::NotFound("project not found".to_string()))?;
    queries::require_org_writer(&state.pool, auth.user_id, project.org_id).await?;
    let tz = req.timezone.as_deref().unwrap_or("UTC");
    let next_fire = if let Some(expr) = &req.cron_expr {
        let sched =
            schedule::parse_cron(expr, tz).map_err(|e| AppError::Validation(e.to_string()))?;
        let tz_parsed: chrono_tz::Tz = tz
            .parse()
            .map_err(|_| AppError::Validation(format!("invalid timezone {tz}")))?;
        schedule::next_occurrence(&sched, tz_parsed, Utc::now())
    } else {
        req.run_once_at
    };
    let job_type = req.job_type.unwrap_or_else(|| "scheduled".to_string());
    let sj = queries::create_scheduled_job(
        &state.pool,
        req.queue_id,
        &req.name,
        &job_type,
        req.payload,
        req.priority.unwrap_or(5),
        req.cron_expr.as_deref(),
        tz,
        req.run_once_at,
        next_fire,
    )
    .await?;
    Ok((StatusCode::CREATED, Json(serde_json::json!(sj))))
}

#[derive(Debug, Deserialize)]
pub struct ListScheduledQuery {
    pub queue_id: Option<Uuid>,
}

pub async fn list(
    auth: AuthUser,
    State(state): State<AppState>,
    Query(q): Query<ListScheduledQuery>,
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
    let jobs = queries::list_scheduled_jobs_for_user(&state.pool, auth.user_id, q.queue_id).await?;
    Ok(Json(serde_json::json!(jobs)))
}

pub async fn delete(
    auth: AuthUser,
    State(state): State<AppState>,
    Path(id): Path<Uuid>,
) -> AppResult<StatusCode> {
    let sj: Option<db::models::ScheduledJob> =
        sqlx::query_as("SELECT * FROM scheduled_jobs WHERE id = $1")
            .bind(id)
            .fetch_optional(&state.pool)
            .await?;
    let sj = sj.ok_or_else(|| AppError::NotFound("scheduled job not found".to_string()))?;
    let queue = queries::get_queue(&state.pool, sj.queue_id)
        .await?
        .ok_or_else(|| AppError::NotFound("queue not found".to_string()))?;
    let project = queries::get_project(&state.pool, queue.project_id)
        .await?
        .ok_or_else(|| AppError::NotFound("project not found".to_string()))?;
    queries::require_org_writer(&state.pool, auth.user_id, project.org_id).await?;
    queries::deactivate_scheduled_job(&state.pool, id).await?;
    sqlx::query("DELETE FROM scheduled_jobs WHERE id = $1")
        .bind(id)
        .execute(&state.pool)
        .await?;
    Ok(StatusCode::NO_CONTENT)
}
