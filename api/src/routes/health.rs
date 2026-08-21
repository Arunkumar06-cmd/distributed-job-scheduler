use axum::{Json, extract::State, response::IntoResponse};
use serde_json::json;

use crate::state::AppState;

pub async fn health() -> impl IntoResponse {
    Json(json!({"status":"ok","timestamp": chrono::Utc::now()}))
}

pub async fn metrics(State(state): State<AppState>) -> Json<serde_json::Value> {
    let pools = state.pool.size();
    let idle = state.pool.num_idle();
    // Global queue stats
    let total_jobs: (i64,) = sqlx::query_as("SELECT COUNT(*) FROM jobs")
        .fetch_one(&state.pool).await.unwrap_or((0,));
    let queued: (i64,) = sqlx::query_as("SELECT COUNT(*) FROM jobs WHERE status = 'QUEUED'")
        .fetch_one(&state.pool).await.unwrap_or((0,));
    let running: (i64,) = sqlx::query_as("SELECT COUNT(*) FROM jobs WHERE status = 'RUNNING'")
        .fetch_one(&state.pool).await.unwrap_or((0,));
    let completed: (i64,) = sqlx::query_as("SELECT COUNT(*) FROM jobs WHERE status = 'COMPLETED'")
        .fetch_one(&state.pool).await.unwrap_or((0,));
    let failed: (i64,) = sqlx::query_as("SELECT COUNT(*) FROM jobs WHERE status = 'FAILED'")
        .fetch_one(&state.pool).await.unwrap_or((0,));
    let retry_wait: (i64,) = sqlx::query_as("SELECT COUNT(*) FROM jobs WHERE status = 'RETRY_WAIT'")
        .fetch_one(&state.pool).await.unwrap_or((0,));
    let dlq: (i64,) = sqlx::query_as("SELECT COUNT(*) FROM dead_letter_entries")
        .fetch_one(&state.pool).await.unwrap_or((0,));
    let workers: (i64,) = sqlx::query_as("SELECT COUNT(*) FROM workers WHERE is_active = TRUE")
        .fetch_one(&state.pool).await.unwrap_or((0,));

    Json(json!({
        "jobs": {
            "total": total_jobs.0,
            "queued": queued.0,
            "running": running.0,
            "completed": completed.0,
            "failed": failed.0,
            "retry_wait": retry_wait.0,
            "dlq": dlq.0,
        },
        "workers": { "active": workers.0 },
        "db": { "pool_size": pools, "idle": idle },
        "nats": { "connected": state.nats.is_some() }
    }))
}
