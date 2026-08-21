use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

use domain::{BatchStatus, ExecutionStatus, JobKind, JobStatus, RetryStrategy, WorkerStatus};

#[derive(Debug, Clone, sqlx::FromRow, Serialize, Deserialize)]
pub struct User {
    pub id: Uuid,
    pub email: String,
    #[serde(skip_serializing)]
    pub password_hash: String,
    pub display_name: String,
    pub is_active: bool,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}

#[derive(Debug, Clone, sqlx::FromRow, Serialize)]
pub struct Organization {
    pub id: Uuid,
    pub name: String,
    pub slug: String,
    pub created_by: Uuid,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}

#[derive(Debug, Clone, sqlx::FromRow, Serialize)]
pub struct Project {
    pub id: Uuid,
    pub org_id: Uuid,
    pub name: String,
    pub slug: String,
    pub description: String,
    pub is_archived: bool,
    pub created_by: Uuid,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}

#[derive(Debug, Clone, sqlx::FromRow, Serialize)]
pub struct Queue {
    pub id: Uuid,
    pub project_id: Uuid,
    pub name: String,
    pub description: String,
    pub max_concurrency: i32,
    pub is_paused: bool,
    pub default_priority: i32,
    pub retry_policy_id: Option<Uuid>,
    pub ack_wait_secs: i32,
    pub max_receives: i32,
    pub rate_limit: Option<i32>,
    pub rate_window_secs: i32,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}

#[derive(Debug, Clone, sqlx::FromRow, Serialize)]
pub struct Job {
    pub id: Uuid,
    pub queue_id: Uuid,
    pub batch_id: Option<Uuid>,
    pub workflow_id: Option<Uuid>,
    #[sqlx(rename = "type")]
    #[serde(rename = "type")]
    pub kind: JobKind,
    pub status: JobStatus,
    pub payload: serde_json::Value,
    pub priority: i32,
    pub attempt: i32,
    pub max_attempts: i32,
    pub retry_strategy: RetryStrategy,
    pub base_delay_secs: i64,
    pub max_delay_secs: i64,
    pub lease_epoch: i64,
    pub lease_owner: Option<Uuid>,
    pub lease_expires_at: Option<DateTime<Utc>>,
    pub token_id: Option<Uuid>,
    pub scheduled_for: Option<DateTime<Utc>>,
    pub next_retry_at: Option<DateTime<Utc>>,
    pub idempotency_key: Option<String>,
    pub result: Option<serde_json::Value>,
    pub error_message: Option<String>,
    pub error_kind: Option<String>,
    pub created_at: DateTime<Utc>,
    pub queued_at: Option<DateTime<Utc>>,
    pub claimed_at: Option<DateTime<Utc>>,
    pub started_at: Option<DateTime<Utc>>,
    pub completed_at: Option<DateTime<Utc>>,
    pub failed_at: Option<DateTime<Utc>>,
    pub updated_at: DateTime<Utc>,
}

#[derive(Debug, Clone, sqlx::FromRow, Serialize)]
pub struct JobExecution {
    pub id: Uuid,
    pub job_id: Uuid,
    pub worker_id: Option<Uuid>,
    pub attempt: i32,
    pub lease_epoch: i64,
    pub status: ExecutionStatus,
    pub started_at: DateTime<Utc>,
    pub finished_at: Option<DateTime<Utc>>,
    pub duration_ms: Option<i64>,
    pub result: Option<serde_json::Value>,
    pub error_message: Option<String>,
    pub error_kind: Option<String>,
    pub nats_msg_id: Option<String>,
}

#[derive(Debug, Clone, sqlx::FromRow, Serialize)]
pub struct JobLog {
    pub id: i64,
    pub job_id: Uuid,
    pub execution_id: Option<Uuid>,
    pub worker_id: Option<Uuid>,
    pub level: String,
    pub message: String,
    pub meta: serde_json::Value,
    pub created_at: DateTime<Utc>,
}

#[derive(Debug, Clone, sqlx::FromRow, Serialize)]
pub struct Worker {
    pub id: Uuid,
    pub worker_name: String,
    pub version: String,
    pub hostname: String,
    pub max_concurrency: i32,
    pub is_active: bool,
    pub last_heartbeat_at: Option<DateTime<Utc>>,
    pub started_at: DateTime<Utc>,
    pub stopped_at: Option<DateTime<Utc>>,
}

#[derive(Debug, Clone, sqlx::FromRow, Serialize)]
pub struct WorkerHeartbeat {
    pub id: i64,
    pub worker_id: Uuid,
    pub heartbeat_at: DateTime<Utc>,
    pub running_jobs: i32,
    pub processed_total: i64,
    pub failed_total: i64,
}

#[derive(Debug, Clone, sqlx::FromRow, Serialize)]
pub struct ScheduledJob {
    pub id: Uuid,
    pub queue_id: Uuid,
    pub name: String,
    pub job_type: String,
    pub payload: serde_json::Value,
    pub priority: i32,
    pub cron_expr: Option<String>,
    pub timezone: String,
    pub run_once_at: Option<DateTime<Utc>>,
    pub is_active: bool,
    pub last_fired_at: Option<DateTime<Utc>>,
    pub next_fire_at: Option<DateTime<Utc>>,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}

#[derive(Debug, Clone, sqlx::FromRow, Serialize)]
pub struct OutboxEvent {
    pub id: Uuid,
    pub job_id: Uuid,
    pub queue_id: Uuid,
    pub org_id: Uuid,
    pub project_id: Uuid,
    pub subject: String,
    pub payload: serde_json::Value,
    pub priority: i32,
    pub nats_msg_id: String,
    pub relay_owner_id: Option<String>,
    pub relay_locked_until: Option<DateTime<Utc>>,
    pub created_at: DateTime<Utc>,
    pub published_at: Option<DateTime<Utc>>,
}

#[derive(Debug, Clone, sqlx::FromRow, Serialize)]
pub struct DeadLetterEntry {
    pub id: Uuid,
    pub job_id: Uuid,
    pub queue_id: Uuid,
    pub org_id: Uuid,
    pub project_id: Uuid,
    pub reason: domain::DlqReason,
    pub attempt: i32,
    pub payload: serde_json::Value,
    pub final_error: Option<String>,
    pub error_kind: Option<String>,
    pub moved_at: DateTime<Utc>,
    pub replayed_to_job_id: Option<Uuid>,
    pub replayed_at: Option<DateTime<Utc>>,
}

#[derive(Debug, Clone, sqlx::FromRow, Serialize)]
pub struct Batch {
    pub id: Uuid,
    pub project_id: Uuid,
    pub queue_id: Uuid,
    pub name: String,
    pub total_jobs: i32,
    pub completed_jobs: i32,
    pub failed_jobs: i32,
    pub status: BatchStatus,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}

#[derive(Debug, Clone, sqlx::FromRow, Serialize)]
pub struct QueueStats {
    pub queue_id: Uuid,
    pub queued: i64,
    pub running: i64,
    pub retry_wait: i64,
    pub completed: i64,
    pub failed: i64,
    pub scheduled: i64,
    pub claimed: i64,
    pub dlq: i64,
}

#[derive(Debug, Clone, sqlx::FromRow, Serialize)]
pub struct WorkerView {
    pub id: Uuid,
    pub worker_name: String,
    pub version: String,
    pub hostname: String,
    pub max_concurrency: i32,
    pub is_active: bool,
    pub last_heartbeat_at: Option<DateTime<Utc>>,
    pub started_at: DateTime<Utc>,
    pub stopped_at: Option<DateTime<Utc>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub status: Option<WorkerStatus>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub running_jobs: Option<i32>,
}
