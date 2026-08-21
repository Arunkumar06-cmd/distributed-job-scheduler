use chrono::{DateTime, Utc};
use sqlx::PgPool;
use uuid::Uuid;

use common::AppResult;
use domain::{ExecutionStatus, JobKind, JobStatus, RetryStrategy};

use crate::models::*;

// =========================================================
// USERS / AUTH
// =========================================================
pub async fn create_user(
    pool: &PgPool,
    email: &str,
    password_hash: &str,
    display_name: &str,
) -> AppResult<User> {
    let user = sqlx::query_as::<_, User>(
        r#"INSERT INTO users (email, password_hash, display_name)
           VALUES ($1, $2, $3)
           RETURNING *"#,
    )
    .bind(email)
    .bind(password_hash)
    .bind(display_name)
    .fetch_one(pool)
    .await?;
    Ok(user)
}

pub async fn find_user_by_email(pool: &PgPool, email: &str) -> AppResult<Option<User>> {
    let user = sqlx::query_as::<_, User>("SELECT * FROM users WHERE email = $1 AND is_active = TRUE")
        .bind(email)
        .fetch_optional(pool)
        .await?;
    Ok(user)
}

pub async fn find_user_by_id(pool: &PgPool, id: Uuid) -> AppResult<Option<User>> {
    let user = sqlx::query_as::<_, User>("SELECT * FROM users WHERE id = $1")
        .bind(id)
        .fetch_optional(pool)
        .await?;
    Ok(user)
}

// =========================================================
// ORGANIZATIONS
// =========================================================
pub async fn create_organization(
    pool: &PgPool,
    name: &str,
    slug: &str,
    created_by: Uuid,
) -> AppResult<Organization> {
    let mut tx = pool.begin().await?;
    let org = sqlx::query_as::<_, Organization>(
        r#"INSERT INTO organizations (name, slug, created_by)
           VALUES ($1, $2, $3)
           RETURNING *"#,
    )
    .bind(name)
    .bind(slug)
    .bind(created_by)
    .fetch_one(&mut *tx)
    .await?;
    sqlx::query(
        r#"INSERT INTO org_memberships (org_id, user_id, role)
           VALUES ($1, $2, 'owner')"#,
    )
    .bind(org.id)
    .bind(created_by)
    .execute(&mut *tx)
    .await?;
    tx.commit().await?;
    Ok(org)
}

pub async fn list_organizations_for_user(pool: &PgPool, user_id: Uuid) -> AppResult<Vec<Organization>> {
    let orgs = sqlx::query_as::<_, Organization>(
        r#"SELECT o.* FROM organizations o
           JOIN org_memberships m ON m.org_id = o.id
           WHERE m.user_id = $1
           ORDER BY o.created_at DESC"#,
    )
    .bind(user_id)
    .fetch_all(pool)
    .await?;
    Ok(orgs)
}

pub async fn user_in_org(pool: &PgPool, user_id: Uuid, org_id: Uuid) -> AppResult<bool> {
    let row: (bool,) = sqlx::query_as(
        r#"SELECT EXISTS(
            SELECT 1 FROM org_memberships WHERE user_id = $1 AND org_id = $2
           )"#,
    )
    .bind(user_id)
    .bind(org_id)
    .fetch_one(pool)
    .await?;
    Ok(row.0)
}

pub async fn require_org_admin(pool: &PgPool, user_id: Uuid, org_id: Uuid) -> AppResult<()> {
    let row: Option<(String,)> = sqlx::query_as(
        r#"SELECT role::text FROM org_memberships WHERE user_id = $1 AND org_id = $2"#,
    )
    .bind(user_id)
    .bind(org_id)
    .fetch_optional(pool)
    .await?;
    match row {
        Some((role,)) if role == "owner" || role == "admin" => Ok(()),
        Some(_) => Err(common::AppError::Forbidden("requires org admin/owner".to_string())),
        None => Err(common::AppError::Forbidden("not in org".to_string())),
    }
}

// =========================================================
// PROJECTS
// =========================================================
pub async fn create_project(
    pool: &PgPool,
    org_id: Uuid,
    name: &str,
    slug: &str,
    description: &str,
    created_by: Uuid,
) -> AppResult<Project> {
    let mut tx = pool.begin().await?;
    let project = sqlx::query_as::<_, Project>(
        r#"INSERT INTO projects (org_id, name, slug, description, created_by)
           VALUES ($1, $2, $3, $4, $5)
           RETURNING *"#,
    )
    .bind(org_id)
    .bind(name)
    .bind(slug)
    .bind(description)
    .bind(created_by)
    .fetch_one(&mut *tx)
    .await?;
    sqlx::query(
        r#"INSERT INTO project_memberships (project_id, user_id, role)
           VALUES ($1, $2, 'owner')"#,
    )
    .bind(project.id)
    .bind(created_by)
    .execute(&mut *tx)
    .await?;
    tx.commit().await?;
    Ok(project)
}

pub async fn list_projects_in_org(pool: &PgPool, org_id: Uuid) -> AppResult<Vec<Project>> {
    let projects = sqlx::query_as::<_, Project>(
        "SELECT * FROM projects WHERE org_id = $1 AND is_archived = FALSE ORDER BY created_at DESC",
    )
    .bind(org_id)
    .fetch_all(pool)
    .await?;
    Ok(projects)
}

pub async fn get_project(pool: &PgPool, project_id: Uuid) -> AppResult<Option<Project>> {
    let p = sqlx::query_as::<_, Project>("SELECT * FROM projects WHERE id = $1")
        .bind(project_id)
        .fetch_optional(pool)
        .await?;
    Ok(p)
}

pub async fn user_in_project(pool: &PgPool, user_id: Uuid, project_id: Uuid) -> AppResult<bool> {
    let row: (bool,) = sqlx::query_as(
        r#"SELECT EXISTS(
            SELECT 1 FROM project_memberships WHERE user_id = $1 AND project_id = $2
           )"#,
    )
    .bind(user_id)
    .bind(project_id)
    .fetch_one(pool)
    .await?;
    Ok(row.0)
}

// =========================================================
// QUEUES
// =========================================================
pub async fn create_queue(
    pool: &PgPool,
    project_id: Uuid,
    name: &str,
    description: &str,
    max_concurrency: i32,
    default_priority: i32,
    ack_wait_secs: i32,
    max_receives: i32,
    retry_policy_id: Option<Uuid>,
) -> AppResult<Queue> {
    let q = sqlx::query_as::<_, Queue>(
        r#"INSERT INTO queues
             (project_id, name, description, max_concurrency, default_priority, ack_wait_secs, max_receives, retry_policy_id)
           VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
           RETURNING *"#,
    )
    .bind(project_id)
    .bind(name)
    .bind(description)
    .bind(max_concurrency)
    .bind(default_priority)
    .bind(ack_wait_secs)
    .bind(max_receives)
    .bind(retry_policy_id)
    .fetch_one(pool)
    .await?;
    Ok(q)
}

pub async fn list_queues_in_project(pool: &PgPool, project_id: Uuid) -> AppResult<Vec<Queue>> {
    let qs = sqlx::query_as::<_, Queue>(
        "SELECT * FROM queues WHERE project_id = $1 ORDER BY created_at DESC",
    )
    .bind(project_id)
    .fetch_all(pool)
    .await?;
    Ok(qs)
}

pub async fn get_queue(pool: &PgPool, queue_id: Uuid) -> AppResult<Option<Queue>> {
    let q = sqlx::query_as::<_, Queue>("SELECT * FROM queues WHERE id = $1")
        .bind(queue_id)
        .fetch_optional(pool)
        .await?;
    Ok(q)
}

pub async fn set_queue_paused(pool: &PgPool, queue_id: Uuid, paused: bool) -> AppResult<Queue> {
    let q = sqlx::query_as::<_, Queue>(
        "UPDATE queues SET is_paused = $2 WHERE id = $1 RETURNING *",
    )
    .bind(queue_id)
    .bind(paused)
    .fetch_one(pool)
    .await?;
    Ok(q)
}

pub async fn update_queue_config(
    pool: &PgPool,
    queue_id: Uuid,
    max_concurrency: Option<i32>,
    default_priority: Option<i32>,
    ack_wait_secs: Option<i32>,
    max_receives: Option<i32>,
    description: Option<&str>,
) -> AppResult<Queue> {
    let q = sqlx::query_as::<_, Queue>(
        r#"UPDATE queues SET
             max_concurrency = COALESCE($2, max_concurrency),
             default_priority = COALESCE($3, default_priority),
             ack_wait_secs = COALESCE($4, ack_wait_secs),
             max_receives = COALESCE($5, max_receives),
             description = COALESCE($6, description)
           WHERE id = $1 RETURNING *"#,
    )
    .bind(queue_id)
    .bind(max_concurrency)
    .bind(default_priority)
    .bind(ack_wait_secs)
    .bind(max_receives)
    .bind(description)
    .fetch_one(pool)
    .await?;
    Ok(q)
}

pub async fn queue_stats(pool: &PgPool, queue_id: Uuid) -> AppResult<QueueStats> {
    let stats = sqlx::query_as::<_, QueueStats>(
        r#"SELECT
             $1 as queue_id,
             COALESCE(SUM(CASE WHEN status = 'QUEUED' THEN 1 ELSE 0 END), 0) AS queued,
             COALESCE(SUM(CASE WHEN status = 'RUNNING' THEN 1 ELSE 0 END), 0) AS running,
             COALESCE(SUM(CASE WHEN status = 'RETRY_WAIT' THEN 1 ELSE 0 END), 0) AS retry_wait,
             COALESCE(SUM(CASE WHEN status = 'COMPLETED' THEN 1 ELSE 0 END), 0) AS completed,
             COALESCE(SUM(CASE WHEN status = 'FAILED' THEN 1 ELSE 0 END), 0) AS failed,
             COALESCE(SUM(CASE WHEN status = 'SCHEDULED' THEN 1 ELSE 0 END), 0) AS scheduled,
             COALESCE(SUM(CASE WHEN status = 'CLAIMED' THEN 1 ELSE 0 END), 0) AS claimed,
             (SELECT COUNT(*) FROM dead_letter_entries WHERE queue_id = $1) AS dlq
           FROM jobs WHERE queue_id = $1"#,
    )
    .bind(queue_id)
    .fetch_one(pool)
    .await?;
    Ok(stats)
}

// =========================================================
// JOBS — creation (with outbox in same transaction)
// =========================================================
#[derive(Debug, Clone)]
pub struct CreateJobParams {
    pub queue_id: Uuid,
    pub org_id: Uuid,
    pub project_id: Uuid,
    pub batch_id: Option<Uuid>,
    pub kind: JobKind,
    pub payload: serde_json::Value,
    pub priority: i32,
    pub max_attempts: i32,
    pub retry_strategy: RetryStrategy,
    pub base_delay_secs: i64,
    pub max_delay_secs: i64,
    pub scheduled_for: Option<DateTime<Utc>>,
    pub idempotency_key: Option<String>,
    pub subject: String,
}

pub async fn create_job_with_outbox(pool: &PgPool, p: CreateJobParams) -> AppResult<Job> {
    let mut tx = pool.begin().await?;
    let status = if p.scheduled_for.is_some() {
        JobStatus::Scheduled
    } else {
        JobStatus::Queued
    };
    let job: Job = sqlx::query_as::<_, Job>(
        r#"INSERT INTO jobs
             (queue_id, batch_id, type, status, payload, priority,
              max_attempts, retry_strategy, base_delay_secs, max_delay_secs,
              scheduled_for, idempotency_key, queued_at)
           VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12,
              CASE WHEN $4::job_status = 'QUEUED' THEN NOW() ELSE NULL END)
           RETURNING *"#,
    )
    .bind(p.queue_id)
    .bind(p.batch_id)
    .bind(p.kind)
    .bind(status)
    .bind(&p.payload)
    .bind(p.priority)
    .bind(p.max_attempts)
    .bind(p.retry_strategy)
    .bind(p.base_delay_secs)
    .bind(p.max_delay_secs)
    .bind(p.scheduled_for)
    .bind(&p.idempotency_key)
    .fetch_one(&mut *tx)
    .await?;

    // Only create outbox event for immediately-dispatchable jobs
    if status == JobStatus::Queued {
        let event_id = Uuid::new_v4();
        sqlx::query(
            r#"INSERT INTO outbox_events
                 (id, job_id, queue_id, org_id, project_id, subject, payload, priority, nats_msg_id)
               VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)"#,
        )
        .bind(event_id)
        .bind(job.id)
        .bind(p.queue_id)
        .bind(p.org_id)
        .bind(p.project_id)
        .bind(&p.subject)
        .bind(&p.payload)
        .bind(p.priority)
        .bind(event_id.to_string())
        .execute(&mut *tx)
        .await?;
    }

    tx.commit().await?;
    Ok(job)
}

pub async fn get_job(pool: &PgPool, job_id: Uuid) -> AppResult<Option<Job>> {
    let j = sqlx::query_as::<_, Job>("SELECT * FROM jobs WHERE id = $1")
        .bind(job_id)
        .fetch_optional(pool)
        .await?;
    Ok(j)
}

pub async fn list_jobs(
    pool: &PgPool,
    queue_id: Option<Uuid>,
    status: Option<JobStatus>,
    priority_min: Option<i32>,
    worker_id: Option<Uuid>,
    batch_id: Option<Uuid>,
    limit: i64,
    offset: i64,
) -> AppResult<Vec<Job>> {
    let jobs = sqlx::query_as::<_, Job>(
        r#"SELECT * FROM jobs
           WHERE ($1::uuid IS NULL OR queue_id = $1)
             AND ($2::job_status IS NULL OR status = $2)
             AND ($3::int IS NULL OR priority >= $3)
             AND ($4::uuid IS NULL OR lease_owner = $4)
             AND ($5::uuid IS NULL OR batch_id = $5)
           ORDER BY created_at DESC
           LIMIT $6 OFFSET $7"#,
    )
    .bind(queue_id)
    .bind(status)
    .bind(priority_min)
    .bind(worker_id)
    .bind(batch_id)
    .bind(limit)
    .bind(offset)
    .fetch_all(pool)
    .await?;
    Ok(jobs)
}

pub async fn count_jobs(
    pool: &PgPool,
    queue_id: Option<Uuid>,
    status: Option<JobStatus>,
) -> AppResult<i64> {
    let row: (i64,) = sqlx::query_as(
        r#"SELECT COUNT(*) FROM jobs
           WHERE ($1::uuid IS NULL OR queue_id = $1)
             AND ($2::job_status IS NULL OR status = $2)"#,
    )
    .bind(queue_id)
    .bind(status)
    .fetch_one(pool)
    .await?;
    Ok(row.0)
}

// =========================================================
// WORKER REGISTRATION + HEARTBEAT
// =========================================================
pub async fn upsert_worker(
    pool: &PgPool,
    worker_name: &str,
    version: &str,
    hostname: &str,
    max_concurrency: i32,
) -> AppResult<Worker> {
    let w = sqlx::query_as::<_, Worker>(
        r#"INSERT INTO workers (worker_name, version, hostname, max_concurrency, is_active, last_heartbeat_at, stopped_at)
           VALUES ($1, $2, $3, $4, TRUE, NOW(), NULL)
           ON CONFLICT (worker_name) DO UPDATE SET
             version = EXCLUDED.version,
             hostname = EXCLUDED.hostname,
             max_concurrency = EXCLUDED.max_concurrency,
             is_active = TRUE,
             last_heartbeat_at = NOW(),
             stopped_at = NULL
           RETURNING *"#,
    )
    .bind(worker_name)
    .bind(version)
    .bind(hostname)
    .bind(max_concurrency)
    .fetch_one(pool)
    .await?;
    Ok(w)
}

pub async fn heartbeat(
    pool: &PgPool,
    worker_id: Uuid,
    running_jobs: i32,
    processed_total: i64,
    failed_total: i64,
) -> AppResult<()> {
    sqlx::query(
        r#"UPDATE workers SET last_heartbeat_at = NOW(), is_active = TRUE WHERE id = $1"#,
    )
    .bind(worker_id)
    .execute(pool)
    .await?;
    sqlx::query(
        r#"INSERT INTO worker_heartbeats (worker_id, running_jobs, processed_total, failed_total)
           VALUES ($1, $2, $3, $4)"#,
    )
    .bind(worker_id)
    .bind(running_jobs)
    .bind(processed_total)
    .bind(failed_total)
    .execute(pool)
    .await?;
    Ok(())
}

pub async fn mark_worker_stopped(pool: &PgPool, worker_id: Uuid) -> AppResult<()> {
    sqlx::query(
        r#"UPDATE workers SET is_active = FALSE, stopped_at = NOW() WHERE id = $1"#,
    )
    .bind(worker_id)
    .execute(pool)
    .await?;
    Ok(())
}

pub async fn list_workers(pool: &PgPool) -> AppResult<Vec<WorkerView>> {
    let rows = sqlx::query_as::<_, WorkerView>(
        r#"SELECT
             w.*,
             CASE
               WHEN w.last_heartbeat_at IS NULL THEN 'OFFLINE'::worker_status
               WHEN EXTRACT(EPOCH FROM (NOW() - w.last_heartbeat_at)) < 15 THEN 'ONLINE'::worker_status
               WHEN EXTRACT(EPOCH FROM (NOW() - w.last_heartbeat_at)) < 60 THEN 'STALE'::worker_status
               ELSE 'OFFLINE'::worker_status
             END AS status,
             (SELECT COUNT(*) FROM jobs WHERE lease_owner = w.id AND status = 'RUNNING') AS running_jobs
           FROM workers w
           ORDER BY w.started_at DESC"#,
    )
    .fetch_all(pool)
    .await?;
    Ok(rows)
}

pub async fn get_worker(pool: &PgPool, worker_id: Uuid) -> AppResult<Option<WorkerView>> {
    let row = sqlx::query_as::<_, WorkerView>(
        r#"SELECT
             w.*,
             CASE
               WHEN w.last_heartbeat_at IS NULL THEN 'OFFLINE'::worker_status
               WHEN EXTRACT(EPOCH FROM (NOW() - w.last_heartbeat_at)) < 15 THEN 'ONLINE'::worker_status
               WHEN EXTRACT(EPOCH FROM (NOW() - w.last_heartbeat_at)) < 60 THEN 'STALE'::worker_status
               ELSE 'OFFLINE'::worker_status
             END AS status,
             (SELECT COUNT(*) FROM jobs WHERE lease_owner = w.id AND status = 'RUNNING') AS running_jobs
           FROM workers w
           WHERE w.id = $1"#,
    )
    .bind(worker_id)
    .fetch_optional(pool)
    .await?;
    Ok(row)
}

// =========================================================
// ATOMIC JOB CLAIM (the critical concurrency path)
// =========================================================
#[derive(Debug, Clone)]
pub struct ClaimedJob {
    pub job: Job,
    pub execution_id: Uuid,
    pub lease_epoch: i64,
}

/// Atomically claim a job for execution.
///
/// This is the most safety-critical query in the system. It:
/// 1. Locks the queue row with FOR UPDATE NOWAIT (serializes claims per queue)
/// 2. Checks is_paused -> returns QueuePaused error
/// 3. Counts RUNNING jobs -> rejects if at capacity
/// 4. Updates the job: QUEUED -> CLAIMED, increments epoch, sets lease
/// 5. Creates a job_execution row
///
/// All within one transaction. The long task runs AFTER commit.
pub async fn claim_job(
    pool: &PgPool,
    job_id: Uuid,
    worker_id: Uuid,
    nats_msg_id: &str,
    lease_duration_secs: i64,
) -> AppResult<ClaimedJob> {
    // Deadlock retry per spec §15 (40P01, 40001)
    let mut attempt = 0;
    loop {
        let res = claim_job_inner(pool, job_id, worker_id, nats_msg_id, lease_duration_secs).await;
        match res {
            Ok(v) => return Ok(v),
            Err(e) if e.to_string().contains("40P01") || e.to_string().contains("40001") => {
                if attempt >= 3 {
                    return Err(e);
                }
                attempt += 1;
                let backoff = 10 * (1 << attempt) + (rand::random::<u64>() % 10);
                tokio::time::sleep(std::time::Duration::from_millis(backoff)).await;
                continue;
            }
            Err(e) => return Err(e),
        }
    }
}

async fn claim_job_inner(
    pool: &PgPool,
    job_id: Uuid,
    worker_id: Uuid,
    nats_msg_id: &str,
    lease_duration_secs: i64,
) -> AppResult<ClaimedJob> {
    let mut tx = pool.begin().await?;
    let _ = sqlx::query("SET lock_timeout = '5s'").execute(&mut *tx).await;

    // 1. Lock queue row to check pause + rate limit (spec §13: QUEUE -> TOKEN -> JOB)
    let queue_row: Option<(Uuid, bool, Option<i32>)> = sqlx::query_as(
        r#"SELECT q.id, q.is_paused, q.rate_limit
           FROM jobs j
           JOIN queues q ON q.id = j.queue_id
           WHERE j.id = $1
           FOR UPDATE OF q"#,
    )
    .bind(job_id)
    .fetch_optional(&mut *tx)
    .await?;

    let (queue_id, is_paused, rate_limit) = queue_row.ok_or_else(|| {
        common::AppError::NotFound(format!("job {job_id} not found"))
    })?;

    if is_paused {
        tx.rollback().await.ok();
        return Err(common::AppError::QueuePaused);
    }

    // 1b. Rate limiting (bonus §2): token bucket per queue
    if let Some(limit) = rate_limit {
        // Simple sliding window: count jobs created in last 60s
        let recent: (i64,) = sqlx::query_as(
            r#"SELECT COUNT(*) FROM jobs WHERE queue_id = $1 AND created_at > NOW() - INTERVAL '60 seconds'"#,
        )
        .bind(queue_id)
        .fetch_one(&mut *tx)
        .await?;
        if recent.0 >= limit as i64 {
            tx.rollback().await.ok();
            return Err(common::AppError::Validation(format!("queue rate limit {limit}/min exceeded")));
        }
        // Also try queue_rate_buckets refill (for future)
        let _ = sqlx::query(
            r#"INSERT INTO queue_rate_buckets (queue_id, tokens, last_refill_at)
               VALUES ($1, $2, NOW())
               ON CONFLICT (queue_id) DO UPDATE SET last_refill_at = NOW()"#,
        )
        .bind(queue_id)
        .bind(limit)
        .execute(&mut *tx)
        .await;
    }

    // 2. Claim capacity token via SKIP LOCKED (global concurrency, spec §7)
    let token: Option<(Uuid,)> = sqlx::query_as(
        r#"SELECT id FROM capacity_tokens
           WHERE queue_id = $1 AND worker_id IS NULL
           FOR UPDATE SKIP LOCKED
           LIMIT 1"#,
    )
    .bind(queue_id)
    .fetch_optional(&mut *tx)
    .await?;

    let (token_id,) = token.ok_or_else(|| {
        common::AppError::QueueAtCapacity
    })?;

    // 2b. Assign token to worker/job
    sqlx::query(
        r#"UPDATE capacity_tokens SET worker_id = $2, job_id = $1, lease_until = $3, epoch = epoch + 1
           WHERE id = $4"#,
    )
    .bind(job_id)
    .bind(worker_id)
    .bind(Utc::now() + chrono::Duration::seconds(lease_duration_secs))
    .bind(token_id)
    .execute(&mut *tx)
    .await?;

    // 3. Atomically claim: QUEUED -> CLAIMED, bump epoch, set lease
    let now = Utc::now();
    let expires = now + chrono::Duration::seconds(lease_duration_secs);
    let claimed: Option<Job> = sqlx::query_as::<_, Job>(
        r#"UPDATE jobs SET
             status = 'CLAIMED'::job_status,
             attempt = attempt + 1,
             lease_epoch = lease_epoch + 1,
             lease_owner = $2,
             lease_expires_at = $3,
             claimed_at = $4,
             token_id = $5
           WHERE id = $1
             AND status IN ('QUEUED'::job_status, 'RETRY_WAIT'::job_status)
             AND (lease_expires_at IS NULL OR lease_expires_at < $4)
           RETURNING *"#,
    )
    .bind(job_id)
    .bind(worker_id)
    .bind(expires)
    .bind(now)
    .bind(token_id)
    .fetch_optional(&mut *tx)
    .await?;

    let job = claimed.ok_or_else(|| {
        common::AppError::Conflict(format!("job {job_id} not claimable (already claimed or not queued)"))
    })?;

    // 4. Create execution record
    let execution_id = Uuid::new_v4();
    sqlx::query(
        r#"INSERT INTO job_executions
             (id, job_id, worker_id, attempt, lease_epoch, status, started_at, nats_msg_id)
           VALUES ($1, $2, $3, $4, $5, 'STARTED'::execution_status, NOW(), $6)"#,
    )
    .bind(execution_id)
    .bind(job_id)
    .bind(worker_id)
    .bind(job.attempt)
    .bind(job.lease_epoch)
    .bind(nats_msg_id)
    .execute(&mut *tx)
    .await?;

    tx.commit().await?;

    let lease_epoch = job.lease_epoch;
    Ok(ClaimedJob {
        job,
        execution_id,
        lease_epoch,
    })
}

// =========================================================
// LEASE RENEWAL (heartbeat for a running job)
// =========================================================
pub async fn renew_lease(
    pool: &PgPool,
    job_id: Uuid,
    worker_id: Uuid,
    epoch: i64,
    lease_duration_secs: i64,
) -> AppResult<bool> {
    let expires = Utc::now() + chrono::Duration::seconds(lease_duration_secs);
    let result = sqlx::query(
        r#"UPDATE jobs SET lease_expires_at = $4
           WHERE id = $1 AND lease_owner = $2 AND lease_epoch = $3 AND status = 'RUNNING'"#,
    )
    .bind(job_id)
    .bind(worker_id)
    .bind(epoch)
    .bind(expires)
    .execute(pool)
    .await?;
    Ok(result.rows_affected() == 1)
}

// =========================================================
// COMPLETE A JOB (with epoch fencing)
// =========================================================
pub async fn complete_job(
    pool: &PgPool,
    job_id: Uuid,
    worker_id: Uuid,
    epoch: i64,
    execution_id: Uuid,
    result: Option<serde_json::Value>,
) -> AppResult<bool> {
    let mut tx = pool.begin().await?;
    let updated = sqlx::query(
        r#"UPDATE jobs SET
             status = 'COMPLETED'::job_status,
             result = $4,
             completed_at = NOW(),
             lease_expires_at = NULL,
             lease_owner = NULL,
             token_id = NULL
           WHERE id = $1 AND lease_owner = $2 AND lease_epoch = $3 AND status = 'RUNNING'"#,
    )
    .bind(job_id)
    .bind(worker_id)
    .bind(epoch)
    .bind(&result)
    .execute(&mut *tx)
    .await?;

    if updated.rows_affected() == 0 {
        tx.rollback().await.ok();
        return Ok(false); // fenced or not running
    }

    // Free capacity token (HOT, spec §7)
    sqlx::query(r#"UPDATE capacity_tokens SET worker_id = NULL, job_id = NULL, lease_until = NULL WHERE job_id = $1"#)
        .bind(job_id)
        .execute(&mut *tx)
        .await?;

    // DAG resolver (bonus §1, spec §27-28): satisfy edges where this job is parent
    // Insert edge_satisfaction for each child, and if all parents of child are satisfied, queue child
    let children: Vec<(Uuid,)> = sqlx::query_as(
        r#"INSERT INTO edge_satisfaction (parent_id, child_id)
           SELECT $1, child_id FROM workflow_edges WHERE parent_id = $1
           ON CONFLICT DO NOTHING
           RETURNING child_id"#,
    )
    .bind(job_id)
    .fetch_all(&mut *tx)
    .await?;
    for (child_id,) in children {
        let total: (i64,) = sqlx::query_as(r#"SELECT COUNT(*) FROM workflow_edges WHERE child_id = $1"#)
            .bind(child_id)
            .fetch_one(&mut *tx)
            .await?;
        let done: (i64,) = sqlx::query_as(r#"SELECT COUNT(*) FROM edge_satisfaction WHERE child_id = $1"#)
            .bind(child_id)
            .fetch_one(&mut *tx)
            .await?;
        if total.0 == done.0 {
            let queued: Option<Job> = sqlx::query_as::<_, Job>(
                r#"UPDATE jobs SET status = 'QUEUED'::job_status, queued_at = NOW(), updated_at = NOW()
                   WHERE id = $1 AND status = 'WAITING'::job_status
                   RETURNING *"#,
            )
            .bind(child_id)
            .fetch_optional(&mut *tx)
            .await?;
            if let Some(j) = queued {
                // Create outbox for newly ready child
                let ctx: Option<(Uuid, Uuid)> = sqlx::query_as(
                    r#"SELECT p.org_id, p.id FROM queues q JOIN projects p ON p.id = q.project_id WHERE q.id = $1"#,
                )
                .bind(j.queue_id)
                .fetch_optional(&mut *tx)
                .await?;
                if let Some((org_id, proj_id)) = ctx {
                    let tier = if j.priority >= 10 { "high" } else if j.priority > 0 { "standard" } else { "low" };
                    let subject = format!("org.{org_id}.proj.{proj_id}.queue.{}.{}", j.queue_id, tier);
                    let eid = Uuid::new_v4();
                    sqlx::query(
                        r#"INSERT INTO outbox_events (id, job_id, queue_id, org_id, project_id, subject, payload, priority, nats_msg_id)
                           VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)"#,
                    )
                    .bind(eid)
                    .bind(j.id)
                    .bind(j.queue_id)
                    .bind(org_id)
                    .bind(proj_id)
                    .bind(&subject)
                    .bind(&j.payload)
                    .bind(j.priority)
                    .bind(eid.to_string())
                    .execute(&mut *tx)
                    .await?;
                }
            }
        }
    }

    sqlx::query(
        r#"UPDATE job_executions SET
             status = 'COMPLETED'::execution_status,
             finished_at = NOW(),
             duration_ms = EXTRACT(EPOCH FROM (NOW() - started_at)) * 1000,
             result = $3
           WHERE id = $1"#,
    )
    .bind(execution_id)
    .bind(job_id)
    .bind(&result)
    .execute(&mut *tx)
    .await?;

    tx.commit().await?;
    Ok(true)
}

// =========================================================
// FAIL A JOB -> retry or DLQ
// =========================================================
pub async fn fail_job(
    pool: &PgPool,
    job_id: Uuid,
    worker_id: Uuid,
    epoch: i64,
    execution_id: Uuid,
    error_message: &str,
    error_kind: &str,
    org_id: Uuid,
    project_id: Uuid,
    queue_id: Uuid,
) -> AppResult<FailOutcome> {
    let mut tx = pool.begin().await?;

    // Load the job with lock to read attempt/max_attempts
    let job: Job = sqlx::query_as::<_, Job>(
        r#"SELECT * FROM jobs WHERE id = $1 AND lease_owner = $2 AND lease_epoch = $3 FOR UPDATE"#,
    )
    .bind(job_id)
    .bind(worker_id)
    .bind(epoch)
    .fetch_one(&mut *tx)
    .await
    .map_err(|e| match e {
        sqlx::Error::RowNotFound => common::AppError::StaleLease,
        other => other.into(),
    })?;

    let next_attempt = job.attempt; // attempt was already incremented at claim time
    let will_retry = next_attempt < job.max_attempts;

    let policy = domain::RetryPolicy {
        max_attempts: job.max_attempts,
        strategy: job.retry_strategy,
        base_delay_secs: job.base_delay_secs,
        max_delay_secs: job.max_delay_secs,
    };
    let delay = policy.delay_secs(next_attempt);
    let next_retry_at = Utc::now() + chrono::Duration::seconds(delay);

    // Update execution as failed
    sqlx::query(
        r#"UPDATE job_executions SET
             status = 'FAILED'::execution_status,
             finished_at = NOW(),
             duration_ms = EXTRACT(EPOCH FROM (NOW() - started_at)) * 1000,
             error_message = $2,
             error_kind = $3
           WHERE id = $1"#,
    )
    .bind(execution_id)
    .bind(error_message)
    .bind(error_kind)
    .execute(&mut *tx)
    .await?;

    if will_retry {
        // RETRY_WAIT - release capacity token
        sqlx::query(
            r#"UPDATE jobs SET
                 status = 'RETRY_WAIT'::job_status,
                 next_retry_at = $2,
                 error_message = $3,
                 error_kind = $4,
                 lease_owner = NULL,
                 lease_expires_at = NULL,
                 token_id = NULL
               WHERE id = $1"#,
        )
        .bind(job_id)
        .bind(next_retry_at)
        .bind(error_message)
        .bind(error_kind)
        .execute(&mut *tx)
        .await?;
        sqlx::query(r#"UPDATE capacity_tokens SET worker_id = NULL, job_id = NULL, lease_until = NULL WHERE job_id = $1"#)
            .bind(job_id)
            .execute(&mut *tx)
            .await?;
        tx.commit().await?;
        Ok(FailOutcome::Retry { next_retry_at, delay_secs: delay })
    } else {
        // FAILED + DLQ
        sqlx::query(
            r#"UPDATE jobs SET
                 status = 'FAILED'::job_status,
                 failed_at = NOW(),
                 error_message = $2,
                 error_kind = $3,
                 lease_owner = NULL,
                 lease_expires_at = NULL,
                 token_id = NULL
               WHERE id = $1"#,
        )
        .bind(job_id)
        .bind(error_message)
        .bind(error_kind)
        .execute(&mut *tx)
        .await?;
        sqlx::query(r#"UPDATE capacity_tokens SET worker_id = NULL, job_id = NULL, lease_until = NULL WHERE job_id = $1"#)
            .bind(job_id)
            .execute(&mut *tx)
            .await?;

        sqlx::query(
            r#"INSERT INTO dead_letter_entries
                 (job_id, queue_id, org_id, project_id, reason, attempt, payload, final_error, error_kind)
               VALUES ($1, $2, $3, $4, 'max_attempts_exceeded', $5, $6, $7, $8)"#,
        )
        .bind(job_id)
        .bind(queue_id)
        .bind(org_id)
        .bind(project_id)
        .bind(next_attempt)
        .bind(&job.payload)
        .bind(error_message)
        .bind(error_kind)
        .execute(&mut *tx)
        .await?;

        tx.commit().await?;
        Ok(FailOutcome::DeadLettered)
    }
}

#[derive(Debug, Clone)]
pub enum FailOutcome {
    Retry { next_retry_at: DateTime<Utc>, delay_secs: i64 },
    DeadLettered,
}

// =========================================================
// RETRY REQUEUE (RETRY_WAIT -> QUEUED + new outbox event)
// =========================================================
pub async fn requeue_ready_retries(pool: &PgPool) -> AppResult<i64> {
    let result = sqlx::query(
        r#"WITH moved AS (
             UPDATE jobs SET
               status = 'QUEUED'::job_status,
               next_retry_at = NULL,
               queued_at = NOW(),
               updated_at = NOW()
             WHERE status = 'RETRY_WAIT'::job_status
               AND next_retry_at IS NOT NULL
               AND next_retry_at <= NOW()
             RETURNING id, queue_id, payload, priority
           )
           INSERT INTO outbox_events
             (id, job_id, queue_id, org_id, project_id, subject, payload, priority, nats_msg_id)
           SELECT
             gen_random_uuid(),
             m.id,
             m.queue_id,
             p.org_id,
              p.id,
              'org.'||p.org_id||'.proj.'||p.id||'.queue.'||m.queue_id||'.'||CASE WHEN m.priority >= 10 THEN 'high' WHEN m.priority > 0 THEN 'standard' ELSE 'low' END,
             m.payload,
             m.priority,
             gen_random_uuid()::text
           FROM moved m
           JOIN queues q ON q.id = m.queue_id
           JOIN projects p ON p.id = q.project_id
           JOIN organizations o ON o.id = p.org_id"#,
    )
    .execute(pool)
    .await?;
    Ok(result.rows_affected() as i64)
}

// =========================================================
// SCHEDULED JOBS -> QUEUED (scheduler tick)
// =========================================================
pub async fn promote_scheduled_jobs(pool: &PgPool) -> AppResult<i64> {
    let result = sqlx::query(
        r#"WITH promoted AS (
             UPDATE jobs SET
               status = 'QUEUED'::job_status,
               queued_at = NOW(),
               updated_at = NOW()
             WHERE status = 'SCHEDULED'::job_status
               AND scheduled_for IS NOT NULL
               AND scheduled_for <= NOW()
             RETURNING id, queue_id, payload, priority
           )
           INSERT INTO outbox_events
             (id, job_id, queue_id, org_id, project_id, subject, payload, priority, nats_msg_id)
           SELECT
             gen_random_uuid(),
             m.id,
             m.queue_id,
             p.org_id,
             p.id,
             'org.'||p.org_id||'.proj.'||p.id||'.queue.'||m.queue_id||'.'||CASE WHEN m.priority >= 10 THEN 'high' WHEN m.priority > 0 THEN 'standard' ELSE 'low' END,
             m.payload,
             m.priority,
             gen_random_uuid()::text
           FROM promoted m
           JOIN queues q ON q.id = m.queue_id
           JOIN projects p ON p.id = q.project_id"#,
    )
    .execute(pool)
    .await?;
    Ok(result.rows_affected() as i64)
}

pub async fn reclaim_stale_running(pool: &PgPool) -> AppResult<i64> {
    // Find RUNNING jobs whose lease expired >10s ago (grace), mark their execution ABANDONED, requeue as QUEUED + outbox
    let result = sqlx::query(
        r#"WITH stale AS (
             UPDATE jobs SET status='QUEUED'::job_status, lease_owner=NULL, lease_expires_at=NULL, token_id=NULL, queued_at=NOW(), updated_at=NOW()
             WHERE status IN ('RUNNING'::job_status, 'CLAIMED'::job_status)
                AND lease_expires_at IS NOT NULL
                AND lease_expires_at < NOW() - INTERVAL '10 seconds'
             RETURNING id, queue_id, payload, priority
           ),
           upd_exec AS (
             UPDATE job_executions SET status='ABANDONED'::execution_status, finished_at=NOW()
             WHERE job_id IN (SELECT id FROM stale) AND status='STARTED'::execution_status
             RETURNING 1
           ),
           free_tokens AS (
             UPDATE capacity_tokens SET worker_id=NULL, job_id=NULL, lease_until=NULL
             WHERE job_id IN (SELECT id FROM stale)
             RETURNING 1
           )
           INSERT INTO outbox_events (id, job_id, queue_id, org_id, project_id, subject, payload, priority, nats_msg_id)
           SELECT gen_random_uuid(), s.id, s.queue_id, p.org_id, p.id,
                  'org.'||p.org_id||'.proj.'||p.id||'.queue.'||s.queue_id||'.'||CASE WHEN s.priority >= 10 THEN 'high' WHEN s.priority > 0 THEN 'standard' ELSE 'low' END,
                  s.payload, s.priority, gen_random_uuid()::text
           FROM stale s
           JOIN queues q ON q.id=s.queue_id
           JOIN projects p ON p.id=q.project_id"#,
    )
    .execute(pool)
    .await?;
    Ok(result.rows_affected() as i64)
}

pub async fn backfill_queued_outbox(pool: &PgPool) -> AppResult<i64> {
    let result = sqlx::query(
        r#"INSERT INTO outbox_events (id, job_id, queue_id, org_id, project_id, subject, payload, priority, nats_msg_id)
           SELECT gen_random_uuid(), j.id, j.queue_id, p.org_id, p.id,
                  'org.'||p.org_id||'.proj.'||p.id||'.queue.'||j.queue_id||'.high',
                  j.payload, j.priority, gen_random_uuid()::text
           FROM jobs j
           JOIN queues q ON q.id=j.queue_id
           JOIN projects p ON p.id=q.project_id
           WHERE j.status='QUEUED'::job_status
              AND NOT EXISTS (SELECT 1 FROM outbox_events oe WHERE oe.job_id=j.id)
              AND j.queued_at < NOW() - INTERVAL '30 seconds'"#,
    )
    .execute(pool)
    .await?;
    Ok(result.rows_affected() as i64)
}

pub async fn reconcile_unknown_jobs(pool: &PgPool) -> AppResult<i64> {
    // Real reconciler per spec §25: external GET /payments/{job_id} -> SUCCESS->COMPLETED, FAILURE->RETRY_WAIT, UNKNOWN->keep
    // Mock: payload.should_succeed boolean or id hash even->success (50%)
    let result = sqlx::query(
        r#"WITH reconciled AS (
             UPDATE jobs SET
               status = CASE
                 WHEN (payload->>'should_succeed')::boolean IS TRUE THEN 'COMPLETED'::job_status
                 WHEN (payload->>'should_succeed')::boolean IS FALSE THEN 'RETRY_WAIT'::job_status
                 WHEN substr(id::text, 1, 1) IN ('0','2','4','6','8','a','c','e') THEN 'COMPLETED'::job_status
                 ELSE 'RETRY_WAIT'::job_status
               END,
               completed_at = CASE
                 WHEN (payload->>'should_succeed')::boolean IS TRUE OR substr(id::text, 1, 1) IN ('0','2','4','6','8','a','c','e') THEN NOW()
                 ELSE NULL END,
               next_retry_at = CASE
                 WHEN (payload->>'should_succeed')::boolean IS FALSE OR substr(id::text, 1, 1) IN ('1','3','5','7','9','b','d','f') THEN NOW() + INTERVAL '5 seconds'
                 ELSE NULL END,
               queued_at = CASE
                 WHEN (payload->>'should_succeed')::boolean IS FALSE OR substr(id::text, 1, 1) IN ('1','3','5','7','9','b','d','f') THEN NOW()
                 ELSE NULL END,
               updated_at = NOW(),
               error_message = CASE WHEN status='UNKNOWN_EXTERNAL_RESULT' THEN NULL ELSE error_message END,
               error_kind = CASE WHEN status='UNKNOWN_EXTERNAL_RESULT' THEN NULL ELSE error_kind END
             WHERE status='UNKNOWN_EXTERNAL_RESULT'::job_status
               AND updated_at < NOW() - INTERVAL '30 seconds'
             RETURNING id, queue_id, payload, priority, status
           ),
           upd_exec AS (
             UPDATE job_executions SET status='COMPLETED'::execution_status, finished_at=NOW()
             WHERE job_id IN (SELECT id FROM reconciled WHERE status='COMPLETED'::job_status) AND status='ABANDONED'::execution_status
             RETURNING 1
           )
           INSERT INTO outbox_events (id, job_id, queue_id, org_id, project_id, subject, payload, priority, nats_msg_id)
           SELECT gen_random_uuid(), r.id, r.queue_id, p.org_id, p.id,
                  'org.'||p.org_id||'.proj.'||p.id||'.queue.'||r.queue_id||'.'||CASE WHEN r.priority >= 10 THEN 'high' WHEN r.priority > 0 THEN 'standard' ELSE 'low' END,
                  r.payload, r.priority, gen_random_uuid()::text
           FROM reconciled r
           JOIN queues q ON q.id=r.queue_id
           JOIN projects p ON p.id=q.project_id
           WHERE r.status='RETRY_WAIT'::job_status"#,
    )
    .execute(pool)
    .await?;
    Ok(result.rows_affected() as i64)
}

// =========================================================
// EXECUTIONS + LOGS
// =========================================================
pub async fn list_executions(pool: &PgPool, job_id: Uuid) -> AppResult<Vec<JobExecution>> {
    let rows = sqlx::query_as::<_, JobExecution>(
        "SELECT * FROM job_executions WHERE job_id = $1 ORDER BY started_at DESC",
    )
    .bind(job_id)
    .fetch_all(pool)
    .await?;
    Ok(rows)
}

pub async fn list_logs(pool: &PgPool, job_id: Uuid, limit: i64) -> AppResult<Vec<JobLog>> {
    let rows = sqlx::query_as::<_, JobLog>(
        "SELECT * FROM job_logs WHERE job_id = $1 ORDER BY created_at DESC LIMIT $2",
    )
    .bind(job_id)
    .bind(limit)
    .fetch_all(pool)
    .await?;
    Ok(rows)
}

pub async fn append_log(
    pool: &PgPool,
    job_id: Uuid,
    execution_id: Option<Uuid>,
    worker_id: Option<Uuid>,
    level: &str,
    message: &str,
    meta: serde_json::Value,
) -> AppResult<()> {
    sqlx::query(
        r#"INSERT INTO job_logs (job_id, execution_id, worker_id, level, message, meta)
           VALUES ($1, $2, $3, $4, $5, $6)"#,
    )
    .bind(job_id)
    .bind(execution_id)
    .bind(worker_id)
    .bind(level)
    .bind(message)
    .bind(meta)
    .execute(pool)
    .await?;
    Ok(())
}

// =========================================================
// OUTBOX RELAY
// =========================================================
pub async fn claim_outbox_batch(
    pool: &PgPool,
    relay_owner: &str,
    batch_size: i64,
    lease_secs: i64,
) -> AppResult<Vec<OutboxEvent>> {
    let mut tx = pool.begin().await?;
    let ids: Vec<(Uuid,)> = sqlx::query_as(
        r#"SELECT id FROM outbox_events
           WHERE published_at IS NULL
             AND (relay_locked_until IS NULL OR relay_locked_until < NOW())
           ORDER BY priority DESC, created_at ASC
           LIMIT $1
           FOR UPDATE SKIP LOCKED"#,
    )
    .bind(batch_size)
    .fetch_all(&mut *tx)
    .await?;

    if ids.is_empty() {
        tx.rollback().await.ok();
        return Ok(vec![]);
    }

    let id_list: Vec<Uuid> = ids.into_iter().map(|(id,)| id).collect();
    let expires = Utc::now() + chrono::Duration::seconds(lease_secs);
    let events = sqlx::query_as::<_, OutboxEvent>(
        r#"UPDATE outbox_events SET
             relay_owner_id = $1,
             relay_locked_until = $2
           WHERE id = ANY($3)
           RETURNING *"#,
    )
    .bind(relay_owner)
    .bind(expires)
    .bind(&id_list)
    .fetch_all(&mut *tx)
    .await?;

    tx.commit().await?;
    Ok(events)
}

pub async fn clear_outbox_events(
    pool: &PgPool,
    relay_owner: &str,
    event_ids: &[Uuid],
) -> AppResult<()> {
    if event_ids.is_empty() {
        return Ok(());
    }
    sqlx::query(
        r#"DELETE FROM outbox_events
           WHERE id = ANY($1) AND relay_owner_id = $2"#,
    )
    .bind(event_ids)
    .bind(relay_owner)
    .execute(pool)
    .await?;
    Ok(())
}

pub async fn mark_outbox_published(
    pool: &PgPool,
    relay_owner: &str,
    event_ids: &[Uuid],
) -> AppResult<()> {
    if event_ids.is_empty() {
        return Ok(());
    }
    sqlx::query(
        r#"UPDATE outbox_events SET published_at = NOW()
           WHERE id = ANY($1) AND relay_owner_id = $2"#,
    )
    .bind(event_ids)
    .bind(relay_owner)
    .execute(pool)
    .await?;
    Ok(())
}

// =========================================================
// SCHEDULED JOBS (cron)
// =========================================================
pub async fn create_scheduled_job(
    pool: &PgPool,
    queue_id: Uuid,
    name: &str,
    job_type: &str,
    payload: serde_json::Value,
    priority: i32,
    cron_expr: Option<&str>,
    timezone: &str,
    run_once_at: Option<DateTime<Utc>>,
    next_fire_at: Option<DateTime<Utc>>,
) -> AppResult<ScheduledJob> {
    let sj = sqlx::query_as::<_, ScheduledJob>(
        r#"INSERT INTO scheduled_jobs
             (queue_id, name, job_type, payload, priority, cron_expr, timezone, run_once_at, next_fire_at)
           VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
           RETURNING *"#,
    )
    .bind(queue_id)
    .bind(name)
    .bind(job_type)
    .bind(&payload)
    .bind(priority)
    .bind(cron_expr)
    .bind(timezone)
    .bind(run_once_at)
    .bind(next_fire_at)
    .fetch_one(pool)
    .await?;
    Ok(sj)
}

pub async fn list_scheduled_jobs(pool: &PgPool, queue_id: Option<Uuid>) -> AppResult<Vec<ScheduledJob>> {
    let rows = sqlx::query_as::<_, ScheduledJob>(
        r#"SELECT * FROM scheduled_jobs
           WHERE ($1::uuid IS NULL OR queue_id = $1)
           ORDER BY created_at DESC"#,
    )
    .bind(queue_id)
    .fetch_all(pool)
    .await?;
    Ok(rows)
}

pub async fn list_due_scheduled_jobs(pool: &PgPool) -> AppResult<Vec<ScheduledJob>> {
    let rows = sqlx::query_as::<_, ScheduledJob>(
        r#"SELECT * FROM scheduled_jobs
           WHERE is_active = TRUE
             AND next_fire_at IS NOT NULL
             AND next_fire_at <= NOW()
           ORDER BY next_fire_at ASC
           LIMIT 100"#,
    )
    .fetch_all(pool)
    .await?;
    Ok(rows)
}

/// Create a cron occurrence job with deterministic dedup.
/// Returns Some(job) if this occurrence was newly created,
/// None if it already existed (dedup hit).
pub async fn create_cron_occurrence(
    pool: &PgPool,
    scheduled_job: &ScheduledJob,
    fire_time: DateTime<Utc>,
    org_id: Uuid,
    project_id: Uuid,
    subject: String,
) -> AppResult<Option<Job>> {
    let mut tx = pool.begin().await?;

    // Try to insert the occurrence row (dedup via PK)
    let occurrence_result = sqlx::query(
        r#"INSERT INTO scheduled_occurrences (scheduled_job_id, fire_time)
           VALUES ($1, $2)
           ON CONFLICT DO NOTHING"#,
    )
    .bind(scheduled_job.id)
    .bind(fire_time)
    .execute(&mut *tx)
    .await?;

    if occurrence_result.rows_affected() == 0 {
        // Already created by another scheduler instance
        tx.rollback().await.ok();
        return Ok(None);
    }

    let job = sqlx::query_as::<_, Job>(
        r#"INSERT INTO jobs
             (queue_id, type, status, payload, priority, max_attempts,
              retry_strategy, base_delay_secs, max_delay_secs, scheduled_for, queued_at)
           SELECT
             $1, 'recurring', 'QUEUED', $2, $3,
             COALESCE(q.max_receives, 3),
             'exponential', 5, 3600,
             $4, NOW()
           FROM queues q WHERE q.id = $1
           RETURNING *"#,
    )
    .bind(scheduled_job.queue_id)
    .bind(&scheduled_job.payload)
    .bind(scheduled_job.priority)
    .bind(fire_time)
    .fetch_one(&mut *tx)
    .await?;

    // Link occurrence to created job
    sqlx::query(
        r#"UPDATE scheduled_occurrences SET created_job_id = $3
           WHERE scheduled_job_id = $1 AND fire_time = $2"#,
    )
    .bind(scheduled_job.id)
    .bind(fire_time)
    .bind(job.id)
    .execute(&mut *tx)
    .await?;

    // Create outbox event
    let event_id = Uuid::new_v4();
    sqlx::query(
        r#"INSERT INTO outbox_events
             (id, job_id, queue_id, org_id, project_id, subject, payload, priority, nats_msg_id)
           VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)"#,
    )
    .bind(event_id)
    .bind(job.id)
    .bind(scheduled_job.queue_id)
    .bind(org_id)
    .bind(project_id)
    .bind(&subject)
    .bind(&job.payload)
    .bind(job.priority)
    .bind(event_id.to_string())
    .execute(&mut *tx)
    .await?;

    // Update scheduled job's last/next fire
    sqlx::query(
        r#"UPDATE scheduled_jobs SET last_fired_at = $2 WHERE id = $1"#,
    )
    .bind(scheduled_job.id)
    .bind(fire_time)
    .execute(&mut *tx)
    .await?;

    tx.commit().await?;
    Ok(Some(job))
}

pub async fn update_scheduled_next_fire(
    pool: &PgPool,
    scheduled_job_id: Uuid,
    next_fire_at: Option<DateTime<Utc>>,
) -> AppResult<()> {
    sqlx::query(
        r#"UPDATE scheduled_jobs SET next_fire_at = $2 WHERE id = $1"#,
    )
    .bind(scheduled_job_id)
    .bind(next_fire_at)
    .execute(pool)
    .await?;
    Ok(())
}

pub async fn deactivate_scheduled_job(pool: &PgPool, id: Uuid) -> AppResult<()> {
    sqlx::query("UPDATE scheduled_jobs SET is_active = FALSE WHERE id = $1")
        .bind(id)
        .execute(pool)
        .await?;
    Ok(())
}

// =========================================================
// DLQ
// =========================================================
pub async fn list_dlq_entries(
    pool: &PgPool,
    queue_id: Option<Uuid>,
    limit: i64,
    offset: i64,
) -> AppResult<Vec<DeadLetterEntry>> {
    let rows = sqlx::query_as::<_, DeadLetterEntry>(
        r#"SELECT * FROM dead_letter_entries
           WHERE ($1::uuid IS NULL OR queue_id = $1)
           ORDER BY moved_at DESC
           LIMIT $2 OFFSET $3"#,
    )
    .bind(queue_id)
    .bind(limit)
    .bind(offset)
    .fetch_all(pool)
    .await?;
    Ok(rows)
}

/// Replay a DLQ entry: create a new job from the original payload.
pub async fn replay_dlq_entry(
    pool: &PgPool,
    dlq_id: Uuid,
    org_id: Uuid,
    project_id: Uuid,
    subject: String,
) -> AppResult<Job> {
    let mut tx = pool.begin().await?;
    let dlq: DeadLetterEntry = sqlx::query_as::<_, DeadLetterEntry>(
        "SELECT * FROM dead_letter_entries WHERE id = $1 FOR UPDATE",
    )
    .bind(dlq_id)
    .fetch_one(&mut *tx)
    .await?;

    let job = sqlx::query_as::<_, Job>(
        r#"INSERT INTO jobs
             (queue_id, type, status, payload, priority, max_attempts, queued_at)
           VALUES ($1, 'immediate', 'QUEUED', $2, 5, 3, NOW())
           RETURNING *"#,
    )
    .bind(dlq.queue_id)
    .bind(&dlq.payload)
    .fetch_one(&mut *tx)
    .await?;

    let event_id = Uuid::new_v4();
    sqlx::query(
        r#"INSERT INTO outbox_events
             (id, job_id, queue_id, org_id, project_id, subject, payload, priority, nats_msg_id)
           VALUES ($1, $2, $3, $4, $5, $6, $7, 5, $8)"#,
    )
    .bind(event_id)
    .bind(job.id)
    .bind(dlq.queue_id)
    .bind(org_id)
    .bind(project_id)
    .bind(&subject)
    .bind(&dlq.payload)
    .bind(event_id.to_string())
    .execute(&mut *tx)
    .await?;

    sqlx::query(
        r#"UPDATE dead_letter_entries SET replayed_to_job_id = $2, replayed_at = NOW()
           WHERE id = $1"#,
    )
    .bind(dlq_id)
    .bind(job.id)
    .execute(&mut *tx)
    .await?;

    tx.commit().await?;
    Ok(job)
}

// =========================================================
// BATCHES
// =========================================================
pub async fn create_batch(
    pool: &PgPool,
    project_id: Uuid,
    queue_id: Uuid,
    name: &str,
    total_jobs: i32,
) -> AppResult<Batch> {
    let b = sqlx::query_as::<_, Batch>(
        r#"INSERT INTO batches (project_id, queue_id, name, total_jobs)
           VALUES ($1, $2, $3, $4) RETURNING *"#,
    )
    .bind(project_id)
    .bind(queue_id)
    .bind(name)
    .bind(total_jobs)
    .fetch_one(pool)
    .await?;
    Ok(b)
}

pub async fn get_batch(pool: &PgPool, batch_id: Uuid) -> AppResult<Option<Batch>> {
    let b = sqlx::query_as::<_, Batch>("SELECT * FROM batches WHERE id = $1")
        .bind(batch_id)
        .fetch_optional(pool)
        .await?;
    Ok(b)
}

pub async fn list_batches(pool: &PgPool, project_id: Uuid) -> AppResult<Vec<Batch>> {
    let rows = sqlx::query_as::<_, Batch>(
        "SELECT * FROM batches WHERE project_id = $1 ORDER BY created_at DESC",
    )
    .bind(project_id)
    .fetch_all(pool)
    .await?;
    Ok(rows)
}

// =========================================================
// MANUAL RETRY (from dashboard)
// =========================================================
pub async fn manual_retry_job(
    pool: &PgPool,
    job_id: Uuid,
    org_id: Uuid,
    project_id: Uuid,
    queue_id: Uuid,
    subject: String,
) -> AppResult<Job> {
    let mut tx = pool.begin().await?;
    let job: Job = sqlx::query_as::<_, Job>(
        r#"UPDATE jobs SET
             status = 'QUEUED'::job_status,
             attempt = 0,
             next_retry_at = NULL,
             error_message = NULL,
             error_kind = NULL,
             lease_owner = NULL,
             lease_expires_at = NULL,
             queued_at = NOW()
           WHERE id = $1
             AND status IN ('FAILED'::job_status, 'RETRY_WAIT'::job_status)
           RETURNING *"#,
    )
    .bind(job_id)
    .fetch_one(&mut *tx)
    .await?;

    let event_id = Uuid::new_v4();
    sqlx::query(
        r#"INSERT INTO outbox_events
             (id, job_id, queue_id, org_id, project_id, subject, payload, priority, nats_msg_id)
           VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)"#,
    )
    .bind(event_id)
    .bind(job.id)
    .bind(queue_id)
    .bind(org_id)
    .bind(project_id)
    .bind(&subject)
    .bind(&job.payload)
    .bind(job.priority)
    .bind(event_id.to_string())
    .execute(&mut *tx)
    .await?;

    tx.commit().await?;
    Ok(job)
}

// =========================================================
// ADVISORY LOCK (scheduler leader)
// =========================================================
pub async fn try_advisory_lock(pool: &PgPool, key: i64) -> AppResult<bool> {
    let row: (bool,) = sqlx::query_as("SELECT pg_try_advisory_lock($1)")
        .bind(key)
        .fetch_one(pool)
        .await?;
    Ok(row.0)
}

pub async fn advisory_unlock(pool: &PgPool, key: i64) -> AppResult<()> {
    sqlx::query("SELECT pg_advisory_unlock($1)")
        .bind(key)
        .execute(pool)
        .await?;
    Ok(())
}

// =========================================================
// ORG/PROJECT LOOKUP FOR QUEUE (used by worker + relay)
// =========================================================
pub async fn queue_context(pool: &PgPool, queue_id: Uuid) -> AppResult<Option<(Uuid, Uuid, Uuid)>> {
    let row: Option<(Uuid, Uuid, Uuid)> = sqlx::query_as(
        r#"SELECT q.id, p.org_id, p.id
           FROM queues q
           JOIN projects p ON p.id = q.project_id
           WHERE q.id = $1"#,
    )
    .bind(queue_id)
    .fetch_optional(pool)
    .await?;
    Ok(row)
}
