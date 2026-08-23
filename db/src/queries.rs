use chrono::{DateTime, Utc};
use sqlx::{PgConnection, PgPool};
use uuid::Uuid;

use common::{AppError, AppResult};
use domain::{JobKind, JobStatus, RetryStrategy};

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
           VALUES (LOWER($1), $2, $3)
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
    let user = sqlx::query_as::<_, User>(
        "SELECT * FROM users WHERE LOWER(email) = LOWER($1) AND is_active = TRUE",
    )
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

pub async fn list_organizations_for_user(
    pool: &PgPool,
    user_id: Uuid,
) -> AppResult<Vec<Organization>> {
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
        Some(_) => Err(common::AppError::Forbidden(
            "requires org admin/owner".to_string(),
        )),
        None => Err(common::AppError::Forbidden("not in org".to_string())),
    }
}

/// Members may submit and retry work; administrators control configuration.
/// Viewer is intentionally read-only.
pub async fn require_org_writer(pool: &PgPool, user_id: Uuid, org_id: Uuid) -> AppResult<()> {
    let row: Option<(String,)> = sqlx::query_as(
        r#"SELECT role::text FROM org_memberships WHERE user_id = $1 AND org_id = $2"#,
    )
    .bind(user_id)
    .bind(org_id)
    .fetch_optional(pool)
    .await?;
    match row {
        Some((role,)) if matches!(role.as_str(), "owner" | "admin" | "member") => Ok(()),
        Some(_) => Err(AppError::Forbidden("viewer role is read-only".to_string())),
        None => Err(AppError::Forbidden("not in org".to_string())),
    }
}

pub async fn upsert_org_membership(
    pool: &PgPool,
    org_id: Uuid,
    user_id: Uuid,
    role: &str,
) -> AppResult<()> {
    sqlx::query(
        r#"INSERT INTO org_memberships (org_id, user_id, role)
           VALUES ($1, $2, $3::org_role)
           ON CONFLICT (org_id, user_id) DO UPDATE SET role = EXCLUDED.role"#,
    )
    .bind(org_id)
    .bind(user_id)
    .bind(role)
    .execute(pool)
    .await?;
    Ok(())
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

/// All non-archived projects across a set of orgs in one round trip.
pub async fn list_projects_in_orgs(pool: &PgPool, org_ids: &[Uuid]) -> AppResult<Vec<Project>> {
    if org_ids.is_empty() {
        return Ok(Vec::new());
    }
    let projects = sqlx::query_as::<_, Project>(
        "SELECT * FROM projects WHERE org_id = ANY($1) AND is_archived = FALSE ORDER BY created_at DESC",
    )
    .bind(org_ids)
    .fetch_all(pool)
    .await?;
    Ok(projects)
}

/// Number of workers considered live (heartbeat fresher than the threshold).
pub async fn count_active_workers(pool: &PgPool, online_under_secs: i64) -> AppResult<i64> {
    let row: (i64,) = sqlx::query_as(
        "SELECT COUNT(*) FROM workers
         WHERE is_active AND last_heartbeat_at > NOW() - make_interval(secs => $1)",
    )
    .bind(online_under_secs)
    .fetch_one(pool)
    .await?;
    Ok(row.0)
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
#[allow(clippy::too_many_arguments)]
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
    rate_limit: Option<i32>,
    rate_window_secs: Option<i32>,
    shard_count: i32,
) -> AppResult<Queue> {
    let q = sqlx::query_as::<_, Queue>(
        r#"INSERT INTO queues
             (project_id, name, description, max_concurrency, default_priority,
              ack_wait_secs, max_receives, retry_policy_id, rate_limit, rate_window_secs, shard_count)
           VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, COALESCE($10, 60), $11)
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
    .bind(rate_limit)
    .bind(rate_window_secs)
    .bind(shard_count)
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
    let q =
        sqlx::query_as::<_, Queue>("UPDATE queues SET is_paused = $2 WHERE id = $1 RETURNING *")
            .bind(queue_id)
            .bind(paused)
            .fetch_one(pool)
            .await?;
    Ok(q)
}

#[allow(clippy::too_many_arguments)]
pub async fn update_queue_config(
    pool: &PgPool,
    queue_id: Uuid,
    retry_policy_id: Option<Uuid>,
    max_concurrency: Option<i32>,
    default_priority: Option<i32>,
    ack_wait_secs: Option<i32>,
    max_receives: Option<i32>,
    description: Option<&str>,
) -> AppResult<Queue> {
    let q = sqlx::query_as::<_, Queue>(
        r#"UPDATE queues SET
             retry_policy_id = COALESCE($2, retry_policy_id),
             max_concurrency = COALESCE($3, max_concurrency),
             default_priority = COALESCE($4, default_priority),
             ack_wait_secs = COALESCE($5, ack_wait_secs),
             max_receives = COALESCE($6, max_receives),
             description = COALESCE($7, description)
           WHERE id = $1 RETURNING *"#,
    )
    .bind(queue_id)
    .bind(retry_policy_id)
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

pub async fn batch_queue_stats(pool: &PgPool, queue_ids: &[Uuid]) -> AppResult<Vec<QueueStats>> {
    if queue_ids.is_empty() {
        return Ok(Vec::new());
    }
    let stats = sqlx::query_as::<_, QueueStats>(
        r#"SELECT
              queue_id,
              COALESCE(SUM(CASE WHEN status = 'QUEUED' THEN 1 ELSE 0 END), 0) AS queued,
              COALESCE(SUM(CASE WHEN status = 'RUNNING' THEN 1 ELSE 0 END), 0) AS running,
              COALESCE(SUM(CASE WHEN status = 'RETRY_WAIT' THEN 1 ELSE 0 END), 0) AS retry_wait,
              COALESCE(SUM(CASE WHEN status = 'COMPLETED' THEN 1 ELSE 0 END), 0) AS completed,
              COALESCE(SUM(CASE WHEN status = 'FAILED' THEN 1 ELSE 0 END), 0) AS failed,
              COALESCE(SUM(CASE WHEN status = 'SCHEDULED' THEN 1 ELSE 0 END), 0) AS scheduled,
              COALESCE(SUM(CASE WHEN status = 'CLAIMED' THEN 1 ELSE 0 END), 0) AS claimed,
              0 AS dlq
            FROM jobs WHERE queue_id = ANY($1)
            GROUP BY queue_id"#,
    )
    .bind(queue_ids)
    .fetch_all(pool)
    .await?;
    Ok(stats)
}

pub async fn user_can_access_all_queues(
    pool: &PgPool,
    user_id: Uuid,
    queue_ids: &[Uuid],
) -> AppResult<bool> {
    if queue_ids.is_empty() {
        return Ok(true);
    }
    let visible: (i64,) = sqlx::query_as(
        r#"SELECT COUNT(DISTINCT q.id)
           FROM queues q
           JOIN projects p ON p.id = q.project_id
           JOIN org_memberships m ON m.org_id = p.org_id
           WHERE m.user_id = $1 AND q.id = ANY($2)"#,
    )
    .bind(user_id)
    .bind(queue_ids)
    .fetch_one(pool)
    .await?;
    Ok(visible.0 == queue_ids.len() as i64)
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
    pub shard_id: i32,
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
    enforce_queue_rate_limit(&mut tx, p.queue_id, 1).await?;
    let job = insert_job_with_outbox(&mut tx, &p).await?;
    tx.commit().await?;
    Ok(job)
}

async fn enforce_queue_rate_limit(
    tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    queue_id: Uuid,
    requested_jobs: i64,
) -> AppResult<()> {
    let queue: (Option<i32>, i32) =
        sqlx::query_as("SELECT rate_limit, rate_window_secs FROM queues WHERE id = $1 FOR UPDATE")
            .bind(queue_id)
            .fetch_one(&mut **tx)
            .await?;
    if let Some(limit) = queue.0 {
        let created: (i64,) = sqlx::query_as(
            "SELECT COUNT(*) FROM jobs WHERE queue_id = $1 AND created_at >= NOW() - make_interval(secs => $2)",
        )
        .bind(queue_id)
        .bind(queue.1)
        .fetch_one(&mut **tx)
        .await?;
        if created.0 + requested_jobs > limit as i64 {
            return Err(AppError::RateLimited(format!(
                "queue rate limit of {limit} jobs per {} seconds exceeded",
                queue.1
            )));
        }
    }
    Ok(())
}

async fn insert_job_with_outbox(conn: &mut PgConnection, p: &CreateJobParams) -> AppResult<Job> {
    let status = if p.scheduled_for.is_some() {
        JobStatus::Scheduled
    } else {
        JobStatus::Queued
    };
    let job: Job = sqlx::query_as::<_, Job>(
        r#"INSERT INTO jobs
             (queue_id, batch_id, shard_id, type, status, payload, priority,
              max_attempts, retry_strategy, base_delay_secs, max_delay_secs,
              scheduled_for, idempotency_key, queued_at)
           VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13,
              CASE WHEN $5::job_status = 'QUEUED' THEN NOW() ELSE NULL END)
           RETURNING *"#,
    )
    .bind(p.queue_id)
    .bind(p.batch_id)
    .bind(p.shard_id)
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
    .fetch_one(&mut *conn)
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
        .execute(&mut *conn)
        .await?;
    }
    Ok(job)
}

pub async fn get_job(pool: &PgPool, job_id: Uuid) -> AppResult<Option<Job>> {
    let j = sqlx::query_as::<_, Job>("SELECT * FROM jobs WHERE id = $1")
        .bind(job_id)
        .fetch_optional(pool)
        .await?;
    Ok(j)
}

#[allow(clippy::too_many_arguments)]
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
    let limit = limit.clamp(1, 200);
    let offset = offset.max(0);
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

/// List jobs visible to a user. Cross-queue views must be membership-scoped,
/// otherwise an omitted queue filter would expose every organization's jobs.
#[allow(clippy::too_many_arguments)]
pub async fn list_jobs_for_user(
    pool: &PgPool,
    user_id: Uuid,
    queue_id: Option<Uuid>,
    status: Option<JobStatus>,
    priority_min: Option<i32>,
    batch_id: Option<Uuid>,
    worker_id: Option<Uuid>,
    limit: i64,
    offset: i64,
) -> AppResult<Vec<Job>> {
    let jobs = sqlx::query_as::<_, Job>(
        r#"SELECT j.* FROM jobs j
           JOIN queues q ON q.id = j.queue_id
           JOIN projects p ON p.id = q.project_id
           JOIN org_memberships m ON m.org_id = p.org_id AND m.user_id = $1
           WHERE ($2::uuid IS NULL OR j.queue_id = $2)
             AND ($3::job_status IS NULL OR j.status = $3)
             AND ($4::int IS NULL OR j.priority >= $4)
             AND ($5::uuid IS NULL OR j.batch_id = $5)
             AND ($6::uuid IS NULL OR j.lease_owner = $6)
           ORDER BY j.created_at DESC
           LIMIT $7 OFFSET $8"#,
    )
    .bind(user_id)
    .bind(queue_id)
    .bind(status)
    .bind(priority_min)
    .bind(batch_id)
    .bind(worker_id)
    .bind(limit)
    .bind(offset)
    .fetch_all(pool)
    .await?;
    Ok(jobs)
}

pub async fn count_jobs_for_user(
    pool: &PgPool,
    user_id: Uuid,
    queue_id: Option<Uuid>,
    status: Option<JobStatus>,
    worker_id: Option<Uuid>,
) -> AppResult<i64> {
    let row: (i64,) = sqlx::query_as(
        r#"SELECT COUNT(*) FROM jobs j
           JOIN queues q ON q.id = j.queue_id
           JOIN projects p ON p.id = q.project_id
           JOIN org_memberships m ON m.org_id = p.org_id AND m.user_id = $1
           WHERE ($2::uuid IS NULL OR j.queue_id = $2)
             AND ($3::job_status IS NULL OR j.status = $3)
             AND ($4::uuid IS NULL OR j.lease_owner = $4)"#,
    )
    .bind(user_id)
    .bind(queue_id)
    .bind(status)
    .bind(worker_id)
    .fetch_one(pool)
    .await?;
    Ok(row.0)
}

/// Per-minute completion/failure buckets for the trailing window, zero-filled,
/// plus 24h success rate and average duration for the header metrics.
#[derive(Debug, Clone, serde::Serialize, sqlx::FromRow)]
pub struct ThroughputBucket {
    pub bucket: String,
    pub completed: i64,
    pub failed: i64,
}

pub async fn queue_throughput(
    pool: &PgPool,
    queue_id: Uuid,
    minutes: i32,
) -> AppResult<(Vec<ThroughputBucket>, f64, f64)> {
    let buckets = sqlx::query_as::<_, ThroughputBucket>(
        r#"WITH span AS (
             SELECT generate_series(
                      date_trunc('minute', NOW()) - make_interval(mins => $2),
                      date_trunc('minute', NOW()),
                      '1 minute') AS m
           )
           SELECT to_char(span.m, 'HH24:MI') AS bucket,
                  COALESCE(COUNT(e.id) FILTER (WHERE e.status = 'COMPLETED'), 0)::int8 AS completed,
                  COALESCE(COUNT(e.id) FILTER (WHERE e.status IN ('FAILED','ABANDONED')), 0)::int8 AS failed
           FROM span
           LEFT JOIN job_executions e
             ON date_trunc('minute', e.finished_at) = span.m
            AND e.job_id IN (SELECT id FROM jobs WHERE queue_id = $1)
           GROUP BY span.m ORDER BY span.m"#,
    )
    .bind(queue_id)
    .bind(minutes.clamp(5, 240))
    .fetch_all(pool)
    .await?;

    let day: (i64, i64, f64) = sqlx::query_as(
        r#"SELECT
             COUNT(*) FILTER (WHERE e.status = 'COMPLETED')::int8,
             COUNT(*) FILTER (WHERE e.status IN ('FAILED','ABANDONED'))::int8,
             COALESCE(AVG(e.duration_ms), 0)::float8
           FROM job_executions e
           JOIN jobs j ON j.id = e.job_id
           WHERE j.queue_id = $1 AND e.started_at > NOW() - INTERVAL '24 hours'"#,
    )
    .bind(queue_id)
    .fetch_one(pool)
    .await?;

    let (done_n, bad_n, avg_ms_raw) = day;
    let total = done_n + bad_n;
    let rate = if total > 0 { done_n as f64 * 100.0 / total as f64 } else { 100.0 };
    let avg_ms = avg_ms_raw;
    Ok((buckets, rate, avg_ms))
}

/// Latest terminal/retry events for the live activity ticker.
pub async fn recent_activity(pool: &PgPool, limit: i64) -> AppResult<Vec<serde_json::Value>> {
    let rows = sqlx::query_as::<_, crate::models::Job>(
        r#"SELECT * FROM jobs
           WHERE status IN ('COMPLETED','FAILED','RETRY_WAIT','CANCELLED')
           ORDER BY COALESCE(completed_at, failed_at, updated_at) DESC
           LIMIT $1"#,
    )
    .bind(limit.clamp(1, 20))
    .fetch_all(pool)
    .await?;
    Ok(rows
        .into_iter()
        .map(|j| {
            serde_json::json!({
                "id": j.id,
                "queue_id": j.queue_id,
                "type": j.payload.get("type").and_then(|v| v.as_str()).unwrap_or("unknown"),
                "status": j.status.as_str(),
                "attempt": j.attempt,
                "at": j.updated_at,
            })
        })
        .collect())
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
    sqlx::query(r#"UPDATE workers SET last_heartbeat_at = NOW(), is_active = TRUE WHERE id = $1"#)
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
    sqlx::query(r#"UPDATE workers SET is_active = FALSE, stopped_at = NOW() WHERE id = $1"#)
        .bind(worker_id)
        .execute(pool)
        .await?;
    Ok(())
}

/// Worker liveness classification with thresholds derived from the configured
/// heartbeat interval (stale after ~3 missed beats, offline after ~12).
pub async fn list_workers(
    pool: &PgPool,
    online_under_secs: i64,
    stale_under_secs: i64,
) -> AppResult<Vec<WorkerView>> {
    let rows = sqlx::query_as::<_, WorkerView>(
        r#"SELECT
             w.*,
             CASE
               WHEN w.last_heartbeat_at IS NULL THEN 'OFFLINE'::worker_status
               WHEN EXTRACT(EPOCH FROM (NOW() - w.last_heartbeat_at)) < $1 THEN 'ONLINE'::worker_status
               WHEN EXTRACT(EPOCH FROM (NOW() - w.last_heartbeat_at)) < $2 THEN 'STALE'::worker_status
               ELSE 'OFFLINE'::worker_status
             END AS status,
             (SELECT COUNT(*) FROM jobs WHERE lease_owner = w.id AND status = 'RUNNING') AS running_jobs
           FROM workers w
           ORDER BY w.started_at DESC"#,
    )
    .bind(online_under_secs)
    .bind(stale_under_secs)
    .fetch_all(pool)
    .await?;
    Ok(rows)
}

pub async fn get_worker(
    pool: &PgPool,
    worker_id: Uuid,
    online_under_secs: i64,
    stale_under_secs: i64,
) -> AppResult<Option<WorkerView>> {
    let row = sqlx::query_as::<_, WorkerView>(
        r#"SELECT
             w.*,
             CASE
               WHEN w.last_heartbeat_at IS NULL THEN 'OFFLINE'::worker_status
               WHEN EXTRACT(EPOCH FROM (NOW() - w.last_heartbeat_at)) < $2 THEN 'ONLINE'::worker_status
               WHEN EXTRACT(EPOCH FROM (NOW() - w.last_heartbeat_at)) < $3 THEN 'STALE'::worker_status
               ELSE 'OFFLINE'::worker_status
             END AS status,
             (SELECT COUNT(*) FROM jobs WHERE lease_owner = w.id AND status = 'RUNNING') AS running_jobs
           FROM workers w
           WHERE w.id = $1"#,
    )
    .bind(worker_id)
    .bind(online_under_secs)
    .bind(stale_under_secs)
    .fetch_optional(pool)
    .await?;
    Ok(row)
}

/// Trim heartbeat history; called periodically from the worker loop so the
/// table stays proportional to live workers instead of growing forever.
pub async fn prune_worker_heartbeats(pool: &PgPool, retention_secs: i64) -> AppResult<i64> {
    let row: (i64,) = sqlx::query_as("SELECT prune_worker_heartbeats(make_interval(secs => $1))")
        .bind(retention_secs)
        .fetch_one(pool)
        .await?;
    Ok(row.0)
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
/// Postgres error codes worth retrying under concurrency:
/// 40P01 deadlock_detected, 40001 serialization_failure, 55P03 lock_not_available.
fn retriable_pg_code(e: &AppError) -> Option<String> {
    if let AppError::Sqlx(sqlx::Error::Database(db)) = e {
        let code = db.code().unwrap_or_default().to_string();
        if matches!(code.as_str(), "40P01" | "40001" | "55P03") {
            return Some(code);
        }
    }
    None
}

pub async fn claim_job(
    pool: &PgPool,
    job_id: Uuid,
    worker_id: Uuid,
    nats_msg_id: &str,
    lease_duration_secs: i64,
) -> AppResult<ClaimedJob> {
    let mut attempt = 0;
    loop {
        match claim_job_inner(pool, job_id, worker_id, nats_msg_id, lease_duration_secs).await {
            Ok(v) => return Ok(v),
            Err(e) => {
                if retriable_pg_code(&e).is_some() && attempt < 3 {
                    attempt += 1;
                    let backoff = 10 * (1 << attempt) + (rand::random::<u64>() % 10);
                    tokio::time::sleep(std::time::Duration::from_millis(backoff)).await;
                    continue;
                }
                return Err(e);
            }
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
    let _ = sqlx::query("SET lock_timeout = '5s'")
        .execute(&mut *tx)
        .await;

    // 1. Lock queue row to check pause (spec §13: QUEUE -> TOKEN -> JOB).
    // Admission rate limiting is enforced when the job is created, not here.
    let queue_row: Option<(Uuid, bool)> = sqlx::query_as(
        r#"SELECT q.id, q.is_paused
           FROM jobs j
           JOIN queues q ON q.id = j.queue_id
           WHERE j.id = $1
           FOR UPDATE OF q"#,
    )
    .bind(job_id)
    .fetch_optional(&mut *tx)
    .await?;

    let (queue_id, is_paused) = {
        match queue_row {
            Some(row) => row,
            None => {
                tx.rollback().await.ok();
                return Err(common::AppError::NotFound(format!("job {job_id} not found")));
            }
        }
    };

    if is_paused {
        tx.rollback().await.ok();
        return Err(common::AppError::QueuePaused);
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

    let (token_id,) = token.ok_or_else(|| common::AppError::QueueAtCapacity)?;

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

    let job = match claimed {
        Some(job) => job,
        None => {
            // Explicit rollback (instead of drop-spawned) so the capacity token
            // assignment above is released synchronously before we return.
            tx.rollback().await.ok();
            return Err(common::AppError::Conflict(format!(
                "job {job_id} not claimable (already claimed or not queued)"
            )));
        }
    };

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
        r#"WITH renewed_job AS (
             UPDATE jobs SET lease_expires_at = $4
             WHERE id = $1
               AND lease_owner = $2
               AND lease_epoch = $3
               AND status = 'RUNNING'::job_status
               AND token_id IS NOT NULL
               AND EXISTS (
                   SELECT 1 FROM capacity_tokens ct
                   WHERE ct.id = jobs.token_id AND ct.job_id = $1 AND ct.worker_id = $2
               )
             RETURNING token_id
           )
           UPDATE capacity_tokens ct SET lease_until = $4
           FROM renewed_job r
           WHERE ct.id = r.token_id AND ct.job_id = $1 AND ct.worker_id = $2"#,
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

    // DAG resolver: satisfy edges where this job is the parent, then queue any
    // child whose parents are now fully satisfied. Scoped to THIS job's
    // children so completion cost doesn't grow with the global edge count.
    sqlx::query(
        r#"INSERT INTO edge_satisfaction (parent_id, child_id)
           SELECT $1, child_id FROM workflow_edges WHERE parent_id = $1
           ON CONFLICT DO NOTHING"#,
    )
    .bind(job_id)
    .execute(&mut *tx)
    .await?;

    let ready_children: Vec<Job> = sqlx::query_as::<_, Job>(
        r#"UPDATE jobs SET status = 'QUEUED'::job_status, queued_at = NOW(), updated_at = NOW()
           WHERE id IN (
               SELECT we.child_id
               FROM workflow_edges we
               WHERE we.parent_id = $1
                 AND NOT EXISTS (
                     SELECT 1 FROM workflow_edges we2
                     WHERE we2.child_id = we.child_id
                       AND NOT EXISTS (
                           SELECT 1 FROM edge_satisfaction es
                           WHERE es.parent_id = we2.parent_id AND es.child_id = we.child_id
                       )
                 )
           ) AND status = 'WAITING'::job_status
           RETURNING *"#,
    )
    .bind(job_id)
    .fetch_all(&mut *tx)
    .await?;

    for j in ready_children {
        // Create outbox for newly ready child
        let ctx: Option<(Uuid, Uuid)> = sqlx::query_as(
            r#"SELECT p.org_id, p.id FROM queues q JOIN projects p ON p.id = q.project_id WHERE q.id = $1"#,
        )
        .bind(j.queue_id)
        .fetch_optional(&mut *tx)
        .await?;
        if let Some((org_id, proj_id)) = ctx {
            let subject = common::ids::nats_shard_subject(
                &org_id,
                &proj_id,
                &j.queue_id,
                j.shard_id,
                j.priority,
            );
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
#[allow(clippy::too_many_arguments)]
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
        Ok(FailOutcome::Retry {
            next_retry_at,
            delay_secs: delay,
        })
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
    Retry {
        next_retry_at: DateTime<Utc>,
        delay_secs: i64,
    },
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
             RETURNING id, queue_id, shard_id, payload, priority
           )
           INSERT INTO outbox_events
             (id, job_id, queue_id, org_id, project_id, subject, payload, priority, nats_msg_id)
           SELECT
             gen_random_uuid(),
             m.id,
             m.queue_id,
             p.org_id,
             p.id,
             'org.'||p.org_id||'.proj.'||p.id||'.queue.'||m.queue_id
               || CASE WHEN q.shard_count > 1 THEN '.shard.'||m.shard_id ELSE '' END
               || CASE WHEN m.priority >= 10 THEN '.high' WHEN m.priority > 0 THEN '.standard' ELSE '.low' END,
             m.payload,
             m.priority,
             gen_random_uuid()::text
           FROM moved m
           JOIN queues q ON q.id = m.queue_id
           JOIN projects p ON p.id = q.project_id"#,
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
             RETURNING id, queue_id, shard_id, payload, priority
           )
           INSERT INTO outbox_events
             (id, job_id, queue_id, org_id, project_id, subject, payload, priority, nats_msg_id)
           SELECT
             gen_random_uuid(),
             m.id,
             m.queue_id,
             p.org_id,
             p.id,
             'org.'||p.org_id||'.proj.'||p.id||'.queue.'||m.queue_id
               || CASE WHEN q.shard_count > 1 THEN '.shard.'||m.shard_id ELSE '' END
               || CASE WHEN m.priority >= 10 THEN '.high' WHEN m.priority > 0 THEN '.standard' ELSE '.low' END,
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
             RETURNING id, queue_id, shard_id, payload, priority
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
                  'org.'||p.org_id||'.proj.'||p.id||'.queue.'||s.queue_id
                    || CASE WHEN q.shard_count > 1 THEN '.shard.'||s.shard_id ELSE '' END
                    || CASE WHEN s.priority >= 10 THEN '.high' WHEN s.priority > 0 THEN '.standard' ELSE '.low' END,
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
                  'org.'||p.org_id||'.proj.'||p.id||'.queue.'||j.queue_id
                    || CASE WHEN q.shard_count > 1 THEN '.shard.'||j.shard_id ELSE '' END
                    || CASE WHEN j.priority >= 10 THEN '.high' WHEN j.priority > 0 THEN '.standard' ELSE '.low' END,
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

/// Resolve UNKNOWN_EXTERNAL_RESULT jobs once they outlive the reconciliation
/// grace period.
///
/// There is no generic way to learn an external system's outcome, so the safe
/// default is `dlq`: park the job for human inspection rather than guess.
/// `retry` redrives the job (only safe when handlers are idempotent);
/// `complete` is provided for read-only side effects and must be chosen
/// deliberately. Returns (resolved, dlq_ed).
pub async fn reconcile_unknown_jobs(
    pool: &PgPool,
    policy: &str,
    grace_secs: i64,
) -> AppResult<(i64, i64)> {
    let new_status = match policy {
        "complete" => "COMPLETED",
        // Redrive: only safe when handlers are idempotent (documented).
        "retry" => "QUEUED",
        "failed" | "dlq" => "FAILED",
        other => {
            return Err(AppError::Validation(format!(
                "unknown resolution policy {other:?} (expected dlq|retry|complete)"
            )))
        }
    };

    let mut tx = pool.begin().await?;

    let resolved: Vec<Job> = sqlx::query_as::<_, Job>(
        r#"UPDATE jobs SET
             status = $2::job_status,
             completed_at = CASE WHEN $2 = 'COMPLETED' THEN NOW() ELSE NULL END,
             failed_at = CASE WHEN $2 = 'FAILED' THEN NOW() ELSE NULL END,
             queued_at = CASE WHEN $2 = 'QUEUED' THEN NOW() ELSE queued_at END,
             next_retry_at = NULL,
             error_message = COALESCE(error_message, 'external result unresolved'),
             error_kind = COALESCE(error_kind, 'unresolved_external_result'),
             lease_owner = NULL,
             lease_expires_at = NULL,
             token_id = NULL,
             updated_at = NOW()
           WHERE status = 'UNKNOWN_EXTERNAL_RESULT'::job_status
             AND updated_at < NOW() - make_interval(secs => $1)
           RETURNING *"#,
    )
    .bind(grace_secs)
    .bind(new_status)
    .fetch_all(&mut *tx)
    .await?;

    if resolved.is_empty() {
        tx.commit().await.ok();
        return Ok((0, 0));
    }

    // Free any capacity still pinned by these jobs (defensive; handle_unknown
    // normally releases at mark time).
    sqlx::query(
        r#"UPDATE capacity_tokens SET worker_id = NULL, job_id = NULL, lease_until = NULL
           WHERE job_id = ANY($1)"#,
    )
    .bind(resolved.iter().map(|j| j.id).collect::<Vec<_>>())
    .execute(&mut *tx)
    .await?;

    if policy == "dlq" || policy == "failed" {
        for j in &resolved {
            sqlx::query(
                r#"INSERT INTO dead_letter_entries
                     (job_id, queue_id, org_id, project_id, reason, attempt, payload, final_error, error_kind)
                   SELECT $1, $2, p.org_id, p.id, 'permanent_failure', $3, $4,
                          'external result unresolved beyond grace period', 'unresolved_external_result'
                   FROM queues q JOIN projects p ON p.id = q.project_id WHERE q.id = $2"#,
            )
            .bind(j.id)
            .bind(j.queue_id)
            .bind(j.attempt)
            .bind(&j.payload)
            .execute(&mut *tx)
            .await?;
        }
    } else if policy == "retry" {
        // Redriven jobs need fresh outbox events to reach consumers.
        sqlx::query(
            r#"INSERT INTO outbox_events
                 (id, job_id, queue_id, org_id, project_id, subject, payload, priority, nats_msg_id)
               SELECT gen_random_uuid(), j.id, j.queue_id, p.org_id, p.id,
                      'org.'||p.org_id||'.proj.'||p.id||'.queue.'||j.queue_id
                        || CASE WHEN q.shard_count > 1 THEN '.shard.'||j.shard_id ELSE '' END
                        || CASE WHEN j.priority >= 10 THEN '.high' WHEN j.priority > 0 THEN '.standard' ELSE '.low' END,
                      j.payload, j.priority, gen_random_uuid()::text
               FROM jobs j
               JOIN queues q ON q.id = j.queue_id
               JOIN projects p ON p.id = q.project_id
               WHERE j.id = ANY($1)"#,
        )
        .bind(resolved.iter().map(|j| j.id).collect::<Vec<_>>())
        .execute(&mut *tx)
        .await?;
    }

    tx.commit().await?;
    Ok((resolved.len() as i64, resolved.len() as i64))
}

/// Single-round-trip authorization context for anything addressed by queue:
/// the queue row, its project's org, and the caller's org role. Replaces the
/// get_queue → get_project → membership-check chain every handler repeated.
#[derive(Debug, Clone)]
pub struct QueueAuthz {
    pub queue: Queue,
    pub org_id: Uuid,
    pub project_id: Uuid,
    /// 'owner' | 'admin' | 'member' | 'viewer'
    pub role: String,
}

impl QueueAuthz {
    pub fn can_write(&self) -> bool {
        matches!(self.role.as_str(), "owner" | "admin" | "member")
    }
    pub fn can_admin(&self) -> bool {
        matches!(self.role.as_str(), "owner" | "admin")
    }
    /// Enforce writer-level access (member and above).
    pub fn require_writer(&self) -> AppResult<()> {
        if self.can_write() {
            Ok(())
        } else {
            Err(AppError::Forbidden("viewer role is read-only".to_string()))
        }
    }
    /// Enforce admin-level access (configuration changes).
    pub fn require_admin(&self) -> AppResult<()> {
        if self.can_admin() {
            Ok(())
        } else {
            Err(AppError::Forbidden("requires org admin/owner".to_string()))
        }
    }
}

pub async fn authorize_queue(
    pool: &PgPool,
    user_id: Uuid,
    queue_id: Uuid,
) -> AppResult<Option<QueueAuthz>> {
    let row: Option<QueueAuthzRow> = sqlx::query_as(
        r#"SELECT q.*, p.org_id, m.role::text
           FROM queues q
           JOIN projects p ON p.id = q.project_id
           LEFT JOIN org_memberships m ON m.org_id = p.org_id AND m.user_id = $2
           WHERE q.id = $1"#,
    )
    .bind(queue_id)
    .bind(user_id)
    .fetch_optional(pool)
    .await?;
    Ok(row.and_then(QueueAuthzRow::into))
}

#[derive(sqlx::FromRow)]
struct QueueAuthzRow {
    #[sqlx(flatten)]
    queue: crate::models::Queue,
    org_id: Uuid,
    // NULL via the LEFT JOIN means the caller has no membership at all.
    role: Option<String>,
}

impl From<QueueAuthzRow> for Option<QueueAuthz> {
    fn from(r: QueueAuthzRow) -> Self {
        match r.role {
            None => None,
            Some(role) => {
                let project_id = r.queue.project_id;
                Some(QueueAuthz {
                    queue: r.queue,
                    org_id: r.org_id,
                    project_id,
                    role,
                })
            }
        }
    }
}

/// Append a privileged-mutation audit record. Best-effort by design: audit
/// failures are logged but never block the user-facing operation.
pub async fn append_audit(
    pool: &PgPool,
    actor_id: Uuid,
    org_id: Option<Uuid>,
    action: &str,
    target: &str,
    details: serde_json::Value,
) -> AppResult<()> {
    let res = sqlx::query(
        r#"INSERT INTO audit_log (actor_id, org_id, action, target, details)
           VALUES ($1, $2, $3, $4, $5)"#,
    )
    .bind(actor_id)
    .bind(org_id)
    .bind(action)
    .bind(target)
    .bind(details)
    .execute(pool)
    .await;
    if let Err(e) = res {
        tracing::warn!(actor = %actor_id, action, error = %e, "audit write failed");
    }
    Ok(())
}

/// Effective retry defaults for a queue: its attached retry policy template
/// when one exists, else the system defaults. Job creation layers request
/// values over this so the retry_policies table is live configuration rather
/// than decoration.
pub async fn resolve_retry_defaults(
    pool: &PgPool,
    queue_id: Uuid,
) -> AppResult<(i32, RetryStrategy, i64, i64)> {
    let row: (i32, RetryStrategy, i64, i64) = sqlx::query_as(
        r#"SELECT
             COALESCE(rp.max_attempts, 3),
             COALESCE(rp.strategy, 'exponential'::retry_strategy),
             COALESCE(rp.base_delay_secs, 5),
             COALESCE(rp.max_delay_secs, 3600)
           FROM queues q
           LEFT JOIN retry_policies rp ON rp.id = q.retry_policy_id
           WHERE q.id = $1"#,
    )
    .bind(queue_id)
    .fetch_one(pool)
    .await?;
    Ok(row)
}

/// Move up to `batch` terminal jobs older than `older_than_secs` into the
/// archive twins. Returns the number of jobs archived.
pub async fn archive_terminal_jobs(pool: &PgPool, older_than_days: i32, batch: i64) -> AppResult<i64> {
    let moved: i32 = sqlx::query_scalar(
        "SELECT archive_terminal_jobs(make_interval(days => $1), $2::int)",
    )
    .bind(older_than_days)
    .bind(batch)
    .fetch_one(pool)
    .await?;
    Ok(moved as i64)
}

/// Trim job logs; keeps the hot table proportional to recent activity.
pub async fn prune_job_logs(pool: &PgPool, retention_secs: i64) -> AppResult<i64> {
    let n: i64 = sqlx::query_scalar("SELECT prune_job_logs(make_interval(secs => $1))")
        .bind(retention_secs)
        .fetch_one(pool)
        .await?;
    Ok(n)
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

/// Release failed events with exponential backoff (30s doubling, capped at
/// 10 minutes) so poison pills stop hammering NATS while staying eligible for
/// redelivery. Only events still owned by this relay are touched.
pub async fn fail_outbox_events(
    pool: &PgPool,
    relay_owner: &str,
    event_ids: &[Uuid],
    base_backoff_secs: i64,
) -> AppResult<()> {
    if event_ids.is_empty() {
        return Ok(());
    }
    sqlx::query(
        r#"UPDATE outbox_events SET
             publish_attempts = publish_attempts + 1,
             relay_owner_id = NULL,
             relay_locked_until = NOW()
               + LEAST(
                   make_interval(secs => ($2 * pow(2, LEAST(publish_attempts, 5)))::double precision),
                   make_interval(secs => $3)
                 )
           WHERE id = ANY($1) AND relay_owner_id = $4"#,
    )
    .bind(event_ids)
    .bind(base_backoff_secs)
    .bind(600_i64)
    .bind(relay_owner)
    .execute(pool)
    .await?;
    Ok(())
}

// =========================================================
// SCHEDULED JOBS (cron)
// =========================================================
#[allow(clippy::too_many_arguments)]
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

pub async fn list_scheduled_jobs(
    pool: &PgPool,
    queue_id: Option<Uuid>,
) -> AppResult<Vec<ScheduledJob>> {
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

pub async fn list_scheduled_jobs_for_user(
    pool: &PgPool,
    user_id: Uuid,
    queue_id: Option<Uuid>,
) -> AppResult<Vec<ScheduledJob>> {
    let rows = sqlx::query_as::<_, ScheduledJob>(
        r#"SELECT sj.* FROM scheduled_jobs sj
           JOIN queues q ON q.id = sj.queue_id
           JOIN projects p ON p.id = q.project_id
           JOIN org_memberships m ON m.org_id = p.org_id AND m.user_id = $1
           WHERE ($2::uuid IS NULL OR sj.queue_id = $2)
           ORDER BY sj.created_at DESC"#,
    )
    .bind(user_id)
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

    let ctx: Option<(Uuid, Uuid, i32)> = sqlx::query_as(
        r#"SELECT p.org_id, p.id, q.shard_count
           FROM queues q JOIN projects p ON p.id = q.project_id
           WHERE q.id = $1"#,
    )
    .bind(scheduled_job.queue_id)
    .fetch_optional(&mut *tx)
    .await?;
    let Some((org_id, project_id, shard_count)) = ctx else {
        tx.rollback().await.ok();
        return Ok(None);
    };

    // max_attempts is a job-lifecycle setting; the old code borrowed the
    // queue's NATS max_receives here, which is an unrelated redelivery knob.
    let job = sqlx::query_as::<_, Job>(
        r#"INSERT INTO jobs
             (queue_id, type, status, payload, priority, max_attempts,
              retry_strategy, base_delay_secs, max_delay_secs, scheduled_for, queued_at)
           SELECT
             $1, 'recurring', 'QUEUED', $2, $3, 3,
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

    // Assign a stable shard derived from the job id so sharded consumers can
    // reach the occurrence.
    let shard_id = if shard_count > 1 {
        let sh = common::ids::shard_for_key(&job.id.to_string(), shard_count);
        sqlx::query("UPDATE jobs SET shard_id = $2 WHERE id = $1")
            .bind(job.id)
            .bind(sh)
            .execute(&mut *tx)
            .await?;
        sh
    } else {
        0
    };
    let subject = common::ids::nats_subject_for_shard(
        &org_id,
        &project_id,
        &scheduled_job.queue_id,
        shard_count,
        shard_id,
        job.priority,
    );

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
    sqlx::query(r#"UPDATE scheduled_jobs SET last_fired_at = $2 WHERE id = $1"#)
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
    sqlx::query(r#"UPDATE scheduled_jobs SET next_fire_at = $2 WHERE id = $1"#)
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
    let limit = limit.clamp(1, 200);
    let offset = offset.max(0);
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

pub async fn list_dlq_entries_for_user(
    pool: &PgPool,
    user_id: Uuid,
    queue_id: Option<Uuid>,
    limit: i64,
    offset: i64,
) -> AppResult<Vec<DeadLetterEntry>> {
    let limit = limit.clamp(1, 200);
    let offset = offset.max(0);
    let rows = sqlx::query_as::<_, DeadLetterEntry>(
        r#"SELECT d.* FROM dead_letter_entries d
           JOIN queues q ON q.id = d.queue_id
           JOIN projects p ON p.id = q.project_id
           JOIN org_memberships m ON m.org_id = p.org_id AND m.user_id = $1
           WHERE ($2::uuid IS NULL OR d.queue_id = $2)
           ORDER BY d.moved_at DESC
           LIMIT $3 OFFSET $4"#,
    )
    .bind(user_id)
    .bind(queue_id)
    .bind(limit)
    .bind(offset)
    .fetch_all(pool)
    .await?;
    Ok(rows)
}

pub async fn count_dlq_entries_for_user(
    pool: &PgPool,
    user_id: Uuid,
    queue_id: Option<Uuid>,
) -> AppResult<i64> {
    let row: (i64,) = sqlx::query_as(
        r#"SELECT COUNT(*) FROM dead_letter_entries d
           JOIN queues q ON q.id = d.queue_id
           JOIN projects p ON p.id = q.project_id
           JOIN org_memberships m ON m.org_id = p.org_id AND m.user_id = $1
           WHERE ($2::uuid IS NULL OR d.queue_id = $2)"#,
    )
    .bind(user_id)
    .bind(queue_id)
    .fetch_one(pool)
    .await?;
    Ok(row.0)
}

/// Replay a DLQ entry: create a new job inheriting the original job's
/// priority, retry policy, and shard placement. The subject is derived inside
/// the transaction so replays land on exactly the same NATS routing as fresh jobs.
pub async fn replay_dlq_entry(pool: &PgPool, dlq_id: Uuid) -> AppResult<Job> {
    let mut tx = pool.begin().await?;
    let dlq: DeadLetterEntry = sqlx::query_as::<_, DeadLetterEntry>(
        "SELECT * FROM dead_letter_entries WHERE id = $1 FOR UPDATE",
    )
    .bind(dlq_id)
    .fetch_one(&mut *tx)
    .await?;

    if dlq.replayed_to_job_id.is_some() {
        tx.rollback().await.ok();
        return Err(AppError::Conflict("DLQ entry already replayed".to_string()));
    }

    let orig: Option<(i32, i32, RetryStrategy, i64, i64, i32)> = sqlx::query_as(
        r#"SELECT j.priority, j.max_attempts, j.retry_strategy,
                  j.base_delay_secs, j.max_delay_secs, j.shard_id
           FROM jobs j WHERE j.id = $1"#,
    )
    .bind(dlq.job_id)
    .fetch_optional(&mut *tx)
    .await?;

    let (priority, max_attempts, strategy, base_delay, max_delay, shard_id) =
        orig.unwrap_or((5, 3, RetryStrategy::Exponential, 5, 3600, 0));

    let job = sqlx::query_as::<_, Job>(
        r#"INSERT INTO jobs
             (queue_id, type, status, payload, priority, shard_id,
              max_attempts, retry_strategy, base_delay_secs, max_delay_secs, queued_at)
           VALUES ($1, 'immediate', 'QUEUED', $2, $3, $4, $5, $6, $7, $8, NOW())
           RETURNING *"#,
    )
    .bind(dlq.queue_id)
    .bind(&dlq.payload)
    .bind(priority)
    .bind(shard_id)
    .bind(max_attempts)
    .bind(strategy)
    .bind(base_delay)
    .bind(max_delay)
    .fetch_one(&mut *tx)
    .await?;

    let ctx: Option<(Uuid, Uuid, i32)> = sqlx::query_as(
        r#"SELECT p.org_id, p.id, q.shard_count
           FROM queues q JOIN projects p ON p.id = q.project_id WHERE q.id = $1"#,
    )
    .bind(dlq.queue_id)
    .fetch_optional(&mut *tx)
    .await?;
    let Some((org_id, project_id, shard_count)) = ctx else {
        tx.rollback().await.ok();
        return Err(AppError::NotFound("queue not found".to_string()));
    };
    let subject = common::ids::nats_subject_for_shard(
        &org_id,
        &project_id,
        &dlq.queue_id,
        shard_count,
        shard_id,
        priority,
    );

    let event_id = Uuid::new_v4();
    sqlx::query(
        r#"INSERT INTO outbox_events
             (id, job_id, queue_id, org_id, project_id, subject, payload, priority, nats_msg_id)
           VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)"#,
    )
    .bind(event_id)
    .bind(job.id)
    .bind(dlq.queue_id)
    .bind(org_id)
    .bind(project_id)
    .bind(&subject)
    .bind(&dlq.payload)
    .bind(priority)
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

/// Create the batch record, all child jobs, and their outbox events atomically.
/// A client never observes a partially-created batch after a validation or DB error.
pub async fn create_batch_with_jobs(
    pool: &PgPool,
    project_id: Uuid,
    queue_id: Uuid,
    name: &str,
    jobs: Vec<CreateJobParams>,
) -> AppResult<(Batch, Vec<Job>)> {
    let mut tx = pool.begin().await?;
    enforce_queue_rate_limit(&mut tx, queue_id, jobs.len() as i64).await?;
    let batch = sqlx::query_as::<_, Batch>(
        r#"INSERT INTO batches (project_id, queue_id, name, total_jobs)
           VALUES ($1, $2, $3, $4) RETURNING *"#,
    )
    .bind(project_id)
    .bind(queue_id)
    .bind(name)
    .bind(jobs.len() as i32)
    .fetch_one(&mut *tx)
    .await?;

    let mut created = Vec::with_capacity(jobs.len());
    for mut params in jobs {
        params.batch_id = Some(batch.id);
        created.push(insert_job_with_outbox(&mut tx, &params).await?);
    }
    tx.commit().await?;
    Ok((batch, created))
}

pub async fn get_batch(pool: &PgPool, batch_id: Uuid) -> AppResult<Option<Batch>> {
    let b = sqlx::query_as::<_, Batch>("SELECT * FROM batches WHERE id = $1")
        .bind(batch_id)
        .fetch_optional(pool)
        .await?;
    Ok(b)
}

pub async fn list_batches(
    pool: &PgPool,
    project_id: Uuid,
    limit: i64,
    offset: i64,
) -> AppResult<Vec<Batch>> {
    let limit = limit.clamp(1, 200);
    let offset = offset.max(0);
    let rows = sqlx::query_as::<_, Batch>(
        "SELECT * FROM batches WHERE project_id = $1 ORDER BY created_at DESC LIMIT $2 OFFSET $3",
    )
    .bind(project_id)
    .bind(limit)
    .bind(offset)
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
    let job: Option<Job> = sqlx::query_as::<_, Job>(
        r#"UPDATE jobs SET
             status = 'QUEUED'::job_status,
             attempt = 0,
             next_retry_at = NULL,
             error_message = NULL,
             error_kind = NULL,
             failed_at = NULL,
             result = NULL,
             lease_owner = NULL,
             lease_expires_at = NULL,
             queued_at = NOW()
           WHERE id = $1
             AND status IN ('FAILED'::job_status, 'RETRY_WAIT'::job_status)
           RETURNING *"#,
    )
    .bind(job_id)
    .fetch_optional(&mut *tx)
    .await?;

    let job = job.ok_or_else(|| {
        AppError::Conflict("job is not in FAILED or RETRY_WAIT status".to_string())
    })?;

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
