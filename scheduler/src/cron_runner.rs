use chrono::Utc;
use chrono_tz::Tz;
use std::sync::Arc;
use std::time::Duration;
use tokio::time::sleep;
use tracing::{debug, error, info, warn};

use common::Config;
use db::models::ScheduledJob;
use db::queries;

/// The scheduler runner: promotes scheduled jobs, requeues retries,
/// and fires cron occurrences — all behind an advisory lock.
pub struct SchedulerRunner {
    pool: sqlx::PgPool,
    config: Arc<Config>,
    shutdown: tokio_util::sync::CancellationToken,
}

impl SchedulerRunner {
    pub fn new(
        pool: sqlx::PgPool,
        config: Arc<Config>,
        shutdown: tokio_util::sync::CancellationToken,
    ) -> Self {
        Self {
            pool,
            config,
            shutdown,
        }
    }

    pub async fn run(self) {
        self.leader_loop().await;
    }

    async fn leader_loop(&self) {
        info!("scheduler runner started");
        loop {
            if self.shutdown.is_cancelled() {
                info!("scheduler shutting down");
                break;
            }

            // Advisory locks are session-scoped. Keep this acquired connection alive for
            // the entire leadership term; using PgPool directly could switch sessions.
            let mut leader_connection = match self.pool.acquire().await {
                Ok(connection) => connection,
                Err(e) => {
                    error!(error = %e, "failed to acquire scheduler leader connection");
                    sleep(Duration::from_secs(
                        self.config.scheduler_poll_interval_secs,
                    ))
                    .await;
                    continue;
                }
            };
            let got_lock: bool = match sqlx::query_scalar("SELECT pg_try_advisory_lock($1)")
                .bind(0x73636865_i64)
                .fetch_one(&mut *leader_connection)
                .await
            {
                Ok(b) => b,
                Err(e) => {
                    error!(error = %e, "advisory lock attempt failed");
                    sleep(Duration::from_secs(
                        self.config.scheduler_poll_interval_secs,
                    ))
                    .await;
                    continue;
                }
            };

            if !got_lock {
                debug!("not the scheduler leader; sleeping");
                sleep(Duration::from_secs(
                    self.config.scheduler_poll_interval_secs,
                ))
                .await;
                continue;
            }

            let _ = sqlx::query("SET lock_timeout = '5s'")
                .execute(&mut *leader_connection)
                .await;

            info!("acquired scheduler leader lock");
            while !self.shutdown.is_cancelled() {
                if let Err(e) = self.tick().await {
                    error!(error = %e, "scheduler tick failed");
                }
                sleep(Duration::from_secs(
                    self.config.scheduler_poll_interval_secs,
                ))
                .await;
            }

            let _ = sqlx::query("SELECT pg_advisory_unlock($1)")
                .bind(0x73636865_i64)
                .execute(&mut *leader_connection)
                .await;
            info!("released scheduler leader lock");
        }
    }

    async fn tick(&self) -> anyhow::Result<()> {
        // 0. Reclaim stale RUNNING (worker crash, lease expired >30s)
        let reclaimed = queries::reclaim_stale_running(&self.pool).await?;
        if reclaimed > 0 {
            info!(reclaimed, "reclaimed stale running jobs");
        }
        // Backfill outbox for QUEUED without outbox (e.g., after manual reclaim or missed publish)
        let backfilled = queries::backfill_queued_outbox(&self.pool).await?;
        if backfilled > 0 {
            info!(backfilled, "backfilled outbox for queued jobs");
        }
        // UNKNOWN_EXTERNAL_RESULT jobs deliberately remain fenced from automatic
        // state changes until a real, idempotent downstream reconciliation is
        // configured. Guessing an external outcome is unsafe.
        // 1. Promote SCHEDULED -> QUEUED (one-shot future jobs)
        let promoted = queries::promote_scheduled_jobs(&self.pool).await?;
        if promoted > 0 {
            info!(promoted, "promoted scheduled jobs to queued");
        }

        // 2. Requeue RETRY_WAIT -> QUEUED
        let requeued = queries::requeue_ready_retries(&self.pool).await?;
        if requeued > 0 {
            info!(requeued, "requeued retry-wait jobs");
        }

        // 3. Fire cron occurrences
        let due = queries::list_due_scheduled_jobs(&self.pool).await?;
        for sj in due {
            if let Err(e) = self.fire_cron(&sj).await {
                error!(scheduled_job_id = %sj.id, error = %e, "cron fire failed");
            }
        }

        Ok(())
    }

    async fn fire_cron(&self, sj: &ScheduledJob) -> anyhow::Result<()> {
        // Determine the fire time (the occurrence we're creating now)
        let fire_time = sj.next_fire_at.unwrap_or_else(Utc::now);

        // Get org/project context
        let ctx = match queries::queue_context(&self.pool, sj.queue_id).await? {
            Some(c) => c,
            None => {
                warn!(scheduled_job_id = %sj.id, "queue not found; deactivating schedule");
                queries::deactivate_scheduled_job(&self.pool, sj.id).await?;
                return Ok(());
            }
        };
        let (queue_id, org_id, project_id) = ctx;

        let subject = common::ids::nats_subject(&org_id, &project_id, &queue_id, sj.priority);

        // Create the occurrence (dedup via PK on scheduled_occurrences)
        match queries::create_cron_occurrence(
            &self.pool, sj, fire_time, org_id, project_id, subject,
        )
        .await?
        {
            Some(job) => {
                info!(scheduled_job_id = %sj.id, job_id = %job.id, fire_time = %fire_time, "fired cron occurrence");
            }
            None => {
                debug!(scheduled_job_id = %sj.id, fire_time = %fire_time, "cron occurrence already exists (dedup)");
            }
        }

        // Compute next fire time
        let next = if let Some(expr) = &sj.cron_expr {
            let tz: Tz = sj.timezone.parse().unwrap_or(Tz::UTC);
            match domain::schedule::parse_cron(expr, &sj.timezone) {
                Ok(schedule) => domain::schedule::next_occurrence(&schedule, tz, Utc::now()),
                Err(e) => {
                    warn!(scheduled_job_id = %sj.id, error = %e, "bad cron expr; deactivating");
                    queries::deactivate_scheduled_job(&self.pool, sj.id).await?;
                    None
                }
            }
        } else if let Some(run_once) = sj.run_once_at {
            // One-shot: if we've fired it, deactivate
            if fire_time >= run_once {
                queries::deactivate_scheduled_job(&self.pool, sj.id).await?;
                None
            } else {
                Some(run_once)
            }
        } else {
            None
        };

        queries::update_scheduled_next_fire(&self.pool, sj.id, next).await?;
        Ok(())
    }
}
