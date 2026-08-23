use chrono::Utc;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::Notify;
use tokio::time::sleep;
use tracing::{debug, error, info, warn};

use common::Config;
use db::models::ScheduledJob;
use db::queries;

/// The scheduler runner: promotes scheduled jobs, requeues retries,
/// reconciles unknown outcomes, and fires cron occurrences — all behind an
/// advisory lock.
pub struct SchedulerRunner {
    pool: sqlx::PgPool,
    config: Arc<Config>,
    shutdown: tokio_util::sync::CancellationToken,
    /// Woken by the queue_events LISTEN task so job creation/promotion
    /// latency is bounded by the outbox, not by the poll interval.
    wake: Arc<Notify>,
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
            wake: Arc::new(Notify::new()),
        }
    }

    /// LISTEN on queue_events (fired by the jobs NOTIFY trigger) and wake the
    /// leader loop immediately. Falls back to the poll interval on any
    /// listener failure, so this is a latency optimization, not a dependency.
    async fn run_wake_listener(&self) {
        let pool = self.pool.clone();
        let wake = self.wake.clone();
        let shutdown = self.shutdown.clone();
        tokio::spawn(async move {
            while !shutdown.is_cancelled() {
                match sqlx::postgres::PgListener::connect_with(&pool).await {
                    Ok(mut listener) => match listener.listen("queue_events").await {
                        Ok(()) => {
                            info!("scheduler listening on queue_events for instant wakeup");
                            loop {
                                tokio::select! {
                                    _ = shutdown.cancelled() => break,
                                    n = listener.recv() => match n {
                                        Ok(_) => wake.notify_one(),
                                        Err(e) => {
                                            warn!(error = %e, "queue_events recv error; reconnecting");
                                            break;
                                        }
                                    },
                                }
                            }
                        }
                        Err(e) => warn!(error = %e, "queue_events listen failed"),
                    },
                    Err(e) => warn!(error = %e, "wake listener connect failed"),
                }
                tokio::select! {
                    _ = shutdown.cancelled() => break,
                    _ = sleep(Duration::from_secs(5)) => {}
                }
            }
            debug!("scheduler wake listener stopped");
        });
    }

    pub async fn run(self) {
        self.run_wake_listener().await;
        self.leader_loop().await;
    }

    /// Sleep that wakes immediately on shutdown or on a queue_events NOTIFY,
    /// instead of holding the loop hostage for a full poll interval.
    async fn nap(&self, secs: u64) {
        tokio::select! {
            _ = sleep(Duration::from_secs(secs)) => {}
            _ = self.shutdown.cancelled() => {}
            _ = self.wake.notified() => {}
        }
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
                    self.nap(self.config.scheduler_poll_interval_secs).await;
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
                    self.nap(self.config.scheduler_poll_interval_secs).await;
                    continue;
                }
            };

            if !got_lock {
                debug!("not the scheduler leader; sleeping");
                self.nap(self.config.scheduler_poll_interval_secs).await;
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
                self.nap(self.config.scheduler_poll_interval_secs).await;
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
        // UNKNOWN_EXTERNAL_RESULT jobs are resolved by the reconciler once
        // they outlive the configured grace period, per UNKNOWN_RESOLUTION_POLICY.
        let (resolved, _) = queries::reconcile_unknown_jobs(
            &self.pool,
            &self.config.unknown_resolution_policy,
            self.config.unknown_grace_secs,
        )
        .await?;
        if resolved > 0 {
            info!(resolved, policy = %self.config.unknown_resolution_policy, "reconciled unknown-external-result jobs");
        }
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

        self.maintain().await?;

        Ok(())
    }

    /// Retention + archival housekeeping. Cheap when there is nothing to do;
    /// each step is a single bounded statement.
    async fn maintain(&self) -> anyhow::Result<()> {
        let pruned_logs =
            queries::prune_job_logs(&self.pool, self.config.log_retention_secs).await?;
        if pruned_logs > 0 {
            debug!(pruned_logs, "pruned old job logs");
        }
        let pruned_hb =
            queries::prune_worker_heartbeats(&self.pool, 24 * 3600).await?;
        if pruned_hb > 0 {
            debug!(pruned_hb, "pruned old worker heartbeats");
        }
        if self.config.archive_after_days > 0 {
            let archived = queries::archive_terminal_jobs(
                &self.pool,
                self.config.archive_after_days,
                self.config.archive_batch_size,
            )
            .await?;
            if archived > 0 {
                info!(archived, "archived terminal jobs");
            }
        }
        Ok(())
    }

    async fn fire_cron(&self, sj: &ScheduledJob) -> anyhow::Result<()> {
        // Determine the fire time (the occurrence we're creating now)
        let fire_time = sj.next_fire_at.unwrap_or_else(Utc::now);

        // Deactivate schedules whose queue vanished.
        if queries::queue_context(&self.pool, sj.queue_id).await?.is_none() {
            warn!(scheduled_job_id = %sj.id, "queue not found; deactivating schedule");
            queries::deactivate_scheduled_job(&self.pool, sj.id).await?;
            return Ok(());
        }

        // Create the occurrence (dedup via PK on scheduled_occurrences);
        // org/project/shard routing is resolved inside the transaction.
        match queries::create_cron_occurrence(&self.pool, sj, fire_time).await? {
            Some(job) => {
                info!(scheduled_job_id = %sj.id, job_id = %job.id, fire_time = %fire_time, "fired cron occurrence");
            }
            None => {
                debug!(scheduled_job_id = %sj.id, fire_time = %fire_time, "cron occurrence already exists (dedup)");
            }
        }

        // Compute next fire time
        let next = if let Some(expr) = &sj.cron_expr {
            match domain::schedule::parse_cron(expr, &sj.timezone) {
                Ok((schedule, tz)) => domain::schedule::next_occurrence(&schedule, tz, Utc::now()),
                Err(e) => {
                    warn!(scheduled_job_id = %sj.id, error = %e, "bad cron expr; deactivating");
                    queries::deactivate_scheduled_job(&self.pool, sj.id).await?;
                    None
                }
            }
        } else if let Some(run_once) = sj.run_once_at {
            // One-shot: deactivate once the scheduled instant has passed. The
            // old comparison (fire_time >= run_once) was inverted in effect —
            // fire_time comes FROM next_fire_at, so a past-due run_once could
            // be rescheduled instead of retired.
            if run_once <= Utc::now() {
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
