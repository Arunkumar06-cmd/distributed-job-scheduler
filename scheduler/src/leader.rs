use std::time::Duration;
use tokio::time::sleep;
use tracing::{debug, info, warn};

use db::queries;

/// Advisory lock key for the scheduler leader.
/// Any constant works; we use a fixed value so all instances compete for the same lock.
const SCHEDULER_LOCK_KEY: i64 = 0x73636865; // "sche"

/// Runs the scheduler leader election loop.
/// Only one instance holds the advisory lock at a time.
/// The lock is session-scoped: if the process dies, the lock is released.
pub struct SchedulerLeader {
    pool: sqlx::PgPool,
    poll_interval: Duration,
    shutdown: tokio_util::sync::CancellationToken,
    work: Box<dyn Fn() + Send + Sync>,
}

impl SchedulerLeader {
    pub fn new<F>(pool: sqlx::PgPool, poll_interval_secs: u64, shutdown: tokio_util::sync::CancellationToken, work: F) -> Self
    where
        F: Fn() + Send + Sync + 'static,
    {
        Self {
            pool,
            poll_interval: Duration::from_secs(poll_interval_secs),
            shutdown,
            work: Box::new(work),
        }
    }

    pub async fn run(self) {
        info!("scheduler leader loop started");
        loop {
            if self.shutdown.is_cancelled() {
                info!("scheduler shutting down");
                break;
            }

            let got_lock = match queries::try_advisory_lock(&self.pool, SCHEDULER_LOCK_KEY).await {
                Ok(b) => b,
                Err(e) => {
                    warn!(error = %e, "advisory lock attempt failed");
                    sleep(self.poll_interval).await;
                    continue;
                }
            };

            if !got_lock {
                debug!("not the leader; sleeping");
                sleep(self.poll_interval).await;
                continue;
            }

            info!("acquired scheduler leader lock");

            // Do work while we hold the lock
            while !self.shutdown.is_cancelled() {
                // Check we still hold the lock (session-scoped, so it's ours until disconnect)
                (self.work)();
                sleep(self.poll_interval).await;
            }

            // Release the lock
            let _ = queries::advisory_unlock(&self.pool, SCHEDULER_LOCK_KEY).await;
            info!("released scheduler leader lock");
        }
        info!("scheduler leader loop stopped");
    }
}
