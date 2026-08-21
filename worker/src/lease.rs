use chrono::Utc;
use std::time::Duration;
use tokio::time::interval;
use tracing::{debug, error, warn};

use common::Config;
use db::queries;
use uuid::Uuid;

/// Background task that renews the PostgreSQL lease for a running job.
///
/// Returns `false` via the result if the lease was lost (fenced).
/// The caller must stop work when this returns.
pub async fn lease_renewer(
    pool: sqlx::PgPool,
    job_id: Uuid,
    worker_id: Uuid,
    epoch: i64,
    config: std::sync::Arc<Config>,
    cancel: tokio_util::sync::CancellationToken,
) -> bool {
    let mut ticker = interval(Duration::from_secs(config.heartbeat_interval_secs));
    ticker.tick().await; // skip first immediate tick

    loop {
        tokio::select! {
            _ = cancel.cancelled() => {
                debug!(job_id = %job_id, "lease renewer cancelled");
                return true;
            }
            _ = ticker.tick() => {
                match queries::renew_lease(
                    &pool,
                    job_id,
                    worker_id,
                    epoch,
                    config.lease_duration_secs as i64,
                ).await {
                    Ok(true) => {
                        debug!(job_id = %job_id, epoch, "lease renewed");
                    }
                    Ok(false) => {
                        warn!(job_id = %job_id, epoch, "lease LOST (fenced by another worker)");
                        return false;
                    }
                    Err(e) => {
                        error!(job_id = %job_id, error = %e, "lease renewal error");
                        // Don't immediately give up on transient DB errors
                    }
                }
            }
        }
    }
}
