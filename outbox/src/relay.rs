use std::time::Duration;
use tokio::time::sleep;
use tracing::{debug, error, info, warn};

use common::Config;
use db::queries;

use crate::publisher::Publisher;

/// The transactional outbox relay.
///
/// Runs a loop:
///   1. Claim a batch of unpublished outbox events (FOR UPDATE SKIP LOCKED)
///   2. Publish each to NATS JetStream (outside the DB transaction)
///   3. On success: delete the outbox rows
///   4. On failure: leave them (lease will expire, another relay reclaims)
///
/// This NEVER holds a DB transaction open during network I/O.
pub struct OutboxRelay {
    pool: sqlx::PgPool,
    publisher: Publisher,
    relay_id: String,
    batch_size: i64,
    poll_interval: Duration,
    lease_secs: i64,
    shutdown: tokio_util::sync::CancellationToken,
}

impl OutboxRelay {
    pub fn new(
        pool: sqlx::PgPool,
        publisher: Publisher,
        relay_id: String,
        config: &Config,
        shutdown: tokio_util::sync::CancellationToken,
    ) -> Self {
        Self {
            pool,
            publisher,
            relay_id,
            batch_size: config.outbox_batch_size,
            poll_interval: Duration::from_millis(config.outbox_poll_interval_ms),
            lease_secs: config.outbox_lease_secs as i64,
            shutdown,
        }
    }

    pub async fn run(self) {
        info!(relay_id = %self.relay_id, "outbox relay started");
        loop {
            if self.shutdown.is_cancelled() {
                info!("outbox relay shutting down");
                break;
            }
            match self.tick().await {
                Ok(0) => {
                    sleep(self.poll_interval).await;
                }
                Ok(n) => {
                    debug!(count = n, "outbox relay published batch");
                }
                Err(e) => {
                    error!(error = %e, "outbox relay tick failed");
                    sleep(Duration::from_secs(1)).await;
                }
            }
        }
        info!("outbox relay stopped");
    }

    async fn tick(&self) -> anyhow::Result<usize> {
        // Phase 1: claim (short transaction)
        let events = queries::claim_outbox_batch(
            &self.pool,
            &self.relay_id,
            self.batch_size,
            self.lease_secs,
        )
        .await?;

        if events.is_empty() {
            return Ok(0);
        }

        // Phase 2: publish (network I/O, NO db transaction held). Cancellation
        // is honored between events; unattempted rows keep their lease and are
        // reclaimed by whichever relay runs next.
        let mut published_ids: Vec<uuid::Uuid> = Vec::with_capacity(events.len());
        let mut failed: Vec<uuid::Uuid> = Vec::new();

        for event in &events {
            if self.shutdown.is_cancelled() {
                break;
            }
            match self.publisher.publish(event).await {
                Ok(()) => {
                    published_ids.push(event.id);
                }
                Err(e) => {
                    warn!(event_id = %event.id, error = %e, "publish failed; backing off");
                    failed.push(event.id);
                }
            }
        }

        // Phase 3a: clear successfully published rows (short transaction)
        if !published_ids.is_empty() {
            if let Err(e) =
                queries::clear_outbox_events(&self.pool, &self.relay_id, &published_ids).await
            {
                error!(error = %e, "failed to clear outbox events (will be reclaimed after lease)");
            }
        }

        // Phase 3b: release failed rows with exponential backoff instead of a
        // fixed-lease drumbeat, so poison pills stop hammering the broker.
        if !failed.is_empty() {
            if let Err(e) =
                queries::fail_outbox_events(&self.pool, &self.relay_id, &failed, 30).await
            {
                error!(error = %e, "failed to back off outbox events");
            }
        }

        Ok(published_ids.len())
    }
}
