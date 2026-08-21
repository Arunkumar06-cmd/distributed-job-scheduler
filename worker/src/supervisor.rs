use std::sync::Arc;
use std::time::Duration;
use tokio::signal;
use tracing::{error, info};

use async_nats::jetstream;
use common::Config;
use db::queries;
use uuid::Uuid;

use crate::consumer::WorkerConsumer;
use crate::handler::{
    AlwaysFailHandler, EchoHandler, ExternalPaymentHandler, HandlerRegistry, SleepHandler,
};

/// The worker supervisor: registers the worker, starts heartbeat,
/// starts consumers for assigned subjects, and handles graceful shutdown.
pub struct WorkerSupervisor {
    pool: sqlx::PgPool,
    nats_client: async_nats::Client,
    config: Arc<Config>,
    worker_id: Uuid,
    worker_name: String,
    registry: Arc<HandlerRegistry>,
    subjects: Vec<String>,
    shutdown: tokio_util::sync::CancellationToken,
}

impl WorkerSupervisor {
    pub fn new(
        pool: sqlx::PgPool,
        nats_client: async_nats::Client,
        config: Arc<Config>,
        subjects: Vec<String>,
        shutdown: tokio_util::sync::CancellationToken,
    ) -> Self {
        let worker_name = config.worker_id.clone();
        let registry = Arc::new(HandlerRegistry::new());
        Self {
            pool,
            nats_client,
            config,
            worker_id: Uuid::nil(), // set in start()
            worker_name,
            registry,
            subjects,
            shutdown,
        }
    }

    pub async fn with_default_handlers(self) -> Self {
        self.registry.register(Arc::new(EchoHandler)).await;
        self.registry.register(Arc::new(SleepHandler)).await;
        self.registry
            .register(Arc::new(ExternalPaymentHandler))
            .await;
        self.registry
            .register(Arc::new(AlwaysFailHandler {
                message: "intentional test failure".to_string(),
            }))
            .await;
        self
    }

    pub async fn start(mut self) -> anyhow::Result<()> {
        let hostname = hostname::get()
            .map(|h| h.to_string_lossy().to_string())
            .unwrap_or_else(|_| "unknown".to_string());

        let worker = queries::upsert_worker(
            &self.pool,
            &self.worker_name,
            "0.1.0",
            &hostname,
            self.config.worker_concurrency as i32,
        )
        .await?;
        self.worker_id = worker.id;
        info!(worker_id = %self.worker_id, name = %self.worker_name, "worker registered");

        // Start heartbeat
        let hb_pool = self.pool.clone();
        let hb_worker_id = self.worker_id;
        let hb_shutdown = self.shutdown.clone();
        let hb_interval = self.config.heartbeat_interval_secs;
        let hb_handle = tokio::spawn(async move {
            heartbeat_loop(hb_pool, hb_worker_id, hb_interval, hb_shutdown).await;
        });

        // Start consumers
        let js = jetstream::new(self.nats_client.clone());
        let mut consumer_handles = Vec::new();

        for subject in self.subjects.clone() {
            let stream_name = stream_name_for_subject(&subject);
            let consumer = WorkerConsumer::new(
                self.pool.clone(),
                js.clone(),
                self.worker_id,
                self.worker_name.clone(),
                self.config.clone(),
                self.registry.clone(),
                self.shutdown.clone(),
            );
            let subject_clone = subject.clone();
            let stream_clone = stream_name.clone();
            let handle = tokio::spawn(async move {
                consumer.consume_subject(subject_clone, stream_clone).await;
            });
            consumer_handles.push(handle);
        }

        // Wait for shutdown signal
        tokio::select! {
            _ = signal::ctrl_c() => {
                info!("received SIGINT; initiating graceful shutdown");
            }
            _ = self.shutdown.cancelled() => {
                info!("shutdown signal received");
            }
        }

        // Graceful shutdown: stop accepting new work, wait for running jobs
        self.shutdown.cancel();
        info!(
            "waiting for consumers to drain (grace: {}s)",
            self.config.shutdown_grace_secs
        );

        let deadline =
            tokio::time::Instant::now() + Duration::from_secs(self.config.shutdown_grace_secs);
        for handle in consumer_handles {
            let _ = tokio::time::timeout_at(deadline, handle).await;
        }

        // Stop heartbeat
        hb_handle.abort();
        let _ = hb_handle.await;

        // Mark worker stopped
        let _ = queries::mark_worker_stopped(&self.pool, self.worker_id).await;
        info!("worker shutdown complete");
        Ok(())
    }
}

async fn heartbeat_loop(
    pool: sqlx::PgPool,
    worker_id: Uuid,
    interval_secs: u64,
    shutdown: tokio_util::sync::CancellationToken,
) {
    let mut ticker = tokio::time::interval(Duration::from_secs(interval_secs));
    ticker.tick().await; // skip first immediate tick

    loop {
        tokio::select! {
            _ = shutdown.cancelled() => {
                info!("heartbeat loop stopping");
                break;
            }
            _ = ticker.tick() => {
                let running: i64 = match sqlx::query_scalar(
                    "SELECT COUNT(*) FROM jobs WHERE lease_owner = $1 AND status = 'RUNNING'",
                )
                .bind(worker_id)
                .fetch_one(&pool)
                .await
                {
                    Ok(n) => n,
                    Err(e) => {
                        error!(error = %e, "heartbeat: failed to count running jobs");
                        0
                    }
                };

                if let Err(e) = queries::heartbeat(&pool, worker_id, running as i32, 0, 0).await {
                    error!(error = %e, "heartbeat failed");
                }
            }
        }
    }
}

fn stream_name_for_subject(subject: &str) -> String {
    if subject == "org.*.proj.*.queue.*.>" {
        return "JOBS_WILDCARD".to_string();
    }

    // org.{org}.proj.{proj}.queue.{queue}.{tier} -> JOBS_{org}_{proj}_{queue}
    let parts: Vec<&str> = subject.split('.').collect();
    if parts.len() >= 6 && parts[0] == "org" && parts[2] == "proj" && parts[4] == "queue" {
        format!("JOBS_{}_{}_{}", parts[1], parts[3], parts[5]).replace('-', "_")
    } else {
        format!("JOBS_{}", subject.replace(['.', '-'], "_"))
    }
}
