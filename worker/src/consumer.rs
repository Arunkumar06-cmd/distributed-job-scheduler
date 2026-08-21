use std::sync::Arc;
use std::time::Duration;
use futures::StreamExt;
use tracing::{debug, error, info, warn};

use async_nats::jetstream;
use async_nats::jetstream::AckKind;
use common::Config;
use db::queries;
use db::models::Job;
use uuid::Uuid;

use crate::handler::{HandlerRegistry, HandlerResult};
use crate::lease::lease_renewer;

/// A NATS JetStream consumer that pulls jobs and executes them.
pub struct WorkerConsumer {
    pool: sqlx::PgPool,
    js: jetstream::Context,
    worker_id: Uuid,
    worker_name: String,
    config: Arc<Config>,
    registry: Arc<HandlerRegistry>,
    shutdown: tokio_util::sync::CancellationToken,
}

impl WorkerConsumer {
    pub fn new(
        pool: sqlx::PgPool,
        js: jetstream::Context,
        worker_id: Uuid,
        worker_name: String,
        config: Arc<Config>,
        registry: Arc<HandlerRegistry>,
        shutdown: tokio_util::sync::CancellationToken,
    ) -> Self {
        Self {
            pool,
            js,
            worker_id,
            worker_name,
            config,
            registry,
            shutdown,
        }
    }

    /// Consume from a single queue's stream subject.
    pub async fn consume_subject(self, subject: String, stream_name: String) {
        let consumer_name = format!("{}-{}", self.worker_name, stream_name);
        let consumer = match self
            .js
            .create_consumer_on_stream(
                jetstream::consumer::pull::Config {
                    name: Some(consumer_name.clone()),
                    durable_name: Some(consumer_name.clone()),
                    filter_subject: subject.clone(),
                    ack_policy: jetstream::consumer::AckPolicy::Explicit,
                    ack_wait: Duration::from_secs(self.config.lease_duration_secs as u64 * 2),
                    max_deliver: 10,
                    ..Default::default()
                },
                &stream_name,
            )
            .await
        {
            Ok(c) => c,
            Err(e) => {
                error!(subject = %subject, error = %e, "failed to create consumer");
                return;
            }
        };

        info!(subject = %subject, consumer = %consumer_name, "consumer started");

        loop {
            if self.shutdown.is_cancelled() {
                info!(subject = %subject, "consumer shutting down");
                break;
            }

            let batch = match consumer
                .batch()
                .max_messages(1)
                .expires(Duration::from_secs(5))
                .messages()
                .await
            {
                Ok(b) => b,
                Err(e) => {
                    error!(subject = %subject, error = %e, "consumer batch error");
                    tokio::time::sleep(Duration::from_secs(1)).await;
                    continue;
                }
            };

            tokio::pin!(batch);

            while let Some(Ok(msg)) = batch.next().await {
                if self.shutdown.is_cancelled() {
                    info!("shutdown: leaving message for redelivery");
                    let _ = msg.ack_with(AckKind::Nak(None)).await;
                    break;
                }
                self.process_message(msg).await;
            }
        }
        info!(subject = %subject, "consumer stopped");
    }

    async fn process_message(&self, msg: jetstream::Message) {
        let subject = msg.subject.as_str().to_string();
        let nats_msg_id = msg
            .headers
            .as_ref()
            .and_then(|h| h.get("Nats-Msg-Id"))
            .map(|v| v.as_str().to_string())
            .unwrap_or_default();

        let job_id = match msg
            .headers
            .as_ref()
            .and_then(|h| h.get("Job-Id"))
            .and_then(|v| v.as_str().parse::<Uuid>().ok())
        {
            Some(id) => id,
            None => {
                match serde_json::from_slice::<serde_json::Value>(&msg.payload) {
                    Ok(v) => {
                        match v.get("job_id").and_then(|j| j.as_str()).and_then(|s| s.parse::<Uuid>().ok()) {
                            Some(id) => id,
                            None => {
                                error!(subject = %subject, "no Job-Id header and no job_id in payload; discarding");
                                let _ = msg.ack().await;
                                return;
                            }
                        }
                    }
                    Err(e) => {
                        error!(subject = %subject, error = %e, "bad payload; discarding");
                        let _ = msg.ack().await;
                        return;
                    }
                }
            }
        };

        debug!(job_id = %job_id, subject = %subject, nats_msg_id = %nats_msg_id, "received job");

        // Claim the job atomically
        let claimed = match queries::claim_job(
            &self.pool,
            job_id,
            self.worker_id,
            &nats_msg_id,
            self.config.lease_duration_secs as i64,
        )
        .await
        {
            Ok(c) => c,
            Err(common::AppError::QueuePaused) => {
                warn!(job_id = %job_id, "queue paused; NAK with delay");
                let _ = msg.ack_with(AckKind::Nak(Some(Duration::from_secs(5)))).await;
                return;
            }
            Err(common::AppError::QueueAtCapacity) => {
                warn!(job_id = %job_id, "queue at capacity; NAK with delay");
                let _ = msg.ack_with(AckKind::Nak(Some(Duration::from_secs(2)))).await;
                return;
            }
            Err(common::AppError::Conflict(_)) => {
                debug!(job_id = %job_id, "already claimed; acking duplicate");
                let _ = msg.ack().await;
                return;
            }
            Err(e) => {
                error!(job_id = %job_id, error = %e, "claim failed; NAK");
                let _ = msg.ack_with(AckKind::Nak(None)).await;
                return;
            }
        };

        // Transition CLAIMED -> RUNNING
        let job = match self.transition_to_running(claimed.job.id, claimed.lease_epoch).await {
            Ok(j) => j,
            Err(e) => {
                error!(job_id = %job_id, error = %e, "failed to transition to RUNNING; NAK");
                let _ = msg.ack_with(AckKind::Nak(None)).await;
                return;
            }
        };

        // Start lease renewal + NATS InProgress in background (two separate leases per playbook 16-18)
        let lease_cancel = tokio_util::sync::CancellationToken::new();
        let lease_pool = self.pool.clone();
        let cfg = self.config.clone();
        let lease_handle = tokio::spawn(lease_renewer(
            lease_pool,
            job.id,
            self.worker_id,
            claimed.lease_epoch,
            cfg,
            lease_cancel.clone(),
        ));
        // NATS InProgress: extend AckWait every 15s (AckWait = lease*2 = 60s)
        let progress_cancel = lease_cancel.clone();
        let progress_msg = msg.clone();
        let progress_handle = tokio::spawn(async move {
            let mut ticker = tokio::time::interval(std::time::Duration::from_secs(15));
            ticker.tick().await;
            loop {
                tokio::select! {
                    _ = progress_cancel.cancelled() => break,
                    _ = ticker.tick() => {
                        let _ = progress_msg.ack_with(AckKind::Progress).await;
                        tracing::debug!(job_id = %job.id, "sent NATS InProgress");
                    }
                }
            }
        });

        // Execute the handler
        let result = self.execute_handler(&job).await;

        // Stop lease renewal + InProgress
        lease_cancel.cancel();
        progress_handle.abort();
        let lease_ok = lease_handle.await.unwrap_or(true);
        let _ = progress_handle.await;

        if !lease_ok {
            warn!(job_id = %job.id, "lease was lost during execution; cannot commit result");
            let _ = msg.ack().await;
            return;
        }

        // Commit the result
        match result {
            HandlerResult::Ok(result_value) => {
                match queries::complete_job(
                    &self.pool,
                    job.id,
                    self.worker_id,
                    claimed.lease_epoch,
                    claimed.execution_id,
                    result_value,
                )
                .await
                {
                    Ok(true) => {
                        info!(job_id = %job.id, "job completed");
                        let _ = msg.ack().await;
                    }
                    Ok(false) => {
                        warn!(job_id = %job.id, "complete_job: fenced (0 rows). acking to avoid redelivery loop");
                        let _ = msg.ack().await;
                    }
                    Err(e) => {
                        error!(job_id = %job.id, error = %e, "complete_job failed; NAK for redelivery");
                        let _ = msg.ack_with(AckKind::Nak(None)).await;
                    }
                }
            }
            HandlerResult::Retry { message, kind } => {
                self.handle_failure(
                    &job,
                    claimed.execution_id,
                    claimed.lease_epoch,
                    &message,
                    &kind,
                    &msg,
                )
                .await;
            }
            HandlerResult::Permanent { message, kind } => {
                self.handle_permanent_failure(
                    &job,
                    claimed.execution_id,
                    claimed.lease_epoch,
                    &message,
                    &kind,
                    &msg,
                )
                .await;
            }
            HandlerResult::Unknown { message, kind } => {
                self.handle_unknown(
                    &job,
                    claimed.execution_id,
                    claimed.lease_epoch,
                    &message,
                    &kind,
                    &msg,
                )
                .await;
            }
        }
    }

    async fn transition_to_running(&self, job_id: Uuid, epoch: i64) -> anyhow::Result<Job> {
        let job = sqlx::query_as::<_, Job>(
            r#"UPDATE jobs SET
                 status = 'RUNNING'::job_status,
                 started_at = NOW()
               WHERE id = $1
                 AND lease_epoch = $2
                 AND lease_owner = $3
                 AND status = 'CLAIMED'::job_status
               RETURNING *"#,
        )
        .bind(job_id)
        .bind(epoch)
        .bind(self.worker_id)
        .fetch_one(&self.pool)
        .await?;
        Ok(job)
    }

    async fn execute_handler(&self, job: &Job) -> HandlerResult {
        let job_type = job
            .payload
            .get("type")
            .and_then(|v| v.as_str())
            .unwrap_or("echo");

        let handler = match self.registry.get(job_type).await {
            Some(h) => h,
            None => {
                match self.registry.get("echo").await {
                    Some(h) => h,
                    None => {
                        return HandlerResult::Permanent {
                            message: format!("no handler for job type '{job_type}'"),
                            kind: "no_handler".to_string(),
                        };
                    }
                }
            }
        };

        let _ = queries::append_log(
            &self.pool,
            job.id,
            None,
            Some(self.worker_id),
            "INFO",
            &format!("executing handler '{job_type}' attempt {}", job.attempt),
            serde_json::json!({}),
        )
        .await;

        handler.handle(job).await
    }

    async fn handle_failure(
        &self,
        job: &Job,
        execution_id: Uuid,
        epoch: i64,
        message: &str,
        kind: &str,
        msg: &jetstream::Message,
    ) {
        let ctx = match queries::queue_context(&self.pool, job.queue_id).await {
            Ok(Some((queue_id, org_id, project_id))) => (queue_id, org_id, project_id),
            _ => {
                error!(job_id = %job.id, "failed to get queue context for failure");
                let _ = msg.ack_with(AckKind::Nak(None)).await;
                return;
            }
        };

        match queries::fail_job(
            &self.pool,
            job.id,
            self.worker_id,
            epoch,
            execution_id,
            message,
            kind,
            ctx.1,
            ctx.2,
            ctx.0,
        )
        .await
        {
            Ok(queries::FailOutcome::Retry { next_retry_at, .. }) => {
                info!(job_id = %job.id, next_retry_at = %next_retry_at, "job failed; scheduled for retry");
                let _ = queries::append_log(
                    &self.pool,
                    job.id,
                    Some(execution_id),
                    Some(self.worker_id),
                    "WARN",
                    &format!("job failed: {message}"),
                    serde_json::json!({ "kind": kind, "next_retry_at": next_retry_at }),
                )
                .await;
                // ACK: retry is now safely represented in PostgreSQL as RETRY_WAIT.
                let _ = msg.ack().await;
            }
            Ok(queries::FailOutcome::DeadLettered) => {
                warn!(job_id = %job.id, "job moved to DLQ");
                let _ = queries::append_log(
                    &self.pool,
                    job.id,
                    Some(execution_id),
                    Some(self.worker_id),
                    "ERROR",
                    &format!("job permanently failed: {message}"),
                    serde_json::json!({ "kind": kind, "dlq": true }),
                )
                .await;
                let _ = msg.ack().await;
            }
            Err(common::AppError::StaleLease) => {
                warn!(job_id = %job.id, "fail_job: stale lease (fenced). acking");
                let _ = msg.ack().await;
            }
            Err(e) => {
                error!(job_id = %job.id, error = %e, "fail_job error; NAK");
                let _ = msg.ack_with(AckKind::Nak(None)).await;
            }
        }
    }

    async fn handle_permanent_failure(
        &self,
        job: &Job,
        execution_id: Uuid,
        epoch: i64,
        message: &str,
        kind: &str,
        msg: &jetstream::Message,
    ) {
        let mut tx = match self.pool.begin().await {
            Ok(tx) => tx,
            Err(e) => {
                error!(error = %e, "begin tx failed");
                let _ = msg.ack_with(AckKind::Nak(None)).await;
                return;
            }
        };
        let _ = sqlx::query("UPDATE jobs SET max_attempts = attempt WHERE id = $1 AND lease_epoch = $2")
            .bind(job.id)
            .bind(epoch)
            .execute(&mut *tx)
            .await;
        let _ = tx.commit().await;

        self.handle_failure(job, execution_id, epoch, message, kind, msg).await;
    }

    async fn handle_unknown(
        &self,
        job: &Job,
        execution_id: Uuid,
        epoch: i64,
        message: &str,
        kind: &str,
        msg: &jetstream::Message,
    ) {
        // UNKNOWN_EXTERNAL_RESULT: release capacity, keep execution for reconciler (spec §25)
        let mut tx = match self.pool.begin().await {
            Ok(tx) => tx,
            Err(e) => {
                error!(error = %e, "begin tx failed for unknown");
                let _ = msg.ack_with(AckKind::Nak(None)).await;
                return;
            }
        };
        let res = sqlx::query(
            r#"UPDATE jobs SET status='UNKNOWN_EXTERNAL_RESULT'::job_status, error_message=$2, error_kind=$3, lease_owner=NULL, lease_expires_at=NULL, token_id=NULL
               WHERE id=$1 AND lease_epoch=$4 AND status='RUNNING'::job_status"#,
        )
        .bind(job.id)
        .bind(message)
        .bind(kind)
        .bind(epoch)
        .execute(&mut *tx)
        .await;
        if let Err(e) = res {
            error!(error = %e, "unknown update failed");
            let _ = tx.rollback().await;
            let _ = msg.ack_with(AckKind::Nak(None)).await;
            return;
        }
        let _ = sqlx::query(r#"UPDATE capacity_tokens SET worker_id=NULL, job_id=NULL, lease_until=NULL WHERE job_id=$1"#)
            .bind(job.id)
            .execute(&mut *tx)
            .await;
        let _ = sqlx::query(
            r#"UPDATE job_executions SET status='ABANDONED'::execution_status, finished_at=NOW(), error_message=$2, error_kind=$3 WHERE id=$1"#,
        )
        .bind(execution_id)
        .bind(message)
        .bind(kind)
        .execute(&mut *tx)
        .await;
        let _ = tx.commit().await;
        let _ = queries::append_log(
            &self.pool,
            job.id,
            Some(execution_id),
            Some(self.worker_id),
            "WARN",
            &format!("job unknown: {message}"),
            serde_json::json!({"kind": kind, "unknown": true}),
        )
        .await;
        let _ = msg.ack().await;
        // Capacity released, reconciler will resolve (success/failure/manual)
        tracing::warn!(job_id = %job.id, "job UNKNOWN_EXTERNAL_RESULT, capacity released for reconciler");
    }
}
