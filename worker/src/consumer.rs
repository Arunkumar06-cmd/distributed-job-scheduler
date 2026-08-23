use futures::StreamExt;
use std::sync::Arc;
use std::time::Duration;
use tracing::{debug, error, info, warn};

use async_nats::jetstream;
use async_nats::jetstream::AckKind;
use common::Config;
use db::models::Job;
use db::queries;
use uuid::Uuid;

use crate::handler::{HandlerRegistry, HandlerResult};
use crate::lease::lease_renewer;

/// A NATS JetStream consumer that pulls jobs and executes them.
pub struct WorkerConsumer {
    pool: sqlx::PgPool,
    js: jetstream::Context,
    worker_id: Uuid,
    config: Arc<Config>,
    registry: Arc<HandlerRegistry>,
    shutdown: tokio_util::sync::CancellationToken,
    /// Bounds in-flight handler executions to the configured worker
    /// concurrency; fetching backpressures when all slots are busy.
    permits: Arc<tokio::sync::Semaphore>,
}

impl WorkerConsumer {
    pub fn new(
        pool: sqlx::PgPool,
        js: jetstream::Context,
        worker_id: Uuid,
        config: Arc<Config>,
        registry: Arc<HandlerRegistry>,
        shutdown: tokio_util::sync::CancellationToken,
    ) -> Self {
        let permits = Arc::new(tokio::sync::Semaphore::new(config.worker_concurrency));
        Self {
            pool,
            js,
            worker_id,
            config,
            registry,
            shutdown,
            permits,
        }
    }

    /// Consume from a single queue's stream subject.
    ///
    /// Messages are dispatched to a bounded worker pool (`WORKER_CONCURRENCY`
    /// permits) instead of being processed serially: one slow job can no
    /// longer starve every other job on the subject.
    pub async fn consume_subject(self: Arc<Self>, subject: String, stream_name: String) {
        // One SHARED durable per stream+subject, stable across worker restarts
        // and identical across workers: competing consumers then distribute
        // work instead of each boot abandoning last boot's durable (with its
        // unacked deliveries) and leaking a new one. Hashing keeps the name
        // far below NATS's 255-char limit for UUID-laden subjects.
        let mut hasher = std::collections::hash_map::DefaultHasher::new();
        use std::hash::{Hash, Hasher};
        stream_name.hash(&mut hasher);
        subject.hash(&mut hasher);
        let consumer_name = format!("jobs-{:016x}", hasher.finish());
        // Streams are provisioned lazily (API creates them on queue/job
        // creation), so the stream may not exist yet. Retry instead of dying:
        // a one-shot failure here silently starves the queue forever.
        let consumer = loop {
            if self.shutdown.is_cancelled() {
                info!(subject = %subject, "consumer shutting down before start");
                return;
            }
            match self
                .js
                .create_consumer_on_stream(
                    jetstream::consumer::pull::Config {
                        name: Some(consumer_name.clone()),
                        durable_name: Some(consumer_name.clone()),
                        filter_subject: subject.clone(),
                        ack_policy: jetstream::consumer::AckPolicy::Explicit,
                        ack_wait: Duration::from_secs(self.config.lease_duration_secs * 2),
                        max_deliver: 10,
                        ..Default::default()
                    },
                    &stream_name,
                )
                .await
            {
                Ok(c) => break c,
                Err(e) => {
                    error!(subject = %subject, stream = %stream_name, error = %e, "consumer create failed; retrying in 5s");
                    tokio::select! {
                        _ = tokio::time::sleep(Duration::from_secs(5)) => {}
                        _ = self.shutdown.cancelled() => {
                            info!(subject = %subject, "consumer shutting down during retry");
                            return;
                        }
                    }
                }
            }
        };

        info!(
            subject = %subject,
            consumer = %consumer_name,
            concurrency = self.config.worker_concurrency,
            "consumer started"
        );

        let mut inflight = tokio::task::JoinSet::new();

        loop {
            if self.shutdown.is_cancelled() {
                info!(subject = %subject, "consumer shutting down; draining in-flight jobs");
                break;
            }

            // Reap finished tasks so the set does not grow unbounded.
            while let Some(done) = inflight.try_join_next() {
                if let Err(e) = done {
                    // A panic inside process_message itself (outside the
                    // handler guard) must not kill the consumer.
                    error!(subject = %subject, panic = %e, "dispatch task panicked");
                }
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
                    tokio::select! {
                        _ = tokio::time::sleep(Duration::from_secs(1)) => {}
                        _ = self.shutdown.cancelled() => break,
                    }
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
                // Backpressure: waits for a free concurrency slot before
                // pulling more work from JetStream.
                let permit = self.permits.clone().acquire_owned().await;
                let me = Arc::clone(&self);
                inflight.spawn(async move {
                    let _permit = permit;
                    me.process_message(msg).await;
                });
            }
        }

        // Graceful drain: wait for every dispatched job to finish committing.
        while let Some(done) = inflight.join_next().await {
            if let Err(e) = done {
                error!(subject = %subject, panic = %e, "dispatch task panicked during drain");
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
            None => match serde_json::from_slice::<serde_json::Value>(&msg.payload) {
                Ok(v) => {
                    match v
                        .get("job_id")
                        .and_then(|j| j.as_str())
                        .and_then(|s| s.parse::<Uuid>().ok())
                    {
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
            },
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
                let _ = msg
                    .ack_with(AckKind::Nak(Some(Duration::from_secs(5))))
                    .await;
                return;
            }
            Err(common::AppError::QueueAtCapacity) => {
                warn!(job_id = %job_id, "queue at capacity; NAK with delay");
                let _ = msg
                    .ack_with(AckKind::Nak(Some(Duration::from_secs(2))))
                    .await;
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

        // Transition CLAIMED -> RUNNING. None means we lost the job between
        // claim and transition (lease expired and the reaper requeued it);
        // the fresh outbox event will deliver it again, so ACK this delivery.
        let job = match self
            .transition_to_running(claimed.job.id, claimed.lease_epoch)
            .await
        {
            Ok(Some(j)) => j,
            Ok(None) => {
                warn!(job_id = %claimed.job.id, "claim lost before RUNNING (raced reaper); acking redelivery");
                let _ = msg.ack().await;
                return;
            }
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

        // Execute the handler guarded against panics and hangs.
        let result =
            crate::handler::run_protected(self.config.handler_timeout_secs, async {
                self.execute_handler(&job, claimed.execution_id).await
            })
            .await;

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

    async fn transition_to_running(
        &self,
        job_id: Uuid,
        epoch: i64,
    ) -> anyhow::Result<Option<Job>> {
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
        .fetch_optional(&self.pool)
        .await?;
        Ok(job)
    }

    async fn execute_handler(&self, job: &Job, execution_id: Uuid) -> HandlerResult {
        // An absent type must NOT silently fall back to the echo handler:
        // that would mark arbitrary jobs "successfully completed" without
        // doing their work. Explicit registration or explicit DLQ.
        let Some(job_type) = job.payload.get("type").and_then(|v| v.as_str()) else {
            return HandlerResult::Permanent {
                message: "payload has no 'type' field; no handler can be selected".to_string(),
                kind: "no_handler".to_string(),
            };
        };

        let handler = match self.registry.get(job_type).await {
            Some(h) => h,
            None => {
                return HandlerResult::Permanent {
                    message: format!("no handler registered for job type '{job_type}'"),
                    kind: "no_handler".to_string(),
                };
            }
        };

        let _ = queries::append_log(
            &self.pool,
            job.id,
            Some(execution_id),
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
        // Clamp max_attempts to the current attempt so the subsequent fail_job
        // routes straight to the DLQ. If the clamp fails we still proceed —
        // fail_job's own fencing decides the outcome, but log loudly.
        match sqlx::query(
            "UPDATE jobs SET max_attempts = GREATEST(attempt, 1)
             WHERE id = $1 AND lease_epoch = $2 AND status = 'RUNNING'::job_status",
        )
        .bind(job.id)
        .bind(epoch)
        .execute(&mut *tx)
        .await
        {
            Ok(r) if r.rows_affected() == 1 => {}
            _ => {
                error!(job_id = %job.id, "permanent-failure clamp missed; failing via normal path");
            }
        }
        if let Err(e) = tx.commit().await {
            error!(error = %e, "permanent-failure clamp commit failed");
        }

        self.handle_failure(job, execution_id, epoch, message, kind, msg)
            .await;
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
