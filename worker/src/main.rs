use std::sync::Arc;
use tracing_subscriber::{layer::SubscriberExt, util::SubscriberInitExt};

use common::Config;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::registry()
        .with(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| "info,sqlx=warn".into()),
        )
        .with(
            tracing_subscriber::fmt::layer()
                .with_target(false)
                .with_level(true),
        )
        .init();

    let config = Arc::new(Config::from_env());
    tracing::info!(worker_id = %config.worker_id, worker_concurrency = config.worker_concurrency, "starting worker");

    let pool = db::pool::connect_with_size(&config.database_url, config.worker_pool_size).await?;
    tracing::info!("connected to postgres");

    let nats = async_nats::connect(&config.nats_url).await?;
    tracing::info!(url = %config.nats_url, "connected to nats");

    let queues: Vec<(uuid::Uuid, uuid::Uuid, uuid::Uuid, i32)> =
        sqlx::query_as::<_, (uuid::Uuid, uuid::Uuid, uuid::Uuid, i32)>(
            r#"SELECT q.id, p.id, p.org_id, q.shard_count FROM queues q JOIN projects p ON p.id = q.project_id"#,
        )
        .fetch_all(&pool)
        .await
        .unwrap_or_default();

    let mut subjects = Vec::new();
    let js = async_nats::jetstream::new(nats.clone());
    for (qid, pid, oid, shard_count) in &queues {
        let stream_name = format!("JOBS_{}_{}_{}", oid, pid, qid).replace('-', "_");
        let stream_subject = format!("org.{oid}.proj.{pid}.queue.{qid}.>");
        if js.get_stream(&stream_name).await.is_err() {
            match js
                .create_stream(async_nats::jetstream::stream::Config {
                    name: stream_name.clone(),
                    subjects: vec![stream_subject.clone()],
                    max_messages: 100_000,
                    ..Default::default()
                })
                .await
            {
                Ok(_) => {
                    tracing::info!(stream = %stream_name, subject = %stream_subject, "created stream for queue")
                }
                Err(e) => {
                    tracing::warn!(error = %e, stream = %stream_name, "failed to create stream (may already exist with overlapping subject)")
                }
            }
        } else {
            tracing::info!(stream = %stream_name, "found existing stream");
        }
        if *shard_count <= 1 {
            subjects.push(format!("org.{oid}.proj.{pid}.queue.{qid}.*"));
        } else {
            for shard_id in 0..*shard_count {
                subjects.push(format!(
                    "org.{oid}.proj.{pid}.queue.{qid}.shard.{shard_id}.*"
                ));
            }
        }
    }

    if subjects.is_empty() {
        tracing::warn!("no queues found; worker will listen on wildcard");
        let stream_name = "JOBS_WILDCARD";
        let subject = "org.*.proj.*.queue.*.>".to_string();
        if js.get_stream(stream_name).await.is_err() {
            let _ = js
                .create_stream(async_nats::jetstream::stream::Config {
                    name: stream_name.to_string(),
                    subjects: vec![subject.clone()],
                    max_messages: 100_000,
                    ..Default::default()
                })
                .await;
        }
        subjects.push(subject);
    }

    let shutdown = tokio_util::sync::CancellationToken::new();
    let sd = shutdown.clone();
    tokio::spawn(async move {
        tokio::signal::ctrl_c().await.ok();
        tracing::info!("received ctrl-c; shutting down worker");
        sd.cancel();
    });

    // Background watcher for new queues: polls DB every 10s and spawns consumers for new queues
    let pool_w = pool.clone();
    let nats_w = nats.clone();
    let cfg_w = config.clone();
    let sd_w = shutdown.clone();
    let mut known: std::collections::HashSet<uuid::Uuid> =
        queues.iter().map(|(qid, _, _, _)| *qid).collect();
    tokio::spawn(async move {
        let mut interval = tokio::time::interval(std::time::Duration::from_secs(10));
        loop {
            interval.tick().await;
            if sd_w.is_cancelled() {
                break;
            }
            let new_queues: Vec<(uuid::Uuid, uuid::Uuid, uuid::Uuid)> = sqlx::query_as::<_, (uuid::Uuid, uuid::Uuid, uuid::Uuid)>(
                r#"SELECT q.id, p.id, p.org_id FROM queues q JOIN projects p ON p.id = q.project_id"#
            ).fetch_all(&pool_w).await.unwrap_or_default();
            for (qid, pid, oid) in new_queues {
                if known.contains(&qid) {
                    continue;
                }
                known.insert(qid);
                let subject = format!("org.{oid}.proj.{pid}.queue.{qid}.>");
                let stream_name = format!("JOBS_{}_{}_{}", oid, pid, qid).replace('-', "_");
                let js = async_nats::jetstream::new(nats_w.clone());
                if js.get_stream(&stream_name).await.is_err() {
                    let _ = js
                        .create_stream(async_nats::jetstream::stream::Config {
                            name: stream_name.clone(),
                            subjects: vec![subject.clone()],
                            max_messages: 100_000,
                            ..Default::default()
                        })
                        .await;
                }
                let pool_c = pool_w.clone();
                let nats_c = nats_w.clone();
                let cfg_c = cfg_w.clone();
                let sd_c = sd_w.clone();
                let subj = subject.clone();
                let stm = stream_name.clone();
                tracing::info!(queue_id=%qid, subject=%subj, stream=%stm, "spawning consumer for new queue");
                tokio::spawn(async move {
                    let registry = std::sync::Arc::new(worker::handler::HandlerRegistry::new());
                    registry
                        .register(std::sync::Arc::new(worker::handler::EchoHandler))
                        .await;
                    registry
                        .register(std::sync::Arc::new(worker::handler::SleepHandler))
                        .await;
                    registry
                        .register(std::sync::Arc::new(worker::handler::ExternalPaymentHandler))
                        .await;
                    registry
                        .register(std::sync::Arc::new(worker::handler::AlwaysFailHandler {
                            message: "intentional test failure".to_string(),
                        }))
                        .await;
                    let worker_name = format!("worker-{}-{}", cfg_c.worker_id, qid.as_simple());
                    let hostname = hostname::get()
                        .map(|h| h.to_string_lossy().to_string())
                        .unwrap_or_else(|_| "unknown".to_string());
                    // Try to reuse existing worker registration if possible; create new ephemeral worker for this queue
                    let w = db::queries::upsert_worker(
                        &pool_c,
                        &worker_name,
                        "0.1.0",
                        &hostname,
                        cfg_c.worker_concurrency as i32,
                    )
                    .await
                    .unwrap();
                    let js2 = async_nats::jetstream::new(nats_c);
                    let consumer = worker::consumer::WorkerConsumer::new(
                        pool_c,
                        js2,
                        w.id,
                        worker_name,
                        cfg_c,
                        registry,
                        sd_c,
                    );
                    consumer.consume_subject(subj, stm).await;
                });
            }
        }
    });

    // PG NOTIFY listener for event-driven wakeup (spec §18, best-effort)
    let pool_notify = pool.clone();
    let sd_notify = shutdown.clone();
    tokio::spawn(async move {
        loop {
            if sd_notify.is_cancelled() {
                break;
            }
            match sqlx::postgres::PgListener::connect_with(&pool_notify).await {
                Ok(mut listener) => {
                    if let Err(e) = listener.listen("queue_events").await {
                        tracing::error!(error=%e, "NOTIFY listen failed");
                        tokio::time::sleep(std::time::Duration::from_secs(5)).await;
                        continue;
                    }
                    tracing::info!("listening for queue_events NOTIFY (wakeup only)");
                    loop {
                        tokio::select! {
                            _ = sd_notify.cancelled() => break,
                            n = listener.recv() => match n {
                                Ok(notif) => tracing::debug!(channel=%notif.channel(), payload=%notif.payload(), "NOTIFY wakeup"),
                                Err(e) => { tracing::warn!(error=%e, "NOTIFY recv error"); break; }
                            }
                        }
                    }
                }
                Err(e) => {
                    tracing::error!(error=%e, "PgListener connect failed");
                    tokio::time::sleep(std::time::Duration::from_secs(5)).await;
                }
            }
            if sd_notify.is_cancelled() {
                break;
            }
            tokio::time::sleep(std::time::Duration::from_secs(5)).await;
        }
    });

    let supervisor = worker::supervisor::WorkerSupervisor::new(
        pool,
        nats,
        config.clone(),
        subjects,
        shutdown.clone(),
    );
    let supervisor = supervisor.with_default_handlers().await;

    supervisor.start().await?;
    Ok(())
}
