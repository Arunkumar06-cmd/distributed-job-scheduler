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
        worker::supervisor::ensure_stream(&js, &stream_name, &stream_subject).await;
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
        worker::supervisor::ensure_stream(&js, stream_name, &subject).await;
        subjects.push(subject);
    }

    let shutdown = tokio_util::sync::CancellationToken::new();
    let sd = shutdown.clone();
    tokio::spawn(async move {
        tokio::signal::ctrl_c().await.ok();
        tracing::info!("received ctrl-c; shutting down worker");
        sd.cancel();
    });

    // Queue discovery is event-driven: a DB trigger NOTIFYs 'queue_created'
    // on insert; we attach consumers immediately and keep a slow interval as
    // a fallback for missed notifications.
    let pool_w = pool.clone();
    let nats_w = nats.clone();
    let cfg_w = config.clone();
    let sd_w = shutdown.clone();
    let mut known: std::collections::HashSet<uuid::Uuid> =
        queues.iter().map(|(qid, _, _, _)| *qid).collect();
    let discovery_wake = Arc::new(tokio::sync::Notify::new());
    {
        let wake = discovery_wake.clone();
        let pool_w = pool_w.clone();
        let sd = shutdown.clone();
        tokio::spawn(async move {
            while !sd.is_cancelled() {
                match sqlx::postgres::PgListener::connect_with(&pool_w).await {
                    Ok(mut listener) => match listener.listen("queue_created").await {
                        Ok(()) => loop {
                            tokio::select! {
                                _ = sd.cancelled() => return,
                                n = listener.recv() => match n {
                                    Ok(notif) => {
                                        if let Ok(qid) = uuid::Uuid::parse_str(notif.payload()) {
                                            wake.notify_one();
                                            let _ = qid;
                                        }
                                    }
                                    Err(_) => break,
                                },
                            }
                        },
                        Err(e) => tracing::warn!(error=%e, "queue_created listen failed"),
                    },
                    Err(e) => tracing::warn!(error=%e, "discovery listener connect failed"),
                }
                tokio::time::sleep(std::time::Duration::from_secs(5)).await;
            }
        });
    }
    tokio::spawn(async move {
        let mut interval = tokio::time::interval(std::time::Duration::from_secs(10));
        loop {
            tokio::select! {
                _ = sd_w.cancelled() => break,
                _ = interval.tick() => {}
                _ = discovery_wake.notified() => {}
            }
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
                // Await provisioning; a fire-and-forget create raced the
                // consumer's create_consumer_on_stream and lost.
                worker::supervisor::ensure_stream(&js, &stream_name, &subject).await;
                let pool_c = pool_w.clone();
                let nats_c = nats_w.clone();
                let cfg_c = cfg_w.clone();
                let sd_c = sd_w.clone();
                let subj = subject.clone();
                let stm = stream_name.clone();
                tracing::info!(queue_id=%qid, subject=%subj, stream=%stm, "spawning consumer for new queue");
                tokio::spawn(async move {
                    let registry = worker::handler::with_default_handlers().await;
                    let hostname = hostname::get()
                        .map(|h| h.to_string_lossy().to_string())
                        .unwrap_or_else(|_| "unknown".to_string());
                    // Reuse the parent worker registration: a per-queue row
                    // would show up in the dashboard as a phantom worker that
                    // never sends its own heartbeats.
                    let w = match db::queries::upsert_worker(
                        &pool_c,
                        &cfg_c.worker_id,
                        "0.1.0",
                        &hostname,
                        cfg_c.worker_concurrency as i32,
                    )
                    .await
                    {
                        Ok(w) => w,
                        Err(e) => {
                            tracing::error!(error=%e, queue_id=%qid, "watcher: worker upsert failed");
                            return;
                        }
                    };
                    let consumer = std::sync::Arc::new(worker::consumer::WorkerConsumer::new(
                        pool_c,
                        async_nats::jetstream::new(nats_c),
                        w.id,
                        cfg_c,
                        registry,
                        sd_c,
                    ));
                    consumer.consume_subject(subj, stm).await;
                });
            }
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
