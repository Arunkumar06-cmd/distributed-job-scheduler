//! Multi-worker race harness: R consumer replicas race M jobs through the
//! real JetStream + claim path. Asserts:
//!   • every job reaches COMPLETED exactly once (no double execution),
//!   • one execution-ledger row per job (a second attempt would mean two
//!     workers believed they owned the same work),
//!   • work was actually distributed across more than one replica,
//!   • capacity tokens were never leaked (queue drains cleanly).
//!
//! Requires live Postgres (DATABASE_URL) + JetStream NATS (NATS_URL).
//! Skipped by default; CI runs with --include-ignored.

use std::sync::Arc;
use std::time::{Duration, Instant};

use common::Config;
use db::queries;
use uuid::Uuid;
use worker::handler::{with_default_handlers, AlwaysFailHandler};
use worker::supervisor::ensure_stream;
use worker::WorkerConsumer;

const REPLICAS: usize = 3;
const JOBS: usize = 40;
const CAPACITY: i32 = 6;

async fn migrated_pool() -> sqlx::PgPool {
    let url = std::env::var("DATABASE_URL").expect("DATABASE_URL");
    let pool = db::pool::connect_with_size(&url, 20).await.unwrap();
    static MIGRATOR: sqlx::migrate::Migrator = sqlx::migrate!("../db/migrations");
    MIGRATOR.run(&pool).await.unwrap();
    pool
}

#[tokio::test(flavor = "multi_thread")]
#[ignore = "requires live Postgres + JetStream-enabled NATS (CI runs with --include-ignored)"]
async fn race_replicas_never_double_execute() {
    let pool = migrated_pool().await;

    let user = queries::create_user(&pool, &format!("race{}@t.io", Uuid::new_v4()), "h", "R")
        .await
        .unwrap();
    let org = queries::create_organization(&pool, "Race Org", &format!("race-{}", Uuid::new_v4()), user.id)
        .await
        .unwrap();
    let proj = queries::create_project(&pool, org.id, "P", &format!("race-{}", Uuid::new_v4()), "", user.id)
        .await
        .unwrap();
    let queue = queries::create_queue(&pool, proj.id, "race-q", "", CAPACITY, 5, 60, 3, None, None, None, 1)
        .await
        .unwrap();

    let cfg = Arc::new(Config::from_env());
    let nats = async_nats::connect(&cfg.nats_url).await.unwrap();
    let js = async_nats::jetstream::new(nats.clone());
    let stream_name_base = common::ids::nats_stream_name(&org.id, &proj.id, &queue.id);
    ensure_stream(
        &js,
        &stream_name_base,
        &format!("org.{}.proj.{}.queue.{}.>", org.id, proj.id, queue.id),
    )
    .await;

    // R independent replicas: separate worker rows, registries, consumers —
    // all sharing ONE durable so JetStream distributes partitions between them.
    let shutdown = tokio_util::sync::CancellationToken::new();
    let mut consumer_tasks = Vec::new();
    let mut worker_ids = Vec::new();

    for r in 0..REPLICAS {
        let w = queries::upsert_worker(&pool, &format!("race-{r}-{}", Uuid::new_v4()), "0.1", "h", 4)
            .await
            .unwrap();
        worker_ids.push(w.id);

        let registry = with_default_handlers().await;
        registry
            .register(Arc::new(AlwaysFailHandler { message: "unused".into() }))
            .await;

        let consumer = Arc::new(WorkerConsumer::new(
            pool.clone(),
            js.clone(),
            w.id,
            cfg.clone(),
            registry,
            shutdown.clone(),
        ));
        let subject = format!("org.{}.proj.{}.queue.{}.*", org.id, proj.id, queue.id);
        consumer_tasks.push(tokio::spawn({
            let c = Arc::clone(&consumer);
            let (subject, stream_name) = (subject.clone(), stream_name_base.clone());
            async move { c.consume_subject(subject, stream_name).await }
        }));
    }

    // Seed JOBS echo jobs.
    let mut job_ids = Vec::with_capacity(JOBS);
    for k in 0..JOBS {
        let j = queries::create_job_with_outbox(
            &pool,
            queries::CreateJobParams {
                queue_id: queue.id,
                org_id: org.id,
                project_id: proj.id,
                batch_id: None,
                shard_id: 0,
                kind: domain::JobKind::Immediate,
                payload: serde_json::json!({"type": "echo", "k": k}),
                priority: 5,
                max_attempts: 3,
                retry_strategy: domain::RetryStrategy::Exponential,
                base_delay_secs: 5,
                max_delay_secs: 3600,
                scheduled_for: None,
                idempotency_key: None,
                subject: format!("org.{}.proj.{}.queue.{}.standard", org.id, proj.id, queue.id),
            },
        )
        .await
        .unwrap();
        job_ids.push(j.id);
    }

    // Relay leg: publish every outbox row (real relay code path).
    let publisher = outbox::publisher::Publisher::new(nats.clone());
    let deadline = Instant::now() + Duration::from_secs(30);
    loop {
        let events =
            queries::claim_outbox_batch(&pool, "race-relay", 100, 30).await.unwrap();
        if events.is_empty() {
            if Instant::now() > deadline {
                break;
            }
            tokio::time::sleep(Duration::from_millis(200)).await;
            continue;
        }
        // Mirror the production relay: successes clear, failures back off.
        let (mut ok, mut failed) = (Vec::new(), Vec::new());
        for e in &events {
            if publisher.publish(e).await.is_ok() {
                ok.push(e.id);
            } else {
                failed.push(e.id);
            }
        }
        if !ok.is_empty() {
            queries::clear_outbox_events(&pool, "race-relay", &ok).await.unwrap();
        }
        if !failed.is_empty() {
            queries::fail_outbox_events(&pool, "race-relay", &failed, 30).await.unwrap();
        }
    }

    // Wait for all jobs to complete under contention.
    let wait_deadline = Instant::now() + Duration::from_secs(90);
    loop {
        let done: i64 = sqlx::query_scalar(
            "SELECT COUNT(*) FROM jobs WHERE queue_id=$1 AND status='COMPLETED'",
        )
        .bind(queue.id)
        .fetch_one(&pool)
        .await
        .unwrap();
        if done as usize == JOBS {
            break;
        }
        assert!(
            Instant::now() < wait_deadline,
            "only {done}/{JOBS} completed in time"
        );
        tokio::time::sleep(Duration::from_millis(300)).await;
    }

    // INVARIANT 1: exactly one execution-ledger row per job.
    let dupes: Vec<(Uuid, i64)> = sqlx::query_as(
        r#"SELECT j.id, COUNT(e.id)::int8 FROM jobs j
           LEFT JOIN job_executions e ON e.job_id = j.id
           WHERE j.queue_id = $1 GROUP BY j.id HAVING COUNT(e.id) <> 1"#,
    )
    .bind(queue.id)
    .fetch_all(&pool)
    .await
    .unwrap();
    assert!(dupes.is_empty(), "double-execution detected: {dupes:?}");

    // INVARIANT 2: work actually distributed across replicas.
    let touched: i64 = sqlx::query_scalar(
        "SELECT COUNT(DISTINCT worker_id) FROM job_executions WHERE worker_id = ANY($1)",
    )
    .bind(&worker_ids)
    .fetch_one(&pool)
    .await
    .unwrap();
    assert!(touched >= 2, "expected distribution across replicas, got {touched}");

    // INVARIANT 3: capacity tokens all released.
    let stuck_tokens: i64 =
        sqlx::query_scalar("SELECT COUNT(*) FROM capacity_tokens WHERE queue_id=$1 AND job_id IS NOT NULL")
            .bind(queue.id)
            .fetch_one(&pool)
            .await
            .unwrap();
    assert_eq!(stuck_tokens, 0, "capacity tokens leaked");

    shutdown.cancel();
    for t in consumer_tasks {
        let _ = tokio::time::timeout(Duration::from_secs(10), t).await;
    }
}
