//! Automated end-to-end pipeline proof:
//!   outbox row → JetStream stream → durable pull consumer → atomic claim →
//!   handler execution → terminal state (+ DLQ path).
//!
//! Requires live services: Postgres (DATABASE_URL) and a JetStream-enabled
//! NATS (NATS_URL). Skipped by default (`--include-ignored` runs it); CI has
//! both services wired.

use std::sync::Arc;
use std::time::{Duration, Instant};

use common::Config;
use db::queries;
use uuid::Uuid;
use worker::handler::{with_default_handlers, AlwaysFailHandler};
use worker::supervisor::ensure_stream;
use worker::WorkerConsumer;

async fn migrated_pool() -> sqlx::PgPool {
    let url = std::env::var("DATABASE_URL").expect("DATABASE_URL for pipeline e2e");
    let pool = db::pool::connect_with_size(&url, 10).await.unwrap();
    static MIGRATOR: sqlx::migrate::Migrator = sqlx::migrate!("../db/migrations");
    MIGRATOR.run(&pool).await.unwrap();
    pool
}

/// Drive the real relay code path: claim → publish → clear.
async fn run_relay_once(pool: &sqlx::PgPool, nats: async_nats::Client) {
    let publisher = outbox::publisher::Publisher::new(nats);
    let events = queries::claim_outbox_batch(pool, "e2e-relay", 100, 30).await.unwrap();
    for e in &events {
        publisher.publish(e).await.unwrap();
    }
    let ids: Vec<Uuid> = events.iter().map(|e| e.id).collect();
    queries::clear_outbox_events(pool, "e2e-relay", &ids).await.unwrap();
}

async fn wait_status(
    pool: &sqlx::PgPool,
    job_id: Uuid,
    want: &[domain::JobStatus],
    timeout: Duration,
) -> db::models::Job {
    let deadline = Instant::now() + timeout;
    loop {
        let job = queries::get_job(pool, job_id).await.unwrap().unwrap();
        if want.contains(&job.status) {
            return job;
        }
        assert!(Instant::now() < deadline, "timed out waiting for {want:?}, last={job:?}");
        tokio::time::sleep(Duration::from_millis(250)).await;
    }
}

#[tokio::test(flavor = "multi_thread")]
#[ignore = "requires live Postgres + JetStream-enabled NATS (CI runs with --include-ignored)"]
async fn pipeline_echo_job_completes() {
    let pool = migrated_pool().await;

    // Tenancy fixture through the same code paths the API uses.
    let user = queries::create_user(&pool, &format!("pipe{}@t.io", Uuid::new_v4()), "h", "P")
        .await
        .unwrap();
    let org = queries::create_organization(&pool, "Pipe Org", &format!("pipe-{}", Uuid::new_v4()), user.id)
        .await
        .unwrap();
    let proj = queries::create_project(&pool, org.id, "P", &format!("pipe-{}", Uuid::new_v4()), "", user.id)
        .await
        .unwrap();
    let queue = queries::create_queue(&pool, proj.id, "pipe-q", "", 4, 5, 60, 3, None, None, None, 1)
        .await
        .unwrap();

    let cfg = Arc::new(Config::from_env());
    let shutdown = tokio_util::sync::CancellationToken::new();

    let worker = queries::upsert_worker(&pool, &format!("pipe-{}", Uuid::new_v4()), "0.1", "h", 4)
        .await
        .unwrap();

    // Real JetStream plumbing: provision stream + start a real consumer.
    let nats = async_nats::connect(&cfg.nats_url).await.unwrap();
    let js = async_nats::jetstream::new(nats.clone());
    let subject = format!("org.{}.proj.{}.queue.{}.*", org.id, proj.id, queue.id);
    let stream_name = common::ids::nats_stream_name(&org.id, &proj.id, &queue.id);
    ensure_stream(&js, &stream_name, &format!("org.{}.proj.{}.queue.{}.>", org.id, proj.id, queue.id)).await;

    let consumer = Arc::new(WorkerConsumer::new(
        pool.clone(),
        js.clone(),
        worker.id,
        cfg.clone(),
        with_default_handlers().await,
        shutdown.clone(),
    ));
    let consumer_task = tokio::spawn({
        let c = Arc::clone(&consumer);
        { let subject = subject.clone(); async move { c.consume_subject(subject, stream_name).await } }
    });

    // 1. Happy path: echo job must reach COMPLETED with its result stored.
    let job = queries::create_job_with_outbox(
        &pool,
        queries::CreateJobParams {
            queue_id: queue.id,
            org_id: org.id,
            project_id: proj.id,
            batch_id: None,
            shard_id: 0,
            kind: domain::JobKind::Immediate,
            payload: serde_json::json!({"type": "echo"}),
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
    run_relay_once(&pool, nats.clone()).await;

    let done = wait_status(&pool, job.id, &[domain::JobStatus::Completed], Duration::from_secs(30)).await;
    assert_eq!(done.attempt, 1);
    assert_eq!(done.result.as_ref().unwrap()["echoed"], serde_json::json!(true));

    // Execution ledger must hold exactly one completed attempt tied to our worker.
    let execs = queries::list_executions(&pool, job.id).await.unwrap();
    assert_eq!(execs.len(), 1);
    assert_eq!(execs[0].status, domain::ExecutionStatus::Completed);
    assert_eq!(execs[0].worker_id, Some(worker.id));

    // 2. Failure path: always_fail with max_attempts=1 lands straight in DLQ.
    let bad = queries::create_job_with_outbox(
        &pool,
        queries::CreateJobParams {
            queue_id: queue.id,
            org_id: org.id,
            project_id: proj.id,
            batch_id: None,
            shard_id: 0,
            kind: domain::JobKind::Immediate,
            payload: serde_json::json!({"type": "always_fail"}),
            priority: 5,
            max_attempts: 1,
            retry_strategy: domain::RetryStrategy::Fixed,
            base_delay_secs: 1,
            max_delay_secs: 5,
            scheduled_for: None,
            idempotency_key: None,
            subject: format!("org.{}.proj.{}.queue.{}.standard", org.id, proj.id, queue.id),
        },
    )
    .await
    .unwrap();
    run_relay_once(&pool, nats.clone()).await;

    let failed = wait_status(&pool, bad.id, &[domain::JobStatus::Failed], Duration::from_secs(30)).await;
    assert_eq!(failed.error_kind.as_deref(), Some("test_failure"));

    let dlq: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM dead_letter_entries WHERE job_id = $1")
        .bind(bad.id)
        .fetch_one(&pool)
        .await
        .unwrap();
    assert_eq!(dlq, 1, "exhausted job must have a dead-letter entry");

    // Heartbeat ledger reflects processed work for this worker.
    let (processed, failures): (i64, i64) = sqlx::query_as(
        r#"SELECT COALESCE(SUM((status='COMPLETED')::int),0), COALESCE(SUM((status IN ('FAILED','ABANDONED'))::int),0)
           FROM job_executions WHERE worker_id = $1"#,
    )
    .bind(worker.id)
    .fetch_one(&pool)
    .await
    .unwrap();
    assert!(processed >= 1 && failures >= 1);

    // Graceful stop: cancel, consumer drains and exits.
    shutdown.cancel();
    let _ = tokio::time::timeout(Duration::from_secs(10), consumer_task).await;
    queries::mark_worker_stopped(&pool, worker.id).await.unwrap();

    // Silence unused-import warnings for fixtures used indirectly.
    let _ = AlwaysFailHandler { message: String::new() };
}
