#[cfg(test)]
#[allow(clippy::module_inception)]
mod integration_tests {
    use crate::pool::connect;
    use common::ids;
    use domain::{JobKind, RetryStrategy};
    use uuid::Uuid;

    async fn test_pool() -> sqlx::PgPool {
        let url = std::env::var("DATABASE_URL")
            .unwrap_or_else(|_| "postgres:///job_scheduler_test".to_string());
        let pool = connect(&url).await.unwrap();
        // Ensure migrations apply cleanly; schema failures must not be ignored.
        sqlx::raw_sql(include_str!("../migrations/0001_init.sql"))
            .execute(&pool)
            .await
            .unwrap();
        sqlx::raw_sql(include_str!(
            "../migrations/0002_capacity_tokens_and_workflow.sql"
        ))
        .execute(&pool)
        .await
        .unwrap();
        sqlx::raw_sql(include_str!("../migrations/0003_dag_and_waiting.sql"))
            .execute(&pool)
            .await
            .unwrap();
        sqlx::raw_sql(include_str!("../migrations/0004_notify_fix.sql"))
            .execute(&pool)
            .await
            .unwrap();
        sqlx::raw_sql(include_str!(
            "../migrations/0005_add_waiting_job_status.sql"
        ))
        .execute(&pool)
        .await
        .unwrap();
        sqlx::raw_sql(include_str!("../migrations/0006_add_waiting_job_index.sql"))
            .execute(&pool)
            .await
            .unwrap();
        sqlx::raw_sql(include_str!("../migrations/0007_queue_sharding.sql"))
            .execute(&pool)
            .await
            .unwrap();
        // Clean (keep capacity_tokens etc, but truncate hot tables)
        sqlx::raw_sql("TRUNCATE jobs, outbox_events, dead_letter_entries, job_executions, job_logs, workflow_edges, edge_satisfaction, workflows, capacity_tokens, queue_rate_buckets, failure_summaries, scheduled_occurrences, scheduled_jobs, queues, projects, organizations, users, workers CASCADE").execute(&pool).await.ok();
        pool
    }

    async fn seed_org_proj_queue(pool: &sqlx::PgPool) -> (Uuid, Uuid, Uuid) {
        let user = crate::queries::create_user(
            pool,
            &format!("test{}@ex.com", Uuid::new_v4()),
            "$argon2id$v=19$m=19456,t=2,p=1$dummy",
            "Test",
        )
        .await
        .unwrap();
        let org = crate::queries::create_organization(
            pool,
            "Test Org",
            &format!("test-{}", &Uuid::new_v4().to_string()[..8]),
            user.id,
        )
        .await
        .unwrap();
        let proj = crate::queries::create_project(
            pool,
            org.id,
            "Proj",
            &format!("proj-{}", &Uuid::new_v4().to_string()[..8]),
            "",
            user.id,
        )
        .await
        .unwrap();
        let q = crate::queries::create_queue(pool, proj.id, "q1", "", 3, 5, 60, 3, None)
            .await
            .unwrap();
        (org.id, proj.id, q.id)
    }

    #[tokio::test]
    async fn test_idempotency_duplicate_rejected() {
        let pool = test_pool().await;
        let (org, proj, qid) = seed_org_proj_queue(&pool).await;
        let subject = ids::nats_subject(&org, &proj, &qid, 5);
        let p1 = crate::queries::CreateJobParams {
            queue_id: qid,
            org_id: org,
            project_id: proj,
            batch_id: None,
            shard_id: 0,
            kind: JobKind::Immediate,
            payload: serde_json::json!({"type":"echo"}),
            priority: 5,
            max_attempts: 3,
            retry_strategy: RetryStrategy::Exponential,
            base_delay_secs: 5,
            max_delay_secs: 3600,
            scheduled_for: None,
            idempotency_key: Some("idem-1".to_string()),
            subject: subject.clone(),
        };
        let _j1 = crate::queries::create_job_with_outbox(&pool, p1)
            .await
            .unwrap();
        let p2 = crate::queries::CreateJobParams {
            queue_id: qid,
            org_id: org,
            project_id: proj,
            batch_id: None,
            shard_id: 0,
            kind: JobKind::Immediate,
            payload: serde_json::json!({"type":"echo"}),
            priority: 5,
            max_attempts: 3,
            retry_strategy: RetryStrategy::Exponential,
            base_delay_secs: 5,
            max_delay_secs: 3600,
            scheduled_for: None,
            idempotency_key: Some("idem-1".to_string()),
            subject,
        };
        let r2 = crate::queries::create_job_with_outbox(&pool, p2).await;
        assert!(r2.is_err(), "duplicate idempotency should fail");
        let err = format!("{:?}", r2.unwrap_err());
        assert!(
            err.contains("duplicate") || err.contains("Conflict") || err.contains("unique"),
            "err: {}",
            err
        );
    }

    #[tokio::test]
    async fn test_lease_fencing() {
        let pool = test_pool().await;
        let (org, proj, qid) = seed_org_proj_queue(&pool).await;
        let w1 =
            crate::queries::upsert_worker(&pool, &format!("w-{}", Uuid::new_v4()), "0.1", "h1", 8)
                .await
                .unwrap();
        let w2 =
            crate::queries::upsert_worker(&pool, &format!("w-{}", Uuid::new_v4()), "0.1", "h2", 8)
                .await
                .unwrap();
        let subject = ids::nats_subject(&org, &proj, &qid, 5);
        let p = crate::queries::CreateJobParams {
            queue_id: qid,
            org_id: org,
            project_id: proj,
            batch_id: None,
            shard_id: 0,
            kind: JobKind::Immediate,
            payload: serde_json::json!({"type":"echo"}),
            priority: 5,
            max_attempts: 3,
            retry_strategy: RetryStrategy::Exponential,
            base_delay_secs: 5,
            max_delay_secs: 3600,
            scheduled_for: None,
            idempotency_key: None,
            subject,
        };
        let job = crate::queries::create_job_with_outbox(&pool, p)
            .await
            .unwrap();
        // w1 claims
        let c1 = crate::queries::claim_job(&pool, job.id, w1.id, "msg1", 30)
            .await
            .unwrap();
        // w2 tries to claim same job (already claimed) -> conflict
        let c2 = crate::queries::claim_job(&pool, job.id, w2.id, "msg2", 30).await;
        assert!(c2.is_err());
        // w1 transitions to running then completes with correct epoch -> success
        sqlx::query("UPDATE jobs SET status='RUNNING' WHERE id=$1")
            .bind(job.id)
            .execute(&pool)
            .await
            .unwrap();
        let ok = crate::queries::complete_job(
            &pool,
            job.id,
            w1.id,
            c1.lease_epoch,
            c1.execution_id,
            None,
        )
        .await
        .unwrap();
        assert!(ok, "w1 should complete with correct epoch");
        // w2 tries to complete with stale epoch -> fenced (0 rows)
        let stale = crate::queries::complete_job(&pool, job.id, w2.id, 999, c1.execution_id, None)
            .await
            .unwrap_or(false);
        assert!(!stale, "stale worker should be fenced");
    }

    #[tokio::test]
    async fn test_cron_dedup() {
        let pool = test_pool().await;
        let (org, proj, qid) = seed_org_proj_queue(&pool).await;
        let sj = crate::queries::create_scheduled_job(
            &pool,
            qid,
            "cron1",
            "echo",
            serde_json::json!({}),
            5,
            Some("0 * * * * *"),
            "UTC",
            None,
            Some(chrono::Utc::now()),
        )
        .await
        .unwrap();
        let fire = chrono::Utc::now();
        let s1 = ids::nats_subject(&org, &proj, &qid, 5);
        let j1 = crate::queries::create_cron_occurrence(&pool, &sj, fire, org, proj, s1)
            .await
            .unwrap();
        assert!(j1.is_some());
        let s2 = ids::nats_subject(&org, &proj, &qid, 5);
        let j2 = crate::queries::create_cron_occurrence(&pool, &sj, fire, org, proj, s2)
            .await
            .unwrap();
        assert!(
            j2.is_none(),
            "second occurrence with same fire_time should be deduped"
        );
    }

    #[tokio::test]
    async fn test_queue_concurrency_nowait() {
        let pool = test_pool().await;
        let (org, proj, qid) = seed_org_proj_queue(&pool).await;
        // Set concurrency 1
        sqlx::query("UPDATE queues SET max_concurrency=1 WHERE id=$1")
            .bind(qid)
            .execute(&pool)
            .await
            .unwrap();
        let w1 =
            crate::queries::upsert_worker(&pool, &format!("w-{}", Uuid::new_v4()), "0.1", "h1", 8)
                .await
                .unwrap();
        let w2 =
            crate::queries::upsert_worker(&pool, &format!("w-{}", Uuid::new_v4()), "0.1", "h2", 8)
                .await
                .unwrap();
        let s = ids::nats_subject(&org, &proj, &qid, 5);
        let mk = |k| crate::queries::CreateJobParams {
            queue_id: qid,
            org_id: org,
            project_id: proj,
            batch_id: None,
            shard_id: 0,
            kind: JobKind::Immediate,
            payload: serde_json::json!({"type":"echo","k":k}),
            priority: 5,
            max_attempts: 3,
            retry_strategy: RetryStrategy::Exponential,
            base_delay_secs: 5,
            max_delay_secs: 3600,
            scheduled_for: None,
            idempotency_key: None,
            subject: s.clone(),
        };
        let j1 = crate::queries::create_job_with_outbox(&pool, mk(1))
            .await
            .unwrap();
        let j2 = crate::queries::create_job_with_outbox(&pool, mk(2))
            .await
            .unwrap();
        let _c1 = crate::queries::claim_job(&pool, j1.id, w1.id, "m1", 30)
            .await
            .unwrap();
        sqlx::query("UPDATE jobs SET status='RUNNING' WHERE id=$1")
            .bind(j1.id)
            .execute(&pool)
            .await
            .unwrap();
        // Now running=1, max=1, w2 try to claim j2 should get QueueAtCapacity
        let c2 = crate::queries::claim_job(&pool, j2.id, w2.id, "m2", 30).await;
        assert!(c2.is_err());
        let err = format!("{:?}", c2.unwrap_err());
        assert!(
            err.contains("Capacity") || err.contains("AtCapacity") || err.contains("capacity"),
            "err: {}",
            err
        );
    }
}
