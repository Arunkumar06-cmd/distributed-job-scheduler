//! Throughput benchmark for the hot→cold archival path.
//! Seeds N terminal jobs backdated past the cutoff, then drains them with
//! `archive_terminal_jobs` in bounded batches, reporting elapsed time and
//! rows/second. Requires DATABASE_URL; ignored by default (CI runs it).

use std::time::Instant;
use uuid::Uuid;

#[tokio::test(flavor = "multi_thread")]
#[ignore = "requires live Postgres (CI runs with --include-ignored)"]
async fn archive_throughput() {
    const N: i64 = 5_000;
    const BATCH: i64 = 500;

    let url = std::env::var("DATABASE_URL").expect("DATABASE_URL");
    let pool = db::pool::connect_with_size(&url, 10).await.unwrap();
    static MIGRATOR: sqlx::migrate::Migrator = sqlx::migrate!("../db/migrations");
    MIGRATOR.run(&pool).await.unwrap();

    // Fixture: org/project/queue + one live worker (token seeding trigger).
    let user = db::queries::create_user(&pool, &format!("arch{}@t.io", Uuid::new_v4()), "h", "A")
        .await
        .unwrap();
    let org = db::queries::create_organization(
        &pool,
        "Arch Org",
        &format!("arch-{}", Uuid::new_v4()),
        user.id,
    )
    .await
    .unwrap();
    let proj = db::queries::create_project(
        &pool,
        org.id,
        "P",
        &format!("arch-{}", Uuid::new_v4()),
        "",
        user.id,
    )
    .await
    .unwrap();
    let queue = db::queries::create_queue(
        &pool, proj.id, "bench-q", "", 2, 5, 60, 3, None, None, None, 1,
    )
    .await
    .unwrap();

    // Bulk-seed N completed jobs backdated 30 days in one statement.
    let t_seed = Instant::now();
    sqlx::query(
        r#"INSERT INTO jobs (queue_id, status, payload, priority, updated_at)
           SELECT $1, 'COMPLETED', jsonb_build_object('i', g), 5, NOW() - INTERVAL '30 days'
           FROM generate_series(1, $2) AS g"#,
    )
    .bind(queue.id)
    .bind(N)
    .execute(&pool)
    .await
    .unwrap();
    let seed_secs = t_seed.elapsed().as_secs_f64();

    // Drain in batches until dry.
    let t_archive = Instant::now();
    let mut moved_total = 0i64;
    let mut batches = 0;
    loop {
        let moved = db::queries::archive_terminal_jobs(&pool, 5, BATCH)
            .await
            .unwrap();
        if moved == 0 {
            break;
        }
        moved_total += moved;
        batches += 1;
    }
    let arch_secs = t_archive.elapsed().as_secs_f64();

    let hot_left: i64 =
        sqlx::query_scalar("SELECT COUNT(*) FROM jobs WHERE queue_id=$1 AND status='COMPLETED'")
            .bind(queue.id)
            .fetch_one(&pool)
            .await
            .unwrap();
    let archived: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM jobs_archive WHERE queue_id=$1")
        .bind(queue.id)
        .fetch_one(&pool)
        .await
        .unwrap();

    assert_eq!(hot_left, 0, "hot table must be drained");
    assert_eq!(archived, N, "every seeded job must land in the archive");

    println!(
        "\n=== ARCHIVE BENCH ===\nseeded: {N} rows in {seed_secs:.2}s\narchived: {moved_total} rows in {batches} batches, {arch_secs:.2}s ({:.0} rows/s)\n=====================",
        moved_total as f64 / arch_secs.max(0.001)
    );
}

// Silence uuid import when bench is compiled without the test running.
#[allow(unused)]
fn _uuid_touch() -> uuid::Uuid {
    uuid::Uuid::nil()
}
