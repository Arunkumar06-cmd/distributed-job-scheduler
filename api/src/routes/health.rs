use axum::{extract::State, response::IntoResponse, Json};
use serde_json::json;

use crate::middleware::AuthUser;
use crate::state::AppState;

pub async fn health() -> impl IntoResponse {
    Json(json!({"status":"ok","timestamp": chrono::Utc::now()}))
}

/// Legacy JSON summary consumed by the dashboard.
pub async fn stats(auth: AuthUser, State(state): State<AppState>) -> Json<serde_json::Value> {
    let pools = state.pool.size();
    let idle = state.pool.num_idle();
    let hb = state.config.heartbeat_interval_secs as i64;

    let rows: Vec<(String, i64)> = sqlx::query_as(
        r#"SELECT j.status::text, COUNT(*)::int8 FROM jobs j
           JOIN queues q ON q.id = j.queue_id
           JOIN projects p ON p.id = q.project_id
           JOIN org_memberships m ON m.org_id = p.org_id
           WHERE m.user_id = $1 GROUP BY j.status
           UNION ALL
           SELECT 'DLQ', COUNT(*)::int8 FROM dead_letter_entries d
           JOIN queues q ON q.id = d.queue_id
           JOIN projects p ON p.id = q.project_id
           JOIN org_memberships m ON m.org_id = p.org_id
           WHERE m.user_id = $1"#,
    )
    .bind(auth.user_id)
    .fetch_all(&state.pool)
    .await
    .unwrap_or_default();

    let active_workers =
        db::queries::count_active_workers(&state.pool, hb * 3).await.unwrap_or(0);

    let mut m = std::collections::HashMap::new();
    let mut total = 0i64;
    for (k, v) in &rows {
        match k.as_str() {
            "DLQ" | "ACTIVE_WORKERS" => {
                m.insert(k.clone(), *v);
            }
            _ => {
                total += v;
                m.insert(k.clone(), *v);
            }
        }
    }

    Json(json!({
        "jobs": {
            "total": total,
            "queued": *m.get("QUEUED").unwrap_or(&0),
            "running": *m.get("RUNNING").unwrap_or(&0),
            "completed": *m.get("COMPLETED").unwrap_or(&0),
            "failed": *m.get("FAILED").unwrap_or(&0),
            "retry_wait": *m.get("RETRY_WAIT").unwrap_or(&0),
            "dlq": *m.get("DLQ").unwrap_or(&0),
        },
        "workers": { "active": active_workers },
        "db": { "pool_size": pools, "idle": idle },
        "nats": { "connected": state.nats.is_some() }
    }))
}

fn escape_label(value: &str) -> String {
    value.replace('\\', "\\\\").replace('"', "\\\"").replace('\n', "\\n")
}

/// Prometheus text exposition format. System-wide counters (not tenant-scoped):
/// scraping is authenticated because the endpoint also exposes pool internals.
pub async fn metrics(_auth: AuthUser, State(state): State<AppState>) -> impl IntoResponse {
    use std::fmt::Write as _;

    let hb = state.config.heartbeat_interval_secs as i64;

    let mut out = String::with_capacity(2048);
    let _ = writeln!(out, "# HELP jobflow_build_info Build metadata.");
    let _ = writeln!(out, "# TYPE jobflow_build_info gauge");
    let _ = writeln!(
        out,
        "jobflow_build_info{{version=\"{}\"}} 1",
        env!("CARGO_PKG_VERSION")
    );

    let rows: Vec<(String, i64)> = sqlx::query_as(
        "SELECT status::text, COUNT(*)::int8 FROM jobs GROUP BY status",
    )
    .fetch_all(&state.pool)
    .await
    .unwrap_or_default();

    let _ = writeln!(out, "# HELP jobflow_jobs_total Jobs by lifecycle state.");
    let _ = writeln!(out, "# TYPE jobflow_jobs_total gauge");
    for (status, count) in &rows {
        let _ = writeln!(
            out,
            "jobflow_jobs_total{{status=\"{}\"}} {}",
            escape_label(status),
            count
        );
    }

    let dlq: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM dead_letter_entries")
        .fetch_one(&state.pool)
        .await
        .unwrap_or(0);
    let unknown: i64 =
        sqlx::query_scalar("SELECT COUNT(*) FROM jobs WHERE status = 'UNKNOWN_EXTERNAL_RESULT'")
            .fetch_one(&state.pool)
            .await
            .unwrap_or(0);

    let _ = writeln!(out, "# HELP jobflow_dlq_total Dead-letter entries.");
    let _ = writeln!(out, "# TYPE jobflow_dlq_total gauge");
    let _ = writeln!(out, "jobflow_dlq_total {dlq}");

    let _ = writeln!(
        out,
        "# HELP jobflow_jobs_unknown_external_result Jobs awaiting outcome reconciliation."
    );
    let _ = writeln!(
        out,
        "# TYPE jobflow_jobs_unknown_external_result gauge"
    );
    let _ = writeln!(out, "jobflow_jobs_unknown_external_result {unknown}");

    let outbox_pending: i64 =
        sqlx::query_scalar("SELECT COUNT(*) FROM outbox_events WHERE published_at IS NULL")
            .fetch_one(&state.pool)
            .await
            .unwrap_or(0);
    let _ = writeln!(out, "# HELP jobflow_outbox_pending Unpublished outbox events.");
    let _ = writeln!(out, "# TYPE jobflow_outbox_pending gauge");
    let _ = writeln!(out, "jobflow_outbox_pending {outbox_pending}");

    let online_under = hb * 3;
    let stale_under = hb * 12;
    let workers: Vec<(String, i64)> = sqlx::query_as(
        r#"SELECT CASE
                 WHEN last_heartbeat_at IS NULL THEN 'OFFLINE'
                 WHEN EXTRACT(EPOCH FROM (NOW() - last_heartbeat_at)) < $1 THEN 'ONLINE'
                 WHEN EXTRACT(EPOCH FROM (NOW() - last_heartbeat_at)) < $2 THEN 'STALE'
                 ELSE 'OFFLINE' END AS s,
               COUNT(*)::int8
             FROM workers GROUP BY s"#,
    )
    .bind(online_under)
    .bind(stale_under)
    .fetch_all(&state.pool)
    .await
    .unwrap_or_default();

    let _ = writeln!(out, "# HELP jobflow_workers_total Workers by liveness.");
    let _ = writeln!(out, "# TYPE jobflow_workers_total gauge");
    for (liveness, count) in &workers {
        let _ = writeln!(
            out,
            "jobflow_workers_total{{state=\"{}\"}} {}",
            escape_label(liveness),
            count
        );
    }

    // Execution-duration histogram over the last 24h, aggregated at scrape
    // time from the ledger — no agent or client-side instrumentation needed.
    let edges_ms = [100i64, 250, 500, 1_000, 2_500, 5_000, 10_000, 30_000];
    let row: (
        i64,
        i64,
        i64,
        i64,
        i64,
        i64,
        i64,
        i64,
        i64,
        i64,
        i64,
    ) = sqlx::query_as(
        r#"SELECT
             COUNT(*)::int8,
             COALESCE(SUM(duration_ms), 0)::int8,
             COUNT(*) FILTER (WHERE duration_ms <=   100)::int8,
             COUNT(*) FILTER (WHERE duration_ms <=   250)::int8,
             COUNT(*) FILTER (WHERE duration_ms <=   500)::int8,
             COUNT(*) FILTER (WHERE duration_ms <=  1000)::int8,
             COUNT(*) FILTER (WHERE duration_ms <=  2500)::int8,
             COUNT(*) FILTER (WHERE duration_ms <=  5000)::int8,
             COUNT(*) FILTER (WHERE duration_ms <= 10000)::int8,
             COUNT(*) FILTER (WHERE duration_ms <= 30000)::int8,
             COUNT(*)::int8
           FROM job_executions
           WHERE status IN ('COMPLETED', 'FAILED')
             AND started_at > NOW() - INTERVAL '24 hours'
             AND duration_ms IS NOT NULL"#,
    )
    .fetch_one(&state.pool)
    .await
    .unwrap_or((0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0));

    let _ = writeln!(
        out,
        "# HELP jobflow_execution_duration_seconds Handler execution durations over the last 24h."
    );
    let _ = writeln!(out, "# TYPE jobflow_execution_duration_seconds histogram");
    let cumulative = [row.2, row.3, row.4, row.5, row.6, row.7, row.8, row.9];
    for (edge, count) in edges_ms.iter().zip(cumulative.iter()) {
        let le = if *edge >= 1000 {
            format!("{:.1}", *edge as f64 / 1000.0)
        } else {
            format!("0.{}", edge)
        };
        let _ = writeln!(
            out,
            "jobflow_execution_duration_seconds_bucket{{le=\"{le}\"}} {count}"
        );
    }
    let _ = writeln!(
        out,
        "jobflow_execution_duration_seconds_bucket{{le=\"+Inf\"}} {}",
        row.0
    );
    let _ = writeln!(out, "jobflow_execution_duration_seconds_sum {:.3}", row.1 as f64 / 1000.0);
    let _ = writeln!(out, "jobflow_execution_duration_seconds_count {}", row.0);

    let _ = writeln!(out, "# HELP jobflow_db_pool_connections PgPool connections.");
    let _ = writeln!(out, "# TYPE jobflow_db_pool_connections gauge");
    let _ = writeln!(
        out,
        "jobflow_db_pool_connections{{state=\"size\"}} {}",
        state.pool.size()
    );
    let _ = writeln!(
        out,
        "jobflow_db_pool_connections{{state=\"idle\"}} {}",
        state.pool.num_idle()
    );
    let _ = writeln!(
        out,
        "jobflow_nats_connected {}",
        if state.nats.is_some() { 1 } else { 0 }
    );

    (
        [(axum::http::header::CONTENT_TYPE, "text/plain; version=0.0.4")],
        out,
    )
}
