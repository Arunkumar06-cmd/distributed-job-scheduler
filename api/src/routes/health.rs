use axum::{extract::State, response::IntoResponse, Json};
use serde_json::json;

use crate::middleware::AuthUser;
use crate::state::AppState;

pub async fn health() -> impl IntoResponse {
    Json(json!({"status":"ok","timestamp": chrono::Utc::now()}))
}

pub async fn metrics(auth: AuthUser, State(state): State<AppState>) -> Json<serde_json::Value> {
    let pools = state.pool.size();
    let idle = state.pool.num_idle();

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
        "workers": { "active": null },
        "db": { "pool_size": pools, "idle": idle },
        "nats": { "connected": state.nats.is_some() }
    }))
}
