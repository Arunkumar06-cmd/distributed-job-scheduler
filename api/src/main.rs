use std::net::SocketAddr;
use std::sync::Arc;

use axum::{
    http::{HeaderValue, Method},
    routing::{delete, get, patch, post},
    Router,
};
use tower_http::cors::{Any, CorsLayer};
use tower_http::request_id::{MakeRequestUuid, PropagateRequestIdLayer, SetRequestIdLayer};
use tower_http::services::{ServeDir, ServeFile};
use tracing_subscriber::{layer::SubscriberExt, util::SubscriberInitExt};

use api::{auth, middleware, routes, state::AppState};
use common::Config;
use uuid::Uuid;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::registry()
        .with(tracing_subscriber::EnvFilter::try_from_default_env().unwrap_or_else(|_| "info,tower_http=debug,sqlx=warn".into()))
        .with(tracing_subscriber::fmt::layer().json())
        .init();

    let config = Arc::new(Config::from_env());
    tracing::info!(config = ?config, "starting api");

    // DB (pool isolation §37: separate pools for api/worker/scheduler)
    let pool = db::pool::connect_with_size(&config.database_url, config.api_pool_size).await?;
    tracing::info!("connected to postgres");

    // Run migrations via sqlx::migrate (proper versioning, _sqlx_migrations table)
    static MIGRATOR: sqlx::migrate::Migrator = sqlx::migrate!("../db/migrations");
    MIGRATOR.run(&pool).await?;
    tracing::info!("migrations applied via sqlx::migrate");

    // NATS (optional)
    let nats = match async_nats::connect(&config.nats_url).await {
        Ok(c) => {
            tracing::info!(url = %config.nats_url, "connected to nats");
            Some(c)
        }
        Err(e) => {
            tracing::warn!(error = %e, "failed to connect to nats; running without nats (outbox will accumulate)");
            None
        }
    };

    // Broadcast for SSE / live updates
    let (tx, _rx) = tokio::sync::broadcast::channel::<String>(1000);

    let state = AppState {
        pool: pool.clone(),
        config: config.clone(),
        nats: nats.clone(),
        broadcast: tx.clone(),
    };

    // Background tasks: outbox relay + scheduler
    let shutdown = tokio_util::sync::CancellationToken::new();

    // Outbox relay
    if let Some(nats_client) = nats.clone() {
        let pool_c = pool.clone();
        let cfg = config.clone();
        let sd = shutdown.clone();
        tokio::spawn(async move {
            let publisher = outbox::publisher::Publisher::new(nats_client);
            let relay = outbox::relay::OutboxRelay::new(pool_c, publisher, format!("relay-{}", uuid::Uuid::new_v4()), &cfg, sd);
            relay.run().await;
        });
        tracing::info!("outbox relay spawned");
    }

    // Scheduler
    {
        let pool_c = pool.clone();
        let cfg = config.clone();
        let sd = shutdown.clone();
        tokio::spawn(async move {
            let runner = scheduler::cron_runner::SchedulerRunner::new(pool_c, cfg, sd);
            runner.run().await;
        });
        tracing::info!("scheduler spawned");
    }

    // AI failure summaries (bonus §8, async outside correctness path)
    {
        let pool_ai = pool.clone();
        let sd_ai = shutdown.clone();
        tokio::spawn(async move {
            let mut interval = tokio::time::interval(std::time::Duration::from_secs(10));
            loop {
                interval.tick().await;
                if sd_ai.is_cancelled() { break; }
                let pending: Vec<(uuid::Uuid,)> = sqlx::query_as(
                    r#"SELECT dlq.id FROM dead_letter_entries dlq LEFT JOIN failure_summaries fs ON fs.dlq_id = dlq.id WHERE fs.id IS NULL LIMIT 5"#,
                )
                .fetch_all(&pool_ai)
                .await
                .unwrap_or_default();
                for (dlq_id,) in pending {
                    // Mock LLM: in prod would call OpenAI API, here deterministic template
                    let summary = "Downstream service repeatedly returned error. Job exhausted retries via exponential backoff. Likely dependency outage.";
                    let root = "External dependency failure (503/timeout)";
                    let remediation = "Inspect downstream health, then POST /dlq/:id/replay after fix.";
                    let _ = sqlx::query(
                        r#"INSERT INTO failure_summaries (dlq_id, job_id, summary, root_cause, remediation, model)
                           SELECT id, job_id, $2, $3, $4, 'mock-llm' FROM dead_letter_entries WHERE id = $1
                           ON CONFLICT (dlq_id) DO NOTHING"#,
                    )
                    .bind(dlq_id)
                    .bind(summary)
                    .bind(root)
                    .bind(remediation)
                    .execute(&pool_ai)
                    .await;
                    tracing::info!(dlq_id = %dlq_id, "generated AI failure summary (mock)");
                }
            }
        });
        tracing::info!("AI summarizer spawned (mock)");
    }

    // Build router
    let app = Router::new()
        // health / metrics (public)
        .route("/health", get(routes::health::health))
        .route("/metrics", get(routes::health::metrics))
        .route("/events", get(routes::events::public_sse))
        // auth
        .route("/auth/register", post(routes::auth::register))
        .route("/auth/login", post(routes::auth::login))
        .route("/auth/me", get(routes::auth::me))
        // orgs
        .route("/organizations", post(routes::organizations::create).get(routes::organizations::list))
        .route("/organizations/:id", get(routes::organizations::get))
        // projects
        .route("/projects", post(routes::projects::create).get(routes::projects::list))
        .route("/projects/:id", get(routes::projects::get))
        // queues
        .route("/queues", post(routes::queues::create).get(routes::queues::list))
        .route("/queues/:id", get(routes::queues::get).patch(routes::queues::update))
        .route("/queues/:id/pause", post(routes::queues::pause))
        .route("/queues/:id/resume", post(routes::queues::resume))
        .route("/queues/:id/stats", get(routes::queues::stats))
        // jobs
        .route("/jobs", post(routes::jobs::create).get(routes::jobs::list))
        .route("/jobs/batch", post(routes::jobs::create_batch))
        .route("/jobs/:id", get(routes::jobs::get))
        .route("/jobs/:id/retry", post(routes::jobs::retry))
        // scheduled
        .route("/scheduled-jobs", post(routes::scheduled::create).get(routes::scheduled::list))
        .route("/scheduled-jobs/:id", delete(routes::scheduled::delete))
        // workers
        .route("/workers", get(routes::workers::list))
        .route("/workers/:id", get(routes::workers::get))
        // executions / logs / dlq / batches
        .route("/jobs/:id/executions", get(routes::executions::list))
        .route("/jobs/:id/logs", get(routes::logs::list))
        .route("/dlq", get(routes::dlq::list))
        .route("/dlq/:id/replay", post(routes::dlq::replay))
        .route("/batches", get(routes::batches::list))
        .route("/batches/:id", get(routes::batches::get))
        .route("/workflows", post(routes::workflows::create))
        .route("/workflows/:id", get(routes::workflows::get))
        // sse authed
        .route("/events/stream", get(routes::events::sse_handler))
        .route("/ws", get(routes::events::ws_handler))
        .with_state(state)
        .layer(
            CorsLayer::new()
                .allow_origin(Any)
                .allow_methods([Method::GET, Method::POST, Method::PATCH, Method::DELETE, Method::OPTIONS])
                .allow_headers(Any),
        )
        .layer(SetRequestIdLayer::x_request_id(MakeRequestUuid))
        .layer(PropagateRequestIdLayer::x_request_id())
        .layer(tower_http::trace::TraceLayer::new_for_http())
        .fallback_service(
            ServeDir::new("frontend/dist")
                .not_found_service(ServeFile::new("frontend/dist/index.html"))
        );

    let addr: SocketAddr = format!("{}:{}", config.api_host, config.api_port).parse()?;
    tracing::info!(%addr, "listening");
    let listener = tokio::net::TcpListener::bind(addr).await?;
    axum::serve(listener, app)
        .with_graceful_shutdown(shutdown_signal(shutdown))
        .await?;
    tracing::info!("shutdown complete");
    Ok(())
}

async fn shutdown_signal(token: tokio_util::sync::CancellationToken) {
    let _ = tokio::signal::ctrl_c().await;
    tracing::info!("received shutdown signal");
    token.cancel();
    tokio::time::sleep(std::time::Duration::from_secs(1)).await;
}
