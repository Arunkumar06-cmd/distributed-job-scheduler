use std::net::SocketAddr;
use std::sync::Arc;

use axum::{
    http::{HeaderValue, Method},
    middleware,
    routing::{delete, get, post},
    Router,
};
use tower_http::cors::{Any, CorsLayer};
use tower_http::request_id::{MakeRequestUuid, PropagateRequestIdLayer, SetRequestIdLayer};
use tower_http::services::{ServeDir, ServeFile};
use tracing_subscriber::{layer::SubscriberExt, util::SubscriberInitExt};

use api::{ai_summaries, routes, state::AppState};
use axum::extract::DefaultBodyLimit;
use common::Config;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::registry()
        .with(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| "info,tower_http=debug,sqlx=warn".into()),
        )
        .with(
            tracing_subscriber::fmt::layer()
                .with_target(false)
                .with_level(true)
                .json(),
        )
        .init();

    let config = Arc::new(Config::from_env());
    tracing::info!(api_host = %config.api_host, api_port = config.api_port, ai_summaries_enabled = config.ai_summaries_enabled, "starting api");
    let cors_origin = match std::env::var("CORS_ALLOWED_ORIGIN") {
        Ok(origin) => origin,
        Err(_) if std::env::var("RUST_ENV").as_deref() == Ok("production") => {
            anyhow::bail!("CORS_ALLOWED_ORIGIN must be set in production")
        }
        Err(_) => "http://localhost:5173".to_string(),
    };
    let cors_origin: HeaderValue = cors_origin.parse()?;

    // DB (pool isolation: separate pools for api/worker/scheduler)
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
        rate_limiter: Some(api::middleware::RateLimiter::new(
            config.api_rate_limit_per_min,
        )),
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
            let relay = outbox::relay::OutboxRelay::new(
                pool_c,
                publisher,
                format!("relay-{}", uuid::Uuid::new_v4()),
                &cfg,
                sd,
            );
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

    // Optional, non-critical OpenAI Responses integration. Scheduling does not
    // depend on this task; it runs only when explicitly configured.
    ai_summaries::spawn(pool.clone(), config.clone(), shutdown.clone());

    // Public surface: health probes and token issuance only.
    let public = Router::new()
        .route("/health", get(routes::health::health))
        .route("/auth/register", post(routes::auth::register))
        .route("/auth/login", post(routes::auth::login))
        .route("/auth/refresh", post(routes::auth::refresh));

    // Everything below requires a valid bearer token. The route_layer makes the
    // default explicit: a new handler is protected unless deliberately moved out.
    let protected = Router::new()
        .route("/auth/me", get(routes::auth::me))
        .route(
            "/organizations",
            post(routes::organizations::create).get(routes::organizations::list),
        )
        .route("/organizations/:id", get(routes::organizations::get))
        .route(
            "/organizations/:id/members",
            post(routes::organizations::upsert_membership),
        )
        .route(
            "/projects",
            post(routes::projects::create).get(routes::projects::list),
        )
        .route("/projects/:id", get(routes::projects::get))
        .route(
            "/queues",
            post(routes::queues::create).get(routes::queues::list),
        )
        .route(
            "/queues/:id",
            get(routes::queues::get).patch(routes::queues::update),
        )
        .route("/queues/:id/pause", post(routes::queues::pause))
        .route("/queues/:id/resume", post(routes::queues::resume))
        .route("/queues/:id/stats", get(routes::queues::stats))
        .route("/queues/:id/throughput", get(routes::queues::throughput))
        .route("/queues/batch-stats", get(routes::queues::batch_stats))
        .route("/jobs", post(routes::jobs::create).get(routes::jobs::list))
        .route("/jobs/batch", post(routes::jobs::create_batch))
        .route("/jobs/:id", get(routes::jobs::get))
        .route("/jobs/:id/retry", post(routes::jobs::retry))
        .route(
            "/scheduled-jobs",
            post(routes::scheduled::create).get(routes::scheduled::list),
        )
        .route("/scheduled-jobs/:id", delete(routes::scheduled::delete))
        .route("/workers", get(routes::workers::list))
        .route("/workers/:id", get(routes::workers::get))
        .route("/jobs/:id/executions", get(routes::executions::list))
        .route("/jobs/:id/logs", get(routes::logs::list))
        .route("/dlq", get(routes::dlq::list))
        .route("/dlq/:id/replay", post(routes::dlq::replay))
        .route("/dlq/:id/summary", get(routes::dlq::summary))
        .route("/batches", get(routes::batches::list))
        .route("/batches/:id", get(routes::batches::get))
        .route("/workflows", post(routes::workflows::create))
        .route("/workflows/:id", get(routes::workflows::get))
        // Live updates are scoped to an authorized project; no process-wide event
        // broadcast is exposed to a tenant.
        .route("/events/stream", get(routes::events::sse_handler))
        .route("/events/ws", get(routes::events::ws_handler))
        .route("/stats", get(routes::health::stats))
        .route("/metrics", get(routes::health::metrics))
        .route_layer(middleware::from_fn_with_state(
            state.clone(),
            api::middleware::auth_middleware,
        ));

    // Versioned surface: /api/v1/* is the stable contract. The unversioned
    // legacy aliases remain so existing clients keep working; new consumers
    // MUST use the prefix.
    let core = Router::new().merge(public).merge(protected);
    let app = Router::new()
        .nest("/api/v1", core.clone())
        .merge(core)
        .with_state(state)
        .layer(middleware::from_fn(api::middleware::envelope_plain_errors))
        .layer(DefaultBodyLimit::max(512 * 1024))
        .layer(
            CorsLayer::new()
                .allow_origin(cors_origin)
                .allow_methods([
                    Method::GET,
                    Method::POST,
                    Method::PATCH,
                    Method::DELETE,
                    Method::OPTIONS,
                ])
                .allow_headers(Any),
        )
        // Innermost so every downstream layer/handler sees the id; error bodies
        // read it back through the task-local in common::ids.
        .layer(middleware::from_fn(api::middleware::request_id_middleware))
        .layer(PropagateRequestIdLayer::x_request_id())
        .layer(SetRequestIdLayer::x_request_id(MakeRequestUuid))
        .layer(tower_http::trace::TraceLayer::new_for_http())
        .fallback_service(
            ServeDir::new("frontend/dist")
                .not_found_service(ServeFile::new("frontend/dist/index.html")),
        );

    let addr: SocketAddr = format!("{}:{}", config.api_host, config.api_port).parse()?;
    tracing::info!(%addr, "listening");
    let listener = tokio::net::TcpListener::bind(addr).await?;
    axum::serve(listener, app)
        .with_graceful_shutdown(shutdown_signal(shutdown, config.shutdown_grace_secs))
        .await?;
    tracing::info!("shutdown complete");
    Ok(())
}

async fn shutdown_signal(token: tokio_util::sync::CancellationToken, grace_secs: u64) {
    let _ = tokio::signal::ctrl_c().await;
    tracing::info!(grace_secs, "received shutdown signal");
    token.cancel();
    tokio::time::sleep(std::time::Duration::from_secs(grace_secs)).await;
}
