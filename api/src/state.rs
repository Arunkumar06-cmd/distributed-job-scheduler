use axum::extract::FromRef;
use sqlx::PgPool;
use std::sync::Arc;

use crate::middleware::RateLimiter;
use common::Config;

#[derive(Clone)]
pub struct AppState {
    pub pool: PgPool,
    pub config: Arc<Config>,
    pub nats: Option<async_nats::Client>,
    pub broadcast: tokio::sync::broadcast::Sender<String>,
    /// None disables per-user limiting (API_RATE_LIMIT_PER_MIN=0).
    pub rate_limiter: Option<RateLimiter>,
}

impl FromRef<AppState> for PgPool {
    fn from_ref(state: &AppState) -> Self {
        state.pool.clone()
    }
}

impl FromRef<AppState> for Arc<Config> {
    fn from_ref(state: &AppState) -> Self {
        state.config.clone()
    }
}
