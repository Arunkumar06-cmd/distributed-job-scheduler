use std::sync::Arc;
use axum::extract::FromRef;
use sqlx::PgPool;

use common::Config;

#[derive(Clone)]
pub struct AppState {
    pub pool: PgPool,
    pub config: Arc<Config>,
    pub nats: Option<async_nats::Client>,
    pub broadcast: tokio::sync::broadcast::Sender<String>,
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
