use sqlx::postgres::{PgPool, PgPoolOptions};
use std::time::Duration;

pub async fn connect(database_url: &str) -> Result<PgPool, sqlx::Error> {
    PgPoolOptions::new()
        .max_connections(20)
        .min_connections(2)
        .acquire_timeout(Duration::from_secs(10))
        .idle_timeout(Some(Duration::from_secs(600)))
        .max_lifetime(Some(Duration::from_secs(1800)))
        .connect(database_url)
        .await
}

pub async fn connect_with_size(database_url: &str, size: u32) -> Result<PgPool, sqlx::Error> {
    PgPoolOptions::new()
        .max_connections(size)
        .acquire_timeout(Duration::from_secs(10))
        .connect(database_url)
        .await
}
