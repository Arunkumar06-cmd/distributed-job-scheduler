use std::sync::Arc;
use tracing_subscriber::{layer::SubscriberExt, util::SubscriberInitExt};

use common::Config;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::registry()
        .with(tracing_subscriber::EnvFilter::try_from_default_env().unwrap_or_else(|_| "info".into()))
        .with(tracing_subscriber::fmt::layer().json())
        .init();

    let config = Arc::new(Config::from_env());
    let pool = db::pool::connect(&config.database_url).await?;
    let nats = async_nats::connect(&config.nats_url).await?;
    let publisher = outbox::publisher::Publisher::new(nats);
    let shutdown = tokio_util::sync::CancellationToken::new();
    let sd = shutdown.clone();
    tokio::spawn(async move {
        tokio::signal::ctrl_c().await.ok();
        sd.cancel();
    });
    let relay = outbox::relay::OutboxRelay::new(pool, publisher, format!("relay-{}", uuid::Uuid::new_v4()), &config, shutdown);
    relay.run().await;
    Ok(())
}
