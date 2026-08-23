use std::sync::Arc;
use tracing_subscriber::{layer::SubscriberExt, util::SubscriberInitExt};

use common::Config;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::registry()
        .with(
            tracing_subscriber::EnvFilter::try_from_default_env().unwrap_or_else(|_| "info".into()),
        )
        .with(
            tracing_subscriber::fmt::layer()
                .with_target(false)
                .with_level(true)
                .json(),
        )
        .init();

    let config = Arc::new(Config::from_env());
    let pool =
        db::pool::connect_with_size(&config.database_url, config.scheduler_pool_size).await?;
    let shutdown = tokio_util::sync::CancellationToken::new();
    let sd = shutdown.clone();
    tokio::spawn(async move {
        tokio::signal::ctrl_c().await.ok();
        sd.cancel();
    });
    let runner = scheduler::cron_runner::SchedulerRunner::new(pool, config, shutdown);
    runner.run().await;
    Ok(())
}
