use std::env;

#[derive(Clone, Debug)]
pub struct Config {
    pub database_url: String,
    pub nats_url: String,
    pub jwt_secret: String,
    pub api_host: String,
    pub api_port: u16,
    pub worker_id: String,
    pub worker_concurrency: usize,
    pub heartbeat_interval_secs: u64,
    pub lease_duration_secs: u64,
    pub outbox_batch_size: i64,
    pub outbox_poll_interval_ms: u64,
    pub scheduler_poll_interval_secs: u64,
    pub shutdown_grace_secs: u64,
    pub api_pool_size: u32,
    pub worker_pool_size: u32,
    pub scheduler_pool_size: u32,
    pub openai_api_key: Option<String>,
    pub openai_model: String,
    pub ai_summaries_enabled: bool,
}

impl Config {
    pub fn from_env() -> Self {
        Self {
            database_url: env::var("DATABASE_URL")
                .unwrap_or_else(|_| "postgres:///job_scheduler".to_string()),
            nats_url: env::var("NATS_URL").unwrap_or_else(|_| "nats://127.0.0.1:4222".to_string()),
            jwt_secret: env::var("JWT_SECRET").unwrap_or_else(|_| {
                if env::var("RUST_ENV").as_deref() == Ok("production") {
                    panic!("JWT_SECRET must be set in production (ref: docs/design-decisions.md ADR-001)");
                }
                eprintln!("WARN: JWT_SECRET not set, using dev fallback — set JWT_SECRET env for prod");
                "dev-secret-change-in-production-please-32bytes".to_string()
            }),
            api_host: env::var("API_HOST").unwrap_or_else(|_| "0.0.0.0".to_string()),
            api_port: env::var("API_PORT").unwrap_or_else(|_| "8080".to_string()).parse().unwrap_or(8080),
            worker_id: env::var("WORKER_ID")
                .unwrap_or_else(|_| format!("worker-{}", uuid::Uuid::new_v4().as_simple())),
            worker_concurrency: env::var("WORKER_CONCURRENCY")
                .unwrap_or_else(|_| "8".to_string())
                .parse()
                .unwrap_or(8),
            heartbeat_interval_secs: 5,
            lease_duration_secs: 30,
            outbox_batch_size: 100,
            outbox_poll_interval_ms: 250,
            scheduler_poll_interval_secs: 5,
            shutdown_grace_secs: 30,
            api_pool_size: env::var("API_POOL_SIZE").unwrap_or_else(|_| "10".to_string()).parse().unwrap_or(10),
            worker_pool_size: env::var("WORKER_POOL_SIZE").unwrap_or_else(|_| "20".to_string()).parse().unwrap_or(20),
            scheduler_pool_size: env::var("SCHEDULER_POOL_SIZE").unwrap_or_else(|_| "5".to_string()).parse().unwrap_or(5),
            openai_api_key: env::var("OPENAI_API_KEY").ok().filter(|key| !key.trim().is_empty()),
            openai_model: env::var("OPENAI_MODEL").unwrap_or_else(|_| "gpt-4.1-mini".to_string()),
            ai_summaries_enabled: env::var("AI_SUMMARIES_ENABLED")
                .map(|value| matches!(value.as_str(), "1" | "true" | "TRUE"))
                .unwrap_or(false),
        }
    }
}
