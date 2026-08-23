use std::env;
use std::str::FromStr;

fn env_or<T>(key: &str, default: T) -> T
where
    T: FromStr + Copy + std::fmt::Debug,
{
    match env::var(key) {
        Ok(raw) => raw.trim().parse::<T>().unwrap_or_else(|_| {
            eprintln!("WARN: invalid {key}={raw:?}, falling back to default {default:?}");
            default
        }),
        Err(_) => default,
    }
}

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
    pub outbox_lease_secs: u64,
    pub outbox_pool_size: u32,
    pub scheduler_poll_interval_secs: u64,
    pub unknown_resolution_policy: String,
    pub unknown_grace_secs: i64,
    pub archive_after_days: i32,
    pub archive_batch_size: i64,
    pub log_retention_secs: i64,
    /// Max wall-clock time for one handler invocation before it is treated as
    /// failed (handlers must be idempotent, so timeout => retry is safe).
    pub handler_timeout_secs: u64,
    pub shutdown_grace_secs: u64,
    pub api_pool_size: u32,
    pub worker_pool_size: u32,
    pub scheduler_pool_size: u32,
    pub openai_api_key: Option<String>,
    pub openai_model: String,
    /// Any OpenAI-compatible /chat/completions endpoint
    /// (OpenAI, NVIDIA NIM, vLLM, Ollama-openai, …).
    pub llm_base_url: String,
    /// Comma-separated fallback models tried in order if the primary fails.
    pub llm_model_fallbacks: Vec<String>,
    pub ai_summaries_enabled: bool,
    /// Per-user API requests per minute; 0 disables.
    pub api_rate_limit_per_min: u32,
}

impl Config {
    pub fn from_env() -> Self {
        let heartbeat_interval_secs = env_or("HEARTBEAT_INTERVAL_SECS", 5_u64).clamp(1, 300);
        // Lease must comfortably exceed the heartbeat interval or live workers
        // get fenced while healthy.
        let lease_duration_secs =
            env_or("LEASE_DURATION_SECS", 30_u64).clamp(heartbeat_interval_secs * 2, 3600);
        Self {
            database_url: env::var("DATABASE_URL")
                .unwrap_or_else(|_| "postgres:///job_scheduler".to_string()),
            nats_url: env::var("NATS_URL").unwrap_or_else(|_| "nats://127.0.0.1:4222".to_string()),
            jwt_secret: env::var("JWT_SECRET").unwrap_or_else(|_| {
                if env::var("RUST_ENV").as_deref() == Ok("production") {
                    panic!("JWT_SECRET must be set when RUST_ENV=production");
                }
                eprintln!(
                    "WARN: JWT_SECRET not set, using dev fallback — set JWT_SECRET env for prod"
                );
                "dev-secret-change-in-production-please-32bytes".to_string()
            }),
            api_host: env::var("API_HOST").unwrap_or_else(|_| "0.0.0.0".to_string()),
            api_port: env_or("API_PORT", 8080_u16),
            worker_id: env::var("WORKER_ID")
                .ok()
                .filter(|id| !id.trim().is_empty())
                .unwrap_or_else(|| format!("worker-{}", uuid::Uuid::new_v4().as_simple())),
            worker_concurrency: env_or("WORKER_CONCURRENCY", 8_usize).clamp(1, 1024),
            heartbeat_interval_secs,
            lease_duration_secs,
            outbox_batch_size: env_or("OUTBOX_BATCH_SIZE", 100_i64).clamp(1, 10_000),
            outbox_poll_interval_ms: env_or("OUTBOX_POLL_INTERVAL_MS", 250_u64).clamp(10, 60_000),
            outbox_lease_secs: env_or("OUTBOX_LEASE_SECS", 30_u64).clamp(5, 3600),
            outbox_pool_size: env_or("OUTBOX_POOL_SIZE", 10_u32).clamp(1, 100),
            scheduler_poll_interval_secs: env_or("SCHEDULER_POLL_INTERVAL_SECS", 5_u64)
                .clamp(1, 3600),
            // dlq (safe default) | retry (idempotent handlers only) | complete
            unknown_resolution_policy: match env::var("UNKNOWN_RESOLUTION_POLICY")
                .unwrap_or_else(|_| "dlq".to_string())
                .to_lowercase()
                .as_str()
            {
                "complete" => "complete".to_string(),
                "retry" => "retry".to_string(),
                _ => "dlq".to_string(),
            },
            unknown_grace_secs: env_or("UNKNOWN_GRACE_SECS", 900_i64).clamp(10, 86_400),
            // 0 disables archival (safe default); hot table otherwise sheds
            // terminal jobs older than N days into jobs_archive.
            archive_after_days: env_or("ARCHIVE_AFTER_DAYS", 0_i32).clamp(0, 3650),
            archive_batch_size: env_or("ARCHIVE_BATCH_SIZE", 500_i64).clamp(1, 10_000),
            log_retention_secs: env_or("LOG_RETENTION_SECS", 7 * 24 * 3600_i64)
                .clamp(3600, 365 * 24 * 3600),
            handler_timeout_secs: env_or("HANDLER_TIMEOUT_SECS", 600_u64).clamp(5, 86_400),
            shutdown_grace_secs: env_or("SHUTDOWN_GRACE_SECS", 30_u64).clamp(1, 600),
            api_pool_size: env_or("API_POOL_SIZE", 10_u32).clamp(1, 1000),
            worker_pool_size: env_or("WORKER_POOL_SIZE", 20_u32).clamp(1, 2000),
            scheduler_pool_size: env_or("SCHEDULER_POOL_SIZE", 5_u32).clamp(1, 100),
            openai_api_key: env::var("OPENAI_API_KEY")
                .ok()
                .filter(|key| !key.trim().is_empty()),
            openai_model: env::var("OPENAI_MODEL")
                .ok()
                .filter(|m| !m.trim().is_empty())
                .unwrap_or_else(|| "gpt-4o-mini".to_string()),
            llm_base_url: env::var("AI_LLM_BASE_URL")
                .unwrap_or_else(|_| "https://api.openai.com/v1".to_string())
                .trim_end_matches('/')
                .to_string(),
            llm_model_fallbacks: env::var("AI_MODEL_FALLBACKS")
                .unwrap_or_default()
                .split(',')
                .map(str::trim)
                .filter(|m| !m.is_empty())
                .map(str::to_string)
                .collect(),
            ai_summaries_enabled: env::var("AI_SUMMARIES_ENABLED")
                .map(|value| matches!(value.as_str(), "1" | "true" | "TRUE" | "True"))
                .unwrap_or(false),
            api_rate_limit_per_min: env_or("API_RATE_LIMIT_PER_MIN", 600_u32).clamp(0, 100_000),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Mutex;

    // Env vars are process-global; force the config tests to run serially.
    static ENV_LOCK: Mutex<()> = Mutex::new(());

    fn with_env<F: FnOnce(&mut Config)>(vars: &[(&str, &str)], f: F) {
        let _guard = ENV_LOCK.lock().unwrap();
        let saved: Vec<(String, Option<String>)> = vars
            .iter()
            .map(|(k, _)| (k.to_string(), std::env::var(k).ok()))
            .collect();
        for (k, v) in vars {
            std::env::set_var(k, v);
        }
        let mut cfg = Config::from_env();
        for (k, prev) in &saved {
            match prev {
                Some(v) => std::env::set_var(k, v),
                None => std::env::remove_var(k),
            }
        }
        f(&mut cfg);
    }

    #[test]
    fn lease_is_always_at_least_twice_the_heartbeat() {
        with_env(
            &[
                ("HEARTBEAT_INTERVAL_SECS", "60"),
                ("LEASE_DURATION_SECS", "10"),
            ],
            |cfg| {
                assert_eq!(cfg.heartbeat_interval_secs, 60);
                assert_eq!(cfg.lease_duration_secs, 120);
            },
        );
    }

    #[test]
    fn invalid_numeric_env_falls_back_to_default() {
        with_env(&[("WORKER_CONCURRENCY", "not-a-number")], |cfg| {
            assert_eq!(cfg.worker_concurrency, 8);
        });
    }

    #[test]
    fn concurrency_is_never_zero() {
        with_env(&[("WORKER_CONCURRENCY", "0")], |cfg| {
            assert_eq!(cfg.worker_concurrency, 1);
        });
    }
}
