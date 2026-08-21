use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct QueueConfig {
    pub max_concurrency: i32,
    pub default_priority: i32,
    pub default_max_attempts: i32,
    pub default_retry_strategy: String,
    pub default_base_delay_secs: i64,
    pub default_max_delay_secs: i64,
    pub ack_wait_secs: i64,
    pub max_receives: i64,
}

impl Default for QueueConfig {
    fn default() -> Self {
        Self {
            max_concurrency: 5,
            default_priority: 5,
            default_max_attempts: 3,
            default_retry_strategy: "exponential".to_string(),
            default_base_delay_secs: 5,
            default_max_delay_secs: 3600,
            ack_wait_secs: 60,
            max_receives: 3,
        }
    }
}

pub fn validate_priority(p: i32) -> Result<(), String> {
    if (0..=100).contains(&p) {
        Ok(())
    } else {
        Err(format!("priority must be in [0,100], got {p}"))
    }
}

pub fn validate_concurrency(c: i32) -> Result<(), String> {
    if c > 0 && c <= 1000 {
        Ok(())
    } else {
        Err(format!("max_concurrency must be in [1,1000], got {c}"))
    }
}
