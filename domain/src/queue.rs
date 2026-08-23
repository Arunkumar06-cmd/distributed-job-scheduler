use crate::retry::RetryStrategy;
use serde::{Deserialize, Serialize};

/// Canonical per-queue defaults. The API layer adopts these as the baseline
/// before applying request overrides so validation lives in exactly one place.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(default)]
pub struct QueueConfig {
    pub max_concurrency: i32,
    pub default_priority: i32,
    pub default_max_attempts: i32,
    #[serde(default)]
    pub default_retry_strategy: RetryStrategy,
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
            default_retry_strategy: RetryStrategy::Exponential,
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

pub fn validate_max_attempts(a: i32) -> Result<(), String> {
    // max_attempts counts the initial delivery too, hence >= 1.
    if (1..=100).contains(&a) {
        Ok(())
    } else {
        Err(format!("max_attempts must be in [1,100], got {a}"))
    }
}

pub fn validate_delay_bounds(base: i64, max: i64) -> Result<(), String> {
    if !(0..=86_400).contains(&base) {
        return Err(format!("base_delay_secs must be in [0,86400], got {base}"));
    }
    if !(0..=604_800).contains(&max) {
        return Err(format!("max_delay_secs must be in [0,604800], got {max}"));
    }
    if base > max {
        return Err(format!(
            "base_delay_secs ({base}) must not exceed max_delay_secs ({max})"
        ));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn defaults_are_valid() {
        let cfg = QueueConfig::default();
        validate_concurrency(cfg.max_concurrency).unwrap();
        validate_priority(cfg.default_priority).unwrap();
        validate_max_attempts(cfg.default_max_attempts).unwrap();
        validate_delay_bounds(cfg.default_base_delay_secs, cfg.default_max_delay_secs).unwrap();
    }

    #[test]
    fn validator_edges() {
        assert!(validate_priority(0).is_ok());
        assert!(validate_priority(100).is_ok());
        assert!(validate_priority(101).is_err());
        assert!(validate_priority(-1).is_err());

        assert!(validate_concurrency(1).is_ok());
        assert!(validate_concurrency(0).is_err());
        assert!(validate_concurrency(1001).is_err());

        assert!(validate_max_attempts(0).is_err());
        assert!(validate_max_attempts(1).is_ok());

        assert!(validate_delay_bounds(5, 5).is_ok());
        assert!(validate_delay_bounds(10, 5).is_err());
        assert!(validate_delay_bounds(0, 0).is_ok());
    }

    #[test]
    fn retry_strategy_defaults_to_exponential_when_absent() {
        let json = r#"{"max_concurrency":5,"default_priority":5,"default_max_attempts":3,"default_retry_strategy":"fixed","default_base_delay_secs":5,"default_max_delay_secs":3600,"ack_wait_secs":60,"max_receives":3}"#;
        let cfg: QueueConfig = serde_json::from_str(json).unwrap();
        assert_eq!(cfg.default_retry_strategy, RetryStrategy::Fixed);
        let minimal: QueueConfig =
            serde_json::from_str(r#"{"max_concurrency":2,"ack_wait_secs":30}"#).unwrap();
        assert_eq!(minimal.default_retry_strategy, RetryStrategy::Exponential);
    }
}
