use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize, sqlx::Type)]
#[sqlx(type_name = "retry_strategy", rename_all = "snake_case")]
#[serde(rename_all = "snake_case")]
pub enum RetryStrategy {
    Fixed,
    Linear,
    #[default]
    Exponential,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct RetryPolicy {
    pub max_attempts: i32,
    pub strategy: RetryStrategy,
    pub base_delay_secs: i64,
    pub max_delay_secs: i64,
}

impl Default for RetryPolicy {
    fn default() -> Self {
        Self {
            max_attempts: 3,
            strategy: RetryStrategy::Exponential,
            base_delay_secs: 5,
            max_delay_secs: 3600,
        }
    }
}

impl RetryPolicy {
    /// Delay before the retry of a job that failed on `attempt`.
    ///
    /// All arithmetic is saturating: hostile configs (huge base delays) and
    /// huge attempt counts must never panic or overflow to a negative delay.
    pub fn delay_secs(&self, attempt: i32) -> i64 {
        let n = attempt.max(1) as i64;
        let base = self.base_delay_secs.max(0);
        let cap = self.max_delay_secs.max(0);
        let raw = match self.strategy {
            RetryStrategy::Fixed => base,
            RetryStrategy::Linear => base.saturating_mul(n),
            // 2^62 already saturates any realistic base; capping the exponent
            // keeps `saturating_pow` from ever wrapping.
            RetryStrategy::Exponential => {
                let exp = (n - 1).min(62) as u32;
                base.saturating_mul(2_i64.saturating_pow(exp))
            }
        };
        raw.min(cap)
    }

    pub fn is_exhausted(&self, attempts_done: i32) -> bool {
        attempts_done >= self.max_attempts
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn policy(strategy: RetryStrategy, base: i64, max: i64, attempts: i32) -> RetryPolicy {
        RetryPolicy {
            max_attempts: attempts,
            strategy,
            base_delay_secs: base,
            max_delay_secs: max,
        }
    }

    #[test]
    fn fixed_delay() {
        let p = policy(RetryStrategy::Fixed, 10, 3600, 4);
        assert_eq!(p.delay_secs(1), 10);
        assert_eq!(p.delay_secs(2), 10);
        assert_eq!(p.delay_secs(3), 10);
    }

    #[test]
    fn linear_delay() {
        let p = policy(RetryStrategy::Linear, 10, 3600, 4);
        assert_eq!(p.delay_secs(1), 10);
        assert_eq!(p.delay_secs(2), 20);
        assert_eq!(p.delay_secs(3), 30);
    }

    #[test]
    fn exponential_delay() {
        let p = policy(RetryStrategy::Exponential, 5, 3600, 4);
        assert_eq!(p.delay_secs(1), 5);
        assert_eq!(p.delay_secs(2), 10);
        assert_eq!(p.delay_secs(3), 20);
        assert_eq!(p.delay_secs(4), 40);
    }

    #[test]
    fn max_delay_cap() {
        let p = policy(RetryStrategy::Exponential, 5, 30, 10);
        assert_eq!(p.delay_secs(10), 30);
    }

    #[test]
    fn huge_attempt_counts_never_overflow_or_panic() {
        let p = policy(RetryStrategy::Exponential, 60, 3600, i32::MAX);
        assert_eq!(p.delay_secs(1_000), 3600);
        assert_eq!(p.delay_secs(i32::MAX), 3600);

        let l = policy(RetryStrategy::Linear, i64::MAX / 2, 3600, 100);
        assert_eq!(l.delay_secs(i32::MAX), 3600);
    }

    #[test]
    fn non_positive_bounds_are_sanitized() {
        let p = policy(RetryStrategy::Linear, -5, -10, 3);
        assert_eq!(p.delay_secs(2), 0);
        let z = policy(RetryStrategy::Fixed, 0, 3600, 3);
        assert_eq!(z.delay_secs(1), 0);
    }

    #[test]
    fn attempt_zero_behaves_like_first_retry() {
        let p = policy(RetryStrategy::Exponential, 5, 3600, 3);
        assert_eq!(p.delay_secs(0), 5);
    }

    #[test]
    fn exhaustion_check() {
        let p = policy(RetryStrategy::Fixed, 1, 1, 3);
        assert!(!p.is_exhausted(2));
        assert!(p.is_exhausted(3));
        assert!(p.is_exhausted(4));
    }

    #[test]
    fn serde_matches_sql_labels() {
        assert_eq!(serde_json::to_string(&RetryStrategy::Fixed).unwrap(), "\"fixed\"");
        assert_eq!(
            serde_json::to_string(&RetryStrategy::Exponential).unwrap(),
            "\"exponential\""
        );
    }
}
