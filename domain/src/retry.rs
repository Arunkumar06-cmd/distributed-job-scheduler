use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, sqlx::Type)]
#[sqlx(type_name = "retry_strategy", rename_all = "snake_case")]
pub enum RetryStrategy {
    Fixed,
    Linear,
    Exponential,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
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
    pub fn delay_secs(&self, attempt: i32) -> i64 {
        let n = attempt.max(1) as i64;
        let raw = match self.strategy {
            RetryStrategy::Fixed => self.base_delay_secs,
            RetryStrategy::Linear => self.base_delay_secs * n,
            RetryStrategy::Exponential => self.base_delay_secs * (2_i64.pow((n - 1) as u32)),
        };
        raw.min(self.max_delay_secs).max(0)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn fixed_delay() {
        let p = RetryPolicy {
            max_attempts: 4,
            strategy: RetryStrategy::Fixed,
            base_delay_secs: 10,
            max_delay_secs: 3600,
        };
        assert_eq!(p.delay_secs(1), 10);
        assert_eq!(p.delay_secs(2), 10);
        assert_eq!(p.delay_secs(3), 10);
    }

    #[test]
    fn linear_delay() {
        let p = RetryPolicy {
            max_attempts: 4,
            strategy: RetryStrategy::Linear,
            base_delay_secs: 10,
            max_delay_secs: 3600,
        };
        assert_eq!(p.delay_secs(1), 10);
        assert_eq!(p.delay_secs(2), 20);
        assert_eq!(p.delay_secs(3), 30);
    }

    #[test]
    fn exponential_delay() {
        let p = RetryPolicy {
            max_attempts: 4,
            strategy: RetryStrategy::Exponential,
            base_delay_secs: 5,
            max_delay_secs: 3600,
        };
        assert_eq!(p.delay_secs(1), 5);
        assert_eq!(p.delay_secs(2), 10);
        assert_eq!(p.delay_secs(3), 20);
        assert_eq!(p.delay_secs(4), 40);
    }

    #[test]
    fn max_delay_cap() {
        let p = RetryPolicy {
            max_attempts: 10,
            strategy: RetryStrategy::Exponential,
            base_delay_secs: 5,
            max_delay_secs: 30,
        };
        assert_eq!(p.delay_secs(10), 30);
    }
}
