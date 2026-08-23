//! Shared time helpers. Centralized so naive/UTC conversion stays consistent
//! across crates.

use chrono::{DateTime, Utc};

pub fn now() -> DateTime<Utc> {
    Utc::now()
}

#[cfg(test)]
mod tests {
    #[test]
    fn now_is_utc() {
        let t = super::now();
        assert_eq!(*t.offset(), chrono::Utc);
    }
}
