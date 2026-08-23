use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, sqlx::Type)]
#[sqlx(type_name = "worker_status", rename_all = "UPPERCASE")]
#[serde(rename_all = "UPPERCASE")]
pub enum WorkerStatus {
    Online,
    Stale,
    Offline,
}

impl WorkerStatus {
    pub fn as_str(&self) -> &'static str {
        match self {
            WorkerStatus::Online => "ONLINE",
            WorkerStatus::Stale => "STALE",
            WorkerStatus::Offline => "OFFLINE",
        }
    }
}

/// Thresholds are derived from the configured heartbeat interval instead of
/// being hardcoded: a worker is STALE after missing ~3 beats and OFFLINE after
/// missing ~12. With the 5s default this matches the historical 15s/60s.
///
/// `classify_at` is the pure core; `classify` reads the wall clock.
pub fn classify_at(
    now: DateTime<Utc>,
    last_heartbeat: DateTime<Utc>,
    heartbeat_interval_secs: u64,
) -> WorkerStatus {
    let age = (now - last_heartbeat).num_seconds();
    let beat = heartbeat_interval_secs.max(1) as i64;
    if age < beat * 3 {
        WorkerStatus::Online
    } else if age < beat * 12 {
        WorkerStatus::Stale
    } else {
        WorkerStatus::Offline
    }
}

pub fn classify(last_heartbeat: DateTime<Utc>, heartbeat_interval_secs: u64) -> WorkerStatus {
    classify_at(Utc::now(), last_heartbeat, heartbeat_interval_secs)
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::Duration;

    #[test]
    fn thresholds_scale_with_interval() {
        let now = Utc::now();
        let hb = now - Duration::seconds(4);
        assert_eq!(classify_at(now, hb, 5), WorkerStatus::Online);

        let stale = now - Duration::seconds(20);
        assert_eq!(classify_at(now, stale, 5), WorkerStatus::Stale);

        let offline = now - Duration::seconds(61);
        assert_eq!(classify_at(now, offline, 5), WorkerStatus::Offline);
    }

    #[test]
    fn thirty_second_heartbeat_does_not_flag_live_worker_stale() {
        let now = Utc::now();
        // 20s old is normal for a 30s interval; old hardcoded thresholds
        // would have called it STALE.
        let recent = now - Duration::seconds(20);
        assert_eq!(classify_at(now, recent, 30), WorkerStatus::Online);
    }

    #[test]
    fn future_timestamp_is_online_not_panicking() {
        let now = Utc::now();
        let ahead = now + Duration::seconds(120);
        assert_eq!(classify_at(now, ahead, 5), WorkerStatus::Online);
    }

    #[test]
    fn serde_uppercase() {
        assert_eq!(serde_json::to_string(&WorkerStatus::Stale).unwrap(), "\"STALE\"");
    }
}
