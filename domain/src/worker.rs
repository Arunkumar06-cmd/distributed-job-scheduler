use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, sqlx::Type)]
#[sqlx(type_name = "worker_status", rename_all = "UPPERCASE")]
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

pub fn classify(last_heartbeat: chrono::DateTime<chrono::Utc>) -> WorkerStatus {
    let age = (chrono::Utc::now() - last_heartbeat).num_seconds();
    if age < 15 {
        WorkerStatus::Online
    } else if age < 60 {
        WorkerStatus::Stale
    } else {
        WorkerStatus::Offline
    }
}
