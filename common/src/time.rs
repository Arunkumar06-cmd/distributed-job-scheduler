use chrono::{DateTime, Utc};

pub fn now() -> DateTime<Utc> {
    Utc::now()
}

pub fn now_naive() -> chrono::NaiveDateTime {
    chrono::Utc::now().naive_utc()
}
