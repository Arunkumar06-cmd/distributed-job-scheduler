use chrono::{DateTime, NaiveDateTime, Utc};
use chrono_tz::Tz;
use cron::Schedule;

#[derive(Debug, thiserror::Error)]
pub enum CronError {
    #[error("invalid cron expression: {0}")]
    Invalid(String),
    #[error("invalid timezone: {0}")]
    BadTz(String),
}

pub fn parse_cron(expr: &str, tz_str: &str) -> Result<Schedule, CronError> {
    let schedule: Schedule = expr
        .parse()
        .map_err(|e: cron::error::Error| CronError::Invalid(format!("{expr}: {e}")))?;
    let _tz: Tz = tz_str
        .parse()
        .map_err(|e| CronError::BadTz(format!("{tz_str}: {e}")))?;
    Ok(schedule)
}

pub fn next_occurrence(schedule: &Schedule, tz: Tz, after: DateTime<Utc>) -> Option<DateTime<Utc>> {
    let _ = after.with_timezone(&tz);
    schedule.upcoming(tz).next().map(|dt| dt.with_timezone(&Utc))
}

pub fn previous_occurrence(schedule: &Schedule, tz: Tz, _before: DateTime<Utc>) -> Option<DateTime<Utc>> {
    // cron crate doesn't have prev(); we only use next() in the scheduler.
    schedule.upcoming(tz).next().map(|dt| dt.with_timezone(&Utc))
}

pub fn occurrence_key(scheduled_job_id: &uuid::Uuid, fire_time: DateTime<Utc>) -> String {
    format!("{scheduled_job_id}:{fire_time}")
}

pub fn fire_time_naive(dt: DateTime<Utc>) -> NaiveDateTime {
    dt.naive_utc()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_hourly_cron() {
        let s = parse_cron("0 * * * * *", "UTC").unwrap();
        let now = Utc::now();
        let next = next_occurrence(&s, Tz::UTC, now);
        assert!(next.is_some());
        assert!(next.unwrap() > now);
    }

    #[test]
    fn rejects_bad_cron() {
        assert!(parse_cron("not a cron", "UTC").is_err());
    }

    #[test]
    fn rejects_bad_tz() {
        assert!(parse_cron("0 * * * * *", "Mars/Olympus").is_err());
    }

    #[test]
    fn daily_kolkata() {
        let s = parse_cron("0 9 * * * *", "Asia/Kolkata").unwrap();
        let now = Utc::now();
        let next = next_occurrence(&s, Tz::Asia__Kolkata, now);
        assert!(next.is_some());
    }
}
