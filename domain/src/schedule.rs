use chrono::{DateTime, Utc};
use chrono_tz::Tz;
use cron::Schedule;

#[derive(Debug, thiserror::Error)]
pub enum CronError {
    #[error("invalid cron expression: {0}")]
    Invalid(String),
    #[error("invalid timezone: {0}")]
    BadTz(String),
}

/// Validates both the expression and the timezone; the timezone is returned so
/// callers never re-parse it (and can never pair a schedule with a different
/// tz than the one validated).
pub fn parse_cron(expr: &str, tz_str: &str) -> Result<(Schedule, Tz), CronError> {
    let schedule: Schedule = expr
        .parse()
        .map_err(|e: cron::error::Error| CronError::Invalid(format!("{expr}: {e}")))?;
    let tz: Tz = tz_str
        .parse()
        .map_err(|e| CronError::BadTz(format!("{tz_str}: {e}")))?;
    Ok((schedule, tz))
}

/// Validates a timezone string without requiring a cron expression.
pub fn parse_timezone(tz_str: &str) -> Result<Tz, CronError> {
    tz_str
        .parse()
        .map_err(|e| CronError::BadTz(format!("{tz_str}: {e}")))
}

/// First fire strictly after `after`, evaluated in `tz`. Honoring `after` (as
/// opposed to always "now") is what makes catch-up scheduling testable and
/// correct for backfilled recurring jobs.
pub fn next_occurrence(schedule: &Schedule, tz: Tz, after: DateTime<Utc>) -> Option<DateTime<Utc>> {
    let local_after = after.with_timezone(&tz);
    schedule
        .after(&local_after)
        .next()
        .map(|dt| dt.with_timezone(&Utc))
}

/// Idempotency key for a single fire of a recurring job; the scheduler uses it
/// to guarantee each occurrence enqueues exactly one job even across restarts.
pub fn occurrence_key(scheduled_job_id: &uuid::Uuid, fire_time: DateTime<Utc>) -> String {
    format!("{scheduled_job_id}:{}", fire_time.to_rfc3339())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_hourly_cron() {
        // 6-field format: sec min hour dom month dow.
        let (s, tz) = parse_cron("0 0 * * * *", "UTC").unwrap();
        let now = Utc::now();
        let next = next_occurrence(&s, tz, now);
        assert!(next.is_some());
        assert!(next.unwrap() > now);
    }

    #[test]
    fn rejects_bad_cron() {
        assert!(matches!(
            parse_cron("not a cron", "UTC"),
            Err(CronError::Invalid(_))
        ));
    }

    #[test]
    fn rejects_bad_tz() {
        assert!(matches!(
            parse_cron("0 0 * * * *", "Mars/Olympus"),
            Err(CronError::BadTz(_))
        ));
    }

    #[test]
    fn daily_kolkata() {
        let (s, tz) = parse_cron("0 0 9 * * *", "Asia/Kolkata").unwrap();
        let next = next_occurrence(&s, tz, Utc::now());
        assert!(next.is_some());
        // 09:00 IST == 03:30 UTC; the returned instant must be UTC-normalized.
        let ist: Tz = "Asia/Kolkata".parse().unwrap();
        let next = next.unwrap();
        assert_eq!(
            next.with_timezone(&ist).format("%H:%M").to_string(),
            "09:00"
        );
    }

    #[test]
    fn next_occurrence_honors_after_not_wall_clock() {
        let (s, tz) = parse_cron("0 0 * * * *", "UTC").unwrap();
        // Hourly cron; pretend "now" is far in the future.
        let after = Utc::now() + chrono::Duration::hours(48);
        let next = next_occurrence(&s, tz, after).unwrap();
        assert!(next > after);
        assert!(next - after <= chrono::Duration::hours(1));
    }

    #[test]
    fn occurrence_keys_are_unique_per_fire_and_stable() {
        let id = uuid::Uuid::nil();
        let t1 = DateTime::parse_from_rfc3339("2026-08-22T10:00:00Z")
            .unwrap()
            .with_timezone(&Utc);
        let t2 = t1 + chrono::Duration::hours(1);
        let k1a = occurrence_key(&id, t1);
        let k1b = occurrence_key(&id, t1);
        let k2 = occurrence_key(&id, t2);
        assert_eq!(k1a, k1b);
        assert_ne!(k1a, k2);
    }
}
