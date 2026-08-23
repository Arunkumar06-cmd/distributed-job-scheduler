//! Shared request-validation helpers used by the job/batch/workflow surfaces.

use common::{AppError, AppResult};

/// 256 KiB cap keeps payloads out of "database as blob store" territory;
/// large inputs belong in object storage with a reference in the payload.
pub const MAX_PAYLOAD_BYTES: usize = 256 * 1024;

/// Reject C0/C1 control characters (incl. NUL): PostgreSQL TEXT cannot store
/// them and they have no legitimate place in names/keys — without this guard
/// a `\u0000` payload surfaces as an opaque 500.
pub fn reject_control_chars(field: &str, value: &str) -> AppResult<()> {
    if value.chars().any(|c| c.is_control()) {
        return Err(AppError::Validation(format!(
            "{field} must not contain control characters"
        )));
    }
    Ok(())
}

pub fn validate_payload(payload: &serde_json::Value) -> AppResult<()> {
    if !payload.is_object() {
        return Err(AppError::Validation(
            "payload must be a JSON object".to_string(),
        ));
    }
    let size = serde_json::to_vec(payload)
        .map_err(|e| AppError::Validation(format!("payload not serializable: {e}")))?
        .len();
    if size > MAX_PAYLOAD_BYTES {
        return Err(AppError::Validation(format!(
            "payload exceeds {MAX_PAYLOAD_BYTES} bytes"
        )));
    }
    Ok(())
}

pub fn validate_retry_config(
    max_attempts: i32,
    base_delay_secs: i64,
    max_delay_secs: i64,
) -> AppResult<()> {
    domain::queue::validate_max_attempts(max_attempts).map_err(AppError::Validation)?;
    domain::queue::validate_delay_bounds(base_delay_secs, max_delay_secs)
        .map_err(AppError::Validation)
}

/// Header wins over body. Keys are trimmed; empty becomes None because an
/// empty-string key would collapse every such job in a queue onto a single
/// unique-constraint slot; oversized keys are rejected outright.
pub fn normalize_idempotency_key(
    header: Option<String>,
    body: Option<String>,
) -> AppResult<Option<String>> {
    match header.or(body) {
        None => Ok(None),
        Some(k) => {
            let trimmed = k.trim();
            if trimmed.is_empty() {
                return Ok(None);
            }
            reject_control_chars("idempotency_key", trimmed)?;
            if trimmed.len() > 200 {
                return Err(AppError::Validation(
                    "idempotency_key must be at most 200 characters".to_string(),
                ));
            }
            Ok(Some(trimmed.to_string()))
        }
    }
}
