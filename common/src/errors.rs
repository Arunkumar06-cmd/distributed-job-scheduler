use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use serde::Serialize;
use thiserror::Error;

#[derive(Debug, Error)]
pub enum AppError {
    #[error("not found: {0}")]
    NotFound(String),

    #[error("unauthorized")]
    Unauthorized,

    #[error("forbidden: {0}")]
    Forbidden(String),

    #[error("conflict: {0}")]
    Conflict(String),

    #[error("validation: {0}")]
    Validation(String),

    #[error("queue paused")]
    QueuePaused,

    #[error("queue at capacity")]
    QueueAtCapacity,

    #[error("rate limit exceeded: {0}")]
    RateLimited(String),

    #[error("payload too large")]
    PayloadTooLarge,

    #[error("stale lease (fenced worker)")]
    StaleLease,

    #[error("internal: {0}")]
    Internal(String),

    #[error("database error: {0}")]
    Sqlx(sqlx::Error),

    #[error(transparent)]
    Json(#[from] serde_json::Error),

    #[error(transparent)]
    Jwt(#[from] jsonwebtoken::errors::Error),

    #[error(transparent)]
    Io(#[from] std::io::Error),

    #[error(transparent)]
    Nats(#[from] async_nats::Error),
}

impl From<sqlx::Error> for AppError {
    fn from(e: sqlx::Error) -> Self {
        if let sqlx::Error::Database(ref db) = e {
            if db.code().as_deref() == Some("23505") {
                return AppError::Conflict(format!("duplicate key: {}", db.message()));
            }
        }
        AppError::Sqlx(e)
    }
}

impl AppError {
    pub fn code(&self) -> ErrorCode {
        match self {
            AppError::NotFound(_) => ErrorCode::NotFound,
            AppError::Unauthorized | AppError::Jwt(_) => ErrorCode::Unauthorized,
            AppError::Forbidden(_) => ErrorCode::Forbidden,
            AppError::Conflict(_) => ErrorCode::Conflict,
            AppError::Validation(_) => ErrorCode::Validation,
            AppError::QueuePaused => ErrorCode::QueuePaused,
            AppError::QueueAtCapacity => ErrorCode::QueueAtCapacity,
            AppError::RateLimited(_) | AppError::PayloadTooLarge => ErrorCode::PayloadTooLarge,
            AppError::StaleLease => ErrorCode::StaleLease,
            AppError::Internal(_)
            | AppError::Sqlx(_)
            | AppError::Json(_)
            | AppError::Io(_)
            | AppError::Nats(_) => ErrorCode::Internal,
        }
    }

    fn is_internal(&self) -> bool {
        matches!(
            self,
            AppError::Internal(_)
                | AppError::Sqlx(_)
                | AppError::Json(_)
                | AppError::Io(_)
                | AppError::Nats(_)
        )
    }
}

#[derive(Debug, Clone, Copy, Serialize)]
pub enum ErrorCode {
    NotFound,
    Unauthorized,
    Forbidden,
    Conflict,
    Validation,
    QueuePaused,
    QueueAtCapacity,
    RateLimited,
    StaleLease,
    Internal,
    PayloadTooLarge,
}

#[derive(Debug, Serialize)]
pub struct ErrorBody {
    pub error: ErrorDetail,
    /// Populated by the request-id middleware when available; never a
    /// fabricated value that disagrees with the `x-request-id` header.
    pub request_id: Option<String>,
}

#[derive(Debug, Serialize)]
pub struct ErrorDetail {
    pub code: ErrorCode,
    pub message: String,
}

impl IntoResponse for AppError {
    fn into_response(self) -> Response {
        let status = match &self {
            AppError::NotFound(_) => StatusCode::NOT_FOUND,
            AppError::Unauthorized | AppError::Jwt(_) => StatusCode::UNAUTHORIZED,
            AppError::Forbidden(_) => StatusCode::FORBIDDEN,
            AppError::Conflict(_) | AppError::QueuePaused | AppError::StaleLease => {
                StatusCode::CONFLICT
            }
            AppError::Validation(_) => StatusCode::BAD_REQUEST,
            AppError::QueueAtCapacity => StatusCode::CONFLICT,
            AppError::RateLimited(_) | AppError::PayloadTooLarge => StatusCode::PAYLOAD_TOO_LARGE,
            _ => StatusCode::INTERNAL_SERVER_ERROR,
        };

        // Internal failures must not leak driver/SQL/IO details to clients:
        // log the full cause, return an opaque message.
        let (code, message) = if self.is_internal() {
            tracing::error!(error = %self, "unhandled internal error");
            (ErrorCode::Internal, "internal server error".to_string())
        } else {
            tracing::warn!(error = %self, code = ?self.code(), "request rejected");
            (self.code(), self.to_string())
        };

        let body = ErrorBody {
            error: ErrorDetail { code, message },
            // Correlated with the x-request-id header when a request-id
            // middleware scope is active; None in tests/non-HTTP callers.
            request_id: crate::ids::current_request_id(),
        };
        (status, axum::Json(body)).into_response()
    }
}

pub type AppResult<T> = Result<T, AppError>;

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn internal_errors_do_not_leak_cause_to_client() {
        let err = AppError::Internal("password=hunter2 host=db.internal".to_string());
        let resp = err.into_response();
        let bytes = http_body_util::BodyExt::collect(resp.into_body())
            .await
            .unwrap()
            .to_bytes();
        let text = String::from_utf8(bytes.to_vec()).unwrap();
        assert!(!text.contains("hunter2"));
        assert!(text.contains("internal server error"));
    }

    #[tokio::test]
    async fn jwt_errors_map_to_401_not_500() {
        let err = AppError::Jwt(jsonwebtoken::errors::ErrorKind::InvalidToken.into());
        let resp = err.into_response();
        assert_eq!(resp.status(), StatusCode::UNAUTHORIZED);
    }

    #[tokio::test]
    async fn validation_maps_to_400_with_detail() {
        let err = AppError::Validation("priority must be in [0,100]".to_string());
        let resp = err.into_response();
        assert_eq!(resp.status(), StatusCode::BAD_REQUEST);
    }

    #[tokio::test]
    async fn request_id_is_correlated_from_task_scope() {
        let err = AppError::NotFound("thing".to_string());
        let resp =
            crate::ids::with_request_id(
                "req-abc-123".to_string(),
                async move { err.into_response() },
            )
            .await;
        assert_eq!(
            resp.headers().get("x-request-id").is_none(),
            true,
            "header set by outer layer, not here"
        );
        let bytes = http_body_util::BodyExt::collect(resp.into_body())
            .await
            .unwrap()
            .to_bytes();
        let text = String::from_utf8(bytes.to_vec()).unwrap();
        assert!(text.contains("\"req-abc-123\""), "body: {text}");
    }

    #[tokio::test]
    async fn no_task_scope_means_null_request_id() {
        let err = AppError::NotFound("thing".to_string());
        let resp = err.into_response();
        let bytes = http_body_util::BodyExt::collect(resp.into_body())
            .await
            .unwrap()
            .to_bytes();
        let text = String::from_utf8(bytes.to_vec()).unwrap();
        assert!(text.contains("\"request_id\":null"), "body: {text}");
    }
}
