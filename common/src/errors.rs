use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use serde::Serialize;
use thiserror::Error;
use uuid::Uuid;

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
            AppError::Unauthorized => ErrorCode::Unauthorized,
            AppError::Forbidden(_) => ErrorCode::Forbidden,
            AppError::Conflict(_) => ErrorCode::Conflict,
            AppError::Validation(_) => ErrorCode::Validation,
            AppError::QueuePaused => ErrorCode::QueuePaused,
            AppError::QueueAtCapacity => ErrorCode::QueueAtCapacity,
            AppError::StaleLease => ErrorCode::StaleLease,
            AppError::Internal(_)
            | AppError::Sqlx(_)
            | AppError::Json(_)
            | AppError::Jwt(_)
            | AppError::Io(_)
            | AppError::Nats(_) => ErrorCode::Internal,
        }
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
    StaleLease,
    Internal,
}

#[derive(Debug, Serialize)]
pub struct ErrorBody {
    pub error: ErrorDetail,
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
            AppError::Unauthorized => StatusCode::UNAUTHORIZED,
            AppError::Forbidden(_) => StatusCode::FORBIDDEN,
            AppError::Conflict(_) => StatusCode::CONFLICT,
            AppError::Validation(_) => StatusCode::BAD_REQUEST,
            AppError::QueuePaused | AppError::QueueAtCapacity | AppError::StaleLease => {
                StatusCode::CONFLICT
            }
            _ => StatusCode::INTERNAL_SERVER_ERROR,
        };
        let request_id = Uuid::new_v4().to_string();
        let body = ErrorBody {
            error: ErrorDetail {
                code: self.code(),
                message: self.to_string(),
            },
            request_id: Some(request_id),
        };
        tracing::warn!(error = %self, code = ?body.error.code, "request error");
        (status, axum::Json(body)).into_response()
    }
}

pub type AppResult<T> = Result<T, AppError>;
