//! Typed JSON body extractor that converts serde/limit rejections into the
//! standard error envelope. Without this, malformed bodies (bad JSON, depth
//! bombs, oversize payloads, wrong types) escape as plain-text rejections
//! with inconsistent status codes — a contract violation under attack.

use axum::{
    extract::{FromRequest, Request},
    Json,
};
use common::AppError;
use serde::de::DeserializeOwned;

pub struct ApiJson<T>(pub T);

#[async_trait::async_trait]
impl<S, T> FromRequest<S> for ApiJson<T>
where
    S: Send + Sync,
    T: DeserializeOwned,
{
    type Rejection = AppError;

    async fn from_request(req: Request, state: &S) -> Result<Self, Self::Rejection> {
        let Json(value) = Json::<T>::from_request(req, state)
            .await
            .map_err(map_rejection)?;
        Ok(ApiJson(value))
    }
}

fn map_rejection(err: axum::extract::rejection::JsonRejection) -> AppError {
    use axum::extract::rejection::JsonRejection::*;
    match err {
        JsonDataError(e) => {
            // Wrong types / out-of-range numbers inside a well-formed body.
            let msg = e.body_text();
            let detail = msg.rsplit(':').next().unwrap_or("invalid body").trim();
            AppError::Validation(format!("invalid body: {detail}"))
        }
        JsonSyntaxError(_) => {
            AppError::Validation("request body is not valid JSON".to_string())
        }
        MissingJsonContentType(_) => {
            AppError::Validation("Content-Type must be application/json".to_string())
        }
        // At this layer a body-read failure is almost always the
        // DefaultBodyLimit kicking in — report 413, never a 500.
        BytesRejection(_) => AppError::PayloadTooLarge,
        _ => AppError::Validation("invalid request body".to_string()),
    }
}
