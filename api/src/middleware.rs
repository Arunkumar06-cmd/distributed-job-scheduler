use async_trait::async_trait;
use axum::{extract::Request, middleware::Next, response::Response};
use uuid::Uuid;

use crate::auth::verify_token;
use crate::state::AppState;
use common::AppError;

#[derive(Debug, Clone)]
pub struct AuthUser {
    pub user_id: Uuid,
    pub email: String,
}

pub async fn auth_middleware(
    axum::extract::State(state): axum::extract::State<AppState>,
    mut req: Request,
    next: Next,
) -> Result<Response, AppError> {
    let auth = req
        .headers()
        .get("Authorization")
        .and_then(|v| v.to_str().ok())
        .ok_or(AppError::Unauthorized)?
        .to_string();
    let token = if let Some(s) = auth.strip_prefix("Bearer ") {
        s
    } else {
        &auth
    };
    let data = verify_token(token, &state.config).map_err(|_| AppError::Unauthorized)?;
    let user = AuthUser {
        user_id: data.claims.sub,
        email: data.claims.email,
    };
    req.extensions_mut().insert(user);
    Ok(next.run(req).await)
}

#[async_trait]
impl axum::extract::FromRequestParts<AppState> for AuthUser {
    type Rejection = AppError;

    async fn from_request_parts(
        parts: &mut axum::http::request::Parts,
        state: &AppState,
    ) -> Result<Self, Self::Rejection> {
        if let Some(user) = parts.extensions.get::<AuthUser>() {
            return Ok(user.clone());
        }
        let auth = parts
            .headers
            .get("Authorization")
            .and_then(|v| v.to_str().ok())
            .ok_or(AppError::Unauthorized)?;
        let token = if let Some(s) = auth.strip_prefix("Bearer ") {
            s
        } else {
            auth
        };
        let data = verify_token(token, &state.config).map_err(|_| AppError::Unauthorized)?;
        Ok(AuthUser {
            user_id: data.claims.sub,
            email: data.claims.email,
        })
    }
}
