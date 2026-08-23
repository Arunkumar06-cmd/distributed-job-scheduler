use crate::extract::ApiJson;
use axum::{extract::State, http::StatusCode, Json};
use serde::{Deserialize, Serialize};
use validator::Validate;

use crate::auth::{create_token, hash_password, verify_kind, verify_password, TokenKind};
use crate::middleware::AuthUser;
use crate::state::AppState;
use common::{AppError, AppResult};
use db::queries;

#[derive(Debug, Deserialize, Validate)]
pub struct RegisterReq {
    #[validate(email, length(min = 3, max = 255))]
    pub email: String,
    #[validate(length(min = 8, max = 128))]
    pub password: String,
    #[validate(length(min = 1, max = 100))]
    pub display_name: String,
}

#[derive(Debug, Deserialize, Validate)]
pub struct LoginReq {
    #[validate(email)]
    pub email: String,
    #[validate(length(min = 1))]
    pub password: String,
}

#[derive(Debug, Deserialize)]
pub struct RefreshReq {
    pub refresh_token: String,
}

#[derive(Debug, Serialize)]
pub struct AuthResp {
    /// Backwards-compatible alias for the access token.
    pub token: String,
    pub access_token: String,
    pub refresh_token: String,
    pub user: serde_json::Value,
}

fn issue_pair(user_id: uuid::Uuid, email: &str, state: &AppState) -> AppResult<(String, String)> {
    let access =
        create_token(user_id, email, TokenKind::Access, &state.config).map_err(internal)?;
    let refresh =
        create_token(user_id, email, TokenKind::Refresh, &state.config).map_err(internal)?;
    Ok((access, refresh))
}

fn internal(e: impl std::fmt::Display) -> AppError {
    AppError::Internal(e.to_string())
}

pub async fn register(
    State(state): State<AppState>,
    ApiJson(req): crate::extract::ApiJson<RegisterReq>,
) -> AppResult<(StatusCode, Json<AuthResp>)> {
    req.validate()
        .map_err(|e| AppError::Validation(e.to_string()))?;
    let hash = hash_password(&req.password).map_err(internal)?;
    let user = queries::create_user(&state.pool, &req.email, &hash, &req.display_name).await?;
    let (access, refresh) = issue_pair(user.id, &user.email, &state)?;
    Ok((
        StatusCode::CREATED,
        Json(AuthResp {
            token: access.clone(),
            access_token: access,
            refresh_token: refresh,
            user: serde_json::json!({
                "id": user.id,
                "email": user.email,
                "display_name": user.display_name,
            }),
        }),
    ))
}

pub async fn login(
    State(state): State<AppState>,
    ApiJson(req): crate::extract::ApiJson<LoginReq>,
) -> AppResult<Json<AuthResp>> {
    req.validate()
        .map_err(|e| AppError::Validation(e.to_string()))?;
    let user = queries::find_user_by_email(&state.pool, &req.email)
        .await?
        .ok_or(AppError::Unauthorized)?;
    let ok = verify_password(&user.password_hash, &req.password).map_err(internal)?;
    if !ok {
        return Err(AppError::Unauthorized);
    }
    let (access, refresh) = issue_pair(user.id, &user.email, &state)?;
    Ok(Json(AuthResp {
        token: access.clone(),
        access_token: access,
        refresh_token: refresh,
        user: serde_json::json!({
            "id": user.id,
            "email": user.email,
            "display_name": user.display_name,
        }),
    }))
}

/// Rotate a refresh token into a fresh access+refresh pair. Old refresh tokens
/// stay valid until expiry (no reuse-detection store at this tier); the short
/// access TTL bounds any stolen-credential window to one hour.
pub async fn refresh(
    State(state): State<AppState>,
    ApiJson(req): crate::extract::ApiJson<RefreshReq>,
) -> AppResult<Json<AuthResp>> {
    let data = verify_kind(&req.refresh_token, TokenKind::Refresh, &state.config)
        .map_err(|_| AppError::Unauthorized)?;
    let user = queries::find_user_by_id(&state.pool, data.claims.sub)
        .await?
        .ok_or(AppError::Unauthorized)?;
    if !user.is_active {
        return Err(AppError::Unauthorized);
    }
    let (access, refresh) = issue_pair(user.id, &user.email, &state)?;
    Ok(Json(AuthResp {
        token: access.clone(),
        access_token: access,
        refresh_token: refresh,
        user: serde_json::json!({
            "id": user.id,
            "email": user.email,
            "display_name": user.display_name,
        }),
    }))
}

pub async fn me(
    auth: AuthUser,
    State(state): State<AppState>,
) -> AppResult<Json<serde_json::Value>> {
    let user = queries::find_user_by_id(&state.pool, auth.user_id)
        .await?
        .ok_or(AppError::NotFound("user not found".to_string()))?;
    Ok(Json(serde_json::json!({
        "id": user.id,
        "email": user.email,
        "display_name": user.display_name,
        "created_at": user.created_at,
    })))
}
