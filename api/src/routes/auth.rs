use axum::{Json, extract::State, http::StatusCode};
use serde::{Deserialize, Serialize};
use validator::Validate;

use crate::auth::{create_token, hash_password, verify_password};
use crate::middleware::AuthUser;
use crate::state::AppState;
use common::{AppError, AppResult};
use db::queries;
use uuid::Uuid;

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

#[derive(Debug, Serialize)]
pub struct AuthResp {
    pub token: String,
    pub user: serde_json::Value,
}

pub async fn register(
    State(state): State<AppState>,
    Json(req): Json<RegisterReq>,
) -> AppResult<(StatusCode, Json<AuthResp>)> {
    req.validate().map_err(|e| AppError::Validation(e.to_string()))?;
    let hash = hash_password(&req.password).map_err(|e| AppError::Internal(e.to_string()))?;
    let user = queries::create_user(&state.pool, &req.email, &hash, &req.display_name)
        .await
        .map_err(|e| {
            if e.to_string().contains("duplicate") || e.to_string().contains("unique") {
                AppError::Conflict("email already registered".to_string())
            } else {
                e
            }
        })?;
    let token = create_token(user.id, &user.email, &state.config)
        .map_err(|e| AppError::Internal(e.to_string()))?;
    Ok((
        StatusCode::CREATED,
        Json(AuthResp {
            token,
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
    Json(req): Json<LoginReq>,
) -> AppResult<Json<AuthResp>> {
    req.validate().map_err(|e| AppError::Validation(e.to_string()))?;
    let user = queries::find_user_by_email(&state.pool, &req.email)
        .await?
        .ok_or(AppError::Unauthorized)?;
    let ok = verify_password(&user.password_hash, &req.password)
        .map_err(|e| AppError::Internal(e.to_string()))?;
    if !ok {
        return Err(AppError::Unauthorized);
    }
    let token = create_token(user.id, &user.email, &state.config)
        .map_err(|e| AppError::Internal(e.to_string()))?;
    Ok(Json(AuthResp {
        token,
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
