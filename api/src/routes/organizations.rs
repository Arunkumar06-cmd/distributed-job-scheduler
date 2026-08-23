use axum::{
    extract::{Path, State},
    http::StatusCode,
    Json,
};
use serde::{Deserialize, Serialize};
use crate::extract::ApiJson;
use validator::Validate;

use crate::middleware::AuthUser;
use crate::routes::validate::reject_control_chars;
use crate::state::AppState;
use common::{AppError, AppResult};
use db::queries;
use uuid::Uuid;

#[derive(Debug, Deserialize, Validate)]
pub struct CreateOrgReq {
    #[validate(length(min = 1, max = 100))]
    pub name: String,
    #[validate(length(min = 2, max = 50))]
    pub slug: String,
}

#[derive(Debug, Deserialize, Validate)]
pub struct UpsertMembershipReq {
    pub user_id: Uuid,
    #[validate(custom(function = "valid_org_role"))]
    pub role: String,
}

fn valid_org_role(role: &str) -> Result<(), validator::ValidationError> {
    if matches!(role, "admin" | "member" | "viewer") {
        Ok(())
    } else {
        Err(validator::ValidationError::new("invalid_role"))
    }
}

#[derive(Debug, Serialize)]
pub struct MembershipResponse {
    pub org_id: Uuid,
    pub user_id: Uuid,
    pub role: String,
}

pub async fn create(
    auth: AuthUser,
    State(state): State<AppState>,
    ApiJson(req): crate::extract::ApiJson<CreateOrgReq>,
) -> AppResult<(StatusCode, Json<serde_json::Value>)> {
    req.validate()
        .map_err(|e| AppError::Validation(e.to_string()))?;
    reject_control_chars("name", &req.name)?;
    reject_control_chars("slug", &req.slug)?;
    let slug = common::ids::normalize_slug(&req.slug);
    let org = queries::create_organization(&state.pool, &req.name, &slug, auth.user_id).await?;
    let _ = state.broadcast.send(format!("org.created:{}", org.id));
    Ok((StatusCode::CREATED, Json(serde_json::json!(org))))
}

pub async fn list(
    auth: AuthUser,
    State(state): State<AppState>,
) -> AppResult<Json<serde_json::Value>> {
    let orgs = queries::list_organizations_for_user(&state.pool, auth.user_id).await?;
    Ok(Json(serde_json::json!(orgs)))
}

pub async fn get(
    auth: AuthUser,
    State(state): State<AppState>,
    Path(org_id): Path<Uuid>,
) -> AppResult<Json<serde_json::Value>> {
    if !queries::user_in_org(&state.pool, auth.user_id, org_id).await? {
        return Err(AppError::Forbidden(
            "not a member of this organization".to_string(),
        ));
    }
    let org: Option<db::models::Organization> =
        sqlx::query_as("SELECT * FROM organizations WHERE id = $1")
            .bind(org_id)
            .fetch_optional(&state.pool)
            .await?;
    let org = org.ok_or_else(|| AppError::NotFound("organization not found".to_string()))?;
    Ok(Json(serde_json::json!(org)))
}

pub async fn upsert_membership(
    auth: AuthUser,
    State(state): State<AppState>,
    Path(org_id): Path<Uuid>,
    ApiJson(req): crate::extract::ApiJson<UpsertMembershipReq>,
) -> AppResult<Json<MembershipResponse>> {
    req.validate()
        .map_err(|error| AppError::Validation(error.to_string()))?;
    queries::require_org_admin(&state.pool, auth.user_id, org_id).await?;
    queries::find_user_by_id(&state.pool, req.user_id)
        .await?
        .ok_or_else(|| AppError::NotFound("user not found".to_string()))?;

    // Owner grants are immutable through this endpoint: ownership changes are a
    // deliberate transfer, and letting admins demote owners could leave an org
    // with no admin at all.
    let target_role: Option<(String,)> = sqlx::query_as(
        "SELECT role::text FROM org_memberships WHERE org_id = $1 AND user_id = $2",
    )
    .bind(org_id)
    .bind(req.user_id)
    .fetch_optional(&state.pool)
    .await?;
    if matches!(target_role.as_ref().map(|(r,)| r.as_str()), Some("owner")) {
        return Err(AppError::Forbidden(
            "cannot modify an organization owner's role via this endpoint".to_string(),
        ));
    }

    queries::upsert_org_membership(&state.pool, org_id, req.user_id, &req.role).await?;
    queries::append_audit(
        &state.pool,
        auth.user_id,
        Some(org_id),
        "org.membership.upsert",
        &req.user_id.to_string(),
        serde_json::json!({"role": req.role}),
    )
    .await?;
    Ok(Json(MembershipResponse {
        org_id,
        user_id: req.user_id,
        role: req.role,
    }))
}
