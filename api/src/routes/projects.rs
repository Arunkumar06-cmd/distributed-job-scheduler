use axum::{
    extract::{Path, Query, State},
    http::StatusCode,
    Json,
};
use serde::Deserialize;
use crate::extract::ApiJson;
use validator::Validate;

use crate::middleware::AuthUser;
use crate::routes::validate::{reject_control_chars};
use crate::state::AppState;
use common::{AppError, AppResult};
use db::queries;
use uuid::Uuid;

#[derive(Debug, Deserialize, Validate)]
pub struct CreateProjectReq {
    #[validate(length(min = 1, max = 100))]
    pub name: String,
    #[validate(length(min = 2, max = 50))]
    pub slug: String,
    pub description: Option<String>,
    pub org_id: Uuid,
}

pub async fn create(
    auth: AuthUser,
    State(state): State<AppState>,
    ApiJson(req): crate::extract::ApiJson<CreateProjectReq>,
) -> AppResult<(StatusCode, Json<serde_json::Value>)> {
    req.validate()
        .map_err(|e| AppError::Validation(e.to_string()))?;
    reject_control_chars("name", &req.name)?;
    reject_control_chars("description", req.description.as_deref().unwrap_or(""))?;
    queries::require_org_admin(&state.pool, auth.user_id, req.org_id).await?;
    let slug = common::ids::normalize_slug(&req.slug);
    let proj = queries::create_project(
        &state.pool,
        req.org_id,
        &req.name,
        &slug,
        req.description.as_deref().unwrap_or(""),
        auth.user_id,
    )
    .await?;
    Ok((StatusCode::CREATED, Json(serde_json::json!(proj))))
}

#[derive(Debug, Deserialize)]
pub struct ListProjectsQuery {
    pub org_id: Option<Uuid>,
}

pub async fn list(
    auth: AuthUser,
    State(state): State<AppState>,
    Query(q): Query<ListProjectsQuery>,
) -> AppResult<Json<serde_json::Value>> {
    if let Some(org_id) = q.org_id {
        if !queries::user_in_org(&state.pool, auth.user_id, org_id).await? {
            return Err(AppError::Forbidden("not a member".to_string()));
        }
        let projects = queries::list_projects_in_org(&state.pool, org_id).await?;
        Ok(Json(serde_json::json!(projects)))
    } else {
        // Single round trip for every org the user belongs to.
        let orgs = queries::list_organizations_for_user(&state.pool, auth.user_id).await?;
        let org_ids: Vec<Uuid> = orgs.iter().map(|o| o.id).collect();
        let projects = queries::list_projects_in_orgs(&state.pool, &org_ids).await?;
        Ok(Json(serde_json::json!(projects)))
    }
}

pub async fn get(
    auth: AuthUser,
    State(state): State<AppState>,
    Path(project_id): Path<Uuid>,
) -> AppResult<Json<serde_json::Value>> {
    let proj = queries::get_project(&state.pool, project_id)
        .await?
        .ok_or_else(|| AppError::NotFound("project not found".to_string()))?;
    if !queries::user_in_org(&state.pool, auth.user_id, proj.org_id).await? {
        return Err(AppError::Forbidden("not authorized".to_string()));
    }
    Ok(Json(serde_json::json!(proj)))
}
