//! Health check and metrics HTTP handlers.

use axum::Json;
use axum::extract::{Path, Query, State};
use axum::http::StatusCode;
use axum::response::IntoResponse;
use gridtokenx_blockchain_core::auth::ServiceRole;
use serde::{Deserialize, Serialize};
use serde_json::json;
use uuid::Uuid;

use crate::AppState;
use crate::auth::UserContext;

pub async fn health_check() -> impl IntoResponse {
    (
        StatusCode::OK,
        Json(json!({
            "status": "ok",
            "service": "gridtokenx-noti"
        })),
    )
}

pub async fn health_live() -> impl IntoResponse {
    (StatusCode::OK, Json(json!({ "status": "alive" })))
}

// =============================================================================
// Notification REST Handlers (Modernized)
// =============================================================================

#[derive(Debug, Deserialize)]
pub struct ListNotificationsParams {
    pub limit: Option<i64>,
    pub offset: Option<i64>,
}

#[derive(Debug, Serialize)]
pub struct ListNotificationsResponse {
    pub notifications: Vec<noti_core::domain::Notification>,
    pub unread_count: i64,
    pub total: usize,
}

pub async fn list_notifications(
    role: ServiceRole,
    user: UserContext,
    State(state): State<AppState>,
    Query(params): Query<ListNotificationsParams>,
) -> Result<Json<ListNotificationsResponse>, (StatusCode, String)> {
    role.require_any(&[ServiceRole::ApiGateway, ServiceRole::Admin])
        .map_err(|(_code, msg)| (StatusCode::UNAUTHORIZED, msg.to_string()))?;

    let limit = params.limit.unwrap_or(20);
    let offset = params.offset.unwrap_or(0);

    let notifications = state
        .orchestrator
        .list_user_notifications(user.user_id, limit, offset)
        .await
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;

    let unread_count = state
        .orchestrator
        .get_unread_count(user.user_id)
        .await
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;

    let total = notifications.len();

    Ok(Json(ListNotificationsResponse {
        notifications,
        unread_count,
        total,
    }))
}

pub async fn mark_notification_as_read(
    role: ServiceRole,
    user: UserContext,
    State(state): State<AppState>,
    Path(id): Path<Uuid>,
) -> Result<Json<serde_json::Value>, (StatusCode, String)> {
    role.require_any(&[ServiceRole::ApiGateway, ServiceRole::Admin])
        .map_err(|(_code, msg)| (StatusCode::UNAUTHORIZED, msg.to_string()))?;

    state
        .orchestrator
        .mark_as_read(id, user.user_id)
        .await
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;

    Ok(Json(json!({ "success": true })))
}

pub async fn mark_all_notifications_as_read(
    role: ServiceRole,
    user: UserContext,
    State(state): State<AppState>,
) -> Result<Json<serde_json::Value>, (StatusCode, String)> {
    role.require_any(&[ServiceRole::ApiGateway, ServiceRole::Admin])
        .map_err(|(_code, msg)| (StatusCode::UNAUTHORIZED, msg.to_string()))?;

    state
        .orchestrator
        .mark_all_as_read(user.user_id)
        .await
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;

    Ok(Json(json!({ "success": true })))
}
