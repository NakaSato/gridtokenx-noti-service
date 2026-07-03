//! Health check and metrics HTTP handlers.

use std::sync::Arc;

use axum::Json;
use axum::extract::{Extension, Path, Query, State};
use axum::http::StatusCode;
use axum::response::IntoResponse;
use gridtokenx_blockchain_core::auth::ServiceRole;
use noti_core::domain::DevicePlatform;
use noti_core::health::{KafkaConsumerHealth, unix_now_secs};
use serde::{Deserialize, Serialize};
use serde_json::json;
use uuid::Uuid;

use crate::AppState;
use crate::auth::UserContext;

/// Report the consumer not-ready once progress is older than this. Comfortably
/// above the consumer's idle heartbeat tick so a quiet topic never trips it.
const READY_STALE_AFTER_SECS: i64 = 90;

#[allow(clippy::unused_async)]
pub async fn health_check() -> impl IntoResponse {
    (
        StatusCode::OK,
        Json(json!({
            "status": "ok",
            "service": "gridtokenx-noti"
        })),
    )
}

#[allow(clippy::unused_async)]
pub async fn health_live() -> impl IntoResponse {
    (StatusCode::OK, Json(json!({ "status": "alive" })))
}

/// Readiness probe: reflects background Kafka consumer liveness so a wedged or
/// disconnected consumer is reported `503`, instead of the process looking
/// healthy while it silently drops events.
#[allow(clippy::unused_async)]
pub async fn health_ready(
    Extension(health): Extension<Arc<KafkaConsumerHealth>>,
) -> impl IntoResponse {
    let now = unix_now_secs();
    if health.is_ready(now, READY_STALE_AFTER_SECS) {
        (
            StatusCode::OK,
            Json(json!({ "status": "ready", "kafka_consumer": "connected" })),
        )
    } else {
        (
            StatusCode::SERVICE_UNAVAILABLE,
            Json(json!({
                "status": "not_ready",
                "kafka_consumer": "down_or_stale",
                "last_progress_secs_ago": now.saturating_sub(health.last_progress_secs()),
            })),
        )
    }
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

/// # Errors
///
/// Returns an error if the user is unauthorized or the orchestrator call fails.
pub async fn list_notifications(
    role: ServiceRole,
    user: UserContext,
    State(state): State<AppState>,
    Query(params): Query<ListNotificationsParams>,
) -> Result<Json<ListNotificationsResponse>, (StatusCode, String)> {
    role.require_any(&[ServiceRole::ApiGateway, ServiceRole::Admin])
        .map_err(|(_code, msg)| (StatusCode::FORBIDDEN, msg.to_string()))?;

    let limit = params.limit.unwrap_or(20).clamp(1, 100);
    let offset = params.offset.unwrap_or(0).max(0);

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

/// # Errors
///
/// Returns an error if the user is unauthorized or the orchestrator call fails.
pub async fn mark_notification_as_read(
    role: ServiceRole,
    user: UserContext,
    State(state): State<AppState>,
    Path(id): Path<Uuid>,
) -> Result<Json<serde_json::Value>, (StatusCode, String)> {
    role.require_any(&[ServiceRole::ApiGateway, ServiceRole::Admin])
        .map_err(|(_code, msg)| (StatusCode::FORBIDDEN, msg.to_string()))?;

    state
        .orchestrator
        .mark_as_read(id, user.user_id)
        .await
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;

    Ok(Json(json!({ "success": true })))
}

/// # Errors
///
/// Returns an error if the user is unauthorized or the orchestrator call fails.
pub async fn mark_all_notifications_as_read(
    role: ServiceRole,
    user: UserContext,
    State(state): State<AppState>,
) -> Result<Json<serde_json::Value>, (StatusCode, String)> {
    role.require_any(&[ServiceRole::ApiGateway, ServiceRole::Admin])
        .map_err(|(_code, msg)| (StatusCode::FORBIDDEN, msg.to_string()))?;

    state
        .orchestrator
        .mark_all_as_read(user.user_id)
        .await
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;

    Ok(Json(json!({ "success": true })))
}

// =============================================================================
// Push Device-Token Registration (Push / FCM channel)
// =============================================================================

#[derive(Debug, Deserialize)]
pub struct RegisterDeviceRequest {
    pub token: String,
    /// One of `android`, `ios`, `web`.
    pub platform: String,
}

/// Register (or reactivate) a push device token for the authenticated user.
///
/// The mobile/web client calls this on first launch and whenever FCM rotates
/// its token.
///
/// # Errors
///
/// Returns `400` for an unknown platform, `401` if unauthorized, `500` if the
/// registry write fails.
pub async fn register_device(
    role: ServiceRole,
    user: UserContext,
    State(state): State<AppState>,
    Json(req): Json<RegisterDeviceRequest>,
) -> Result<Json<serde_json::Value>, (StatusCode, String)> {
    role.require_any(&[ServiceRole::ApiGateway, ServiceRole::Admin])
        .map_err(|(_code, msg)| (StatusCode::FORBIDDEN, msg.to_string()))?;

    let platform = DevicePlatform::from_db(&req.platform.to_lowercase()).ok_or((
        StatusCode::BAD_REQUEST,
        "platform must be one of: android, ios, web".to_string(),
    ))?;

    if req.token.trim().is_empty() {
        return Err((StatusCode::BAD_REQUEST, "token must not be empty".to_string()));
    }

    let device = state
        .device_repo
        .register(user.user_id, &req.token, platform)
        .await
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;

    Ok(Json(json!({ "success": true, "device_id": device.id })))
}

/// Revoke a push device token (logout / unregister).
///
/// # Errors
///
/// Returns `401` if unauthorized, `500` if the registry write fails.
pub async fn revoke_device(
    role: ServiceRole,
    _user: UserContext,
    State(state): State<AppState>,
    Path(token): Path<String>,
) -> Result<Json<serde_json::Value>, (StatusCode, String)> {
    role.require_any(&[ServiceRole::ApiGateway, ServiceRole::Admin])
        .map_err(|(_code, msg)| (StatusCode::FORBIDDEN, msg.to_string()))?;

    state
        .device_repo
        .revoke(&token)
        .await
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;

    Ok(Json(json!({ "success": true })))
}

#[derive(Debug, Serialize)]
pub struct ListDevicesResponse {
    pub devices: Vec<noti_core::domain::DeviceToken>,
}

/// List the authenticated user's active push device tokens.
///
/// # Errors
///
/// Returns `401` if unauthorized, `500` if the registry read fails.
pub async fn list_devices(
    role: ServiceRole,
    user: UserContext,
    State(state): State<AppState>,
) -> Result<Json<ListDevicesResponse>, (StatusCode, String)> {
    role.require_any(&[ServiceRole::ApiGateway, ServiceRole::Admin])
        .map_err(|(_code, msg)| (StatusCode::FORBIDDEN, msg.to_string()))?;

    let devices = state
        .device_repo
        .active_for_user(user.user_id)
        .await
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;

    Ok(Json(ListDevicesResponse { devices }))
}
