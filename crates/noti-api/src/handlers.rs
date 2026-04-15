//! Health check and metrics HTTP handlers.

use axum::http::StatusCode;
use axum::response::IntoResponse;
use axum::Json;
use serde_json::json;

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
