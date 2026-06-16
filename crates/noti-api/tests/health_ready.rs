//! HTTP-level tests for the `/health/ready` readiness probe (gap #3).
//!
//! `KafkaConsumerHealth::is_ready` is unit-tested in noti-core; this verifies
//! the *wiring* the deploy depends on: the route is registered for GET, the
//! `Extension<Arc<KafkaConsumerHealth>>` extractor resolves, and the handler
//! maps consumer state to the right HTTP status + JSON body. A regression here
//! (wrong method, missing layer, inverted status) is exactly what slips past a
//! pure unit test yet breaks Kubernetes readiness gating.
//!
//! The real router is assembled in the `noti-server` binary, so it can't be
//! imported; this mirrors the same route + Extension layer wiring against the
//! public handler and drives it in-process via `tower`'s `oneshot`.

#![allow(clippy::expect_used, clippy::unwrap_used)]

use std::sync::Arc;

use axum::Router;
use axum::body::{Body, to_bytes};
use axum::extract::Extension;
use axum::http::{Request, StatusCode};
use axum::routing::get;
use tower::ServiceExt; // oneshot

use noti_api::handlers::health_ready;
use noti_core::health::{KafkaConsumerHealth, unix_now_secs};

/// Build the router exactly as `noti-server` wires this route.
fn app(health: Arc<KafkaConsumerHealth>) -> Router {
    Router::new()
        .route("/health/ready", get(health_ready))
        .layer(Extension(health))
}

/// GET `/health/ready` and return (status, parsed-JSON body).
async fn probe(health: Arc<KafkaConsumerHealth>) -> (StatusCode, serde_json::Value) {
    let resp = app(health)
        .oneshot(
            Request::builder()
                .uri("/health/ready")
                .body(Body::empty())
                .expect("build request"),
        )
        .await
        .expect("router response");
    let status = resp.status();
    let bytes = to_bytes(resp.into_body(), usize::MAX)
        .await
        .expect("collect body");
    let json = serde_json::from_slice(&bytes).expect("body is JSON");
    (status, json)
}

#[tokio::test]
async fn fresh_consumer_is_not_ready_503() {
    // Never polled → not connected → probe must report 503 so nothing routes
    // traffic to a process whose consumer never came up.
    let health = Arc::new(KafkaConsumerHealth::new());
    let (status, body) = probe(health).await;
    assert_eq!(status, StatusCode::SERVICE_UNAVAILABLE);
    assert_eq!(body["status"], "not_ready");
    assert_eq!(body["kafka_consumer"], "down_or_stale");
}

#[tokio::test]
async fn connected_consumer_is_ready_200() {
    // Fresh progress at wall-clock now → within the staleness window → 200.
    let health = Arc::new(KafkaConsumerHealth::new());
    health.mark_progress(unix_now_secs());
    let (status, body) = probe(health).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(body["status"], "ready");
    assert_eq!(body["kafka_consumer"], "connected");
}

#[tokio::test]
async fn lost_connection_flips_back_to_503() {
    // Was healthy, then the broker dropped → readiness must revert to 503.
    let health = Arc::new(KafkaConsumerHealth::new());
    health.mark_progress(unix_now_secs());
    health.mark_down();
    let (status, body) = probe(health).await;
    assert_eq!(status, StatusCode::SERVICE_UNAVAILABLE);
    assert_eq!(body["status"], "not_ready");
}

#[tokio::test]
async fn disabled_consumer_reports_ready_200() {
    // No brokers configured → consumer intentionally off → readiness is OK
    // (there's no consumer whose liveness we'd be lying about).
    let health = Arc::new(KafkaConsumerHealth::new());
    health.mark_disabled();
    let (status, body) = probe(health).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(body["status"], "ready");
}
