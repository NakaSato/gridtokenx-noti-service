//! HTTP-level tests for the push device-token REST handlers (closes gap #3).
//!
//! `rest_handlers.rs` covers the notification routes (list / mark-read /
//! mark-all-read) but never touches the device endpoints that back the Push /
//! FCM channel. Those three routes — `POST` / `GET` / `DELETE
//! /api/v1/noti/devices` — carry their own RBAC gate, identity extraction,
//! platform validation, and registry calls, all untested at the HTTP layer. A
//! regression (wrong method, a missing `UserContext` rejection, an accepted bad
//! platform, an unmapped registry error) would slip past the DB-level
//! `device_tokens_pg.rs` and the provider tests.
//!
//! This mirrors the same routes the `noti-server` binary wires
//! (`startup.rs`), against a recording device-registry fake, and drives them
//! in-process via `tower`'s `oneshot`. Asserts routing, the
//! `ServiceRole`/`UserContext` extractors, 400 on bad input, the
//! registry-error → 500 path, and that the handlers forward the right
//! (user, token, platform) to the registry.

#![allow(clippy::expect_used, clippy::unwrap_used)]

use std::sync::{Arc, Mutex};

use axum::Router;
use axum::body::{Body, to_bytes};
use axum::http::{Request, StatusCode};
use axum::routing::get;
use chrono::Utc;
use tower::ServiceExt; // oneshot
use uuid::Uuid;

use noti_api::AppState;
use noti_api::handlers::{list_devices, register_device, revoke_device};
use noti_core::domain::{DevicePlatform, DeviceToken, Notification, NotificationStatus};
use noti_core::error::{NotiError, Result};
use noti_core::traits::{
    DeviceTokenRepositoryTrait, NotificationProviderTrait, NotificationRepositoryTrait,
};
use noti_logic::NotificationOrchestrator;

mod common;
use common::{ROLE_HEADER, StubCache, StubMq, StubProvider, StubTemplate, USER_HEADER};

// --- Device registry fake ---------------------------------------------------

/// Records every `register` / `revoke` and serves canned `active_for_user`
/// data. `fail` flips every method to an error to exercise the 500 path.
struct FakeDeviceRepo {
    active: Vec<DeviceToken>,
    fail: bool,
    registered: Mutex<Vec<(Uuid, String, DevicePlatform)>>,
    revoked: Mutex<Vec<String>>,
}

impl FakeDeviceRepo {
    fn new(active: Vec<DeviceToken>) -> Self {
        Self {
            active,
            fail: false,
            registered: Mutex::new(vec![]),
            revoked: Mutex::new(vec![]),
        }
    }
    fn failing() -> Self {
        Self {
            active: vec![],
            fail: true,
            registered: Mutex::new(vec![]),
            revoked: Mutex::new(vec![]),
        }
    }
}

#[async_trait::async_trait]
impl DeviceTokenRepositoryTrait for FakeDeviceRepo {
    async fn register(
        &self,
        user_id: Uuid,
        token: &str,
        platform: DevicePlatform,
    ) -> Result<DeviceToken> {
        if self.fail {
            return Err(NotiError::Internal("boom".into()));
        }
        self.registered
            .lock()
            .unwrap()
            .push((user_id, token.to_string(), platform));
        Ok(DeviceToken {
            id: Uuid::new_v4(),
            user_id,
            token: token.to_string(),
            platform,
            created_at: Utc::now(),
            last_seen_at: Utc::now(),
        })
    }
    async fn active_for_user(&self, _user_id: Uuid) -> Result<Vec<DeviceToken>> {
        if self.fail {
            return Err(NotiError::Internal("boom".into()));
        }
        Ok(self.active.clone())
    }
    async fn revoke(&self, token: &str) -> Result<()> {
        if self.fail {
            return Err(NotiError::Internal("boom".into()));
        }
        self.revoked.lock().unwrap().push(token.to_string());
        Ok(())
    }
}

// --- Orchestrator stubs (device routes never touch these) -------------------

struct NoopRepo;
#[async_trait::async_trait]
impl NotificationRepositoryTrait for NoopRepo {
    async fn create(&self, n: &Notification) -> Result<Notification> {
        Ok(n.clone())
    }
    async fn get_by_id(&self, _id: Uuid) -> Result<Option<Notification>> {
        Ok(None)
    }
    async fn get_by_idempotency_key(&self, _key: &str) -> Result<Option<Notification>> {
        Ok(None)
    }
    async fn update_status(
        &self,
        _id: Uuid,
        _status: NotificationStatus,
        _error: Option<String>,
        _provider_ref: Option<String>,
    ) -> Result<()> {
        Ok(())
    }
    async fn increment_retry(&self, _id: Uuid, _next: chrono::DateTime<chrono::Utc>) -> Result<()> {
        Ok(())
    }
    async fn get_pending_for_retry(&self, _limit: i32) -> Result<Vec<Notification>> {
        Ok(vec![])
    }
    async fn reset_stuck_processing(&self, _threshold: i64) -> Result<u64> {
        Ok(0)
    }
    async fn list_by_user(&self, _u: Uuid, _limit: i64, _offset: i64) -> Result<Vec<Notification>> {
        Ok(vec![])
    }
    async fn mark_as_read(&self, _id: Uuid, _user: Uuid) -> Result<()> {
        Ok(())
    }
    async fn mark_all_as_read(&self, _user: Uuid) -> Result<()> {
        Ok(())
    }
    async fn get_unread_count(&self, _user: Uuid) -> Result<i64> {
        Ok(0)
    }
}

// --- Harness ----------------------------------------------------------------

/// Build the `/api/v1/noti/devices` router around `device_repo`.
fn app(device_repo: Arc<FakeDeviceRepo>) -> Router {
    let provider: Arc<dyn NotificationProviderTrait> = Arc::new(StubProvider);
    let orchestrator = Arc::new(NotificationOrchestrator::new(
        Arc::new(NoopRepo),
        Arc::new(StubTemplate),
        provider.clone(),
        provider.clone(),
        provider.clone(),
        provider.clone(),
        provider,
        Arc::new(StubCache),
        Some(Arc::new(StubMq)),
    ));
    let state = AppState {
        orchestrator,
        device_repo,
    };
    Router::new()
        .route(
            "/api/v1/noti/devices",
            get(list_devices).post(register_device),
        )
        .route("/api/v1/noti/devices/{token}", axum::routing::delete(revoke_device))
        .with_state(state)
}

async fn send(
    device_repo: Arc<FakeDeviceRepo>,
    req: Request<Body>,
) -> (StatusCode, serde_json::Value) {
    let resp = app(device_repo).oneshot(req).await.expect("router response");
    let status = resp.status();
    let bytes = to_bytes(resp.into_body(), usize::MAX)
        .await
        .expect("collect body");
    let json = if bytes.is_empty() {
        serde_json::Value::Null
    } else {
        serde_json::from_slice(&bytes).unwrap_or(serde_json::Value::Null)
    };
    (status, json)
}

fn device(user: Uuid, token: &str, platform: DevicePlatform) -> DeviceToken {
    DeviceToken {
        id: Uuid::new_v4(),
        user_id: user,
        token: token.to_string(),
        platform,
        created_at: Utc::now(),
        last_seen_at: Utc::now(),
    }
}

fn register_req(role: Option<&str>, user: Option<Uuid>, body: &serde_json::Value) -> Request<Body> {
    let mut b = Request::builder()
        .method("POST")
        .uri("/api/v1/noti/devices")
        .header("content-type", "application/json");
    if let Some(r) = role {
        b = b.header(ROLE_HEADER, r);
    }
    if let Some(u) = user {
        b = b.header(USER_HEADER, u.to_string());
    }
    b.body(Body::from(body.to_string())).expect("build request")
}

// --- Tests: register --------------------------------------------------------

#[tokio::test]
async fn register_device_succeeds_and_forwards_user_token_platform() {
    let user = Uuid::new_v4();
    let repo = Arc::new(FakeDeviceRepo::new(vec![]));
    let body = serde_json::json!({ "token": "tok-abc", "platform": "android" });
    let (status, json) = send(repo.clone(), register_req(Some("admin"), Some(user), &body)).await;

    assert_eq!(status, StatusCode::OK);
    assert_eq!(json["success"], true);
    assert!(json["device_id"].is_string(), "device_id returned");

    let recorded = repo.registered.lock().unwrap();
    assert_eq!(
        recorded.as_slice(),
        &[(user, "tok-abc".to_string(), DevicePlatform::Android)],
        "handler forwards user + token + parsed platform to the registry"
    );
}

#[tokio::test]
async fn register_device_accepts_uppercase_platform() {
    // Handler lowercases before `DevicePlatform::from_db`.
    let user = Uuid::new_v4();
    let repo = Arc::new(FakeDeviceRepo::new(vec![]));
    let body = serde_json::json!({ "token": "tok-1", "platform": "IOS" });
    let (status, _) = send(repo.clone(), register_req(Some("admin"), Some(user), &body)).await;

    assert_eq!(status, StatusCode::OK);
    assert_eq!(repo.registered.lock().unwrap()[0].2, DevicePlatform::Ios);
}

#[tokio::test]
async fn register_device_rejects_unknown_platform_400() {
    let user = Uuid::new_v4();
    let repo = Arc::new(FakeDeviceRepo::new(vec![]));
    let body = serde_json::json!({ "token": "tok-1", "platform": "blackberry" });
    let (status, _) = send(repo.clone(), register_req(Some("admin"), Some(user), &body)).await;

    assert_eq!(status, StatusCode::BAD_REQUEST);
    assert!(repo.registered.lock().unwrap().is_empty(), "nothing registered");
}

#[tokio::test]
async fn register_device_rejects_empty_token_400() {
    let user = Uuid::new_v4();
    let repo = Arc::new(FakeDeviceRepo::new(vec![]));
    let body = serde_json::json!({ "token": "   ", "platform": "web" });
    let (status, _) = send(repo, register_req(Some("admin"), Some(user), &body)).await;
    assert_eq!(status, StatusCode::BAD_REQUEST);
}

#[tokio::test]
async fn register_device_without_role_is_unauthorized() {
    let user = Uuid::new_v4();
    let repo = Arc::new(FakeDeviceRepo::new(vec![]));
    let body = serde_json::json!({ "token": "tok-1", "platform": "web" });
    let (status, _) = send(repo, register_req(None, Some(user), &body)).await;
    assert_eq!(status, StatusCode::UNAUTHORIZED);
}

#[tokio::test]
async fn register_device_without_user_header_is_unauthorized() {
    let repo = Arc::new(FakeDeviceRepo::new(vec![]));
    let body = serde_json::json!({ "token": "tok-1", "platform": "web" });
    let (status, _) = send(repo, register_req(Some("admin"), None, &body)).await;
    assert_eq!(status, StatusCode::UNAUTHORIZED);
}

#[tokio::test]
async fn register_device_maps_registry_error_to_500() {
    let user = Uuid::new_v4();
    let repo = Arc::new(FakeDeviceRepo::failing());
    let body = serde_json::json!({ "token": "tok-1", "platform": "web" });
    let (status, _) = send(repo, register_req(Some("admin"), Some(user), &body)).await;
    assert_eq!(status, StatusCode::INTERNAL_SERVER_ERROR);
}

// --- Tests: list ------------------------------------------------------------

#[tokio::test]
async fn list_devices_returns_active_tokens() {
    let user = Uuid::new_v4();
    let repo = Arc::new(FakeDeviceRepo::new(vec![
        device(user, "tok-a", DevicePlatform::Android),
        device(user, "tok-b", DevicePlatform::Web),
    ]));
    let req = Request::builder()
        .method("GET")
        .uri("/api/v1/noti/devices")
        .header(ROLE_HEADER, "admin")
        .header(USER_HEADER, user.to_string())
        .body(Body::empty())
        .expect("build request");
    let (status, json) = send(repo, req).await;

    assert_eq!(status, StatusCode::OK);
    let devices = json["devices"].as_array().expect("devices array");
    assert_eq!(devices.len(), 2);
    assert_eq!(devices[0]["token"], "tok-a");
}

#[tokio::test]
async fn list_devices_without_role_is_unauthorized() {
    let user = Uuid::new_v4();
    let repo = Arc::new(FakeDeviceRepo::new(vec![]));
    let req = Request::builder()
        .method("GET")
        .uri("/api/v1/noti/devices")
        .header(USER_HEADER, user.to_string())
        .body(Body::empty())
        .expect("build request");
    let (status, _) = send(repo, req).await;
    assert_eq!(status, StatusCode::UNAUTHORIZED);
}

// --- Tests: revoke ----------------------------------------------------------

#[tokio::test]
async fn revoke_device_routes_and_records_token() {
    let user = Uuid::new_v4();
    let repo = Arc::new(FakeDeviceRepo::new(vec![]));
    let req = Request::builder()
        .method("DELETE")
        .uri("/api/v1/noti/devices/tok-to-kill")
        .header(ROLE_HEADER, "admin")
        .header(USER_HEADER, user.to_string())
        .body(Body::empty())
        .expect("build request");
    let (status, json) = send(repo.clone(), req).await;

    assert_eq!(status, StatusCode::OK);
    assert_eq!(json["success"], true);
    assert_eq!(
        repo.revoked.lock().unwrap().as_slice(),
        &["tok-to-kill".to_string()],
        "path token forwarded to the registry revoke"
    );
}

#[tokio::test]
async fn revoke_device_without_role_is_unauthorized() {
    let user = Uuid::new_v4();
    let repo = Arc::new(FakeDeviceRepo::new(vec![]));
    let req = Request::builder()
        .method("DELETE")
        .uri("/api/v1/noti/devices/tok-x")
        .header(USER_HEADER, user.to_string())
        .body(Body::empty())
        .expect("build request");
    let (status, _) = send(repo.clone(), req).await;
    assert_eq!(status, StatusCode::UNAUTHORIZED);
    assert!(repo.revoked.lock().unwrap().is_empty(), "nothing revoked");
}
