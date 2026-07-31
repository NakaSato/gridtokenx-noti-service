//! HTTP-level tests for the notification REST handlers (gap #9).
//!
//! These exercise the wiring the API surface depends on, in-process, with a
//! fake repository behind the orchestrator (no DB):
//!   - routing (method + path) for list / mark-read / mark-all-read,
//!   - RBAC via the `ServiceRole` extractor (`x-gridtokenx-role`),
//!   - identity via the `UserContext` extractor (`x-gridtokenx-user-id`),
//!   - status/body mapping, including the orchestrator-error → 500 path.
//!
//! The real router is built in the `noti-server` binary; this mirrors the same
//! `/api/v1/noti` routes against the public handlers and drives them via
//! `tower`'s `oneshot`.

#![allow(clippy::expect_used, clippy::unwrap_used)]

use std::sync::{Arc, Mutex};

use axum::body::{Body, to_bytes};
use axum::http::{Request, StatusCode};
use axum::routing::{get, patch, post};
use axum::Router;
use chrono::Utc;
use tower::ServiceExt; // oneshot

use noti_api::handlers::{
    list_notifications, mark_all_notifications_as_read, mark_notification_as_read,
};
use noti_api::AppState;
use noti_core::domain::{Notification, NotificationChannel, NotificationStatus};
use noti_core::error::{NotiError, Result};
use noti_core::traits::{NotificationProviderTrait, NotificationRepositoryTrait};
use noti_logic::NotificationOrchestrator;
use uuid::Uuid;

mod common;
use common::{
    ROLE_HEADER, StubCache, StubDeviceRepo, StubMq, StubProvider, StubTemplate, USER_HEADER,
};

// --- Fakes -----------------------------------------------------------------

/// Repository fake: serves canned list/unread data and records the
/// `mark_as_read` / `mark_all_as_read` calls the handlers make. `fail` flips
/// every method to an error to exercise the 500 path.
struct FakeRepo {
    notifications: Vec<Notification>,
    unread: i64,
    fail: bool,
    marked_read: Mutex<Vec<(Uuid, Uuid)>>,
    marked_all: Mutex<Vec<Uuid>>,
}

impl FakeRepo {
    fn new(notifications: Vec<Notification>, unread: i64) -> Self {
        Self {
            notifications,
            unread,
            fail: false,
            marked_read: Mutex::new(vec![]),
            marked_all: Mutex::new(vec![]),
        }
    }
    fn failing() -> Self {
        Self {
            notifications: vec![],
            unread: 0,
            fail: true,
            marked_read: Mutex::new(vec![]),
            marked_all: Mutex::new(vec![]),
        }
    }
}

#[async_trait::async_trait]
impl NotificationRepositoryTrait for FakeRepo {
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
    async fn increment_retry(
        &self,
        _id: Uuid,
        _next: chrono::DateTime<chrono::Utc>,
    ) -> Result<()> {
        Ok(())
    }
    async fn get_pending_for_retry(&self, _limit: i32) -> Result<Vec<Notification>> {
        Ok(vec![])
    }
    async fn reset_stuck_processing(&self, _threshold: i64) -> Result<u64> {
        Ok(0)
    }
    async fn list_by_user(&self, _u: Uuid, limit: i64, _offset: i64) -> Result<Vec<Notification>> {
        if self.fail {
            return Err(NotiError::Internal("boom".into()));
        }
        let take = usize::try_from(limit).unwrap_or(usize::MAX);
        Ok(self.notifications.iter().take(take).cloned().collect())
    }
    async fn mark_as_read(&self, id: Uuid, user: Uuid) -> Result<()> {
        if self.fail {
            return Err(NotiError::Internal("boom".into()));
        }
        self.marked_read.lock().unwrap().push((id, user));
        Ok(())
    }
    async fn mark_all_as_read(&self, user: Uuid) -> Result<()> {
        if self.fail {
            return Err(NotiError::Internal("boom".into()));
        }
        self.marked_all.lock().unwrap().push(user);
        Ok(())
    }
    async fn get_unread_count(&self, _user: Uuid) -> Result<i64> {
        if self.fail {
            return Err(NotiError::Internal("boom".into()));
        }
        Ok(self.unread)
    }
}


// --- Harness ---------------------------------------------------------------

/// Build the `/api/v1/noti` router around an orchestrator backed by `repo`.
/// Returns the router plus the repo handle so tests can assert recorded calls.
fn app(repo: Arc<FakeRepo>) -> Router {
    let provider: Arc<dyn NotificationProviderTrait> = Arc::new(StubProvider);
    let orchestrator = Arc::new(NotificationOrchestrator::new(
        repo,
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
        device_repo: Arc::new(StubDeviceRepo),
    };
    Router::new()
        .route("/api/v1/noti/", get(list_notifications))
        .route("/api/v1/noti/{id}", patch(mark_notification_as_read))
        .route("/api/v1/noti/read-all", post(mark_all_notifications_as_read))
        .with_state(state)
}

/// Issue `req` against a router backed by `repo`; return (status, JSON body).
async fn send(repo: Arc<FakeRepo>, req: Request<Body>) -> (StatusCode, serde_json::Value) {
    let resp = app(repo).oneshot(req).await.expect("router response");
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

/// A sample notification owned by `user`.
fn sample(user: Uuid) -> Notification {
    let now = Utc::now();
    Notification {
        id: Uuid::new_v4(),
        user_id: Some(user),
        channel: NotificationChannel::Email,
        status: NotificationStatus::Sent,
        recipient: "user@example.com".into(),
        template_id: "welcome".into(),
        variables: serde_json::json!({}),
        provider_id: None,
        provider_ref: None,
        retry_count: 0,
        next_retry_at: now,
        error_message: None,
        idempotency_key: None,
        created_at: now,
        updated_at: now,
        sent_at: Some(now),
        read_at: None,
    }
}

fn get_list(role: Option<&str>, user: Option<Uuid>, query: &str) -> Request<Body> {
    let mut b = Request::builder().method("GET").uri(format!("/api/v1/noti/{query}"));
    if let Some(r) = role {
        b = b.header(ROLE_HEADER, r);
    }
    if let Some(u) = user {
        b = b.header(USER_HEADER, u.to_string());
    }
    b.body(Body::empty()).expect("build request")
}

// --- Tests -----------------------------------------------------------------

#[tokio::test]
async fn list_returns_notifications_and_unread_count() {
    let user = Uuid::new_v4();
    let repo = Arc::new(FakeRepo::new(vec![sample(user), sample(user)], 5));
    let (status, body) = send(repo, get_list(Some("admin"), Some(user), "")).await;

    assert_eq!(status, StatusCode::OK);
    assert_eq!(body["total"], 2);
    assert_eq!(body["unread_count"], 5);
    assert_eq!(body["notifications"].as_array().unwrap().len(), 2);
}

/// The list is what the notification bell renders, so each row must carry the
/// display fields — the stored record alone has no title, body, or read flag.
#[tokio::test]
async fn list_projects_rows_to_the_client_wire_shape() {
    let user = Uuid::new_v4();
    let mut read_row = sample(user);
    read_row.read_at = Some(Utc::now());

    let repo = Arc::new(FakeRepo::new(vec![sample(user), read_row], 1));
    let (status, body) = send(repo, get_list(Some("admin"), Some(user), "")).await;

    assert_eq!(status, StatusCode::OK);
    let row = &body["notifications"][0];

    assert_eq!(
        row["type"], "EmailVerified",
        "rows carry the canonical event type, not the template name"
    );
    assert_eq!(row["title"], "Welcome", "no heading → humanized template stem");
    assert_eq!(row["message"], "body", "rendered text lands in `message`");
    assert_eq!(row["is_read"], serde_json::json!(false));
    assert_eq!(
        body["notifications"][1]["is_read"],
        serde_json::json!(true),
        "`is_read` mirrors `read_at`"
    );
    // The stored record is flattened in alongside the projection, so existing
    // consumers of the raw row keep working.
    assert_eq!(row["template_id"], "welcome");
    assert_eq!(row["channel"], "Email");
}

#[tokio::test]
async fn list_clamps_limit_to_100() {
    // 150 rows available, limit=999 must clamp to 100 (the handler's ceiling).
    let user = Uuid::new_v4();
    let many: Vec<_> = (0..150).map(|_| sample(user)).collect();
    let repo = Arc::new(FakeRepo::new(many, 0));
    let (status, body) = send(repo, get_list(Some("admin"), Some(user), "?limit=999")).await;

    assert_eq!(status, StatusCode::OK);
    assert_eq!(body["total"], 100, "limit must clamp to 100");
}

#[tokio::test]
async fn list_without_role_is_forbidden() {
    // User header present but no role → ServiceRole::Unknown → require_any 403.
    let user = Uuid::new_v4();
    let repo = Arc::new(FakeRepo::new(vec![], 0));
    let (status, _) = send(repo, get_list(None, Some(user), "")).await;
    assert_eq!(status, StatusCode::FORBIDDEN);
}

#[tokio::test]
async fn list_without_user_header_is_unauthorized() {
    let repo = Arc::new(FakeRepo::new(vec![], 0));
    let (status, _) = send(repo, get_list(Some("admin"), None, "")).await;
    assert_eq!(status, StatusCode::UNAUTHORIZED);
}

#[tokio::test]
async fn list_maps_repo_error_to_500() {
    let user = Uuid::new_v4();
    let repo = Arc::new(FakeRepo::failing());
    let (status, _) = send(repo, get_list(Some("admin"), Some(user), "")).await;
    assert_eq!(status, StatusCode::INTERNAL_SERVER_ERROR);
}

#[tokio::test]
async fn mark_one_as_read_routes_and_records() {
    let user = Uuid::new_v4();
    let id = Uuid::new_v4();
    let repo = Arc::new(FakeRepo::new(vec![], 0));
    let req = Request::builder()
        .method("PATCH")
        .uri(format!("/api/v1/noti/{id}"))
        .header(ROLE_HEADER, "admin")
        .header(USER_HEADER, user.to_string())
        .body(Body::empty())
        .expect("build request");
    let (status, body) = send(repo.clone(), req).await;

    assert_eq!(status, StatusCode::OK);
    assert_eq!(body["success"], true);
    let recorded = repo.marked_read.lock().unwrap();
    assert_eq!(recorded.as_slice(), &[(id, user)], "path id + user forwarded");
}

#[tokio::test]
async fn mark_all_as_read_routes_and_records() {
    let user = Uuid::new_v4();
    let repo = Arc::new(FakeRepo::new(vec![], 0));
    let req = Request::builder()
        .method("POST")
        .uri("/api/v1/noti/read-all")
        .header(ROLE_HEADER, "admin")
        .header(USER_HEADER, user.to_string())
        .body(Body::empty())
        .expect("build request");
    let (status, body) = send(repo.clone(), req).await;

    assert_eq!(status, StatusCode::OK);
    assert_eq!(body["success"], true);
    assert_eq!(repo.marked_all.lock().unwrap().as_slice(), &[user]);
}
