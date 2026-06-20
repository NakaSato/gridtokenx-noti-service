//! Service-level tests for the `ConnectRPC` / gRPC `NotificationGrpcService` (gap #10).
//!
//! These call the two RPC methods directly against the orchestrator (fake repo,
//! no network, no DB), covering the translation layer the wire contract depends
//! on — exactly what a unit test of the orchestrator alone can't see:
//!   - proto `Channel` enum → domain `NotificationChannel` mapping,
//!   - empty-vs-set `user_id` / `idempotency_key` handling,
//!   - `variables_json` parse + error → `invalid_argument`,
//!   - `Uuid` parse errors → `invalid_argument`,
//!   - status read-back: found → `NotificationStatusResponse`, missing →
//!     `not_found`, malformed id → `invalid_argument`.
//!
//! Request views are built via `OwnedView::from_owned` (encode→decode round
//! trip) from the generated owned message, mirroring what the transport hands
//! the handler at runtime.

#![allow(clippy::expect_used, clippy::unwrap_used)]

use std::sync::{Arc, Mutex};

use axum::http::{HeaderMap, HeaderValue};
use buffa::EnumValue;
use buffa::view::OwnedView;
use connectrpc::error::ErrorCode;
use connectrpc::response::RequestContext;
use jsonwebtoken::{EncodingKey, Header, encode};
use serde::Serialize;

use noti_api::grpc::NotificationGrpcService;
use noti_core::domain::{Notification, NotificationChannel, NotificationStatus};
use noti_core::error::{NotiError, Result};
use noti_core::traits::{NotificationProviderTrait, NotificationRepositoryTrait};
use noti_logic::NotificationOrchestrator;
use noti_protocol::noti::{
    Channel, GetNotificationStatusRequest, GetNotificationStatusRequestView, NotificationService,
    SendNotificationRequest, SendNotificationRequestView,
};
use uuid::Uuid;

mod common;
use common::{StubCache, StubMq, StubProvider, StubTemplate};

// --- Fakes -----------------------------------------------------------------

/// Repository fake: records each `create` so tests can assert the orchestrator
/// persisted the field-mapped notification, and serves a canned row from
/// `get_by_id`. `fail` flips relevant methods to errors for the 500 path.
struct RecordingRepo {
    created: Mutex<Vec<Notification>>,
    stored: Option<Notification>,
    fail: bool,
}

impl RecordingRepo {
    fn new() -> Self {
        Self {
            created: Mutex::new(vec![]),
            stored: None,
            fail: false,
        }
    }
    fn with_stored(n: Notification) -> Self {
        Self {
            created: Mutex::new(vec![]),
            stored: Some(n),
            fail: false,
        }
    }
}

#[async_trait::async_trait]
impl NotificationRepositoryTrait for RecordingRepo {
    async fn create(&self, n: &Notification) -> Result<Notification> {
        if self.fail {
            return Err(NotiError::Internal("boom".into()));
        }
        self.created.lock().unwrap().push(n.clone());
        Ok(n.clone())
    }
    async fn get_by_id(&self, _id: Uuid) -> Result<Option<Notification>> {
        if self.fail {
            return Err(NotiError::Internal("boom".into()));
        }
        Ok(self.stored.clone())
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

// --- Harness ---------------------------------------------------------------

const JWT_SECRET: &str = "grpc-service-test-secret-please-ignore";

#[derive(Serialize)]
struct Claims {
    sub: String,
    exp: usize,
}

/// A `RequestContext` carrying a valid `authorization: Bearer <jwt>` header, so
/// the handlers' auth gate admits the call and the translation-layer assertions
/// below run. Auth rejection itself is covered by `unauthenticated_context_*`.
fn authed_ctx() -> RequestContext {
    let claims = Claims {
        sub: Uuid::new_v4().to_string(),
        exp: 9_999_999_999,
    };
    let token = encode(
        &Header::default(),
        &claims,
        &EncodingKey::from_secret(JWT_SECRET.as_bytes()),
    )
    .expect("encode jwt");

    let mut headers = HeaderMap::new();
    headers.insert(
        "authorization",
        HeaderValue::from_str(&format!("Bearer {token}")).expect("header value"),
    );
    RequestContext::new(headers)
}

/// Build the gRPC service around an orchestrator backed by `repo`.
fn service(repo: Arc<RecordingRepo>) -> NotificationGrpcService {
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
    NotificationGrpcService::new(orchestrator, JWT_SECRET.to_string())
}

/// Round-trip an owned `SendNotificationRequest` into the borrowing view the
/// handler receives at runtime.
fn send_view(req: &SendNotificationRequest) -> OwnedView<SendNotificationRequestView<'static>> {
    OwnedView::from_owned(req).expect("encode/decode SendNotificationRequest")
}

fn status_view(
    req: &GetNotificationStatusRequest,
) -> OwnedView<GetNotificationStatusRequestView<'static>> {
    OwnedView::from_owned(req).expect("encode/decode GetNotificationStatusRequest")
}

/// A sample stored notification.
fn sample() -> Notification {
    let now = chrono::Utc::now();
    Notification {
        id: Uuid::new_v4(),
        user_id: Some(Uuid::new_v4()),
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

// --- auth gate --------------------------------------------------------------

#[tokio::test]
async fn unauthenticated_context_is_rejected_for_send() {
    let repo = Arc::new(RecordingRepo::new());
    let svc = service(repo.clone());

    let req = SendNotificationRequest {
        channel: EnumValue::Known(Channel::EMAIL),
        recipient: "user@example.com".into(),
        template_id: "welcome".into(),
        ..Default::default()
    };
    // Default context → no authorization header.
    let err = svc
        .send_notification(RequestContext::default(), send_view(&req))
        .await
        .expect_err("a call with no bearer token must be rejected");

    assert_eq!(err.code, ErrorCode::Unauthenticated);
    assert!(
        repo.created.lock().unwrap().is_empty(),
        "auth gate runs before the orchestrator — nothing persisted"
    );
}

#[tokio::test]
async fn unauthenticated_context_is_rejected_for_status() {
    let repo = Arc::new(RecordingRepo::with_stored(sample()));
    let svc = service(repo);

    let req = GetNotificationStatusRequest {
        notification_id: Uuid::new_v4().to_string(),
        ..Default::default()
    };
    let err = svc
        .get_notification_status(RequestContext::default(), status_view(&req))
        .await
        .expect_err("status read must also require auth");

    assert_eq!(err.code, ErrorCode::Unauthenticated);
}

// --- send_notification -----------------------------------------------------

#[tokio::test]
async fn send_notification_queues_and_maps_fields() {
    let repo = Arc::new(RecordingRepo::new());
    let svc = service(repo.clone());

    let req = SendNotificationRequest {
        user_id: Uuid::new_v4().to_string(),
        channel: EnumValue::Known(Channel::SMS),
        recipient: "+15550001111".into(),
        template_id: "order_matched".into(),
        variables_json: r#"{"amount":42}"#.into(),
        idempotency_key: "idem-key-1".into(),
        ..Default::default()
    };
    let resp = svc
        .send_notification(authed_ctx(), send_view(&req))
        .await
        .expect("send_notification ok");

    assert_eq!(resp.body.status, "accepted");
    let returned = Uuid::parse_str(&resp.body.notification_id).expect("valid uuid in response");

    let created = repo.created.lock().unwrap();
    assert_eq!(created.len(), 1, "exactly one notification persisted");
    let n = &created[0];
    assert_eq!(n.id, returned, "response id matches persisted row");
    assert!(
        matches!(n.channel, NotificationChannel::Sms),
        "SMS proto → domain Sms"
    );
    assert_eq!(n.recipient, "+15550001111");
    assert_eq!(n.template_id, "order_matched");
    assert_eq!(n.variables, serde_json::json!({"amount": 42}));
    assert_eq!(n.idempotency_key.as_deref(), Some("idem-key-1"));
    assert!(n.user_id.is_some());
}

#[tokio::test]
async fn send_notification_empty_user_and_key_are_none() {
    let repo = Arc::new(RecordingRepo::new());
    let svc = service(repo.clone());

    // user_id + idempotency_key left empty → must persist as NULL/None.
    let req = SendNotificationRequest {
        channel: EnumValue::Known(Channel::EMAIL),
        recipient: "user@example.com".into(),
        template_id: "welcome".into(),
        ..Default::default()
    };
    svc.send_notification(authed_ctx(), send_view(&req))
        .await
        .expect("send_notification ok");

    let created = repo.created.lock().unwrap();
    let n = &created[0];
    assert_eq!(n.user_id, None, "empty user_id → None");
    assert_eq!(n.idempotency_key, None, "empty idempotency_key → None");
    assert!(matches!(n.channel, NotificationChannel::Email));
}

#[tokio::test]
async fn send_notification_invalid_variables_json_is_invalid_argument() {
    let repo = Arc::new(RecordingRepo::new());
    let svc = service(repo.clone());

    let req = SendNotificationRequest {
        channel: EnumValue::Known(Channel::EMAIL),
        recipient: "user@example.com".into(),
        template_id: "welcome".into(),
        variables_json: "{not json".into(),
        ..Default::default()
    };
    let err = svc
        .send_notification(authed_ctx(), send_view(&req))
        .await
        .expect_err("malformed variables_json must be rejected");

    assert_eq!(err.code, ErrorCode::InvalidArgument);
    assert!(repo.created.lock().unwrap().is_empty(), "nothing persisted");
}

#[tokio::test]
async fn send_notification_invalid_user_id_is_invalid_argument() {
    let repo = Arc::new(RecordingRepo::new());
    let svc = service(repo.clone());

    let req = SendNotificationRequest {
        user_id: "not-a-uuid".into(),
        channel: EnumValue::Known(Channel::EMAIL),
        recipient: "user@example.com".into(),
        template_id: "welcome".into(),
        ..Default::default()
    };
    let err = svc
        .send_notification(authed_ctx(), send_view(&req))
        .await
        .expect_err("malformed user_id must be rejected");

    assert_eq!(err.code, ErrorCode::InvalidArgument);
    assert!(repo.created.lock().unwrap().is_empty(), "nothing persisted");
}

// --- get_notification_status -----------------------------------------------

#[tokio::test]
async fn get_status_returns_stored_notification() {
    let stored = sample();
    let id = stored.id;
    let repo = Arc::new(RecordingRepo::with_stored(stored));
    let svc = service(repo);

    let req = GetNotificationStatusRequest {
        notification_id: id.to_string(),
        ..Default::default()
    };
    let resp = svc
        .get_notification_status(authed_ctx(), status_view(&req))
        .await
        .expect("get_status ok");

    assert_eq!(resp.body.notification_id, id.to_string());
    assert_eq!(resp.body.status, "sent", "status formatted lowercase");
    assert!(!resp.body.sent_at.is_empty(), "sent_at rendered as rfc3339");
}

#[tokio::test]
async fn get_status_missing_is_not_found() {
    let repo = Arc::new(RecordingRepo::new()); // stored = None
    let svc = service(repo);

    let req = GetNotificationStatusRequest {
        notification_id: Uuid::new_v4().to_string(),
        ..Default::default()
    };
    let err = svc
        .get_notification_status(authed_ctx(), status_view(&req))
        .await
        .expect_err("absent notification must be not_found");

    assert_eq!(err.code, ErrorCode::NotFound);
}

#[tokio::test]
async fn get_status_invalid_id_is_invalid_argument() {
    let repo = Arc::new(RecordingRepo::new());
    let svc = service(repo);

    let req = GetNotificationStatusRequest {
        notification_id: "not-a-uuid".into(),
        ..Default::default()
    };
    let err = svc
        .get_notification_status(authed_ctx(), status_view(&req))
        .await
        .expect_err("malformed id must be rejected");

    assert_eq!(err.code, ErrorCode::InvalidArgument);
}
