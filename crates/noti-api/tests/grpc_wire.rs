//! Real-wire end-to-end test for the `ConnectRPC` / gRPC service (closes gap #5).
//!
//! `grpc_service.rs` calls the two RPC methods directly against the handler — it
//! proves the field-mapping translation layer but never touches the network.
//! This binds the *production* router (`connectrpc::Router` → `register` →
//! `into_axum_router`, exactly as `startup::run` assembles it) on an ephemeral
//! TCP port, then drives it with the generated `NotificationServiceClient` over
//! real **gRPC / HTTP-2 (h2c)** — the same transport, codec, and method routing
//! a live caller uses. A regression in the wire layer (codec, path, HTTP/2
//! framing, status-code mapping) that the in-process test cannot see fails here.

#![allow(clippy::expect_used, clippy::unwrap_used)]

use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use buffa::EnumValue;

use connectrpc::Protocol;
use connectrpc::client::{ClientConfig, Http2Connection};
use connectrpc::error::ErrorCode;
use jsonwebtoken::{EncodingKey, Header, encode};
use serde::Serialize;

use noti_api::grpc::NotificationGrpcService;
use noti_core::domain::{Notification, NotificationChannel, NotificationStatus};
use noti_core::error::Result;
use noti_core::traits::{NotificationProviderTrait, NotificationRepositoryTrait};
use noti_logic::NotificationOrchestrator;

mod common;
use common::{StubCache, StubMq, StubProvider, StubTemplate};
use noti_protocol::noti::{
    Channel, GetNotificationStatusRequest, NotificationServiceClient, NotificationServiceExt,
    SendNotificationRequest,
};
use uuid::Uuid;

// --- Fakes (no DB / broker; the point is the wire, not persistence) ---------

struct RecordingRepo {
    created: Mutex<Vec<Notification>>,
    stored: Option<Notification>,
}

#[async_trait]
impl NotificationRepositoryTrait for RecordingRepo {
    async fn create(&self, n: &Notification) -> Result<Notification> {
        self.created.lock().unwrap().push(n.clone());
        Ok(n.clone())
    }
    async fn get_by_id(&self, _id: Uuid) -> Result<Option<Notification>> {
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

fn stored_sample(id: Uuid) -> Notification {
    let now = chrono::Utc::now();
    Notification {
        id,
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

const JWT_SECRET: &str = "grpc-wire-test-secret-please-ignore";

#[derive(Serialize)]
struct Claims {
    sub: String,
    exp: usize,
}

/// Mint a valid HS256 bearer token (far-future `exp`) for `subject`.
fn mint_token(subject: &str) -> String {
    let claims = Claims {
        sub: subject.to_string(),
        exp: 9_999_999_999,
    };
    encode(
        &Header::default(),
        &claims,
        &EncodingKey::from_secret(JWT_SECRET.as_bytes()),
    )
    .expect("encode jwt")
}

/// Assemble the production gRPC router (mirrors `startup::run`, including the
/// JWT-gated `NotificationGrpcService`) around an orchestrator backed by `repo`,
/// bind it on an ephemeral port, and serve it in the background. Returns the
/// bound URI so a client can be built with whatever auth the test wants.
async fn spawn_grpc_server(repo: Arc<RecordingRepo>) -> axum::http::Uri {
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

    let grpc_service = NotificationGrpcService::new(orchestrator, JWT_SECRET.to_string());
    let mut router = connectrpc::Router::new();
    router = Arc::new(grpc_service).register(router);
    let axum_router = router.into_axum_router();

    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind ephemeral port");
    let addr = listener.local_addr().expect("local addr");

    tokio::spawn(async move {
        let _ = axum::serve(listener, axum_router).await;
    });

    format!("http://{addr}").parse().expect("parse uri")
}

/// Build a gRPC/HTTP-2 client for `uri`. When `auth` is `Some`, it is sent
/// verbatim as the `authorization` header on every call.
async fn connect_client(
    uri: axum::http::Uri,
    auth: Option<&str>,
) -> NotificationServiceClient<connectrpc::client::SharedHttp2Connection> {
    let conn = Http2Connection::connect_plaintext(uri.clone())
        .await
        .expect("h2c connect")
        .shared(1024);
    let mut config = ClientConfig::new(uri).with_protocol(Protocol::Grpc);
    if let Some(value) = auth {
        config = config.with_default_header("authorization", value);
    }
    NotificationServiceClient::new(conn, config)
}

/// Serve `repo` and return a client carrying a valid bearer token — the default
/// for the happy-path tests.
async fn spawn_grpc(
    repo: Arc<RecordingRepo>,
) -> NotificationServiceClient<connectrpc::client::SharedHttp2Connection> {
    let uri = spawn_grpc_server(repo).await;
    let auth = format!("Bearer {}", mint_token(&Uuid::new_v4().to_string()));
    connect_client(uri, Some(&auth)).await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn send_notification_over_the_wire_queues_and_maps_fields() {
    let repo = Arc::new(RecordingRepo {
        created: Mutex::new(vec![]),
        stored: None,
    });
    let client = spawn_grpc(repo.clone()).await;

    let user = Uuid::new_v4();
    let req = SendNotificationRequest {
        user_id: user.to_string(),
        channel: EnumValue::Known(Channel::SMS),
        recipient: "+15550001111".into(),
        template_id: "order_matched".into(),
        variables_json: r#"{"amount":42}"#.into(),
        idempotency_key: "idem-wire-1".into(),
        ..Default::default()
    };

    let resp = client
        .send_notification(req)
        .await
        .expect("send_notification over wire");
    let view = resp.into_view();
    assert_eq!(view.status, "accepted", "accepted status over the wire");
    let returned = Uuid::parse_str(view.notification_id).expect("uuid in wire response");

    // The request actually crossed the transport into the handler + orchestrator.
    let created = repo.created.lock().unwrap();
    assert_eq!(created.len(), 1, "exactly one notification persisted");
    let n = &created[0];
    assert_eq!(n.id, returned, "wire response id matches persisted row");
    assert!(matches!(n.channel, NotificationChannel::Sms), "SMS enum decoded");
    assert_eq!(n.recipient, "+15550001111");
    assert_eq!(n.variables, serde_json::json!({"amount": 42}));
    assert_eq!(n.idempotency_key.as_deref(), Some("idem-wire-1"));
    assert_eq!(n.user_id, Some(user));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn get_status_over_the_wire_reads_back_stored_notification() {
    let id = Uuid::new_v4();
    let repo = Arc::new(RecordingRepo {
        created: Mutex::new(vec![]),
        stored: Some(stored_sample(id)),
    });
    let client = spawn_grpc(repo).await;

    let resp = client
        .get_notification_status(GetNotificationStatusRequest {
            notification_id: id.to_string(),
            ..Default::default()
        })
        .await
        .expect("get_notification_status over wire");
    let view = resp.into_view();

    assert_eq!(view.notification_id, id.to_string());
    assert_eq!(view.status, "sent", "status formatted lowercase over the wire");
    assert!(!view.sent_at.is_empty(), "sent_at rendered rfc3339 over the wire");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn invalid_user_id_maps_to_invalid_argument_over_the_wire() {
    let repo = Arc::new(RecordingRepo {
        created: Mutex::new(vec![]),
        stored: None,
    });
    let client = spawn_grpc(repo.clone()).await;

    let req = SendNotificationRequest {
        user_id: "not-a-uuid".into(),
        channel: EnumValue::Known(Channel::EMAIL),
        recipient: "user@example.com".into(),
        template_id: "welcome".into(),
        ..Default::default()
    };

    let err = client
        .send_notification(req)
        .await
        .expect_err("malformed user_id must be rejected over the wire");
    assert_eq!(
        err.code,
        ErrorCode::InvalidArgument,
        "error code propagated across the gRPC status mapping"
    );
    assert!(repo.created.lock().unwrap().is_empty(), "nothing persisted");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn request_without_token_is_unauthenticated() {
    let repo = Arc::new(RecordingRepo {
        created: Mutex::new(vec![]),
        stored: None,
    });
    let uri = spawn_grpc_server(repo.clone()).await;
    let client = connect_client(uri, None).await; // no authorization header

    let req = SendNotificationRequest {
        user_id: Uuid::new_v4().to_string(),
        channel: EnumValue::Known(Channel::EMAIL),
        recipient: "user@example.com".into(),
        template_id: "welcome".into(),
        ..Default::default()
    };

    let err = client
        .send_notification(req)
        .await
        .expect_err("an unauthenticated call must be rejected");
    assert_eq!(
        err.code,
        ErrorCode::Unauthenticated,
        "missing bearer token → Unauthenticated"
    );
    assert!(
        repo.created.lock().unwrap().is_empty(),
        "nothing reaches the orchestrator without auth"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn request_with_invalid_token_is_unauthenticated() {
    let id = Uuid::new_v4();
    let repo = Arc::new(RecordingRepo {
        created: Mutex::new(vec![]),
        stored: Some(stored_sample(id)),
    });
    let uri = spawn_grpc_server(repo).await;
    // Well-formed header, but the token is garbage (bad signature/shape).
    let client = connect_client(uri, Some("Bearer not-a-real-jwt")).await;

    let err = client
        .get_notification_status(GetNotificationStatusRequest {
            notification_id: id.to_string(),
            ..Default::default()
        })
        .await
        .expect_err("an invalid token must be rejected");
    assert_eq!(
        err.code,
        ErrorCode::Unauthenticated,
        "invalid token → Unauthenticated, before any lookup"
    );
}
