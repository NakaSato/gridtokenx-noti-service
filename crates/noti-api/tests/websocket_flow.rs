//! WebSocket *full-flow* end-to-end test (closes gap #7).
//!
//! `websocket_e2e.rs` proves the transport leg (JWT gate + handshake + a push
//! issued by calling `ConnectionManager::send_to_user` *directly*). It never
//! drives the real production path: a queued `WebSocket`-channel notification
//! flowing through `orchestrator.dispatch` → template render → channel→provider
//! selection → the real `WebSocketProvider` → `ConnectionManager` → the frame
//! arriving at the connected client, with the Postgres status advanced to
//! `sent`.
//!
//! This wires the *real* `WebSocketProvider` (from `noti-persistence`) into a
//! real orchestrator, registers a real WS client through `ws_handler`'s JWT
//! gate, then calls `orchestrator.dispatch(id)` on a `WebSocket` notification
//! and asserts the rendered body lands on the socket and the repo is marked
//! `sent`. A regression in the channel→provider map, the recipient→user-id
//! parse in `WebSocketProvider`, or the registry hand-off fails here, where the
//! direct-push test cannot see it.

#![allow(clippy::expect_used, clippy::unwrap_used)]

use std::net::SocketAddr;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use async_trait::async_trait;
use axum::Extension;
use axum::Router;
use axum::routing::get;
use futures::StreamExt;
use jsonwebtoken::{EncodingKey, Header, encode};
use serde::Serialize;
use tokio::net::TcpListener;
use tokio_tungstenite::tungstenite::Message;
use uuid::Uuid;

use noti_api::websocket::{ConnectionManager, JwtSecret, ws_handler};
use noti_core::domain::{Notification, NotificationChannel, NotificationStatus};
use noti_core::error::Result;
use noti_core::traits::{
    CacheTrait, MessageQueueTrait, NotificationProviderTrait, NotificationRepositoryTrait,
    TemplateEngineTrait, WebSocketRegistryTrait,
};
use noti_logic::NotificationOrchestrator;
use noti_persistence::providers::websocket::WebSocketProvider;

const SECRET: &str = "test-ws-flow-secret-please-ignore";
/// Readiness sentinel pushed until the client's connection is registered. Exactly
/// one such frame lands on the socket and is drained before the real assertion.
const PROBE: &str = "__ready_probe__";
/// Distinctive rendered body the stub template returns, so the assertion on the
/// received frame is exact (and unmistakably the dispatch payload, not a probe).
const RENDERED_BODY: &str = r#"{"title":"Trade matched","amount":42}"#;

#[derive(Serialize)]
struct Claims {
    sub: String,
    exp: usize,
}

fn mint_token(user_id: Uuid) -> String {
    let claims = Claims {
        sub: user_id.to_string(),
        exp: 9_999_999_999,
    };
    encode(
        &Header::default(),
        &claims,
        &EncodingKey::from_secret(SECRET.as_bytes()),
    )
    .expect("encode jwt")
}

// --- Fakes (the point is the dispatch→provider→registry→socket path, not DB) -

/// Holds one notification, returns it on `get_by_id`, and records every
/// `update_status` transition so the test can assert the terminal `sent`.
struct OneShotRepo {
    notification: Notification,
    statuses: Mutex<Vec<NotificationStatus>>,
}

#[async_trait]
impl NotificationRepositoryTrait for OneShotRepo {
    async fn create(&self, n: &Notification) -> Result<Notification> {
        Ok(n.clone())
    }
    async fn get_by_id(&self, _id: Uuid) -> Result<Option<Notification>> {
        Ok(Some(self.notification.clone()))
    }
    async fn get_by_idempotency_key(&self, _key: &str) -> Result<Option<Notification>> {
        Ok(None)
    }
    async fn update_status(
        &self,
        _id: Uuid,
        status: NotificationStatus,
        _error: Option<String>,
        _provider_ref: Option<String>,
    ) -> Result<()> {
        self.statuses.lock().unwrap().push(status);
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

struct StubCache;
#[async_trait]
impl CacheTrait for StubCache {
    async fn set_value(&self, _k: &str, _v: serde_json::Value, _t: u64) -> Result<()> {
        Ok(())
    }
    async fn get_value(&self, _k: &str) -> Result<Option<serde_json::Value>> {
        Ok(None)
    }
    async fn increment_with_ttl(&self, _k: &str, _t: u64) -> Result<i64> {
        Ok(1)
    }
    async fn delete(&self, _k: &str) -> Result<()> {
        Ok(())
    }
    async fn lock(&self, _k: &str, _t: u64) -> Result<bool> {
        Ok(true)
    }
    async fn unlock(&self, _k: &str) -> Result<()> {
        Ok(())
    }
}

/// Returns a fixed, distinctive body — keeps the test about the WS delivery path
/// (real-template rendering is covered by `full_pipeline.rs` / template tests).
struct StubTemplate;
impl TemplateEngineTrait for StubTemplate {
    fn render(&self, _i: &str, _v: &serde_json::Value) -> Result<String> {
        Ok(RENDERED_BODY.to_string())
    }
}

/// Stub for the non-WebSocket channels the orchestrator constructor still needs.
struct StubProvider;
#[async_trait]
impl NotificationProviderTrait for StubProvider {
    async fn send(&self, _r: &str, _b: &str) -> Result<String> {
        Ok("ref".into())
    }
    fn provider_id(&self) -> &'static str {
        "stub"
    }
}

/// Boot the `/ws` router on an ephemeral port; return the address + the shared
/// `ConnectionManager` (used both as the WS registry and by `WebSocketProvider`).
async fn spawn_server() -> (SocketAddr, Arc<ConnectionManager>) {
    let manager = Arc::new(ConnectionManager::new());
    let app = Router::new()
        .route("/ws", get(ws_handler))
        .layer(Extension(manager.clone()))
        .layer(Extension(JwtSecret(SECRET.to_string())));

    let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind");
    let addr = listener.local_addr().expect("local addr");
    tokio::spawn(async move {
        axum::serve(listener, app).await.expect("serve");
    });
    (addr, manager)
}

fn ws_notification(id: Uuid, user_id: Uuid) -> Notification {
    let now = chrono::Utc::now();
    Notification {
        id,
        user_id: Some(user_id),
        channel: NotificationChannel::WebSocket,
        status: NotificationStatus::Pending,
        // For the WebSocket channel the recipient IS the user-id string that
        // `WebSocketProvider` parses to route through the registry.
        recipient: user_id.to_string(),
        template_id: "trade_matched".into(),
        variables: serde_json::json!({}),
        provider_id: None,
        provider_ref: None,
        retry_count: 0,
        next_retry_at: now,
        error_message: None,
        idempotency_key: None,
        created_at: now,
        updated_at: now,
        sent_at: None,
        read_at: None,
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn dispatch_pushes_rendered_notification_to_connected_client() {
    let (addr, manager) = spawn_server().await;
    let user_id = Uuid::new_v4();

    // Connect a real WS client through the JWT gate.
    let url = format!("ws://{addr}/ws?token={}", mint_token(user_id));
    let (mut socket, _resp) = tokio_tungstenite::connect_async(&url)
        .await
        .expect("ws handshake should succeed for a valid token");

    // The upgraded task registers the connection asynchronously after the
    // handshake. Gate on registration with a sentinel probe so the dispatch
    // below can't race ahead of `add_connection` (which would make the provider
    // see "not connected"). Exactly one probe frame lands — drained later.
    tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            if manager
                .send_to_user(&user_id, PROBE)
                .await
                .expect("probe send")
            {
                break;
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await
    .expect("connection never registered");

    // Real orchestrator with the real WebSocketProvider on the WS channel.
    let repo = Arc::new(OneShotRepo {
        notification: ws_notification(Uuid::new_v4(), user_id),
        statuses: Mutex::new(vec![]),
    });
    let id = repo.notification.id;
    let ws_provider: Arc<dyn NotificationProviderTrait> =
        Arc::new(WebSocketProvider::new(manager.clone()));
    let stub: Arc<dyn NotificationProviderTrait> = Arc::new(StubProvider);

    let orchestrator = Arc::new(NotificationOrchestrator::new(
        repo.clone() as Arc<dyn NotificationRepositoryTrait>,
        Arc::new(StubTemplate) as Arc<dyn TemplateEngineTrait>,
        stub.clone(), // email
        stub.clone(), // sms
        stub.clone(), // push
        stub.clone(), // webhook
        ws_provider,  // websocket  ← the one under test
        Arc::new(StubCache) as Arc<dyn CacheTrait>,
        None::<Arc<dyn MessageQueueTrait>>,
    ));

    // Drive the production dispatch path.
    orchestrator
        .clone()
        .dispatch(id)
        .await
        .expect("dispatch a WebSocket notification");

    // The rendered body must arrive on the socket. Skip the single readiness
    // probe; the next text frame is the dispatch payload.
    let body = tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            let frame = socket
                .next()
                .await
                .expect("stream ended before payload")
                .expect("frame error");
            if let Message::Text(t) = frame {
                if t == PROBE {
                    continue;
                }
                break t.to_string();
            }
        }
    })
    .await
    .expect("rendered notification never reached the client");

    assert_eq!(
        body, RENDERED_BODY,
        "client received the rendered dispatch payload over the WS channel"
    );

    // Terminal status must be `sent` (written only after the provider returned
    // Ok, i.e. the registry reported delivery). Copy out before the await below
    // so no MutexGuard is held across it.
    let transitions = repo.statuses.lock().unwrap().clone();
    assert_eq!(
        transitions.last(),
        Some(&NotificationStatus::Sent),
        "notification advanced to sent after WS delivery (transitions: {transitions:?})"
    );

    socket.close(None).await.ok();
}
