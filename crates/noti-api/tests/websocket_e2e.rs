//! End-to-end tests for the realtime WebSocket channel (gap #12).
//!
//! Drives the full path the live service uses: a real Axum server bound to an
//! ephemeral port, a real WebSocket client handshake through `ws_handler`'s JWT
//! gate, registration into `ConnectionManager`, and a push delivered over the
//! socket via `WebSocketRegistryTrait::send_to_user`.
//!
//! Covers what the in-crate unit test (manager fan-out only) cannot:
//!   - the JWT query-param auth gate accepts a valid HS256 token and rejects a
//!     bad one with a failed handshake (401, no upgrade),
//!   - the upgraded connection actually registers, so a server-side push
//!     reaches the connected client as a text frame.

#![allow(clippy::expect_used, clippy::unwrap_used)]

use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

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
use noti_core::traits::WebSocketRegistryTrait;

const SECRET: &str = "test-ws-secret-please-ignore";

#[derive(Serialize)]
struct Claims {
    sub: String,
    exp: usize,
}

/// Mint an HS256 JWT whose `sub` is `user_id`, valid far into the future.
fn mint_token(user_id: Uuid) -> String {
    let claims = Claims {
        sub: user_id.to_string(),
        // Validation::default() requires a future `exp`.
        exp: 9_999_999_999,
    };
    encode(
        &Header::default(),
        &claims,
        &EncodingKey::from_secret(SECRET.as_bytes()),
    )
    .expect("encode jwt")
}

/// Boot the `/ws` router on an ephemeral port; return the bound address and the
/// shared `ConnectionManager` the server pushes through.
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

#[tokio::test]
async fn valid_token_connects_and_receives_server_push() {
    let (addr, manager) = spawn_server().await;
    let user_id = Uuid::new_v4();
    let url = format!("ws://{addr}/ws?token={}", mint_token(user_id));

    let (mut socket, _resp) = tokio_tungstenite::connect_async(&url)
        .await
        .expect("ws handshake should succeed for a valid token");

    // The upgraded task registers the connection asynchronously after the
    // handshake; poll the push until it lands (or time out) to avoid a race.
    let deadline = Duration::from_secs(5);
    let delivered = tokio::time::timeout(deadline, async {
        loop {
            if manager
                .send_to_user(&user_id, "ping")
                .await
                .expect("send_to_user")
            {
                break;
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await;
    assert!(delivered.is_ok(), "connection never registered for push");

    let msg = tokio::time::timeout(deadline, socket.next())
        .await
        .expect("timed out waiting for push")
        .expect("stream ended")
        .expect("frame error");
    assert_eq!(msg, Message::Text("ping".into()), "client receives the push");

    socket.close(None).await.ok();
}

#[tokio::test]
async fn invalid_token_handshake_is_rejected() {
    let (addr, _manager) = spawn_server().await;
    let url = format!("ws://{addr}/ws?token=not-a-valid-jwt");

    let result = tokio_tungstenite::connect_async(&url).await;
    assert!(
        result.is_err(),
        "handshake must fail (401, no upgrade) for an invalid token"
    );
}

#[tokio::test]
async fn token_with_wrong_secret_is_rejected() {
    let (addr, _manager) = spawn_server().await;
    // Valid JWT shape, signed with the wrong key → signature check fails.
    let claims = Claims {
        sub: Uuid::new_v4().to_string(),
        exp: 9_999_999_999,
    };
    let forged = encode(
        &Header::default(),
        &claims,
        &EncodingKey::from_secret(b"the-wrong-secret"),
    )
    .expect("encode jwt");
    let url = format!("ws://{addr}/ws?token={forged}");

    let result = tokio_tungstenite::connect_async(&url).await;
    assert!(result.is_err(), "wrong-signature token must be rejected");
}
