use std::sync::Arc;
use async_trait::async_trait;
use axum::extract::ws::{Message as WsMessage, WebSocket, WebSocketUpgrade};
use axum::extract::Query;
use axum::Extension;
use axum::http::StatusCode;
use axum::response::IntoResponse;
use dashmap::DashMap;
use jsonwebtoken::{decode, DecodingKey, Validation};
use serde::Deserialize;
use tokio::sync::mpsc;
use tracing::{info, warn};
use uuid::Uuid;

use noti_core::traits::WebSocketRegistryTrait;
use noti_core::error::Result;

#[derive(Deserialize)]
pub struct WsQuery {
    pub token: String,
}

#[derive(Deserialize)]
struct Claims {
    sub: String,
}

/// Shared JWT secret injected via Extension.
#[derive(Clone)]
pub struct JwtSecret(pub String);

/// Manages active WebSocket connections indexed by User ID.
pub struct ConnectionManager {
    connections: DashMap<Uuid, mpsc::Sender<String>>,
}

#[async_trait]
impl WebSocketRegistryTrait for ConnectionManager {
    async fn send_to_user(&self, user_id: &uuid::Uuid, message: &str) -> Result<bool> {
        if let Some(tx) = self.connections.get(user_id) {
            let tx: &mpsc::Sender<String> = tx.value();
            if let Err(e) = tx.send(message.to_string()).await {
                warn!("🌐 Failed to send WS message to user {}: {}", user_id, e);
                return Ok(false);
            }
            return Ok(true);
        }
        Ok(false)
    }
}

impl ConnectionManager {
    pub fn new() -> Self {
        Self {
            connections: DashMap::new(),
        }
    }

    pub fn add_connection(&self, user_id: Uuid, tx: mpsc::Sender<String>) {
        self.connections.insert(user_id, tx);
        info!("🌐 Added WS connection for user {}", user_id);
    }

    pub fn remove_connection(&self, user_id: &Uuid) {
        self.connections.remove(user_id);
        info!("🌐 Removed WS connection for user {}", user_id);
    }
}

impl Default for ConnectionManager {
    fn default() -> Self {
        Self::new()
    }
}



/// Axum handler for WebSocket upgrades.
pub async fn ws_handler(
    ws: WebSocketUpgrade,
    Query(query): Query<WsQuery>,
    Extension(manager): Extension<Arc<ConnectionManager>>,
    Extension(jwt_secret): Extension<JwtSecret>,
) -> impl IntoResponse {
    let key = DecodingKey::from_secret(jwt_secret.0.as_bytes());
    let token_data = decode::<Claims>(&query.token, &key, &Validation::default());

    let user_id = match token_data {
        Ok(data) => match Uuid::parse_str(&data.claims.sub) {
            Ok(id) => id,
            Err(_) => return StatusCode::UNAUTHORIZED.into_response(),
        },
        Err(e) => {
            warn!("🌐 WS JWT validation failed: {}", e);
            return StatusCode::UNAUTHORIZED.into_response();
        }
    };

    ws.on_upgrade(move |socket| handle_socket(socket, manager, user_id))
}

async fn handle_socket(mut socket: WebSocket, manager: Arc<ConnectionManager>, user_id: Uuid) {
    let (tx, mut rx) = mpsc::channel::<String>(100);

    manager.add_connection(user_id, tx);

    loop {
        tokio::select! {
            result = socket.recv() => {
                match result {
                    Some(Ok(msg)) => {
                        if let WsMessage::Close(_) = msg {
                            break;
                        }
                        // We could handle inbound messages here if needed
                    }
                    Some(Err(e)) => {
                        warn!("🌐 WS error for user {}: {}", user_id, e);
                        break;
                    }
                    None => break,
                }
            }
            Some(msg) = rx.recv() => {
                if let Err(e) = socket.send(WsMessage::Text(msg.into())).await {
                    warn!("🌐 WS send error for user {}: {}", user_id, e);
                    break;
                }
            }
        }
    }

    manager.remove_connection(&user_id);
}
