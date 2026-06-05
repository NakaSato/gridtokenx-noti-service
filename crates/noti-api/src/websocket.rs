use async_trait::async_trait;
use axum::Extension;
use axum::extract::Query;
use axum::extract::ws::{Message as WsMessage, WebSocket, WebSocketUpgrade};
use axum::http::StatusCode;
use axum::response::IntoResponse;
use dashmap::DashMap;
use jsonwebtoken::{DecodingKey, Validation, decode};
use serde::Deserialize;
use std::sync::Arc;
use tokio::sync::mpsc;
use tracing::{info, warn};
use uuid::Uuid;

use noti_core::error::Result;
use noti_core::traits::WebSocketRegistryTrait;

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
    connections: DashMap<Uuid, DashMap<Uuid, mpsc::Sender<String>>>,
}

#[async_trait]
impl WebSocketRegistryTrait for ConnectionManager {
    async fn send_to_user(&self, user_id: &uuid::Uuid, message: &str) -> Result<bool> {
        let Some(user_connections) = self.connections.get(user_id) else {
            return Ok(false);
        };

        let senders: Vec<(Uuid, mpsc::Sender<String>)> = user_connections
            .iter()
            .map(|entry| (*entry.key(), entry.value().clone()))
            .collect();
        drop(user_connections);

        let mut delivered = false;
        for (connection_id, tx) in senders {
            if let Err(e) = tx.send(message.to_string()).await {
                warn!(
                    "🌐 Failed to send WS message to user {} connection {}: {}",
                    user_id, connection_id, e
                );
                self.remove_connection(user_id, &connection_id);
            } else {
                delivered = true;
            }
        }

        Ok(delivered)
    }
}

impl ConnectionManager {
    #[must_use]
    pub fn new() -> Self {
        Self {
            connections: DashMap::new(),
        }
    }

    pub fn add_connection(&self, user_id: Uuid, tx: mpsc::Sender<String>) -> Uuid {
        let connection_id = Uuid::new_v4();
        self.connections
            .entry(user_id)
            .or_default()
            .insert(connection_id, tx);
        info!(
            "🌐 Added WS connection {} for user {}",
            connection_id, user_id
        );
        connection_id
    }

    pub fn remove_connection(&self, user_id: &Uuid, connection_id: &Uuid) {
        let Some(user_connections) = self.connections.get(user_id) else {
            return;
        };

        user_connections.remove(connection_id);
        let is_empty = user_connections.is_empty();
        drop(user_connections);

        if is_empty {
            self.connections.remove(user_id);
        }

        info!(
            "🌐 Removed WS connection {} for user {}",
            connection_id, user_id
        );
    }
}

impl Default for ConnectionManager {
    fn default() -> Self {
        Self::new()
    }
}

/// Axum handler for WebSocket upgrades.
#[allow(clippy::unused_async)]
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

    let connection_id = manager.add_connection(user_id, tx);

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

    manager.remove_connection(&user_id, &connection_id);
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn connection_manager_sends_to_multiple_connections() -> Result<()> {
        let manager = ConnectionManager::new();
        let user_id = Uuid::new_v4();
        let (tx1, mut rx1) = mpsc::channel::<String>(1);
        let (tx2, mut rx2) = mpsc::channel::<String>(1);

        let connection_id_1 = manager.add_connection(user_id, tx1);
        manager.add_connection(user_id, tx2);

        assert!(manager.send_to_user(&user_id, "hello").await?);
        assert_eq!(rx1.recv().await.as_deref(), Some("hello"));
        assert_eq!(rx2.recv().await.as_deref(), Some("hello"));

        manager.remove_connection(&user_id, &connection_id_1);

        assert!(manager.send_to_user(&user_id, "again").await?);
        assert_eq!(rx1.recv().await, None);
        assert_eq!(rx2.recv().await.as_deref(), Some("again"));

        Ok(())
    }
}
