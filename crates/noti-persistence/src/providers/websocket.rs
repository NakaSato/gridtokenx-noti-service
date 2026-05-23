use async_trait::async_trait;
use std::sync::Arc;
use tracing::{info, warn};
use uuid::Uuid;

use noti_core::error::{NotiError, Result};
use noti_core::traits::{NotificationProviderTrait, WebSocketRegistryTrait};

pub struct WebSocketProvider {
    registry: Arc<dyn WebSocketRegistryTrait>,
}

impl WebSocketProvider {
    pub fn new(registry: Arc<dyn WebSocketRegistryTrait>) -> Self {
        Self { registry }
    }
}

#[async_trait]
impl NotificationProviderTrait for WebSocketProvider {
    async fn send(&self, recipient: &str, content: &str) -> Result<String> {
        info!("🌐 Sending WebSocket message to {}", recipient);

        let user_id = Uuid::parse_str(recipient).map_err(|e| {
            NotiError::Internal(format!("Invalid UUID for WebSocket recipient: {e}"))
        })?;

        match self.registry.send_to_user(&user_id, content).await {
            Ok(true) => {
                info!("✅ WebSocket message delivered to user {}", user_id);
                Ok(format!("ws-{}", Uuid::new_v4()))
            }
            Ok(false) => {
                warn!("⚠️ User {} not connected via WebSocket", user_id);
                // Depending on requirements, this might be an error or a silent skip
                // For a "best effort" channel like WS, we might return Success but with a note
                Ok("not-connected".to_string())
            }
            Err(e) => Err(e),
        }
    }

    fn provider_id(&self) -> &'static str {
        "websocket"
    }
}
