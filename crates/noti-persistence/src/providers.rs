//! Notification delivery providers implementing `NotificationProviderTrait`.

pub mod smtp;
pub mod webhook;
pub mod websocket;

use async_trait::async_trait;
use tracing::info;
use uuid::Uuid;

use noti_core::error::Result;
use noti_core::traits::NotificationProviderTrait;

// ---------------------------------------------------------------------------
// Mock providers (dev / test)
// ---------------------------------------------------------------------------

pub struct MockEmailProvider;

#[async_trait]
impl NotificationProviderTrait for MockEmailProvider {
    async fn send(&self, recipient: &str, content: &str) -> Result<String> {
        info!("📧 [MOCK EMAIL] To: {}, Content: {}", recipient, content);
        Ok(format!("mock-email-{}", Uuid::new_v4()))
    }
    fn provider_id(&self) -> &'static str {
        "mock-email"
    }
}

pub struct MockSmsProvider;

#[async_trait]
impl NotificationProviderTrait for MockSmsProvider {
    async fn send(&self, recipient: &str, content: &str) -> Result<String> {
        info!("📱 [MOCK SMS] To: {}, Content: {}", recipient, content);
        Ok(format!("mock-sms-{}", Uuid::new_v4()))
    }
    fn provider_id(&self) -> &'static str {
        "mock-sms"
    }
}

pub struct MockPushProvider;

#[async_trait]
impl NotificationProviderTrait for MockPushProvider {
    async fn send(&self, recipient: &str, content: &str) -> Result<String> {
        info!("🔔 [MOCK PUSH] To: {}, Content: {}", recipient, content);
        Ok(format!("mock-push-{}", Uuid::new_v4()))
    }
    fn provider_id(&self) -> &'static str {
        "mock-push"
    }
}

pub struct MockWebhookProvider;

#[async_trait]
impl NotificationProviderTrait for MockWebhookProvider {
    async fn send(&self, recipient: &str, content: &str) -> Result<String> {
        info!("🔗 [MOCK WEBHOOK] To: {}, Content: {}", recipient, content);
        Ok(format!("mock-webhook-{}", Uuid::new_v4()))
    }
    fn provider_id(&self) -> &'static str {
        "mock-webhook"
    }
}

pub struct MockWebSocketProvider;

#[async_trait]
impl NotificationProviderTrait for MockWebSocketProvider {
    async fn send(&self, recipient: &str, content: &str) -> Result<String> {
        info!(
            "🌐 [MOCK WEBSOCKET] To: {}, Content: {}",
            recipient, content
        );
        Ok(format!("mock-websocket-{}", Uuid::new_v4()))
    }
    fn provider_id(&self) -> &'static str {
        "mock-websocket"
    }
}
