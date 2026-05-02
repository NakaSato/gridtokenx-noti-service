//! Core notification orchestrator — business logic for queueing,
//! dispatching, and retrying notifications.
//!
//! All dependencies are injected via trait objects from `noti_core::traits`.

use std::sync::Arc;

use chrono::Utc;
use tracing::{error, info};
use uuid::Uuid;

use noti_core::domain::{Notification, NotificationChannel, NotificationStatus};
use noti_core::error::{NotiError, Result};
use noti_core::traits::{
    CacheTrait, MessageQueueTrait, NotificationProviderTrait, NotificationRepositoryTrait,
    TemplateEngineTrait,
};

/// Central orchestrator wiring repositories, providers, cache, templates, and MQ.
pub struct NotificationOrchestrator {
    repo: Arc<dyn NotificationRepositoryTrait>,
    template_engine: Arc<dyn TemplateEngineTrait>,
    email_provider: Arc<dyn NotificationProviderTrait>,
    sms_provider: Arc<dyn NotificationProviderTrait>,
    push_provider: Arc<dyn NotificationProviderTrait>,
    webhook_provider: Arc<dyn NotificationProviderTrait>,
    websocket_provider: Arc<dyn NotificationProviderTrait>,
    cache: Arc<dyn CacheTrait>,
    mq: Option<Arc<dyn MessageQueueTrait>>,
}

impl NotificationOrchestrator {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        repo: Arc<dyn NotificationRepositoryTrait>,
        template_engine: Arc<dyn TemplateEngineTrait>,
        email_provider: Arc<dyn NotificationProviderTrait>,
        sms_provider: Arc<dyn NotificationProviderTrait>,
        push_provider: Arc<dyn NotificationProviderTrait>,
        webhook_provider: Arc<dyn NotificationProviderTrait>,
        websocket_provider: Arc<dyn NotificationProviderTrait>,
        cache: Arc<dyn CacheTrait>,
        mq: Option<Arc<dyn MessageQueueTrait>>,
    ) -> Self {
        Self {
            repo,
            template_engine,
            email_provider,
            sms_provider,
            push_provider,
            webhook_provider,
            websocket_provider,
            cache,
            mq,
        }
    }

    /// Accept a notification request: persist, cache idempotency key,
    /// and trigger async dispatch.
    pub async fn queue_notification(
        self: &Arc<Self>,
        user_id: Option<Uuid>,
        channel: NotificationChannel,
        recipient: String,
        template_id: String,
        variables: serde_json::Value,
        idempotency_key: Option<String>,
    ) -> Result<Uuid> {
        // 1. Check idempotency
        if let Some(ref key) = idempotency_key {
            if let Some(existing) = self.repo.get_by_idempotency_key(key).await? {
                return Ok(existing.id);
            }
        }

        // 2. Create notification record
        let notification = Notification {
            id: Uuid::new_v4(),
            user_id,
            channel,
            status: NotificationStatus::Pending,
            recipient,
            template_id,
            variables,
            provider_id: None,
            provider_ref: None,
            retry_count: 0,
            next_retry_at: Utc::now(),
            error_message: None,
            idempotency_key: idempotency_key.clone(),
            created_at: Utc::now(),
            updated_at: Utc::now(),
            sent_at: None,
            read_at: None,
        };

        let id = notification.id;
        self.repo.create(&notification).await?;

        // 3. Cache idempotency key in Redis
        if let Some(ref key) = idempotency_key {
            let cache_key = format!("idempotency:{key}");
            let _ = self.cache.set_value(&cache_key, serde_json::json!(id), 3600).await;
        }

        // 4. Trigger dispatch via MQ or background task
        if let Some(ref mq) = self.mq {
            if let Err(e) = mq.publish_dispatch(id).await {
                error!(
                    "Failed to publish dispatch task to MQ for {}: {}",
                    id, e
                );
                // Fallback to in-process dispatch
                let orchestrator = self.clone();
                tokio::spawn(async move {
                    let _ = orchestrator.dispatch(id).await;
                });
            }
        } else {
            let orchestrator = self.clone();
            tokio::spawn(async move {
                if let Err(e) = orchestrator.dispatch(id).await {
                    error!("Failed to dispatch notification {}: {}", id, e);
                }
            });
        }

        Ok(id)
    }

    /// Deliver a single notification by ID: render template → select
    /// provider → send → update status.
    pub async fn dispatch(self: Arc<Self>, id: Uuid) -> Result<()> {
        let notification = self
            .repo
            .get_by_id(id)
            .await?
            .ok_or_else(|| NotiError::NotFound(format!("notification {id}")))?;

        if notification.status != NotificationStatus::Pending
            && notification.status != NotificationStatus::Processing
        {
            return Ok(());
        }

        // 1. Mark as processing
        self.repo
            .update_status(id, NotificationStatus::Processing, None, None)
            .await?;

        // 2. Render template
        let content = match self
            .template_engine
            .render(&notification.template_id, &notification.variables)
        {
            Ok(c) => c,
            Err(e) => {
                self.repo
                    .update_status(
                        id,
                        NotificationStatus::PermanentFailure,
                        Some(e.to_string()),
                        None,
                    )
                    .await?;
                return Err(e);
            }
        };

        // 3. Select provider by channel
        let provider = match notification.channel {
            NotificationChannel::Email => &self.email_provider,
            NotificationChannel::Sms => &self.sms_provider,
            NotificationChannel::Push => &self.push_provider,
            NotificationChannel::Webhook => &self.webhook_provider,
            NotificationChannel::WebSocket => &self.websocket_provider,
        };

        // 4. Send via provider
        match provider.send(&notification.recipient, &content).await {
            Ok(provider_ref) => {
                self.repo
                    .update_status(id, NotificationStatus::Sent, None, Some(provider_ref))
                    .await?;
                info!(
                    "Successfully sent notification {} via {}",
                    id,
                    provider.provider_id()
                );
                Ok(())
            }
            Err(e) => {
                const MAX_RETRIES: i32 = 5;
                if notification.retry_count >= MAX_RETRIES {
                    self.repo
                        .update_status(
                            id,
                            NotificationStatus::PermanentFailure,
                            Some(e.to_string()),
                            None,
                        )
                        .await?;
                } else {
                    let retry_count = notification.retry_count + 1;
                    let delay_ms =
                        u32::from(2u16.saturating_pow(retry_count as u32)) * 60 * 1000;
                    let next_retry_at =
                        Utc::now() + chrono::Duration::milliseconds(i64::from(delay_ms));

                    self.repo.increment_retry(id, next_retry_at).await?;
                    self.repo
                        .update_status(id, NotificationStatus::Pending, Some(e.to_string()), None)
                        .await?;

                    if let Some(ref mq) = self.mq {
                        let _ = mq.publish_retry(id, delay_ms).await;
                    }
                }
                Err(e)
            }
        }
    }

    /// Look up the current status of a notification.
    pub async fn get_status(&self, id: Uuid) -> Result<Option<Notification>> {
        self.repo.get_by_id(id).await
    }

    // ── User-facing Operations ───────────────────────────────────────────────

    pub async fn list_user_notifications(&self, user_id: Uuid, limit: i64, offset: i64) -> Result<Vec<Notification>> {
        self.repo.list_by_user(user_id, limit, offset).await
    }

    pub async fn mark_as_read(&self, id: Uuid, user_id: Uuid) -> Result<()> {
        self.repo.mark_as_read(id, user_id).await
    }

    pub async fn mark_all_as_read(&self, user_id: Uuid) -> Result<()> {
        self.repo.mark_all_as_read(user_id).await
    }

    pub async fn get_unread_count(&self, user_id: Uuid) -> Result<i64> {
        self.repo.get_unread_count(user_id).await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Mutex;
    use noti_core::domain::NotificationChannel;
    use serde_json::json;

    struct MockRepo {
        saved: Mutex<Vec<Notification>>,
    }
    #[async_trait::async_trait]
    impl NotificationRepositoryTrait for MockRepo {
        async fn create(&self, n: &Notification) -> Result<()> {
            self.saved.lock().unwrap().push(n.clone());
            Ok(())
        }
        async fn get_by_id(&self, _id: Uuid) -> Result<Option<Notification>> { Ok(None) }
        async fn get_by_idempotency_key(&self, _key: &str) -> Result<Option<Notification>> { Ok(None) }
        async fn update_status(&self, _id: Uuid, _status: NotificationStatus, _error: Option<String>, _provider_ref: Option<String>) -> Result<()> { Ok(()) }
        async fn increment_retry(&self, _id: Uuid, _next: chrono::DateTime<chrono::Utc>) -> Result<()> { Ok(()) }
        async fn get_pending_for_retry(&self, _limit: i32) -> Result<Vec<Notification>> { Ok(vec![]) }
        async fn list_by_user(&self, _u: Uuid, _l: i64, _o: i64) -> Result<Vec<Notification>> { Ok(vec![]) }
        async fn mark_as_read(&self, _i: Uuid, _u: Uuid) -> Result<()> { Ok(()) }
        async fn mark_all_as_read(&self, _u: Uuid) -> Result<()> { Ok(()) }
        async fn get_unread_count(&self, _u: Uuid) -> Result<i64> { Ok(0) }
    }

    struct MockCache;
    #[async_trait::async_trait]
    impl CacheTrait for MockCache {
        async fn set_value(&self, _k: &str, _v: serde_json::Value, _t: u64) -> Result<()> { Ok(()) }
        async fn get_value(&self, _k: &str) -> Result<Option<serde_json::Value>> { Ok(None) }
        async fn increment_with_ttl(&self, _k: &str, _t: u64) -> Result<i64> { Ok(1) }
        async fn delete(&self, _k: &str) -> Result<()> { Ok(()) }
    }

    struct MockTemplate;
    impl TemplateEngineTrait for MockTemplate {
        fn render(&self, _i: &str, _v: &serde_json::Value) -> Result<String> { Ok("body".to_string()) }
    }

    struct MockProvider;
    #[async_trait::async_trait]
    impl NotificationProviderTrait for MockProvider {
        async fn send(&self, _r: &str, _b: &str) -> Result<String> { Ok("ref".to_string()) }
        fn provider_id(&self) -> &'static str { "mock" }
    }

    struct MockMq;
    #[async_trait::async_trait]
    impl MessageQueueTrait for MockMq {
        async fn publish_dispatch(&self, _id: Uuid) -> Result<()> { Ok(()) }
        async fn publish_retry(&self, _id: Uuid, _d: u32) -> Result<()> { Ok(()) }
    }

    #[tokio::test]
    async fn test_queue_notification() {
        let repo = Arc::new(MockRepo { saved: Mutex::new(vec![]) });
        let cache = Arc::new(MockCache);
        let template = Arc::new(MockTemplate);
        let p_email = Arc::new(MockProvider);
        let p_sms = Arc::new(MockProvider);
        let p_push = Arc::new(MockProvider);
        let p_web = Arc::new(MockProvider);
        let p_ws = Arc::new(MockProvider);
        let mq = Arc::new(MockMq);

        let orchestrator = Arc::new(NotificationOrchestrator::new(
            repo.clone(),
            template,
            p_email,
            p_sms,
            p_push,
            p_web,
            p_ws,
            cache,
            Some(mq),
        ));

        let id = orchestrator.queue_notification(
            None,
            NotificationChannel::Email,
            "test@example.com".to_string(),
            "welcome".to_string(),
            json!({}),
            None,
        ).await.unwrap();

        let saved = repo.saved.lock().unwrap();
        assert_eq!(saved.len(), 1);
        assert_eq!(saved[0].id, id);
    }
}
