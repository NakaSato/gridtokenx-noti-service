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
}
