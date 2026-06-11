//! Notification delivery: render template, select provider, send, retry on failure.

use std::sync::Arc;

use chrono::Utc;
use tracing::{error, info};
use uuid::Uuid;

use noti_core::domain::{NotificationChannel, NotificationStatus};
use noti_core::error::{NotiError, Result};

use super::NotificationOrchestrator;

impl NotificationOrchestrator {
    /// Deliver a single notification by ID: render template → select
    /// provider → send → update status.
    ///
    /// # Errors
    ///
    /// Returns an error if the notification is not found, template rendering
    /// fails, or the provider rejects the delivery.
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
                        u32::from(2u16.saturating_pow(retry_count.unsigned_abs())) * 60 * 1000;
                    let next_retry_at =
                        Utc::now() + chrono::Duration::milliseconds(i64::from(delay_ms));

                    self.repo.increment_retry(id, next_retry_at).await?;
                    self.repo
                        .update_status(id, NotificationStatus::Pending, Some(e.to_string()), None)
                        .await?;

                    if let Some(ref mq) = self.mq {
                        if let Err(schedule_error) = mq.publish_retry(id, delay_ms).await {
                            error!(
                                "Failed to publish retry for notification {id}: {schedule_error}; falling back to in-process retry"
                            );
                            self.clone().spawn_in_process_retry(id, delay_ms);
                        }
                    } else {
                        let orchestrator = self.clone();
                        orchestrator.spawn_in_process_retry(id, delay_ms);
                    }
                    return Ok(());
                }
                Err(e)
            }
        }
    }

    fn spawn_in_process_retry(self: Arc<Self>, id: Uuid, delay_ms: u32) {
        tokio::spawn(async move {
            tokio::time::sleep(std::time::Duration::from_millis(u64::from(delay_ms))).await;
            if let Err(e) = self.dispatch(id).await {
                error!("Failed to retry notification {id}: {e}");
            }
        });
    }
}
