//! Notification ingestion: persist, cache idempotency key, trigger dispatch.

use std::sync::Arc;

use chrono::Utc;
use tracing::{error, warn};
use uuid::Uuid;

use noti_core::domain::{Notification, NotificationChannel, NotificationStatus};
use noti_core::error::Result;

use super::NotificationOrchestrator;

impl NotificationOrchestrator {
    /// Accept a notification request: persist, cache idempotency key,
    /// and trigger async dispatch.
    ///
    /// # Errors
    ///
    /// Returns an error if the repository call fails or the notification
    /// cannot be persisted.
    pub async fn queue_notification(
        self: &Arc<Self>,
        user_id: Option<Uuid>,
        channel: NotificationChannel,
        recipient: String,
        template_id: String,
        variables: serde_json::Value,
        idempotency_key: Option<String>,
    ) -> Result<Uuid> {
        // 1. Create notification record atomically. The repository returns an
        // existing row when idempotency_key conflicts, preventing duplicate sends.
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

        let requested_id = notification.id;
        let saved_notification = self.repo.create(&notification).await?;
        let id = saved_notification.id;

        if id != requested_id {
            return Ok(id);
        }

        // 2. Cache idempotency key in Redis
        if let Some(ref key) = idempotency_key {
            let cache_key = format!("idempotency:{key}");
            if let Err(e) = self
                .cache
                .set_value(&cache_key, serde_json::json!(id), 3600)
                .await
            {
                warn!("Failed to cache idempotency key for {id}: {e}");
            }
        }

        // 3. Trigger dispatch via MQ or background task
        self.trigger_dispatch(id).await;

        Ok(id)
    }

    /// Trigger delivery of an already-persisted notification: publish a
    /// dispatch task to the durable queue, falling back to an in-process
    /// `tokio::spawn` if MQ is absent or the publish fails.
    pub(super) async fn trigger_dispatch(self: &Arc<Self>, id: Uuid) {
        if let Some(ref mq) = self.mq {
            if let Err(e) = mq.publish_dispatch(id).await {
                error!("Failed to publish dispatch task to MQ for {}: {}", id, e);
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
    }

    /// Boot-time crash recovery: reset rows stuck in `Processing` (a crash
    /// mid-dispatch left them) back to `Pending`, then re-dispatch every
    /// currently-due `Pending` row. Returns the number re-queued for dispatch.
    ///
    /// # Errors
    ///
    /// Returns an error if a repository call fails.
    pub async fn recover_pending(
        self: &Arc<Self>,
        stuck_threshold_secs: i64,
        batch_limit: i32,
    ) -> Result<usize> {
        let reset = self.repo.reset_stuck_processing(stuck_threshold_secs).await?;
        if reset > 0 {
            warn!("Recovery: reset {reset} stuck 'processing' notification(s) to pending");
        }

        let due = self.repo.get_pending_for_retry(batch_limit).await?;
        let count = due.len();
        for n in &due {
            self.trigger_dispatch(n.id).await;
        }

        Ok(count)
    }
}
