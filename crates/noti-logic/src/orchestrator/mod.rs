//! Core notification orchestrator — business logic for queueing,
//! dispatching, and retrying notifications.
//!
//! All dependencies are injected via trait objects from `noti_core::traits`.
//!
//! The orchestrator's methods are split across submodules by concern:
//! [`queue`] (ingestion), [`dispatch`] (delivery + retry), [`query`]
//! (user-facing reads), and [`counters`] (cache-backed suppression state).
//! They all extend the same [`NotificationOrchestrator`] via separate `impl`
//! blocks.

mod counters;
mod dispatch;
mod query;
mod queue;

use std::sync::Arc;

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
}

#[cfg(test)]
mod tests {
    use super::*;
    use noti_core::domain::{Notification, NotificationChannel, NotificationStatus};
    use noti_core::error::{NotiError, Result};
    use serde_json::json;
    use std::sync::Mutex;
    use uuid::Uuid;

    struct MockRepo {
        saved: Mutex<Vec<Notification>>,
    }
    #[async_trait::async_trait]
    impl NotificationRepositoryTrait for MockRepo {
        async fn create(&self, n: &Notification) -> Result<Notification> {
            let mut saved = self
                .saved
                .lock()
                .map_err(|e| NotiError::Internal(format!("mock repo lock poisoned: {e}")))?;

            if let Some(key) = &n.idempotency_key
                && let Some(existing) = saved
                    .iter()
                    .find(|notification| notification.idempotency_key.as_ref() == Some(key))
            {
                return Ok(existing.clone());
            }

            saved.push(n.clone());
            Ok(n.clone())
        }
        async fn get_by_id(&self, _id: Uuid) -> Result<Option<Notification>> {
            Ok(None)
        }
        async fn get_by_idempotency_key(&self, _key: &str) -> Result<Option<Notification>> {
            Ok(None)
        }
        async fn update_status(
            &self,
            _id: Uuid,
            _status: NotificationStatus,
            _error: Option<String>,
            _provider_ref: Option<String>,
        ) -> Result<()> {
            Ok(())
        }
        async fn increment_retry(
            &self,
            _id: Uuid,
            _next: chrono::DateTime<chrono::Utc>,
        ) -> Result<()> {
            Ok(())
        }
        async fn get_pending_for_retry(&self, _limit: i32) -> Result<Vec<Notification>> {
            Ok(vec![])
        }
        async fn reset_stuck_processing(&self, _threshold: i64) -> Result<u64> {
            Ok(0)
        }
        async fn list_by_user(&self, _u: Uuid, _l: i64, _o: i64) -> Result<Vec<Notification>> {
            Ok(vec![])
        }
        async fn mark_as_read(&self, _i: Uuid, _u: Uuid) -> Result<()> {
            Ok(())
        }
        async fn mark_all_as_read(&self, _u: Uuid) -> Result<()> {
            Ok(())
        }
        async fn get_unread_count(&self, _u: Uuid) -> Result<i64> {
            Ok(0)
        }
    }

    struct MockCache;
    #[async_trait::async_trait]
    impl CacheTrait for MockCache {
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

    struct MockTemplate;
    impl TemplateEngineTrait for MockTemplate {
        fn render(&self, _i: &str, _v: &serde_json::Value) -> Result<String> {
            Ok("body".to_string())
        }
    }

    struct MockProvider;
    #[async_trait::async_trait]
    impl NotificationProviderTrait for MockProvider {
        async fn send(&self, _r: &str, _b: &str) -> Result<String> {
            Ok("ref".to_string())
        }
        fn provider_id(&self) -> &'static str {
            "mock"
        }
    }

    struct MockMq {
        dispatches: Mutex<Vec<Uuid>>,
    }

    #[async_trait::async_trait]
    impl MessageQueueTrait for MockMq {
        async fn publish_dispatch(&self, id: Uuid) -> Result<()> {
            self.dispatches
                .lock()
                .map_err(|e| NotiError::Internal(format!("mock mq lock poisoned: {e}")))?
                .push(id);
            Ok(())
        }
        async fn publish_retry(&self, _id: Uuid, _d: u32) -> Result<()> {
            Ok(())
        }
    }

    #[tokio::test]
    async fn test_queue_notification() -> Result<()> {
        let repo = Arc::new(MockRepo {
            saved: Mutex::new(vec![]),
        });
        let cache = Arc::new(MockCache);
        let template = Arc::new(MockTemplate);
        let p_email = Arc::new(MockProvider);
        let p_sms = Arc::new(MockProvider);
        let p_push = Arc::new(MockProvider);
        let p_web = Arc::new(MockProvider);
        let p_ws = Arc::new(MockProvider);
        let mq = Arc::new(MockMq {
            dispatches: Mutex::new(vec![]),
        });

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

        let id = orchestrator
            .queue_notification(
                None,
                NotificationChannel::Email,
                "test@example.com".to_string(),
                "welcome".to_string(),
                json!({}),
                None,
            )
            .await?;

        let saved = repo
            .saved
            .lock()
            .map_err(|e| NotiError::Internal(format!("mock repo lock poisoned: {e}")))?;
        assert_eq!(saved.len(), 1);
        assert_eq!(saved[0].id, id);
        Ok(())
    }

    #[tokio::test]
    async fn test_queue_notification_deduplicates_idempotency_key() -> Result<()> {
        let repo = Arc::new(MockRepo {
            saved: Mutex::new(vec![]),
        });
        let cache = Arc::new(MockCache);
        let template = Arc::new(MockTemplate);
        let p_email = Arc::new(MockProvider);
        let p_sms = Arc::new(MockProvider);
        let p_push = Arc::new(MockProvider);
        let p_web = Arc::new(MockProvider);
        let p_ws = Arc::new(MockProvider);
        let mq = Arc::new(MockMq {
            dispatches: Mutex::new(vec![]),
        });

        let orchestrator = Arc::new(NotificationOrchestrator::new(
            repo.clone(),
            template,
            p_email,
            p_sms,
            p_push,
            p_web,
            p_ws,
            cache,
            Some(mq.clone()),
        ));

        let first_id = orchestrator
            .queue_notification(
                None,
                NotificationChannel::Email,
                "test@example.com".to_string(),
                "welcome".to_string(),
                json!({}),
                Some("same-key".to_string()),
            )
            .await?;

        let second_id = orchestrator
            .queue_notification(
                None,
                NotificationChannel::Email,
                "test@example.com".to_string(),
                "welcome".to_string(),
                json!({}),
                Some("same-key".to_string()),
            )
            .await?;

        let saved = repo
            .saved
            .lock()
            .map_err(|e| NotiError::Internal(format!("mock repo lock poisoned: {e}")))?;
        let dispatches = mq
            .dispatches
            .lock()
            .map_err(|e| NotiError::Internal(format!("mock mq lock poisoned: {e}")))?;

        assert_eq!(first_id, second_id);
        assert_eq!(saved.len(), 1);
        assert_eq!(dispatches.len(), 1);
        Ok(())
    }
}
