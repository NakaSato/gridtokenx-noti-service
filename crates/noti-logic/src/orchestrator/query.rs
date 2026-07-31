//! User-facing reads: status lookup, listing, read-state, unread counts.

use tracing::warn;
use uuid::Uuid;

use noti_core::domain::Notification;
use noti_core::error::Result;
use noti_core::wire::{self, NotificationView};

use super::NotificationOrchestrator;

impl NotificationOrchestrator {
    /// Look up the current status of a notification.
    ///
    /// # Errors
    ///
    /// Returns an error if the repository call fails.
    pub async fn get_status(&self, id: Uuid) -> Result<Option<Notification>> {
        self.repo.get_by_id(id).await
    }

    /// # Errors
    ///
    /// Returns an error if the repository call fails.
    pub async fn list_user_notifications(
        &self,
        user_id: Uuid,
        limit: i64,
        offset: i64,
    ) -> Result<Vec<Notification>> {
        self.repo.list_by_user(user_id, limit, offset).await
    }

    /// List a user's notifications projected to the client wire shape:
    /// rendered `title`/`message`, canonical event `type`, and read state.
    ///
    /// Rendering uses the notification's **text** template — an email row is
    /// listed from its `.txt.tera` sibling rather than shipping a full HTML
    /// document as the list body. A row whose template is missing or fails to
    /// render still lists (with an empty message) instead of failing the page.
    ///
    /// # Errors
    ///
    /// Returns an error if the repository call fails.
    pub async fn list_user_notification_views(
        &self,
        user_id: Uuid,
        limit: i64,
        offset: i64,
    ) -> Result<Vec<NotificationView>> {
        let notifications = self.repo.list_by_user(user_id, limit, offset).await?;

        Ok(notifications
            .into_iter()
            .map(|n| {
                let template_id = wire::text_template_id(&n.template_id);
                let rendered = self
                    .template_engine
                    .render(&template_id, &n.variables)
                    .unwrap_or_else(|e| {
                        warn!(
                            "Listing notification {} with an empty body: render '{}' failed: {e}",
                            n.id, template_id
                        );
                        String::new()
                    });
                NotificationView::new(n, &rendered)
            })
            .collect())
    }

    /// # Errors
    ///
    /// Returns an error if the repository call fails.
    pub async fn mark_as_read(&self, id: Uuid, user_id: Uuid) -> Result<()> {
        self.repo.mark_as_read(id, user_id).await
    }

    /// # Errors
    ///
    /// Returns an error if the repository call fails.
    pub async fn mark_all_as_read(&self, user_id: Uuid) -> Result<()> {
        self.repo.mark_all_as_read(user_id).await
    }

    /// # Errors
    ///
    /// Returns an error if the repository call fails.
    pub async fn get_unread_count(&self, user_id: Uuid) -> Result<i64> {
        self.repo.get_unread_count(user_id).await
    }
}
