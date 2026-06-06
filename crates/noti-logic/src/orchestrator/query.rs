//! User-facing reads: status lookup, listing, read-state, unread counts.

use uuid::Uuid;

use noti_core::domain::Notification;
use noti_core::error::Result;

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
