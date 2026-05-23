//! ConnectRPC / gRPC service implementation for the notification service.

use std::sync::Arc;

use buffa::view::OwnedView;
use connectrpc::{ConnectError, Context};
use tracing::info;
use uuid::Uuid;

use noti_core::domain::NotificationChannel;
use noti_logic::NotificationOrchestrator;
use noti_protocol::noti::{
    Channel, GetNotificationStatusRequestView, NotificationResponse, NotificationService,
    NotificationStatusResponse, SendNotificationRequestView,
};

pub struct NotificationGrpcService {
    orchestrator: Arc<NotificationOrchestrator>,
}

impl NotificationGrpcService {
    pub fn new(orchestrator: Arc<NotificationOrchestrator>) -> Self {
        Self { orchestrator }
    }
}

impl NotificationService for NotificationGrpcService {
    async fn send_notification(
        &self,
        ctx: Context,
        request: OwnedView<SendNotificationRequestView<'static>>,
    ) -> Result<(NotificationResponse, Context), ConnectError> {
        info!(
            "📬 gRPC: SendNotification request for template {}",
            request.template_id
        );

        let channel = match request.channel {
            buffa::EnumValue::Known(Channel::EMAIL) => NotificationChannel::Email,
            buffa::EnumValue::Known(Channel::SMS) => NotificationChannel::Sms,
            buffa::EnumValue::Known(Channel::PUSH) => NotificationChannel::Push,
            buffa::EnumValue::Known(Channel::WEBHOOK) => NotificationChannel::Webhook,
            buffa::EnumValue::Known(Channel::WEBSOCKET) => NotificationChannel::WebSocket,
            _ => NotificationChannel::Email,
        };

        let user_id = if !request.user_id.is_empty() {
            Some(
                Uuid::parse_str(request.user_id)
                    .map_err(|e| ConnectError::invalid_argument(e.to_string()))?,
            )
        } else {
            None
        };

        let idempotency_key = if !request.idempotency_key.is_empty() {
            Some(request.idempotency_key.to_string())
        } else {
            None
        };

        let variables = if !request.variables_json.is_empty() {
            serde_json::from_str(request.variables_json).unwrap_or_else(|_| serde_json::json!({}))
        } else {
            serde_json::json!({})
        };

        match self
            .orchestrator
            .queue_notification(
                user_id,
                channel,
                request.recipient.to_string(),
                request.template_id.to_string(),
                variables,
                idempotency_key,
            )
            .await
        {
            Ok(id) => Ok((
                NotificationResponse {
                    notification_id: id.to_string(),
                    status: "accepted".to_string(),
                    ..Default::default()
                },
                ctx,
            )),
            Err(e) => Err(ConnectError::internal(e.to_string())),
        }
    }

    async fn get_notification_status(
        &self,
        ctx: Context,
        request: OwnedView<GetNotificationStatusRequestView<'static>>,
    ) -> Result<(NotificationStatusResponse, Context), ConnectError> {
        info!(
            "🔍 gRPC: GetNotificationStatus request for {}",
            request.notification_id
        );

        let id = Uuid::parse_str(request.notification_id)
            .map_err(|e| ConnectError::invalid_argument(e.to_string()))?;

        let notification = self
            .orchestrator
            .get_status(id)
            .await
            .map_err(|e| ConnectError::internal(e.to_string()))?
            .ok_or_else(|| ConnectError::not_found("Notification not found"))?;

        Ok((
            NotificationStatusResponse {
                notification_id: notification.id.to_string(),
                status: format!("{:?}", notification.status).to_lowercase(),
                error_message: notification.error_message.unwrap_or_default(),
                sent_at: notification
                    .sent_at
                    .map(|t| t.to_rfc3339())
                    .unwrap_or_default(),
                ..Default::default()
            },
            ctx,
        ))
    }
}
