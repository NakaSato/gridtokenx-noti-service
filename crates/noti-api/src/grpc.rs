//! `ConnectRPC` / gRPC service implementation for the notification service.

use std::sync::Arc;

use buffa::view::OwnedView;
use connectrpc::{ConnectError, Response, response::RequestContext as Context};
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
    #[must_use]
    pub fn new(orchestrator: Arc<NotificationOrchestrator>) -> Self {
        Self { orchestrator }
    }
}

#[allow(unknown_lints)]
#[allow(refining_impl_trait)]
impl NotificationService for NotificationGrpcService {
    async fn send_notification(
        &self,
        _ctx: Context,
        request: OwnedView<SendNotificationRequestView<'static>>,
    ) -> Result<Response<NotificationResponse>, ConnectError> {
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
            #[allow(clippy::match_same_arms)]
            _ => NotificationChannel::Email,
        };

        let user_id = if request.user_id.is_empty() {
            None
        } else {
            Some(
                Uuid::parse_str(request.user_id)
                    .map_err(|e| ConnectError::invalid_argument(e.to_string()))?,
            )
        };

        let idempotency_key = if request.idempotency_key.is_empty() {
            None
        } else {
            Some(request.idempotency_key.to_string())
        };

        let variables = if request.variables_json.is_empty() {
            serde_json::json!({})
        } else {
            serde_json::from_str(request.variables_json).map_err(|e| {
                ConnectError::invalid_argument(format!("variables_json must be valid JSON: {e}"))
            })?
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
            Ok(id) => Ok(Response::new(NotificationResponse {
                notification_id: id.to_string(),
                status: "accepted".to_string(),
                ..Default::default()
            })),
            Err(e) => Err(ConnectError::internal(e.to_string())),
        }
    }

    async fn get_notification_status(
        &self,
        _ctx: Context,
        request: OwnedView<GetNotificationStatusRequestView<'static>>,
    ) -> Result<Response<NotificationStatusResponse>, ConnectError> {
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

        Ok(Response::new(NotificationStatusResponse {
            notification_id: notification.id.to_string(),
            status: format!("{:?}", notification.status).to_lowercase(),
            error_message: notification.error_message.unwrap_or_default(),
            sent_at: notification
                .sent_at
                .map(|t| t.to_rfc3339())
                .unwrap_or_default(),
            ..Default::default()
        }))
    }
}
