//! `ConnectRPC` / gRPC service implementation for the notification service.

use std::sync::Arc;

use buffa::view::OwnedView;
use connectrpc::{ConnectError, Response, response::RequestContext as Context};
use jsonwebtoken::{DecodingKey, Validation, decode};
use serde::Deserialize;
use tracing::{info, warn};
use uuid::Uuid;

use noti_core::domain::NotificationChannel;
use noti_logic::NotificationOrchestrator;
use noti_protocol::noti::{
    Channel, GetNotificationStatusRequestView, NotificationResponse, NotificationService,
    NotificationStatusResponse, SendNotificationRequestView,
};

/// Minimal claim set we require from a caller's JWT — only the subject is read;
/// `exp` is enforced by `Validation::default()` (which requires the claim).
#[derive(Deserialize)]
struct Claims {
    sub: String,
}

pub struct NotificationGrpcService {
    orchestrator: Arc<NotificationOrchestrator>,
    /// HS256 secret the caller's bearer token must be signed with. Shared with
    /// the WebSocket gate (`JwtSecret`) — same `JWT_SECRET` env var.
    jwt_secret: Arc<String>,
}

impl NotificationGrpcService {
    #[must_use]
    pub fn new(orchestrator: Arc<NotificationOrchestrator>, jwt_secret: String) -> Self {
        Self {
            orchestrator,
            jwt_secret: Arc::new(jwt_secret),
        }
    }

    /// Authenticate a gRPC call from its `authorization: Bearer <jwt>` header.
    ///
    /// Returns the authenticated subject (the JWT `sub`) on success, or
    /// `ConnectError::unauthenticated` if the header is absent, malformed, or
    /// the token fails HS256 signature / expiry validation. Mirrors the
    /// WebSocket JWT gate (`websocket::ws_handler`) so REST, WS, and gRPC share
    /// one auth scheme.
    fn authenticate(&self, ctx: &Context) -> Result<String, ConnectError> {
        let header = ctx
            .header("authorization")
            .and_then(|v| v.to_str().ok())
            .ok_or_else(|| {
                ConnectError::unauthenticated("missing authorization header")
            })?;

        let token = header.strip_prefix("Bearer ").unwrap_or(header).trim();
        if token.is_empty() {
            return Err(ConnectError::unauthenticated("empty bearer token"));
        }

        let key = DecodingKey::from_secret(self.jwt_secret.as_bytes());
        let data = decode::<Claims>(token, &key, &Validation::default()).map_err(|e| {
            warn!("🔒 gRPC JWT validation failed: {e}");
            ConnectError::unauthenticated("invalid or expired token")
        })?;

        Ok(data.claims.sub)
    }
}

#[allow(unknown_lints)]
#[allow(refining_impl_trait)]
impl NotificationService for NotificationGrpcService {
    async fn send_notification(
        &self,
        ctx: Context,
        request: OwnedView<SendNotificationRequestView<'static>>,
    ) -> Result<Response<NotificationResponse>, ConnectError> {
        let subject = self.authenticate(&ctx)?;
        info!(
            "📬 gRPC: SendNotification request for template {} (caller {subject})",
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
        ctx: Context,
        request: OwnedView<GetNotificationStatusRequestView<'static>>,
    ) -> Result<Response<NotificationStatusResponse>, ConnectError> {
        let subject = self.authenticate(&ctx)?;
        info!(
            "🔍 gRPC: GetNotificationStatus request for {} (caller {subject})",
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
