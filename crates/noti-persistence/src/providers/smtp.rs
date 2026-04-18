use async_trait::async_trait;
use lettre::transport::smtp::authentication::Credentials;
use lettre::{Message, AsyncSmtpTransport, AsyncTransport, Tokio1Executor};
use tracing::{error, info};

use noti_core::error::{NotiError, Result};
use noti_core::traits::NotificationProviderTrait;

pub struct SmtpProvider {
    transport: AsyncSmtpTransport<Tokio1Executor>,
    from_email: String,
}

impl SmtpProvider {
    pub fn new(host: &str, port: u16, username: Option<String>, password: Option<String>, from_email: String) -> Self {
        let mut builder = if port == 465 {
            AsyncSmtpTransport::<Tokio1Executor>::relay(host).expect("Valid SMTP host")
        } else {
            AsyncSmtpTransport::<Tokio1Executor>::starttls_relay(host).expect("Valid SMTP host")
        };

        if let (Some(u), Some(p)) = (username, password) {
            builder = builder.credentials(Credentials::new(u, p));
        }

        let transport = builder.port(port).build();

        Self {
            transport,
            from_email,
        }
    }
}

#[async_trait]
impl NotificationProviderTrait for SmtpProvider {
    async fn send(&self, recipient: &str, content: &str) -> Result<String> {
        info!("📧 Sending SMTP email to {}", recipient);

        let email = Message::builder()
            .from(self.from_email.parse().map_err(|e| NotiError::Internal(format!("Invalid from email: {e}")))? )
            .to(recipient.parse().map_err(|e| NotiError::Internal(format!("Invalid recipient email: {e}")))? )
            .subject("GridTokenX Notification")
            .body(content.to_string())
            .map_err(|e| NotiError::Internal(format!("Failed to build email: {e}")))?;

        match self.transport.send(email).await {
            Ok(response) => {
                let id = response.first_line().unwrap_or("unknown-smtp-id").to_string();
                info!("✅ SMTP email sent successfully: {}", id);
                Ok(id)
            }
            Err(e) => {
                error!("❌ Failed to send SMTP email: {}", e);
                Err(NotiError::Internal(format!("SMTP error: {e}")))
            }
        }
    }

    fn provider_id(&self) -> &'static str {
        "smtp"
    }
}
