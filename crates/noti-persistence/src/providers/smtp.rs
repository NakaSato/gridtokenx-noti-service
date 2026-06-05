use async_trait::async_trait;
use lettre::message::{MultiPart, SinglePart, header::ContentType};
use lettre::transport::smtp::authentication::Credentials;
use lettre::{AsyncSmtpTransport, AsyncTransport, Message, Tokio1Executor};
use tracing::{error, info};

use noti_core::error::{NotiError, Result};
use noti_core::traits::NotificationProviderTrait;

pub struct SmtpProvider {
    transport: AsyncSmtpTransport<Tokio1Executor>,
    from_email: String,
}

impl SmtpProvider {
    /// # Errors
    ///
    /// Returns an error if the SMTP host string is invalid for the selected
    /// relay builder.
    pub fn new(
        host: &str,
        port: u16,
        username: Option<String>,
        password: Option<String>,
        from_email: String,
        tls_mode: Option<&str>,
    ) -> anyhow::Result<Self> {
        let effective_mode = tls_mode.unwrap_or(if port == 465 { "tls" } else { "starttls" });

        let mut builder = match effective_mode {
            "tls" => AsyncSmtpTransport::<Tokio1Executor>::relay(host)
                .map_err(|e| anyhow::anyhow!("Invalid SMTP TLS relay host '{host}': {e}"))?,
            "starttls" => AsyncSmtpTransport::<Tokio1Executor>::starttls_relay(host)
                .map_err(|e| anyhow::anyhow!("Invalid SMTP STARTTLS relay host '{host}': {e}"))?,
            "none" | "insecure" => {
                // No TLS - suitable for local testing with Mailpit
                AsyncSmtpTransport::<Tokio1Executor>::builder_dangerous(host).port(port)
            }
            _ => {
                error!(
                    "Unknown SMTP TLS mode: {}, falling back to starttls",
                    effective_mode
                );
                AsyncSmtpTransport::<Tokio1Executor>::starttls_relay(host).map_err(|e| {
                    anyhow::anyhow!("Invalid SMTP STARTTLS relay host '{host}': {e}")
                })?
            }
        };

        // For "none" mode, builder already has port set; for others, set port after TLS setup
        if effective_mode != "none" && effective_mode != "insecure" {
            builder = builder.port(port);
        }

        if let (Some(u), Some(p)) = (username, password) {
            builder = builder.credentials(Credentials::new(u, p));
        }

        let transport = builder.build();

        Ok(Self {
            transport,
            from_email,
        })
    }
}

#[async_trait]
impl NotificationProviderTrait for SmtpProvider {
    async fn send(&self, recipient: &str, content: &str) -> Result<String> {
        info!("📧 Sending SMTP email to {}", recipient);

        // Parse content to detect if it's HTML or multipart
        let is_html =
            content.trim().starts_with("<!DOCTYPE") || content.trim().starts_with("<html");

        let email = if is_html {
            // Extract text fallback from HTML content (simple stripping)
            let text_fallback = html_to_text(content);
            let plain_content_type = ContentType::parse("text/plain; charset=utf-8")
                .map_err(|e| NotiError::Internal(format!("Invalid plain content type: {e}")))?;
            let html_content_type = ContentType::parse("text/html; charset=utf-8")
                .map_err(|e| NotiError::Internal(format!("Invalid HTML content type: {e}")))?;

            Message::builder()
                .from(
                    self.from_email
                        .parse()
                        .map_err(|e| NotiError::Internal(format!("Invalid from email: {e}")))?,
                )
                .to(recipient
                    .parse()
                    .map_err(|e| NotiError::Internal(format!("Invalid recipient email: {e}")))?)
                .subject("GridTokenX Notification")
                .multipart(
                    MultiPart::alternative()
                        .singlepart(
                            SinglePart::builder()
                                .header(plain_content_type)
                                .body(text_fallback),
                        )
                        .singlepart(
                            SinglePart::builder()
                                .header(html_content_type)
                                .body(content.to_string()),
                        ),
                )
                .map_err(|e| NotiError::Internal(format!("Failed to build email: {e}")))?
        } else {
            Message::builder()
                .from(
                    self.from_email
                        .parse()
                        .map_err(|e| NotiError::Internal(format!("Invalid from email: {e}")))?,
                )
                .to(recipient
                    .parse()
                    .map_err(|e| NotiError::Internal(format!("Invalid recipient email: {e}")))?)
                .subject("GridTokenX Notification")
                .body(content.to_string())
                .map_err(|e| NotiError::Internal(format!("Failed to build email: {e}")))?
        };

        match self.transport.send(email).await {
            Ok(response) => {
                let id = response
                    .first_line()
                    .unwrap_or("unknown-smtp-id")
                    .to_string();
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

/// Simple HTML to text conversion for email fallback.
fn html_to_text(html: &str) -> String {
    // SAFETY: These regex patterns are compile-time verified string literals.
    static RE_SCRIPT_STYLE: std::sync::LazyLock<regex::Regex> = std::sync::LazyLock::new(|| {
        regex::Regex::new(r"(?s)<script[^>]*>.*?</script>|<style[^>]*>.*?</style>")
            .expect("valid regex")
    });
    static RE_TAGS: std::sync::LazyLock<regex::Regex> =
        std::sync::LazyLock::new(|| regex::Regex::new(r"<[^>]*>").expect("valid regex"));

    // Remove script and style elements
    let text = RE_SCRIPT_STYLE.replace_all(html, "");

    // Remove all HTML tags
    let text = RE_TAGS.replace_all(&text, "");

    // Decode HTML entities
    let text = text
        .replace("&amp;", "&")
        .replace("&lt;", "<")
        .replace("&gt;", ">")
        .replace("&quot;", "\"")
        .replace("&#39;", "'")
        .replace("&nbsp;", " ");

    // Clean up whitespace
    text.lines()
        .map(str::trim)
        .filter(|line| !line.is_empty())
        .collect::<Vec<_>>()
        .join("\n\n")
}
