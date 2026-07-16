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

/// Effective transport security, resolved from config + port convention.
#[derive(Debug, PartialEq, Eq, Clone, Copy)]
enum TlsMode {
    /// Implicit TLS from the first byte (SMTPS, conventionally port 465).
    Tls,
    /// Plaintext connect upgraded via STARTTLS (the default).
    StartTls,
    /// No TLS at all — local testing only (e.g. Mailpit).
    None,
}

/// Resolve the configured `SMTP_TLS_MODE` string: unset falls back on the port
/// convention (465 → implicit TLS, anything else → STARTTLS); an unrecognized
/// value degrades to STARTTLS (never to plaintext) with an error log.
fn resolve_tls_mode(tls_mode: Option<&str>, port: u16) -> TlsMode {
    let mode = tls_mode.unwrap_or(if port == 465 { "tls" } else { "starttls" });
    match mode {
        "tls" => TlsMode::Tls,
        "starttls" => TlsMode::StartTls,
        "none" | "insecure" => TlsMode::None,
        _ => {
            error!("Unknown SMTP TLS mode: {mode}, falling back to starttls");
            TlsMode::StartTls
        }
    }
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
        let effective_mode = resolve_tls_mode(tls_mode, port);

        let mut builder = match effective_mode {
            TlsMode::Tls => AsyncSmtpTransport::<Tokio1Executor>::relay(host)
                .map_err(|e| anyhow::anyhow!("Invalid SMTP TLS relay host '{host}': {e}"))?,
            TlsMode::StartTls => AsyncSmtpTransport::<Tokio1Executor>::starttls_relay(host)
                .map_err(|e| anyhow::anyhow!("Invalid SMTP STARTTLS relay host '{host}': {e}"))?,
            TlsMode::None => {
                // No TLS - suitable for local testing with Mailpit
                AsyncSmtpTransport::<Tokio1Executor>::builder_dangerous(host).port(port)
            }
        };

        // For `None` mode, builder already has port set; for others, set port after TLS setup
        if effective_mode != TlsMode::None {
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
        let is_html = looks_like_html(content);

        let email = if is_html {
            // HTML templates carry the subject in their `<title>` (the Tera
            // `subject` block). Only HTML content is autoescaped, so extracting
            // the subject here is safe; never parse it out of plain-text
            // content, whose variables are NOT escaped and could smuggle a
            // `<title>` into the Subject header.
            let subject =
                extract_subject(content).unwrap_or_else(|| "GridTokenX Notification".to_string());

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
                .subject(subject)
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

    let text = decode_entities(&text);

    // Clean up whitespace
    text.lines()
        .map(str::trim)
        .filter(|line| !line.is_empty())
        .collect::<Vec<_>>()
        .join("\n\n")
}

/// Decode the HTML entities Tera's autoescaper (and common markup) emits.
/// `&amp;` is decoded last so `&amp;lt;` doesn't double-decode.
fn decode_entities(text: &str) -> String {
    text.replace("&lt;", "<")
        .replace("&gt;", ">")
        .replace("&quot;", "\"")
        .replace("&#39;", "'")
        .replace("&#x27;", "'")
        .replace("&#x2F;", "/")
        .replace("&#x2f;", "/")
        .replace("&nbsp;", " ")
        .replace("&amp;", "&")
}

/// Whether `content` should be treated as an HTML email body.
///
/// Security-relevant: only HTML content (whose variables Tera autoescapes) is
/// ever parsed for a Subject via [`extract_subject`]. Plain-text content is
/// classified `false` here so its unescaped variables can never be parsed into
/// the Subject header. See the `<title>`-injection note in `send`.
fn looks_like_html(content: &str) -> bool {
    let trimmed = content.trim();
    trimmed.starts_with("<!DOCTYPE") || trimmed.starts_with("<html")
}

/// Pull the email subject from the rendered HTML's `<title>` element
/// (templates set it via the Tera `subject` block in `base.html.tera`).
fn extract_subject(html: &str) -> Option<String> {
    static RE_TITLE: std::sync::LazyLock<regex::Regex> = std::sync::LazyLock::new(|| {
        regex::Regex::new(r"(?si)<title[^>]*>(.*?)</title>").expect("valid regex")
    });

    let raw = RE_TITLE.captures(html)?.get(1)?.as_str();
    let subject = decode_entities(raw).split_whitespace().collect::<Vec<_>>().join(" ");
    (!subject.is_empty()).then_some(subject)
}

#[cfg(test)]
mod tests {
    //! Regression tests for the SMTP Subject-header injection fix
    //! (commits 58b5f44 / 684b3e0). Two mechanisms are guarded:
    //!   1. `looks_like_html` — only HTML content is ever parsed for a subject,
    //!      so plain-text (unescaped) variables can't smuggle a `<title>` into
    //!      the Subject header.
    //!   2. `extract_subject` — collapses ALL whitespace (incl. CR/LF), so a
    //!      crafted `<title>` cannot inject extra headers via newlines.

    use super::{SmtpProvider, TlsMode, extract_subject, looks_like_html, resolve_tls_mode};

    // --- Mechanism 1: plain text is never treated as HTML ------------------

    #[test]
    fn plain_text_with_title_is_not_html() {
        // A plain-text template whose (unescaped) variable injected a <title>
        // must NOT be classified as HTML — otherwise its `<title>` would be
        // lifted into the Subject header.
        let content = "Your verification code is <title>spoofed subject</title> 123456";
        assert!(!looks_like_html(content));
    }

    #[test]
    fn plain_text_with_crlf_header_injection_is_not_html() {
        // Classic CRLF header-injection payload arriving via a plain-text body.
        let content = "Verify: 123\r\nBcc: attacker@evil.example\r\nSubject: spoofed";
        assert!(!looks_like_html(content));
    }

    #[test]
    fn html_doctype_is_detected() {
        assert!(looks_like_html(
            "<!DOCTYPE html><html><head><title>Welcome</title></head></html>"
        ));
    }

    #[test]
    fn html_tag_with_leading_whitespace_is_detected() {
        assert!(looks_like_html("   \n<html><title>Welcome</title></html>"));
    }

    // --- Mechanism 2: extracted subject can't carry injected headers -------

    #[test]
    fn extract_subject_collapses_crlf_injection() {
        // Even from trusted (autoescaped) HTML, a newline in the title must be
        // collapsed so it can never split the Subject into extra headers.
        let html = "<html><title>Welcome\r\nBcc: attacker@evil.example</title></html>";
        let subject = extract_subject(html).expect("title present");
        assert!(
            !subject.contains('\r') && !subject.contains('\n'),
            "subject must not contain CR/LF: {subject:?}"
        );
        assert_eq!(subject, "Welcome Bcc: attacker@evil.example");
    }

    #[test]
    fn extract_subject_collapses_tabs_and_runs_of_whitespace() {
        let html = "<html><title>Order\t\t  Matched\n\n  Now</title></html>";
        assert_eq!(extract_subject(html).expect("title present"), "Order Matched Now");
    }

    #[test]
    fn extract_subject_decodes_entities() {
        let html = "<html><title>Tom &amp; Jerry &lt;3</title></html>";
        assert_eq!(extract_subject(html).expect("title present"), "Tom & Jerry <3");
    }

    #[test]
    fn extract_subject_none_when_no_title() {
        assert!(extract_subject("<html><body>no title here</body></html>").is_none());
    }

    #[test]
    fn extract_subject_none_when_title_is_blank() {
        // Whitespace-only title collapses to empty → None, so the caller falls
        // back to the fixed default subject rather than an empty header.
        assert!(extract_subject("<html><title>   \r\n  </title></html>").is_none());
    }

    // --- TLS mode resolution (`SMTP_TLS_MODE` + port convention) -----------

    #[test]
    fn tls_mode_unset_infers_implicit_tls_on_port_465() {
        assert_eq!(resolve_tls_mode(None, 465), TlsMode::Tls);
    }

    #[test]
    fn tls_mode_unset_defaults_to_starttls_on_other_ports() {
        assert_eq!(resolve_tls_mode(None, 587), TlsMode::StartTls);
        assert_eq!(resolve_tls_mode(None, 25), TlsMode::StartTls);
        assert_eq!(resolve_tls_mode(None, 2525), TlsMode::StartTls);
    }

    #[test]
    fn tls_mode_explicit_setting_overrides_port_convention() {
        // Explicit mode wins even on port 465 / the STARTTLS ports.
        assert_eq!(resolve_tls_mode(Some("starttls"), 465), TlsMode::StartTls);
        assert_eq!(resolve_tls_mode(Some("tls"), 587), TlsMode::Tls);
        assert_eq!(resolve_tls_mode(Some("none"), 465), TlsMode::None);
        assert_eq!(resolve_tls_mode(Some("insecure"), 587), TlsMode::None);
    }

    #[test]
    fn tls_mode_unknown_value_degrades_to_starttls_never_plaintext() {
        assert_eq!(resolve_tls_mode(Some("tsl"), 587), TlsMode::StartTls);
        assert_eq!(resolve_tls_mode(Some(""), 465), TlsMode::StartTls);
    }

    #[test]
    fn new_builds_a_transport_for_every_resolved_mode() {
        // Host validation happens at connect time, not build time, so `new`
        // must succeed for every mode; this pins that each resolved branch
        // reaches a working builder (port + credentials wiring included).
        for (port, mode) in [(465, None), (587, None), (25, Some("none")), (587, Some("bogus"))] {
            assert!(
                SmtpProvider::new(
                    "smtp.example.test",
                    port,
                    Some("user".to_string()),
                    Some("pass".to_string()),
                    "from@x.test".to_string(),
                    mode,
                )
                .is_ok(),
                "SmtpProvider::new failed for port {port} mode {mode:?}"
            );
        }
    }
}
