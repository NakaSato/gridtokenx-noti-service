//! HTTP webhook delivery provider.
//!
//! POSTs rendered notification content to a user-supplied URL (the
//! notification `recipient`). Because the destination is attacker-influenced,
//! every request is guarded against SSRF: only `http`/`https` is allowed,
//! redirects are disabled, and the resolved address must be a public unicast
//! IP. The connection is pinned to the vetted address to defeat DNS rebinding.

use std::net::{IpAddr, SocketAddr};
use std::time::Duration;

use async_trait::async_trait;
use tracing::{error, info};
use uuid::Uuid;

use noti_core::error::{NotiError, Result};
use noti_core::traits::NotificationProviderTrait;

const REQUEST_TIMEOUT_SECS: u64 = 10;

pub struct WebhookProvider {
    timeout: Duration,
    /// When `true` (default), reject targets that resolve to private/loopback/
    /// link-local addresses (SSRF guard). Set `false` only for trusted internal
    /// webhooks (or tests against a local stub).
    block_private: bool,
}

impl WebhookProvider {
    #[must_use]
    pub fn new() -> Self {
        Self {
            timeout: Duration::from_secs(REQUEST_TIMEOUT_SECS),
            block_private: true,
        }
    }

    /// Build a provider with the SSRF private-address guard toggled.
    #[must_use]
    pub fn with_block_private(block_private: bool) -> Self {
        Self {
            timeout: Duration::from_secs(REQUEST_TIMEOUT_SECS),
            block_private,
        }
    }
}

impl Default for WebhookProvider {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait]
impl NotificationProviderTrait for WebhookProvider {
    async fn send(&self, recipient: &str, content: &str) -> Result<String> {
        let url = reqwest::Url::parse(recipient)
            .map_err(|e| NotiError::Internal(format!("Invalid webhook URL '{recipient}': {e}")))?;

        if !matches!(url.scheme(), "http" | "https") {
            return Err(NotiError::Internal(format!(
                "Webhook URL scheme '{}' not allowed (http/https only)",
                url.scheme()
            )));
        }

        let host = url
            .host_str()
            .ok_or_else(|| NotiError::Internal("Webhook URL has no host".to_string()))?
            .to_string();
        let port = url.port_or_known_default().unwrap_or(0);

        // SSRF guard: resolve and reject any private / loopback / link-local target.
        let addrs: Vec<SocketAddr> = tokio::net::lookup_host((host.as_str(), port))
            .await
            .map_err(|e| {
                NotiError::Internal(format!("Webhook DNS resolution failed for '{host}': {e}"))
            })?
            .collect();

        if addrs.is_empty() {
            return Err(NotiError::Internal(format!(
                "Webhook host '{host}' resolved to no addresses"
            )));
        }
        // Conservative: block if ANY resolved address is non-public.
        if self.block_private
            && let Some(blocked) = addrs.iter().find(|a| is_blocked_ip(a.ip()))
        {
            return Err(NotiError::Internal(format!(
                "Webhook target '{host}' resolves to blocked address {} (private/loopback/link-local)",
                blocked.ip()
            )));
        }

        // Pin the connection to the vetted address to defeat DNS rebinding (TOCTOU).
        let safe_addr = addrs[0];
        let client = reqwest::Client::builder()
            .timeout(self.timeout)
            .redirect(reqwest::redirect::Policy::none())
            .resolve(&host, safe_addr)
            .build()
            .map_err(|e| {
                NotiError::Internal(format!("Failed to build webhook HTTP client: {e}"))
            })?;

        let trimmed = content.trim_start();
        let content_type = if trimmed.starts_with('{') || trimmed.starts_with('[') {
            "application/json"
        } else {
            "text/plain; charset=utf-8"
        };

        info!("🔗 POST webhook to {}", recipient);
        let resp = client
            .post(url)
            .header(reqwest::header::CONTENT_TYPE, content_type)
            .body(content.to_string())
            .send()
            .await
            .map_err(|e| NotiError::Internal(format!("Webhook POST to '{recipient}' failed: {e}")))?;

        let status = resp.status();
        if status.is_success() {
            info!("✅ Webhook delivered: {} -> {}", recipient, status);
            Ok(format!("webhook-{}-{}", status.as_u16(), Uuid::new_v4()))
        } else {
            error!("❌ Webhook non-2xx: {} -> {}", recipient, status);
            Err(NotiError::Internal(format!(
                "Webhook '{recipient}' returned status {status}"
            )))
        }
    }

    fn provider_id(&self) -> &'static str {
        "webhook"
    }
}

/// Returns `true` if the IP must not be reached by an outbound webhook
/// (loopback, private RFC1918/ULA, link-local incl. cloud metadata, CGNAT,
/// unspecified, broadcast, documentation ranges).
fn is_blocked_ip(ip: IpAddr) -> bool {
    match ip {
        IpAddr::V4(v4) => {
            v4.is_loopback()
                || v4.is_private()
                || v4.is_link_local()
                || v4.is_unspecified()
                || v4.is_broadcast()
                || v4.is_documentation()
                || v4.octets()[0] == 0
                // shared CGNAT space 100.64.0.0/10
                || (v4.octets()[0] == 100 && (v4.octets()[1] & 0xc0) == 64)
        }
        IpAddr::V6(v6) => {
            v6.is_loopback()
                || v6.is_unspecified()
                // unique local fc00::/7
                || (v6.segments()[0] & 0xfe00) == 0xfc00
                // link local fe80::/10
                || (v6.segments()[0] & 0xffc0) == 0xfe80
                // IPv4-mapped — re-check against v4 rules
                || v6.to_ipv4_mapped().is_some_and(|m| is_blocked_ip(IpAddr::V4(m)))
        }
    }
}

#[cfg(test)]
mod tests {
    use super::is_blocked_ip;
    use std::net::IpAddr;

    fn ip(s: &str) -> IpAddr {
        s.parse().expect("valid ip literal")
    }

    #[test]
    fn blocks_private_and_loopback() {
        for s in [
            "127.0.0.1",
            "10.1.2.3",
            "172.16.0.1",
            "192.168.1.1",
            "169.254.169.254", // cloud metadata
            "0.0.0.0",
            "100.64.0.1", // CGNAT
            "::1",
            "fe80::1",
            "fc00::1",
            "::ffff:127.0.0.1", // v4-mapped loopback
        ] {
            assert!(is_blocked_ip(ip(s)), "{s} should be blocked");
        }
    }

    #[test]
    fn allows_public() {
        for s in ["8.8.8.8", "1.1.1.1", "93.184.216.34", "2606:4700:4700::1111"] {
            assert!(!is_blocked_ip(ip(s)), "{s} should be allowed");
        }
    }

    #[tokio::test]
    async fn send_rejects_loopback_target() {
        use noti_core::traits::NotificationProviderTrait;
        let provider = super::WebhookProvider::new(); // guard on (default)
        match provider.send("http://127.0.0.1:9/hook", "{}").await {
            Ok(_) => panic!("expected SSRF rejection for loopback target"),
            Err(e) => assert!(
                e.to_string().contains("blocked address"),
                "unexpected error: {e}"
            ),
        }
    }

    #[tokio::test]
    async fn send_rejects_non_http_scheme() {
        use noti_core::traits::NotificationProviderTrait;
        let provider = super::WebhookProvider::new();
        match provider.send("ftp://example.com/x", "{}").await {
            Ok(_) => panic!("expected rejection for non-http scheme"),
            Err(e) => assert!(e.to_string().contains("not allowed"), "unexpected error: {e}"),
        }
    }
}
