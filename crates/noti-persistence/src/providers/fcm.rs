//! Firebase Cloud Messaging (FCM HTTP v1) push provider.
//!
//! Delivers to mobile (Android/iOS) and web push from one provider. The
//! notification `recipient` is a **`user_id`**, not a single device token: the
//! provider fans out to every active token in the [`DeviceTokenRepositoryTrait`]
//! registry and sends one HTTP v1 request per token (v1 has no multi-token
//! batch endpoint outside the Admin SDK).
//!
//! ## Auth
//! FCM v1 requires an `OAuth2` bearer token. We mint one directly from the Google
//! service-account JSON: build an RS256-signed JWT assertion and exchange it at
//! the account's `token_uri` for an access token (cached until ~1 min before
//! expiry). This avoids pulling a heavyweight Google-auth dependency — the
//! workspace already ships `jsonwebtoken` + `reqwest`.
//!
//! ## Self-healing
//! When FCM reports a token `UNREGISTERED` / `INVALID_ARGUMENT`, that token is
//! revoked in the registry so it is never retried.

use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use jsonwebtoken::{Algorithm, EncodingKey, Header, encode};
use serde::{Deserialize, Serialize};
use serde_json::json;
use tokio::sync::Mutex;
use tokio::time::Instant;
use tracing::{error, info, warn};
use uuid::Uuid;

use noti_core::domain::{DevicePlatform, DeviceToken};
use noti_core::error::{NotiError, Result};
use noti_core::traits::{DeviceTokenRepositoryTrait, NotificationProviderTrait};

const FCM_SCOPE: &str = "https://www.googleapis.com/auth/firebase.messaging";
/// Base origin of the FCM HTTP v1 API. Overridable per-provider (see
/// [`FcmProvider::with_messaging_base_url`]) only so tests can point sends at a
/// local stub; production always uses this.
const FCM_BASE_URL: &str = "https://fcm.googleapis.com";
const TOKEN_GRANT_TYPE: &str = "urn:ietf:params:oauth:grant-type:jwt-bearer";
const REQUEST_TIMEOUT_SECS: u64 = 10;
/// Refresh the cached access token this many seconds before its stated expiry.
const TOKEN_REFRESH_SKEW_SECS: u64 = 60;

/// Subset of the Google service-account JSON we need to mint tokens.
#[derive(Debug, Clone, Deserialize)]
struct ServiceAccount {
    client_email: String,
    private_key: String,
    token_uri: String,
    #[serde(default)]
    private_key_id: Option<String>,
}

/// `OAuth2` JWT assertion claims (RFC 7523 + Google scope).
#[derive(Serialize)]
struct Claims<'a> {
    iss: &'a str,
    scope: &'a str,
    aud: &'a str,
    iat: i64,
    exp: i64,
}

#[derive(Deserialize)]
struct TokenResponse {
    access_token: String,
    expires_in: u64,
}

struct CachedToken {
    value: String,
    /// When the cached token should be considered stale.
    refresh_at: Instant,
}

/// Rendered push body. Push templates render JSON `{"title","body","data"?}`;
/// non-JSON content degrades to a bodied notification with a default title.
#[derive(Deserialize)]
struct PushPayload {
    #[serde(default = "default_title")]
    title: String,
    body: String,
    #[serde(default)]
    data: Option<serde_json::Value>,
}

fn default_title() -> String {
    "GridTokenX".to_string()
}

pub struct FcmProvider {
    project_id: String,
    service_account: ServiceAccount,
    encoding_key: EncodingKey,
    client: reqwest::Client,
    token_cache: Mutex<Option<CachedToken>>,
    device_repo: Arc<dyn DeviceTokenRepositoryTrait>,
    /// Origin for `messages:send` requests. Defaults to [`FCM_BASE_URL`].
    messaging_base_url: String,
}

impl FcmProvider {
    /// Build a provider from a service-account JSON file path.
    ///
    /// # Errors
    ///
    /// Returns an error if the credentials file is missing/malformed, the
    /// private key is not valid RSA PEM, or the HTTP client cannot be built.
    pub fn from_credentials_file(
        project_id: String,
        credentials_path: &str,
        device_repo: Arc<dyn DeviceTokenRepositoryTrait>,
    ) -> anyhow::Result<Self> {
        let raw = std::fs::read_to_string(credentials_path).map_err(|e| {
            anyhow::anyhow!("failed to read FCM credentials '{credentials_path}': {e}")
        })?;
        let service_account: ServiceAccount = serde_json::from_str(&raw)
            .map_err(|e| anyhow::anyhow!("invalid FCM service-account JSON: {e}"))?;

        let encoding_key = EncodingKey::from_rsa_pem(service_account.private_key.as_bytes())
            .map_err(|e| anyhow::anyhow!("FCM service-account private_key is not valid RSA PEM: {e}"))?;

        let client = reqwest::Client::builder()
            .timeout(Duration::from_secs(REQUEST_TIMEOUT_SECS))
            .build()
            .map_err(|e| anyhow::anyhow!("failed to build FCM HTTP client: {e}"))?;

        Ok(Self {
            project_id,
            service_account,
            encoding_key,
            client,
            token_cache: Mutex::new(None),
            device_repo,
            messaging_base_url: FCM_BASE_URL.to_string(),
        })
    }

    /// Override the FCM messaging origin (default [`FCM_BASE_URL`]).
    ///
    /// Exists so integration tests can redirect `messages:send` at a local stub
    /// server; production code never calls this.
    #[must_use]
    pub fn with_messaging_base_url(mut self, base_url: impl Into<String>) -> Self {
        self.messaging_base_url = base_url.into();
        self
    }

    /// Verify the service-account credentials are usable by minting (and
    /// caching) an access token against Google's token endpoint. Intended as a
    /// startup fail-fast / health preflight: a malformed key or wrong
    /// `token_uri` surfaces here instead of on the first real send.
    ///
    /// # Errors
    ///
    /// Returns an error if the token cannot be minted (bad key, network, or a
    /// non-success response from the token endpoint).
    pub async fn preflight(&self) -> Result<()> {
        self.access_token().await.map(|_| ())
    }

    /// Return a valid `OAuth2` access token, minting + caching a fresh one when
    /// the cache is empty or near expiry.
    async fn access_token(&self) -> Result<String> {
        let mut cache = self.token_cache.lock().await;
        if let Some(cached) = cache.as_ref()
            && Instant::now() < cached.refresh_at
        {
            return Ok(cached.value.clone());
        }

        let (token, expires_in) = self.mint_token().await?;
        let refresh_at = Instant::now()
            + Duration::from_secs(expires_in.saturating_sub(TOKEN_REFRESH_SKEW_SECS).max(1));
        *cache = Some(CachedToken {
            value: token.clone(),
            refresh_at,
        });
        Ok(token)
    }

    /// Build the signed JWT assertion and exchange it for an access token.
    async fn mint_token(&self) -> Result<(String, u64)> {
        let now = gridtokenx_telemetry::time::now().timestamp();
        let claims = Claims {
            iss: &self.service_account.client_email,
            scope: FCM_SCOPE,
            aud: &self.service_account.token_uri,
            iat: now,
            exp: now + 3600,
        };

        let mut header = Header::new(Algorithm::RS256);
        header.kid = self.service_account.private_key_id.clone();

        let assertion = encode(&header, &claims, &self.encoding_key)
            .map_err(|e| NotiError::Provider(format!("FCM JWT signing failed: {e}")))?;

        let resp = self
            .client
            .post(&self.service_account.token_uri)
            .form(&[
                ("grant_type", TOKEN_GRANT_TYPE),
                ("assertion", assertion.as_str()),
            ])
            .send()
            .await
            .map_err(|e| NotiError::Provider(format!("FCM token request failed: {e}")))?;

        if !resp.status().is_success() {
            let status = resp.status();
            let body = resp.text().await.unwrap_or_default();
            return Err(NotiError::Provider(format!(
                "FCM token endpoint returned {status}: {body}"
            )));
        }

        let parsed: TokenResponse = resp
            .json()
            .await
            .map_err(|e| NotiError::Provider(format!("FCM token response parse failed: {e}")))?;
        Ok((parsed.access_token, parsed.expires_in))
    }

    /// Send one rendered payload to a single device token.
    /// Returns `Ok(())` on accept, `Err(SendError)` classified for the caller.
    async fn send_to_token(
        &self,
        access_token: &str,
        device: &DeviceToken,
        payload: &PushPayload,
    ) -> std::result::Result<(), SendError> {
        let url = format!(
            "{}/v1/projects/{}/messages:send",
            self.messaging_base_url, self.project_id
        );
        let message = build_message(device, payload);

        let resp = self
            .client
            .post(&url)
            .bearer_auth(access_token)
            .json(&json!({ "message": message }))
            .send()
            .await
            .map_err(|e| SendError::Retryable(format!("FCM send failed: {e}")))?;

        let status = resp.status();
        if status.is_success() {
            return Ok(());
        }

        let body = resp.text().await.unwrap_or_default();
        if is_invalid_token(status.as_u16(), &body) {
            Err(SendError::InvalidToken(body))
        } else {
            Err(SendError::Retryable(format!(
                "FCM send returned {status}: {body}"
            )))
        }
    }
}

/// Per-token send outcome classification.
enum SendError {
    /// Token is dead — revoke it, never retry.
    InvalidToken(String),
    /// Transient/server-side — surface so the notification is retried.
    Retryable(String),
}

#[async_trait]
impl NotificationProviderTrait for FcmProvider {
    /// `recipient` is the target user's id (UUID). Fans out to all of that
    /// user's active device tokens.
    async fn send(&self, recipient: &str, content: &str) -> Result<String> {
        let user_id = Uuid::parse_str(recipient).map_err(|e| {
            NotiError::Validation(format!("FCM recipient must be a user_id UUID: {e}"))
        })?;

        let payload = parse_payload(content);

        let tokens = self.device_repo.active_for_user(user_id).await?;
        if tokens.is_empty() {
            // No registered devices: nothing to deliver, and retrying won't help.
            info!("🔔 FCM: user {user_id} has no active device tokens; skipping");
            return Ok(format!("fcm-no-devices-{user_id}"));
        }

        let access_token = self.access_token().await?;

        let mut delivered = 0_u32;
        let mut pruned = 0_u32;
        let mut last_retryable: Option<String> = None;

        for device in &tokens {
            match self.send_to_token(&access_token, device, &payload).await {
                Ok(()) => delivered += 1,
                Err(SendError::InvalidToken(reason)) => {
                    pruned += 1;
                    warn!(
                        "🔔 FCM: revoking invalid token for user {user_id} ({}): {reason}",
                        device.platform.as_str()
                    );
                    if let Err(e) = self.device_repo.revoke(&device.token).await {
                        error!("Failed to revoke invalid FCM token: {e}");
                    }
                }
                Err(SendError::Retryable(reason)) => {
                    error!("🔔 FCM: retryable send error for user {user_id}: {reason}");
                    last_retryable = Some(reason);
                }
            }
        }

        // Any successful delivery (or only-dead-tokens) is terminal success.
        // Only when nothing was delivered AND at least one error was retryable
        // do we surface an error so the notification is re-queued.
        if delivered > 0 || last_retryable.is_none() {
            info!("✅ FCM: user {user_id} delivered={delivered} pruned={pruned}");
            Ok(format!("fcm-delivered-{delivered}-pruned-{pruned}"))
        } else {
            Err(NotiError::Provider(last_retryable.unwrap_or_else(|| {
                "FCM delivery failed for all tokens".to_string()
            })))
        }
    }

    fn provider_id(&self) -> &'static str {
        "fcm"
    }
}

/// Parse rendered content into a [`PushPayload`]; fall back to treating the
/// whole string as the body with a default title.
fn parse_payload(content: &str) -> PushPayload {
    serde_json::from_str::<PushPayload>(content).unwrap_or_else(|_| PushPayload {
        title: default_title(),
        body: content.to_string(),
        data: None,
    })
}

/// Build the platform-specific FCM v1 `message` object for one device.
fn build_message(device: &DeviceToken, payload: &PushPayload) -> serde_json::Value {
    let mut message = json!({
        "token": device.token,
        "notification": { "title": payload.title, "body": payload.body },
    });

    if let Some(data) = &payload.data
        && let Some(obj) = message.as_object_mut()
    {
        obj.insert("data".to_string(), data.clone());
    }

    // Minimal, valid per-platform blocks. Android/iOS get a high-priority
    // delivery hint; web gets a webpush envelope so background pushes render.
    if let Some(obj) = message.as_object_mut() {
        match device.platform {
            DevicePlatform::Android => {
                obj.insert("android".to_string(), json!({ "priority": "high" }));
            }
            DevicePlatform::Ios => {
                obj.insert(
                    "apns".to_string(),
                    json!({ "headers": { "apns-priority": "10" } }),
                );
            }
            DevicePlatform::Web => {
                obj.insert(
                    "webpush".to_string(),
                    json!({ "headers": { "Urgency": "high" } }),
                );
            }
        }
    }

    message
}

/// Classify an FCM error response as a dead-token signal worth pruning.
///
/// FCM v1 returns HTTP 404 with `errorCode: UNREGISTERED` for tokens whose app
/// was uninstalled, and 400 `INVALID_ARGUMENT` for malformed tokens. Both mean
/// the token will never deliver again.
fn is_invalid_token(http_status: u16, body: &str) -> bool {
    if http_status == 404 {
        return true;
    }
    if let Ok(v) = serde_json::from_str::<serde_json::Value>(body) {
        // Look for the FCM-specific errorCode in error.details[].errorCode,
        // falling back to the top-level error.status.
        if let Some(details) = v
            .get("error")
            .and_then(|e| e.get("details"))
            .and_then(|d| d.as_array())
        {
            for d in details {
                if let Some(code) = d.get("errorCode").and_then(|c| c.as_str())
                    && matches!(code, "UNREGISTERED" | "INVALID_ARGUMENT")
                {
                    return true;
                }
            }
        }
        if let Some(status) = v.get("error").and_then(|e| e.get("status")).and_then(|s| s.as_str())
        {
            return status == "UNREGISTERED";
        }
    }
    false
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::Utc;

    fn device(platform: DevicePlatform) -> DeviceToken {
        DeviceToken {
            id: Uuid::new_v4(),
            user_id: Uuid::new_v4(),
            token: "tok-123".to_string(),
            platform,
            created_at: Utc::now(),
            last_seen_at: Utc::now(),
        }
    }

    #[test]
    fn parses_json_payload() {
        let p = parse_payload(r#"{"title":"Hi","body":"There","data":{"k":"v"}}"#);
        assert_eq!(p.title, "Hi");
        assert_eq!(p.body, "There");
        assert!(p.data.is_some());
    }

    #[test]
    fn falls_back_to_plain_body() {
        let p = parse_payload("just some text");
        assert_eq!(p.title, "GridTokenX");
        assert_eq!(p.body, "just some text");
        assert!(p.data.is_none());
    }

    #[test]
    fn web_message_has_webpush_block() {
        let msg = build_message(
            &device(DevicePlatform::Web),
            &parse_payload(r#"{"body":"hi"}"#),
        );
        assert!(msg.get("webpush").is_some());
        assert_eq!(msg["notification"]["body"], "hi");
    }

    #[test]
    fn android_message_has_android_block() {
        let msg = build_message(
            &device(DevicePlatform::Android),
            &parse_payload(r#"{"body":"hi"}"#),
        );
        assert_eq!(msg["android"]["priority"], "high");
    }

    #[test]
    fn ios_message_has_apns_block() {
        let msg = build_message(&device(DevicePlatform::Ios), &parse_payload(r#"{"body":"hi"}"#));
        assert_eq!(msg["apns"]["headers"]["apns-priority"], "10");
    }

    #[test]
    fn http_404_is_invalid_token() {
        assert!(is_invalid_token(404, ""));
    }

    #[test]
    fn unregistered_error_code_is_invalid_token() {
        let body = r#"{"error":{"code":404,"status":"NOT_FOUND","details":[{"@type":"type.googleapis.com/google.firebase.fcm.v1.FcmError","errorCode":"UNREGISTERED"}]}}"#;
        assert!(is_invalid_token(404, body));
    }

    #[test]
    fn invalid_argument_error_code_is_invalid_token() {
        let body = r#"{"error":{"code":400,"status":"INVALID_ARGUMENT","details":[{"errorCode":"INVALID_ARGUMENT"}]}}"#;
        assert!(is_invalid_token(400, body));
    }

    #[test]
    fn server_error_is_not_invalid_token() {
        let body = r#"{"error":{"code":503,"status":"UNAVAILABLE"}}"#;
        assert!(!is_invalid_token(503, body));
    }
}
