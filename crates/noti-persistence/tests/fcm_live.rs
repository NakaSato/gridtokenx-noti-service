//! Live integration test for `FcmProvider` against the **real** FCM HTTP v1 API.
//!
//! Unlike SMTP (Mailpit) there is no local FCM emulator, so this requires real
//! Google service-account credentials and only runs when they are provided via
//! env. Without them it **skips** (prints a notice and returns) rather than
//! failing — so `cargo test` stays green on machines with no FCM secrets.
//!
//! ## Enabling
//! ```bash
//! export FCM_PROJECT_ID=my-firebase-project
//! export FCM_CREDENTIALS_PATH=/path/to/service-account.json
//! # Optional — exercise an end-to-end send to a real device:
//! export FCM_TEST_TOKEN=<a-real-registration-token>
//! export FCM_TEST_PLATFORM=web        # web | android | ios (default web)
//! cargo test -p noti-persistence --test fcm_live -- --nocapture
//! ```
//!
//! What it proves on the wire (impossible to cover offline):
//! 1. **`OAuth2` mint** — the RS256 JWT assertion is accepted by Google's token
//!    endpoint and exchanged for an access token (`preflight`).
//! 2. **Send** (only with `FCM_TEST_TOKEN`) — a real `messages:send` is
//!    accepted, exercising the per-platform message envelope and bearer auth.

#![allow(clippy::expect_used, clippy::unwrap_used)]

use std::sync::Arc;

use async_trait::async_trait;
use chrono::Utc;
use uuid::Uuid;

use noti_core::domain::{DevicePlatform, DeviceToken};
use noti_core::error::Result;
use noti_core::traits::{DeviceTokenRepositoryTrait, NotificationProviderTrait};
use noti_persistence::providers::fcm::FcmProvider;

/// In-memory device registry that hands back a single fixed token for every
/// user — enough to drive a real send without a database. `revoke` is recorded
/// so the test can assert self-healing when the token is rejected.
struct StubRepo {
    token: String,
    platform: DevicePlatform,
    revoked: tokio::sync::Mutex<Vec<String>>,
}

#[async_trait]
impl DeviceTokenRepositoryTrait for StubRepo {
    async fn register(
        &self,
        user_id: Uuid,
        token: &str,
        platform: DevicePlatform,
    ) -> Result<DeviceToken> {
        Ok(DeviceToken {
            id: Uuid::new_v4(),
            user_id,
            token: token.to_string(),
            platform,
            created_at: Utc::now(),
            last_seen_at: Utc::now(),
        })
    }

    async fn active_for_user(&self, user_id: Uuid) -> Result<Vec<DeviceToken>> {
        Ok(vec![DeviceToken {
            id: Uuid::new_v4(),
            user_id,
            token: self.token.clone(),
            platform: self.platform,
            created_at: Utc::now(),
            last_seen_at: Utc::now(),
        }])
    }

    async fn revoke(&self, token: &str) -> Result<()> {
        self.revoked.lock().await.push(token.to_string());
        Ok(())
    }
}

fn env(name: &str) -> Option<String> {
    std::env::var(name).ok().filter(|s| !s.trim().is_empty())
}

#[tokio::test]
async fn fcm_live_mint_and_send() {
    let (Some(project_id), Some(creds_path)) =
        (env("FCM_PROJECT_ID"), env("FCM_CREDENTIALS_PATH"))
    else {
        eprintln!(
            "SKIP fcm_live: set FCM_PROJECT_ID + FCM_CREDENTIALS_PATH (and optionally \
             FCM_TEST_TOKEN/FCM_TEST_PLATFORM) to run the live FCM test"
        );
        return;
    };

    let token = env("FCM_TEST_TOKEN").unwrap_or_else(|| "stub-token-no-send".to_string());
    let platform = env("FCM_TEST_PLATFORM")
        .as_deref()
        .and_then(DevicePlatform::from_db)
        .unwrap_or(DevicePlatform::Web);

    let repo = Arc::new(StubRepo {
        token,
        platform,
        revoked: tokio::sync::Mutex::new(Vec::new()),
    });

    let provider =
        FcmProvider::from_credentials_file(project_id, &creds_path, repo.clone())
            .expect("build FcmProvider from real credentials");

    // 1. `OAuth2` mint against Google's token endpoint (always runs once creds set).
    provider
        .preflight()
        .await
        .expect("FCM preflight: minting an OAuth2 access token failed");
    eprintln!("fcm_live: OAuth2 token minted OK");

    // 2. Real send — only when a genuine device token is supplied.
    if env("FCM_TEST_TOKEN").is_some() {
        let user_id = Uuid::new_v4();
        let content = r#"{"title":"GridTokenX Live Test","body":"Live FCM send from fcm_live integration test"}"#;
        let provider_ref = provider
            .send(&user_id.to_string(), content)
            .await
            .expect("FCM live send failed");
        eprintln!("fcm_live: send result = {provider_ref}");

        // If FCM rejected the token as dead, the provider must have self-healed
        // by revoking it. (A healthy token leaves `revoked` empty.)
        let revoked = repo.revoked.lock().await;
        if !revoked.is_empty() {
            eprintln!(
                "fcm_live: token reported invalid by FCM and was auto-revoked: {revoked:?}"
            );
        }
    } else {
        eprintln!("fcm_live: FCM_TEST_TOKEN unset — skipped the real send, mint-only");
    }
}
