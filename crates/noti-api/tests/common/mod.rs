//! Shared test scaffolding for the `noti-api` integration tests.
//!
//! Every handler/service test needs the same set of inert adapter stubs to
//! stand up an `AppState`/orchestrator while it exercises one surface. Those
//! stubs were copy-pasted into each test file; this centralizes the identical
//! ones. Test-specific *recording* fakes (which capture the calls a given test
//! asserts on) stay in their own files.
//!
//! `mod common;` compiles once per test binary, so not every test uses every
//! item here — hence the blanket `dead_code` allow.

#![allow(dead_code)]

use async_trait::async_trait;
use chrono::Utc;
use uuid::Uuid;

use noti_core::domain::{DevicePlatform, DeviceToken};
use noti_core::error::Result;
use noti_core::traits::{
    CacheTrait, DeviceTokenRepositoryTrait, MessageQueueTrait, NotificationProviderTrait,
    TemplateEngineTrait,
};

/// Header the `ServiceRole` extractor reads.
pub const ROLE_HEADER: &str = "x-gridtokenx-role";
/// Header the `UserContext` extractor reads.
pub const USER_HEADER: &str = "x-gridtokenx-user-id";

/// Cache that accepts every write, holds nothing, and grants every lock.
pub struct StubCache;
#[async_trait]
impl CacheTrait for StubCache {
    async fn set_value(&self, _k: &str, _v: serde_json::Value, _t: u64) -> Result<()> {
        Ok(())
    }
    async fn get_value(&self, _k: &str) -> Result<Option<serde_json::Value>> {
        Ok(None)
    }
    async fn increment_with_ttl(&self, _k: &str, _t: u64) -> Result<i64> {
        Ok(1)
    }
    async fn delete(&self, _k: &str) -> Result<()> {
        Ok(())
    }
    async fn lock(&self, _k: &str, _t: u64) -> Result<bool> {
        Ok(true)
    }
    async fn unlock(&self, _k: &str) -> Result<()> {
        Ok(())
    }
}

/// Template engine that renders a fixed body for any input.
pub struct StubTemplate;
impl TemplateEngineTrait for StubTemplate {
    fn render(&self, _i: &str, _v: &serde_json::Value) -> Result<String> {
        Ok("body".into())
    }
}

/// Provider that accepts every send.
pub struct StubProvider;
#[async_trait]
impl NotificationProviderTrait for StubProvider {
    async fn send(&self, _r: &str, _b: &str) -> Result<String> {
        Ok("ref".into())
    }
    fn provider_id(&self) -> &'static str {
        "stub"
    }
}

/// Message queue that accepts every publish.
pub struct StubMq;
#[async_trait]
impl MessageQueueTrait for StubMq {
    async fn publish_dispatch(&self, _id: Uuid) -> Result<()> {
        Ok(())
    }
    async fn publish_retry(&self, _id: Uuid, _d: u32) -> Result<()> {
        Ok(())
    }
}

/// Device registry stub: register echoes a row, everything else is a no-op. For
/// tests that never touch device routes.
pub struct StubDeviceRepo;
#[async_trait]
impl DeviceTokenRepositoryTrait for StubDeviceRepo {
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
    async fn active_for_user(&self, _user_id: Uuid) -> Result<Vec<DeviceToken>> {
        Ok(vec![])
    }
    async fn revoke(&self, _token: &str) -> Result<()> {
        Ok(())
    }
}
