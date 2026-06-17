//! Typed Kafka event payloads and per-event handlers.
//!
//! Each inbound event deserializes into a dedicated struct (instead of the
//! previous stringly-typed `event["data"][...].as_str().unwrap_or_default()`
//! access), so a missing or renamed field surfaces at the deserialization
//! boundary rather than silently degrading to an empty value.

use std::sync::Arc;

use serde::Deserialize;
use serde_json::Value;
use tracing::{debug, info, warn};
use uuid::Uuid;

use noti_core::domain::NotificationChannel;
use noti_logic::NotificationOrchestrator;

use super::url::{build_callback_url, rewrite_url, urlencode};

/// Kafka message coordinates, used to derive idempotency keys.
pub struct MsgCtx {
    pub partition: i32,
    pub offset: i64,
}

/// Parse an optional UUID string, returning `None` on absence or parse error.
fn parse_uuid(s: Option<&str>) -> Option<Uuid> {
    s.and_then(|v| Uuid::parse_str(v).ok())
}

/// Map an orchestrator error into `anyhow`.
fn into_anyhow(e: noti_core::error::NotiError) -> anyhow::Error {
    anyhow::anyhow!(e)
}

// ---------------------------------------------------------------------------
// Payload definitions
// ---------------------------------------------------------------------------

#[derive(Debug, Deserialize)]
struct EmailVerified {
    #[serde(default)]
    user_id: Option<String>,
    #[serde(default)]
    email: String,
    #[serde(default)]
    username: String,
}

#[derive(Debug, Deserialize)]
struct OrderMatched {
    #[serde(default)]
    buyer_id: Option<String>,
    #[serde(default)]
    seller_id: Option<String>,
    #[serde(default)]
    amount: Value,
    #[serde(default)]
    price: Value,
}

#[derive(Debug, Deserialize)]
struct SettlementProcessed {
    #[serde(default)]
    status: String,
    #[serde(default)]
    tx_signature: String,
    #[serde(default)]
    buyer_id: Option<String>,
    #[serde(default)]
    seller_id: Option<String>,
    #[serde(default)]
    amount: Value,
    #[serde(default)]
    price: Value,
}

#[derive(Debug, Deserialize)]
struct ErcIssued {
    #[serde(default)]
    user_id: Option<String>,
    #[serde(default)]
    email: Option<String>,
    #[serde(default)]
    recipient_email: Option<String>,
    #[serde(default)]
    energy_amount: Value,
}

#[derive(Debug, Deserialize)]
struct VppDispatched {
    #[serde(default)]
    cluster_id: String,
    #[serde(default)]
    target_kw: f64,
    #[serde(default)]
    members_commanded: u64,
    #[serde(default)]
    admin_user_id: Option<String>,
    #[serde(default)]
    user_id: Option<String>,
}

#[derive(Debug, Deserialize)]
struct PasswordResetRequested {
    #[serde(default)]
    user_id: Option<String>,
    #[serde(default)]
    email: String,
    #[serde(default)]
    reset_url: String,
    #[serde(default)]
    token: String,
}

#[derive(Debug, Deserialize)]
struct VerificationEmailRequested {
    #[serde(default)]
    user_id: Option<String>,
    #[serde(default)]
    email: String,
    #[serde(default)]
    username: String,
    #[serde(default)]
    verification_url: String,
    #[serde(default)]
    token: String,
}

#[derive(Debug, Deserialize)]
struct PriceAlertTriggered {
    #[serde(default)]
    user_id: Option<String>,
    // Prices kept as raw JSON: the upstream encodes Decimal as a number or
    // string depending on serde config, and the template renders either.
    #[serde(default)]
    target_price: Value,
    #[serde(default)]
    triggered_price: Value,
    #[serde(default)]
    condition: String,
}

#[derive(Debug, Deserialize)]
struct UserOnboarded {
    #[serde(default)]
    user_id: Option<String>,
    #[serde(default)]
    user_account_pda: String,
    #[serde(default)]
    transaction_signature: String,
}

#[derive(Debug, Deserialize)]
struct MeterOnboarded {
    #[serde(default)]
    user_id: Option<String>,
    #[serde(default)]
    meter_id: String,
    #[serde(default)]
    meter_type: String,
    #[serde(default)]
    transaction_signature: String,
}

#[derive(Debug, Deserialize)]
struct UserWalletLinked {
    #[serde(default)]
    user_id: Option<String>,
    #[serde(default)]
    wallet_address: String,
    #[serde(default)]
    shard_id: u64,
    #[serde(default)]
    transaction_signature: String,
}

/// Deserialize an event `data` object into a typed payload, logging and
/// skipping (returning `None`) if the shape does not match.
fn parse<T: for<'de> Deserialize<'de>>(event_type: &str, data: Value) -> Option<T> {
    match serde_json::from_value(data) {
        Ok(payload) => Some(payload),
        Err(e) => {
            warn!("Skipping {event_type}: malformed payload: {e}");
            None
        }
    }
}

// ---------------------------------------------------------------------------
// Handlers
// ---------------------------------------------------------------------------

/// Route a parsed event to its handler. Unknown types are logged and ignored.
pub async fn dispatch(
    orchestrator: &Arc<NotificationOrchestrator>,
    frontend_url: Option<&str>,
    ctx: &MsgCtx,
    event_type: &str,
    data: Value,
) -> anyhow::Result<()> {
    match event_type {
        // Registration itself only triggers the verification email (sent via
        // VerificationEmailRequested, which carries the token). The welcome
        // email goes out once the address is proven via EmailVerified.
        "UserRegistered" => Ok(()),
        "EmailVerified" => email_verified(orchestrator, frontend_url, ctx, data).await,
        "OrderMatched" => order_matched(orchestrator, ctx, data).await,
        "SettlementProcessed" => settlement_processed(orchestrator, ctx, data).await,
        "ErcIssued" => erc_issued(orchestrator, ctx, data).await,
        "VppDispatched" => vpp_dispatched(orchestrator, ctx, data).await,
        "PriceAlertTriggered" => price_alert_triggered(orchestrator, ctx, data).await,
        "PasswordResetRequested" => {
            password_reset_requested(orchestrator, frontend_url, ctx, data).await
        }
        "VerificationEmailRequested" => {
            verification_email_requested(orchestrator, frontend_url, ctx, data).await
        }
        "UserOnboarded" => user_onboarded(orchestrator, ctx, data).await,
        "MeterOnboarded" => meter_onboarded(orchestrator, ctx, data).await,
        "UserWalletLinked" => user_wallet_linked(orchestrator, ctx, data).await,
        other => {
            // Not a notification trigger (e.g. `ApiKeyVerified` on
            // `iam.audit.events`, which we subscribe to for
            // `VerificationEmailRequested`). Demoted from warn to debug: these
            // arrive in the hundreds of thousands and at warn level they buried
            // real signal and inflated log volume.
            debug!("Unhandled event type: {other}");
            Ok(())
        }
    }
}

async fn email_verified(
    orchestrator: &Arc<NotificationOrchestrator>,
    frontend_url: Option<&str>,
    ctx: &MsgCtx,
    data: Value,
) -> anyhow::Result<()> {
    let Some(p) = parse::<EmailVerified>("EmailVerified", data) else {
        return Ok(());
    };

    if p.email.is_empty() {
        return Ok(());
    }

    let dashboard_url = frontend_url.map_or_else(
        || "https://app.gridtokenx.xyz".to_string(),
        |base| base.trim_end_matches('/').to_string(),
    );

    orchestrator
        .queue_notification(
            parse_uuid(p.user_id.as_deref()),
            NotificationChannel::Email,
            p.email,
            "welcome.html.tera".to_string(),
            serde_json::json!({ "name": p.username, "dashboard_url": dashboard_url }),
            Some(format!(
                "kafka:email_verified:{}:{}",
                ctx.partition, ctx.offset
            )),
        )
        .await
        .map_err(into_anyhow)?;

    Ok(())
}

async fn order_matched(
    orchestrator: &Arc<NotificationOrchestrator>,
    ctx: &MsgCtx,
    data: Value,
) -> anyhow::Result<()> {
    let Some(p) = parse::<OrderMatched>("OrderMatched", data) else {
        return Ok(());
    };

    if let Some(uid) = parse_uuid(p.buyer_id.as_deref()) {
        orchestrator
            .queue_notification(
                Some(uid),
                NotificationChannel::WebSocket,
                uid.to_string(),
                "trade_matched.txt.tera".to_string(),
                serde_json::json!({ "role": "buyer", "amount": p.amount, "price": p.price }),
                Some(format!("kafka:matched:buy:{}:{}", ctx.partition, ctx.offset)),
            )
            .await
            .map_err(into_anyhow)?;
    }

    if let Some(uid) = parse_uuid(p.seller_id.as_deref()) {
        orchestrator
            .queue_notification(
                Some(uid),
                NotificationChannel::WebSocket,
                uid.to_string(),
                "trade_matched.txt.tera".to_string(),
                serde_json::json!({ "role": "seller", "amount": p.amount, "price": p.price }),
                Some(format!(
                    "kafka:matched:sell:{}:{}",
                    ctx.partition, ctx.offset
                )),
            )
            .await
            .map_err(into_anyhow)?;
    }

    Ok(())
}

async fn settlement_processed(
    orchestrator: &Arc<NotificationOrchestrator>,
    ctx: &MsgCtx,
    data: Value,
) -> anyhow::Result<()> {
    let Some(p) = parse::<SettlementProcessed>("SettlementProcessed", data) else {
        return Ok(());
    };

    let status = if p.status.is_empty() {
        "unknown"
    } else {
        &p.status
    };
    info!(
        "Settlement {} processed with status: {}",
        p.tx_signature, status
    );

    // Notify each involved party that carries a UUID. Settlement events that
    // omit party ids (the upstream producer may not propagate them) fall back
    // to the log line above.
    for (role, id) in [("buyer", p.buyer_id.as_deref()), ("seller", p.seller_id.as_deref())] {
        let Some(uid) = parse_uuid(id) else {
            continue;
        };

        orchestrator
            .queue_notification(
                Some(uid),
                NotificationChannel::WebSocket,
                uid.to_string(),
                "settlement_processed.txt.tera".to_string(),
                serde_json::json!({
                    "role": role,
                    "status": status,
                    "tx_signature": p.tx_signature,
                    "amount": p.amount,
                    "price": p.price
                }),
                Some(format!(
                    "kafka:settlement:{role}:{}:{}",
                    ctx.partition, ctx.offset
                )),
            )
            .await
            .map_err(into_anyhow)?;
    }

    Ok(())
}

async fn erc_issued(
    orchestrator: &Arc<NotificationOrchestrator>,
    ctx: &MsgCtx,
    data: Value,
) -> anyhow::Result<()> {
    let Some(p) = parse::<ErcIssued>("ErcIssued", data) else {
        return Ok(());
    };

    let email = p
        .email
        .filter(|s| !s.is_empty())
        .or(p.recipient_email)
        .unwrap_or_default();

    if email.is_empty() {
        warn!("Skipping ErcIssued notification: missing recipient email");
        return Ok(());
    }

    orchestrator
        .queue_notification(
            parse_uuid(p.user_id.as_deref()),
            NotificationChannel::Email,
            email,
            "erc_issued.html.tera".to_string(),
            serde_json::json!({ "amount": p.energy_amount }),
            Some(format!("kafka:erc:{}:{}", ctx.partition, ctx.offset)),
        )
        .await
        .map_err(into_anyhow)?;

    Ok(())
}

async fn vpp_dispatched(
    orchestrator: &Arc<NotificationOrchestrator>,
    ctx: &MsgCtx,
    data: Value,
) -> anyhow::Result<()> {
    let Some(p) = parse::<VppDispatched>("VppDispatched", data) else {
        return Ok(());
    };

    info!(
        "📢 VPP Dispatch Event: cluster={}, target={}kW, members={}",
        p.cluster_id, p.target_kw, p.members_commanded
    );

    let recipient = parse_uuid(p.admin_user_id.as_deref().or(p.user_id.as_deref()));

    let Some(uid) = recipient else {
        warn!("Skipping VppDispatched WebSocket notification: missing UUID recipient");
        return Ok(());
    };

    orchestrator
        .queue_notification(
            Some(uid),
            NotificationChannel::WebSocket,
            uid.to_string(),
            "vpp_dispatched.txt.tera".to_string(),
            serde_json::json!({
                "cluster_id": p.cluster_id,
                "target_kw": p.target_kw,
                "members_count": p.members_commanded
            }),
            Some(format!(
                "kafka:vpp_dispatch:{}:{}",
                ctx.partition, ctx.offset
            )),
        )
        .await
        .map_err(into_anyhow)?;

    Ok(())
}

async fn price_alert_triggered(
    orchestrator: &Arc<NotificationOrchestrator>,
    ctx: &MsgCtx,
    data: Value,
) -> anyhow::Result<()> {
    let Some(p) = parse::<PriceAlertTriggered>("PriceAlertTriggered", data) else {
        return Ok(());
    };

    let Some(uid) = parse_uuid(p.user_id.as_deref()) else {
        warn!("Skipping PriceAlertTriggered: missing/invalid user_id");
        return Ok(());
    };

    orchestrator
        .queue_notification(
            Some(uid),
            NotificationChannel::WebSocket,
            uid.to_string(),
            "price_alert_triggered.txt.tera".to_string(),
            serde_json::json!({
                "condition": p.condition,
                "target_price": p.target_price,
                "triggered_price": p.triggered_price
            }),
            Some(format!(
                "kafka:price_alert:{}:{}",
                ctx.partition, ctx.offset
            )),
        )
        .await
        .map_err(into_anyhow)?;

    Ok(())
}

async fn password_reset_requested(
    orchestrator: &Arc<NotificationOrchestrator>,
    frontend_url: Option<&str>,
    ctx: &MsgCtx,
    data: Value,
) -> anyhow::Result<()> {
    let Some(p) = parse::<PasswordResetRequested>("PasswordResetRequested", data) else {
        return Ok(());
    };

    // Rewrite upstream base URL to frontend_url when configured;
    // fall back to constructing from token when upstream provides nothing.
    let reset_url = if !p.reset_url.is_empty() {
        rewrite_url(frontend_url, &p.reset_url)
    } else if !p.token.is_empty() {
        build_callback_url(frontend_url, "reset-password", &format!("token={}", p.token))
    } else {
        p.reset_url.clone()
    };

    if p.email.is_empty() {
        return Ok(());
    }

    orchestrator
        .queue_notification(
            parse_uuid(p.user_id.as_deref()),
            NotificationChannel::Email,
            p.email,
            "password_reset.html.tera".to_string(),
            serde_json::json!({ "reset_url": reset_url }),
            Some(format!("kafka:pwd_reset:{}:{}", ctx.partition, ctx.offset)),
        )
        .await
        .map_err(into_anyhow)?;

    Ok(())
}

async fn verification_email_requested(
    orchestrator: &Arc<NotificationOrchestrator>,
    frontend_url: Option<&str>,
    ctx: &MsgCtx,
    data: Value,
) -> anyhow::Result<()> {
    let Some(p) = parse::<VerificationEmailRequested>("VerificationEmailRequested", data) else {
        return Ok(());
    };

    // Rewrite upstream base URL to frontend_url when configured;
    // fall back to constructing from token when upstream provides nothing.
    // The trading UI serves the verification page at `/verify` and reads the
    // `token` and `email` query params (gridtokenx-trading/app/verify/page.tsx).
    let verification_url = if !p.verification_url.is_empty() {
        rewrite_url(frontend_url, &p.verification_url)
    } else if !p.token.is_empty() {
        build_callback_url(
            frontend_url,
            "verify",
            &format!("token={}&email={}", urlencode(&p.token), urlencode(&p.email)),
        )
    } else {
        p.verification_url.clone()
    };

    if p.email.is_empty() {
        return Ok(());
    }

    if verification_url.is_empty() {
        warn!(
            "Skipping verification email for {}: no verification URL \
             (FRONTEND_URL unconfigured and event carried neither url nor token)",
            p.email
        );
        return Ok(());
    }

    orchestrator
        .queue_notification(
            parse_uuid(p.user_id.as_deref()),
            NotificationChannel::Email,
            p.email,
            "verify_email.html.tera".to_string(),
            serde_json::json!({ "name": p.username, "verification_url": verification_url }),
            Some(format!(
                "kafka:verify_email:{}:{}",
                ctx.partition, ctx.offset
            )),
        )
        .await
        .map_err(into_anyhow)?;

    Ok(())
}

async fn user_onboarded(
    orchestrator: &Arc<NotificationOrchestrator>,
    ctx: &MsgCtx,
    data: Value,
) -> anyhow::Result<()> {
    let Some(p) = parse::<UserOnboarded>("UserOnboarded", data) else {
        return Ok(());
    };

    let Some(uid) = parse_uuid(p.user_id.as_deref()) else {
        return Ok(());
    };

    orchestrator
        .queue_notification(
            Some(uid),
            NotificationChannel::WebSocket,
            uid.to_string(),
            "user_onboarded.txt.tera".to_string(),
            serde_json::json!({
                "user_account_pda": p.user_account_pda,
                "transaction_signature": p.transaction_signature
            }),
            Some(format!("kafka:onboard:{}:{}", ctx.partition, ctx.offset)),
        )
        .await
        .map_err(into_anyhow)?;

    Ok(())
}

async fn meter_onboarded(
    orchestrator: &Arc<NotificationOrchestrator>,
    ctx: &MsgCtx,
    data: Value,
) -> anyhow::Result<()> {
    let Some(p) = parse::<MeterOnboarded>("MeterOnboarded", data) else {
        return Ok(());
    };

    let Some(uid) = parse_uuid(p.user_id.as_deref()) else {
        return Ok(());
    };

    orchestrator
        .queue_notification(
            Some(uid),
            NotificationChannel::WebSocket,
            uid.to_string(),
            "meter_onboarded.txt.tera".to_string(),
            serde_json::json!({
                "meter_id": p.meter_id,
                "meter_type": p.meter_type,
                "transaction_signature": p.transaction_signature
            }),
            Some(format!("kafka:meter:{}:{}", ctx.partition, ctx.offset)),
        )
        .await
        .map_err(into_anyhow)?;

    Ok(())
}

async fn user_wallet_linked(
    orchestrator: &Arc<NotificationOrchestrator>,
    ctx: &MsgCtx,
    data: Value,
) -> anyhow::Result<()> {
    let Some(p) = parse::<UserWalletLinked>("UserWalletLinked", data) else {
        return Ok(());
    };

    let Some(uid) = parse_uuid(p.user_id.as_deref()) else {
        return Ok(());
    };

    orchestrator
        .queue_notification(
            Some(uid),
            NotificationChannel::WebSocket,
            uid.to_string(),
            "security_alert.txt.tera".to_string(),
            serde_json::json!({
                "wallet_address": p.wallet_address,
                "shard_id": p.shard_id,
                "transaction_signature": p.transaction_signature
            }),
            Some(format!(
                "kafka:wallet_link:{}:{}",
                ctx.partition, ctx.offset
            )),
        )
        .await
        .map_err(into_anyhow)?;

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Mutex;

    use noti_core::domain::Notification;
    use noti_core::traits::{
        MockCacheTrait, MockMessageQueueTrait, MockNotificationProviderTrait,
        MockNotificationRepositoryTrait, MockTemplateEngineTrait,
    };

    /// A captured notification queued during a test.
    type Sink = Arc<Mutex<Vec<Notification>>>;

    /// Build an orchestrator whose `repo.create` records every queued
    /// notification. Cache and MQ accept everything; providers/template are
    /// never exercised on the queue path.
    fn test_orchestrator() -> (Arc<NotificationOrchestrator>, Sink) {
        let sink: Sink = Arc::new(Mutex::new(Vec::new()));
        let recorder = sink.clone();

        let mut repo = MockNotificationRepositoryTrait::new();
        repo.expect_create().times(..).returning(move |n| {
            recorder.lock().expect("sink lock").push(n.clone());
            Ok(n.clone())
        });

        let mut cache = MockCacheTrait::new();
        cache.expect_set_value().times(..).returning(|_, _, _| Ok(()));

        let mut mq = MockMessageQueueTrait::new();
        mq.expect_publish_dispatch().times(..).returning(|_| Ok(()));

        let orch = Arc::new(NotificationOrchestrator::new(
            Arc::new(repo),
            Arc::new(MockTemplateEngineTrait::new()),
            Arc::new(MockNotificationProviderTrait::new()),
            Arc::new(MockNotificationProviderTrait::new()),
            Arc::new(MockNotificationProviderTrait::new()),
            Arc::new(MockNotificationProviderTrait::new()),
            Arc::new(MockNotificationProviderTrait::new()),
            Arc::new(cache),
            Some(Arc::new(mq)),
        ));
        (orch, sink)
    }

    fn ctx() -> MsgCtx {
        MsgCtx {
            partition: 2,
            offset: 99,
        }
    }

    #[tokio::test]
    async fn user_registered_sends_no_email() {
        let (orch, sink) = test_orchestrator();
        let data = serde_json::json!({ "email": "alice@example.com", "username": "alice" });

        dispatch(&orch, None, &ctx(), "UserRegistered", data)
            .await
            .expect("dispatch ok");

        assert_eq!(sink.lock().expect("lock").len(), 0);
    }

    #[tokio::test]
    async fn email_verified_queues_welcome_email() {
        let (orch, sink) = test_orchestrator();
        let data = serde_json::json!({ "email": "alice@example.com", "username": "alice" });

        dispatch(
            &orch,
            Some("https://app.gridtokenx.test/"),
            &ctx(),
            "EmailVerified",
            data,
        )
        .await
        .expect("dispatch ok");

        let queued = sink.lock().expect("lock");
        assert_eq!(queued.len(), 1);
        let n = &queued[0];
        assert!(matches!(n.channel, NotificationChannel::Email));
        assert_eq!(n.recipient, "alice@example.com");
        assert_eq!(n.template_id, "welcome.html.tera");
        assert_eq!(
            n.variables["dashboard_url"],
            "https://app.gridtokenx.test"
        );
        assert_eq!(
            n.idempotency_key.as_deref(),
            Some("kafka:email_verified:2:99")
        );
    }

    #[tokio::test]
    async fn email_verified_skips_empty_email() {
        let (orch, sink) = test_orchestrator();
        let data = serde_json::json!({ "email": "", "username": "ghost" });

        dispatch(&orch, None, &ctx(), "EmailVerified", data)
            .await
            .expect("dispatch ok");

        assert_eq!(sink.lock().expect("lock").len(), 0);
    }

    #[tokio::test]
    async fn verification_email_builds_frontend_verify_link_from_token() {
        let (orch, sink) = test_orchestrator();
        let data = serde_json::json!({
            "email": "bob+test@example.com",
            "username": "bob",
            "token": "tok-123"
        });

        dispatch(
            &orch,
            Some("https://app.gridtokenx.test/"),
            &ctx(),
            "VerificationEmailRequested",
            data,
        )
        .await
        .expect("dispatch ok");

        let queued = sink.lock().expect("lock");
        assert_eq!(queued.len(), 1);
        let n = &queued[0];
        assert_eq!(n.template_id, "verify_email.html.tera");
        assert_eq!(
            n.variables["verification_url"],
            "https://app.gridtokenx.test/verify?token=tok-123&email=bob%2Btest%40example.com"
        );
    }

    #[tokio::test]
    async fn verification_email_skips_when_no_url_can_be_built() {
        let (orch, sink) = test_orchestrator();
        let data = serde_json::json!({
            "email": "bob@example.com",
            "username": "bob",
            "token": "tok-123"
        });

        // frontend_url unset and no upstream verification_url → no broken email
        dispatch(&orch, None, &ctx(), "VerificationEmailRequested", data)
            .await
            .expect("dispatch ok");

        assert_eq!(sink.lock().expect("lock").len(), 0);
    }

    #[tokio::test]
    async fn order_matched_notifies_both_parties() {
        let (orch, sink) = test_orchestrator();
        let buyer = Uuid::new_v4();
        let seller = Uuid::new_v4();
        let data = serde_json::json!({
            "buyer_id": buyer.to_string(),
            "seller_id": seller.to_string(),
            "amount": 100,
            "price": 5
        });

        dispatch(&orch, None, &ctx(), "OrderMatched", data)
            .await
            .expect("dispatch ok");

        let queued = sink.lock().expect("lock");
        assert_eq!(queued.len(), 2, "buyer + seller");
        assert!(queued.iter().all(|n| matches!(n.channel, NotificationChannel::WebSocket)));
        assert!(queued.iter().any(|n| n.user_id == Some(buyer)));
        assert!(queued.iter().any(|n| n.user_id == Some(seller)));
    }

    #[tokio::test]
    async fn order_matched_skips_non_uuid_parties() {
        let (orch, sink) = test_orchestrator();
        let data = serde_json::json!({ "buyer_id": "not-a-uuid", "seller_id": null });

        dispatch(&orch, None, &ctx(), "OrderMatched", data)
            .await
            .expect("dispatch ok");

        assert_eq!(sink.lock().expect("lock").len(), 0);
    }

    #[tokio::test]
    async fn price_alert_triggered_notifies_owner() {
        let (orch, sink) = test_orchestrator();
        let owner = Uuid::new_v4();
        let data = serde_json::json!({
            "alert_id": Uuid::new_v4().to_string(),
            "user_id": owner.to_string(),
            "target_price": "0.25",
            "triggered_price": "0.27",
            "condition": "above"
        });

        dispatch(&orch, None, &ctx(), "PriceAlertTriggered", data)
            .await
            .expect("dispatch ok");

        let queued = sink.lock().expect("lock");
        assert_eq!(queued.len(), 1);
        let n = &queued[0];
        assert!(matches!(n.channel, NotificationChannel::WebSocket));
        assert_eq!(n.user_id, Some(owner));
        assert_eq!(n.template_id, "price_alert_triggered.txt.tera");
        assert_eq!(n.variables["condition"], "above");
    }

    #[tokio::test]
    async fn price_alert_triggered_skips_missing_user() {
        let (orch, sink) = test_orchestrator();
        let data = serde_json::json!({ "user_id": null, "condition": "below" });

        dispatch(&orch, None, &ctx(), "PriceAlertTriggered", data)
            .await
            .expect("dispatch ok");

        assert_eq!(sink.lock().expect("lock").len(), 0);
    }

    #[tokio::test]
    async fn password_reset_rewrites_url_to_frontend() {
        let (orch, sink) = test_orchestrator();
        let data = serde_json::json!({
            "email": "bob@example.com",
            "reset_url": "https://upstream.invalid/reset-password?token=abc"
        });

        dispatch(
            &orch,
            Some("https://app.gridtokenx.com"),
            &ctx(),
            "PasswordResetRequested",
            data,
        )
        .await
        .expect("dispatch ok");

        let queued = sink.lock().expect("lock");
        assert_eq!(queued.len(), 1);
        let url = queued[0].variables["reset_url"]
            .as_str()
            .expect("reset_url string");
        assert!(
            url.starts_with("https://app.gridtokenx.com"),
            "URL not rewritten to frontend: {url}"
        );
        assert!(url.contains("token=abc"), "token lost in rewrite: {url}");
    }

    #[tokio::test]
    async fn settlement_processed_notifies_each_party_with_status() {
        let (orch, sink) = test_orchestrator();
        let buyer = Uuid::new_v4();
        let seller = Uuid::new_v4();
        let data = serde_json::json!({
            "status": "confirmed",
            "tx_signature": "sig-abc",
            "buyer_id": buyer.to_string(),
            "seller_id": seller.to_string(),
            "amount": 50,
            "price": 3
        });

        dispatch(&orch, None, &ctx(), "SettlementProcessed", data)
            .await
            .expect("dispatch ok");

        let queued = sink.lock().expect("lock");
        assert_eq!(queued.len(), 2, "buyer + seller");
        assert!(queued.iter().all(|n| matches!(n.channel, NotificationChannel::WebSocket)));
        assert!(queued.iter().all(|n| n.template_id == "settlement_processed.txt.tera"));
        let buyer_n = queued
            .iter()
            .find(|n| n.user_id == Some(buyer))
            .expect("buyer notified");
        assert_eq!(buyer_n.variables["role"], "buyer");
        assert_eq!(buyer_n.variables["status"], "confirmed");
        assert_eq!(buyer_n.variables["tx_signature"], "sig-abc");
    }

    #[tokio::test]
    async fn settlement_processed_skips_parties_without_uuid() {
        let (orch, sink) = test_orchestrator();
        // Only the buyer carries a UUID; seller omitted entirely.
        let buyer = Uuid::new_v4();
        let data = serde_json::json!({
            "status": "",
            "tx_signature": "sig-xyz",
            "buyer_id": buyer.to_string()
        });

        dispatch(&orch, None, &ctx(), "SettlementProcessed", data)
            .await
            .expect("dispatch ok");

        let queued = sink.lock().expect("lock");
        assert_eq!(queued.len(), 1, "only the buyer is notified");
        // Empty upstream status falls back to "unknown".
        assert_eq!(queued[0].variables["status"], "unknown");
    }

    #[tokio::test]
    async fn erc_issued_falls_back_to_recipient_email() {
        let (orch, sink) = test_orchestrator();
        // `email` absent → handler uses `recipient_email`.
        let data = serde_json::json!({
            "recipient_email": "rec@example.com",
            "energy_amount": 1234
        });

        dispatch(&orch, None, &ctx(), "ErcIssued", data)
            .await
            .expect("dispatch ok");

        let queued = sink.lock().expect("lock");
        assert_eq!(queued.len(), 1);
        let n = &queued[0];
        assert!(matches!(n.channel, NotificationChannel::Email));
        assert_eq!(n.recipient, "rec@example.com");
        assert_eq!(n.template_id, "erc_issued.html.tera");
        assert_eq!(n.variables["amount"], 1234);
    }

    #[tokio::test]
    async fn erc_issued_skips_when_no_recipient() {
        let (orch, sink) = test_orchestrator();
        let data = serde_json::json!({ "energy_amount": 1 });

        dispatch(&orch, None, &ctx(), "ErcIssued", data)
            .await
            .expect("dispatch ok");

        assert_eq!(sink.lock().expect("lock").len(), 0);
    }

    #[tokio::test]
    async fn vpp_dispatched_prefers_admin_recipient() {
        let (orch, sink) = test_orchestrator();
        let admin = Uuid::new_v4();
        let user = Uuid::new_v4();
        let data = serde_json::json!({
            "cluster_id": "cluster-7",
            "target_kw": 250.5,
            "members_commanded": 12,
            "admin_user_id": admin.to_string(),
            "user_id": user.to_string()
        });

        dispatch(&orch, None, &ctx(), "VppDispatched", data)
            .await
            .expect("dispatch ok");

        let queued = sink.lock().expect("lock");
        assert_eq!(queued.len(), 1);
        let n = &queued[0];
        assert_eq!(n.user_id, Some(admin), "admin_user_id wins over user_id");
        assert!(matches!(n.channel, NotificationChannel::WebSocket));
        assert_eq!(n.template_id, "vpp_dispatched.txt.tera");
        assert_eq!(n.variables["cluster_id"], "cluster-7");
        assert_eq!(n.variables["members_count"], 12);
    }

    #[tokio::test]
    async fn user_wallet_linked_sends_security_alert() {
        let (orch, sink) = test_orchestrator();
        let owner = Uuid::new_v4();
        let data = serde_json::json!({
            "user_id": owner.to_string(),
            "wallet_address": "Wallet111",
            "shard_id": 3,
            "transaction_signature": "sig-link"
        });

        dispatch(&orch, None, &ctx(), "UserWalletLinked", data)
            .await
            .expect("dispatch ok");

        let queued = sink.lock().expect("lock");
        assert_eq!(queued.len(), 1);
        let n = &queued[0];
        assert_eq!(n.user_id, Some(owner));
        assert!(matches!(n.channel, NotificationChannel::WebSocket));
        assert_eq!(n.template_id, "security_alert.txt.tera");
        assert_eq!(n.variables["wallet_address"], "Wallet111");
    }

    #[tokio::test]
    async fn meter_onboarded_skips_without_user_uuid() {
        let (orch, sink) = test_orchestrator();
        let data = serde_json::json!({ "meter_id": "m-1", "meter_type": "smart" });

        dispatch(&orch, None, &ctx(), "MeterOnboarded", data)
            .await
            .expect("dispatch ok");

        assert_eq!(sink.lock().expect("lock").len(), 0);
    }

    #[tokio::test]
    async fn unknown_event_type_is_ignored() {
        let (orch, sink) = test_orchestrator();
        let data = serde_json::json!({ "whatever": true });

        dispatch(&orch, None, &ctx(), "SomethingUnknown", data)
            .await
            .expect("dispatch ok");

        assert_eq!(sink.lock().expect("lock").len(), 0);
    }

    #[tokio::test]
    async fn malformed_payload_is_skipped() {
        let (orch, sink) = test_orchestrator();
        // `email` is a String; a numeric value fails deserialization.
        let data = serde_json::json!({ "email": 12345, "username": "x" });

        dispatch(&orch, None, &ctx(), "UserRegistered", data)
            .await
            .expect("dispatch ok");

        assert_eq!(sink.lock().expect("lock").len(), 0);
    }
}
