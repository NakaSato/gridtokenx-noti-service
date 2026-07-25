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

/// Event identity used to derive idempotency keys. Prefers the producer's
/// stable event id (IAM's `Event.id` UUID, carried as the envelope `id`); the
/// Kafka `partition`/`offset` are only a fallback for events that lack one.
pub struct MsgCtx {
    pub partition: i32,
    pub offset: i64,
    /// Producer-assigned unique event id (envelope `id`), if present.
    pub event_id: Option<String>,
}

impl MsgCtx {
    /// Build an idempotency key for one notification derived from this event.
    ///
    /// `label` discriminates notifications that originate from the **same**
    /// event (e.g. a matched trade fans out to buyer/seller × ws/push), so each
    /// gets a distinct key. The key is seeded with the producer's stable event
    /// id when available — Kafka `(partition, offset)` is NOT unique over time
    /// (topic recreation / cluster rebuild resets offsets), which silently
    /// dedup-dropped fresh notifications against stale rows. Falls back to the
    /// `kafka:` coordinate form only when the envelope carried no id.
    #[must_use]
    pub fn idem(&self, label: &str) -> String {
        match &self.event_id {
            Some(id) => format!("{label}:{id}"),
            None => format!("kafka:{label}:{}:{}", self.partition, self.offset),
        }
    }
}

/// Parse an optional UUID string, returning `None` on absence or parse error.
fn parse_uuid(s: Option<&str>) -> Option<Uuid> {
    s.and_then(|v| Uuid::parse_str(v).ok())
}

/// Render a JSON value as a bare display string for embedding in human text:
/// strings drop their quotes, everything else uses its compact JSON form.
/// (Upstream encodes amounts/prices as either a number or a string.)
fn plain(value: &Value) -> String {
    value
        .as_str()
        .map_or_else(|| value.to_string(), ToString::to_string)
}

/// Queue an FCM push to all of a user's registered devices. The recipient is
/// the `user_id` (`FcmProvider` fans out to its device tokens); the shared
/// `push_notification.txt.tera` template renders the `{title, body}` JSON
/// envelope `FcmProvider` parses, so the human text is built by the caller.
async fn queue_push(
    orchestrator: &Arc<NotificationOrchestrator>,
    uid: Uuid,
    title: &str,
    body: String,
    idempotency_key: String,
) -> anyhow::Result<()> {
    orchestrator
        .queue_notification(
            Some(uid),
            NotificationChannel::Push,
            uid.to_string(),
            "push_notification.txt.tera".to_string(),
            serde_json::json!({ "title": title, "body": body }),
            Some(idempotency_key),
        )
        .await
        .map(|_| ())
        .map_err(into_anyhow)
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

/// Order lifecycle change from the trading service: partial fill, fill,
/// IOC cancellation, or expiry reap.
#[derive(Debug, Deserialize)]
struct OrderUpdate {
    #[serde(default)]
    id: Option<String>,
    #[serde(default)]
    user_id: Option<String>,
    #[serde(default)]
    filled_amount: Value,
    #[serde(default)]
    status: String,
}

/// Meter registry event from meter-service (`meter_events` topic). Distinct
/// from IAM's `MeterOnboarded`, which reports the *on-chain* registration —
/// this one fires when the meter enters the registry.
#[derive(Debug, Deserialize)]
struct MeterRegistered {
    #[serde(default)]
    user_id: Option<String>,
    #[serde(default)]
    serial_number: String,
    #[serde(default)]
    meter_id: Option<String>,
    #[serde(default)]
    zone_id: Option<i32>,
    #[serde(default)]
    status: String,
}

/// Order acknowledgement from the trading service (`trading.triggers`).
/// Amounts stay raw JSON — upstream encodes `Decimal` as a number or a string.
#[derive(Debug, Deserialize)]
struct OrderCreated {
    #[serde(default)]
    id: Option<String>,
    #[serde(default)]
    user_id: Option<String>,
    #[serde(default)]
    order_type: String,
    #[serde(default)]
    side: String,
    #[serde(default)]
    energy_amount: Value,
    #[serde(default)]
    price_per_kwh: Value,
    #[serde(default)]
    status: String,
}

#[derive(Debug, Deserialize)]
struct UserLoggedIn {
    #[serde(default)]
    user_id: Option<String>,
    #[serde(default)]
    username: String,
    #[serde(default)]
    ip_address: Option<String>,
}

/// Account lockout after repeated failed sign-ins. IAM's payload carries the
/// *login identifier* (email or username, `auth_service.rs`) — not a user id —
/// so the email only goes out when the identifier is itself an address.
#[derive(Debug, Deserialize)]
struct AccountLocked {
    #[serde(default)]
    identifier: String,
    #[serde(default)]
    lockout_secs: u64,
}

#[derive(Debug, Deserialize)]
struct UserWalletUnlinked {
    #[serde(default)]
    user_id: Option<String>,
    #[serde(default)]
    wallet_address: String,
}

#[derive(Debug, Deserialize)]
struct UserWalletPrimaryChanged {
    #[serde(default)]
    user_id: Option<String>,
    #[serde(default)]
    wallet_address: String,
}

/// Pre-link confirmation request: the wallet is *not* linked yet — the user
/// must prove ownership via the emailed callback before IAM registers the link
/// on-chain (which then emits `UserWalletLinked`).
#[derive(Debug, Deserialize)]
struct WalletLinkRequested {
    #[serde(default)]
    user_id: Option<String>,
    #[serde(default)]
    email: String,
    #[serde(default)]
    username: String,
    #[serde(default)]
    wallet_address: String,
    #[serde(default)]
    confirmation_url: String,
    #[serde(default)]
    token: String,
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
///
/// # Errors
///
/// Returns an error if the matched handler fails to queue its notification
/// (e.g. the orchestrator's persist/publish step errors). Unknown event types
/// and skipped payloads return `Ok(())`.
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
        "WalletLinkRequested" => {
            wallet_link_requested(orchestrator, frontend_url, ctx, data).await
        }
        "UserWalletLinked" => user_wallet_linked(orchestrator, ctx, data).await,
        "UserWalletUnlinked" => user_wallet_unlinked(orchestrator, ctx, data).await,
        "UserWalletPrimaryChanged" => user_wallet_primary_changed(orchestrator, ctx, data).await,
        "AccountLocked" => account_locked(orchestrator, ctx, data).await,
        "UserLoggedIn" => user_logged_in(orchestrator, ctx, data).await,
        "OrderCreated" => order_created(orchestrator, ctx, data).await,
        "OrderUpdate" => order_update(orchestrator, ctx, data).await,
        // `MeterUpdated` is reserved upstream (meter-service defines it, no
        // update path emits it yet) and is wire-identical, so it shares the
        // handler and template.
        "MeterRegistered" | "MeterUpdated" => meter_registered(orchestrator, ctx, data).await,
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
            Some(ctx.idem("email_verified")),
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

    // Each matched party gets two deliveries: a real-time WebSocket push (seen
    // when the web app is open) AND an FCM push (mobile/web background). The
    // Push channel's recipient is the user_id — FcmProvider fans it out to all
    // of that user's registered device tokens. Distinct idempotency keys keep
    // the two channels independent under redelivery.
    for (role, id) in [
        ("buyer", p.buyer_id.as_deref()),
        ("seller", p.seller_id.as_deref()),
    ] {
        let Some(uid) = parse_uuid(id) else {
            continue;
        };

        orchestrator
            .queue_notification(
                Some(uid),
                NotificationChannel::WebSocket,
                uid.to_string(),
                "trade_matched.txt.tera".to_string(),
                serde_json::json!({ "role": role, "amount": p.amount, "price": p.price }),
                Some(ctx.idem(&format!("matched:{role}:ws"))),
            )
            .await
            .map_err(into_anyhow)?;

        let body = format!(
            "Your {role} order matched: {} kWh @ {}",
            plain(&p.amount),
            plain(&p.price)
        );
        queue_push(
            orchestrator,
            uid,
            "Trade Matched",
            body,
            ctx.idem(&format!("matched:{role}:push")),
        )
        .await?;
    }

    Ok(())
}

/// Order acknowledgement. WebSocket only — the user is in the app at the
/// moment they place an order, so a push would be redundant; the fills that
/// arrive later (`OrderMatched`) are the ones worth pushing off-session.
async fn order_created(
    orchestrator: &Arc<NotificationOrchestrator>,
    ctx: &MsgCtx,
    data: Value,
) -> anyhow::Result<()> {
    let Some(p) = parse::<OrderCreated>("OrderCreated", data) else {
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
            "order_created.txt.tera".to_string(),
            serde_json::json!({
                "order_id": p.id.unwrap_or_default(),
                "order_type": p.order_type,
                "side": p.side,
                "energy_amount": plain(&p.energy_amount),
                "price_per_kwh": plain(&p.price_per_kwh),
                "status": p.status
            }),
            Some(ctx.idem("order_created")),
        )
        .await
        .map_err(into_anyhow)?;

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
                Some(ctx.idem(&format!("settlement:{role}"))),
            )
            .await
            .map_err(into_anyhow)?;

        queue_push(
            orchestrator,
            uid,
            "Trade Settled",
            format!(
                "Your {role} trade settled ({status}): {} kWh @ {}",
                plain(&p.amount),
                plain(&p.price)
            ),
            ctx.idem(&format!("settlement:{role}:push")),
        )
        .await?;
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
            Some(ctx.idem("erc")),
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
            Some(ctx.idem("vpp_dispatch")),
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
            Some(ctx.idem("price_alert")),
        )
        .await
        .map_err(into_anyhow)?;

    queue_push(
        orchestrator,
        uid,
        "Price Alert Triggered",
        format!(
            "Alert fired ({} {}). Market price: {}",
            p.condition,
            plain(&p.target_price),
            plain(&p.triggered_price)
        ),
        ctx.idem("price_alert:push"),
    )
    .await?;

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
            Some(ctx.idem("pwd_reset")),
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
            Some(ctx.idem("verify_email")),
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
            Some(ctx.idem("onboard")),
        )
        .await
        .map_err(into_anyhow)?;

    Ok(())
}

/// Order lifecycle update. WebSocket always; Push only for terminal states —
/// a partially-filled order can tick many times per matching cycle, and each
/// push is a phone buzz.
async fn order_update(
    orchestrator: &Arc<NotificationOrchestrator>,
    ctx: &MsgCtx,
    data: Value,
) -> anyhow::Result<()> {
    let Some(p) = parse::<OrderUpdate>("OrderUpdate", data) else {
        return Ok(());
    };

    // `user_id` is `None` when the emitting path could not resolve the owner
    // (trading-core `Event::OrderUpdate`) — nothing to route to.
    let Some(uid) = parse_uuid(p.user_id.as_deref()) else {
        debug!("Skipping OrderUpdate: event carried no resolvable user id");
        return Ok(());
    };

    let order_id = p.id.unwrap_or_default();
    let filled = plain(&p.filled_amount);

    orchestrator
        .queue_notification(
            Some(uid),
            NotificationChannel::WebSocket,
            uid.to_string(),
            "order_update.txt.tera".to_string(),
            serde_json::json!({
                "order_id": order_id,
                "filled_amount": filled,
                "status": p.status
            }),
            Some(ctx.idem("order_update")),
        )
        .await
        .map_err(into_anyhow)?;

    // Terminal states only (trading-core `OrderStatus::as_str`).
    if matches!(p.status.as_str(), "filled" | "cancelled" | "expired") {
        queue_push(
            orchestrator,
            uid,
            "Order Update",
            format!("Your order is {} ({filled} kWh filled).", p.status),
            ctx.idem("order_update:push"),
        )
        .await?;
    }

    Ok(())
}

/// Registry-level meter registration (meter-service). WebSocket only — the
/// on-chain confirmation that follows (`MeterOnboarded`) is the milestone
/// worth a second notification, so this one stays in-app.
async fn meter_registered(
    orchestrator: &Arc<NotificationOrchestrator>,
    ctx: &MsgCtx,
    data: Value,
) -> anyhow::Result<()> {
    let Some(p) = parse::<MeterRegistered>("MeterRegistered", data) else {
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
            "meter_registered.txt.tera".to_string(),
            serde_json::json!({
                "serial_number": p.serial_number,
                "meter_id": p.meter_id.unwrap_or_default(),
                "zone_id": p.zone_id.map_or_else(String::new, |z| z.to_string()),
                "status": p.status
            }),
            Some(ctx.idem("meter_registered")),
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
            Some(ctx.idem("meter")),
        )
        .await
        .map_err(into_anyhow)?;

    Ok(())
}

async fn wallet_link_requested(
    orchestrator: &Arc<NotificationOrchestrator>,
    frontend_url: Option<&str>,
    ctx: &MsgCtx,
    data: Value,
) -> anyhow::Result<()> {
    let Some(p) = parse::<WalletLinkRequested>("WalletLinkRequested", data) else {
        return Ok(());
    };

    // Rewrite upstream base URL to frontend_url when configured; fall back to
    // constructing from token when upstream provides nothing. The callback
    // carries the wallet so the page can show what is being confirmed.
    let confirmation_url = if !p.confirmation_url.is_empty() {
        rewrite_url(frontend_url, &p.confirmation_url)
    } else if !p.token.is_empty() {
        build_callback_url(
            frontend_url,
            "wallet/confirm",
            &format!(
                "token={}&wallet={}",
                urlencode(&p.token),
                urlencode(&p.wallet_address)
            ),
        )
    } else {
        p.confirmation_url.clone()
    };

    if p.email.is_empty() || p.wallet_address.is_empty() {
        return Ok(());
    }

    if confirmation_url.is_empty() {
        warn!(
            "Skipping wallet confirmation email for {}: no confirmation URL \
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
            "confirm_wallet.html.tera".to_string(),
            serde_json::json!({
                "name": p.username,
                "wallet_address": p.wallet_address,
                "confirmation_url": confirmation_url
            }),
            Some(ctx.idem("wallet_confirm")),
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
                "headline": "New Wallet Linked",
                "summary": "A new Solana wallet has been linked to your GridTokenX account.",
                "wallet_address": p.wallet_address,
                "shard_id": p.shard_id,
                "transaction_signature": p.transaction_signature
            }),
            Some(ctx.idem("wallet_link")),
        )
        .await
        .map_err(into_anyhow)?;

    // Security-sensitive: also push to mobile/web so the owner sees a wallet
    // link even when no web session is open.
    queue_push(
        orchestrator,
        uid,
        "Security Alert: Wallet Linked",
        format!("A wallet ({}) was linked to your account.", p.wallet_address),
        ctx.idem("wallet_link:push"),
    )
    .await?;

    Ok(())
}

async fn user_wallet_unlinked(
    orchestrator: &Arc<NotificationOrchestrator>,
    ctx: &MsgCtx,
    data: Value,
) -> anyhow::Result<()> {
    let Some(p) = parse::<UserWalletUnlinked>("UserWalletUnlinked", data) else {
        return Ok(());
    };

    let Some(uid) = parse_uuid(p.user_id.as_deref()) else {
        return Ok(());
    };

    // No on-chain rows: unlinking is an off-chain account change, so `shard_id`
    // and `transaction_signature` stay empty and the template omits them.
    orchestrator
        .queue_notification(
            Some(uid),
            NotificationChannel::WebSocket,
            uid.to_string(),
            "security_alert.txt.tera".to_string(),
            serde_json::json!({
                "headline": "Wallet Unlinked",
                "summary": "A Solana wallet was removed from your GridTokenX account.",
                "wallet_address": p.wallet_address,
                "shard_id": "",
                "transaction_signature": ""
            }),
            Some(ctx.idem("wallet_unlink")),
        )
        .await
        .map_err(into_anyhow)?;

    queue_push(
        orchestrator,
        uid,
        "Security Alert: Wallet Unlinked",
        format!("A wallet ({}) was removed from your account.", p.wallet_address),
        ctx.idem("wallet_unlink:push"),
    )
    .await?;

    Ok(())
}

async fn user_wallet_primary_changed(
    orchestrator: &Arc<NotificationOrchestrator>,
    ctx: &MsgCtx,
    data: Value,
) -> anyhow::Result<()> {
    let Some(p) = parse::<UserWalletPrimaryChanged>("UserWalletPrimaryChanged", data) else {
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
                "headline": "Primary Wallet Changed",
                "summary": "A different Solana wallet is now the primary wallet \
                            for your GridTokenX account — settlements will use it.",
                "wallet_address": p.wallet_address,
                "shard_id": "",
                "transaction_signature": ""
            }),
            Some(ctx.idem("wallet_primary")),
        )
        .await
        .map_err(into_anyhow)?;

    queue_push(
        orchestrator,
        uid,
        "Security Alert: Primary Wallet Changed",
        format!("Your primary wallet is now {}.", p.wallet_address),
        ctx.idem("wallet_primary:push"),
    )
    .await?;

    Ok(())
}

/// How long a login IP stays "known" for a user. A sign-in from an IP not seen
/// within this window is treated as new and alerted on.
const LOGIN_IP_MEMORY_SECS: u64 = 90 * 24 * 60 * 60;

async fn user_logged_in(
    orchestrator: &Arc<NotificationOrchestrator>,
    ctx: &MsgCtx,
    data: Value,
) -> anyhow::Result<()> {
    let Some(p) = parse::<UserLoggedIn>("UserLoggedIn", data) else {
        return Ok(());
    };

    let Some(uid) = parse_uuid(p.user_id.as_deref()) else {
        return Ok(());
    };

    // Every successful login raises this event, so alerting unconditionally
    // would mail users on each sign-in. IAM carries no device fingerprint, so
    // novelty is decided here: the first sign-in from an IP within the memory
    // window alerts, later ones are silent.
    let Some(ip) = p.ip_address.filter(|ip| !ip.is_empty()) else {
        debug!("Skipping UserLoggedIn alert: event carried no IP address");
        return Ok(());
    };

    let seen = match orchestrator
        .bump_counter(&format!("login_ip:{uid}:{ip}"), LOGIN_IP_MEMORY_SECS)
        .await
    {
        Ok(count) => count,
        Err(e) => {
            // Fail closed: with the cache down every login looks new, which
            // would alert-storm the whole user base. Skip instead.
            warn!("Skipping UserLoggedIn alert for {uid}: IP-history lookup failed: {e}");
            return Ok(());
        }
    };

    if seen > 1 {
        return Ok(());
    }

    orchestrator
        .queue_notification(
            Some(uid),
            NotificationChannel::WebSocket,
            uid.to_string(),
            "new_login.txt.tera".to_string(),
            serde_json::json!({ "username": p.username, "ip_address": ip }),
            Some(ctx.idem("new_login")),
        )
        .await
        .map_err(into_anyhow)?;

    queue_push(
        orchestrator,
        uid,
        "Security Alert: New Sign-In",
        format!("Your account was signed in from a new IP address ({ip})."),
        ctx.idem("new_login:push"),
    )
    .await?;

    Ok(())
}

async fn account_locked(
    orchestrator: &Arc<NotificationOrchestrator>,
    ctx: &MsgCtx,
    data: Value,
) -> anyhow::Result<()> {
    let Some(p) = parse::<AccountLocked>("AccountLocked", data) else {
        return Ok(());
    };

    // The lockout event is raised on the *login identifier*, which IAM accepts
    // as either an email or a username, and carries no user id. With no shared
    // users table (noti owns its own DB), a username cannot be resolved to a
    // mailbox here — skip rather than send to a non-address. Push is impossible
    // for the same reason: the FCM recipient is the user id.
    if !p.identifier.contains('@') {
        debug!("Skipping AccountLocked email: identifier is not an address");
        return Ok(());
    }

    // Round up so a sub-minute lockout never renders as "0 minute(s)".
    let lockout_minutes = p.lockout_secs.div_ceil(60).max(1);

    orchestrator
        .queue_notification(
            None,
            NotificationChannel::Email,
            p.identifier.clone(),
            "account_locked.html.tera".to_string(),
            serde_json::json!({
                "identifier": p.identifier,
                "lockout_minutes": lockout_minutes
            }),
            Some(ctx.idem("account_locked")),
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
        test_orchestrator_with_counter(Ok(1))
    }

    /// As [`test_orchestrator`], but with a fixed result for the cache counter
    /// backing new-IP detection: `Ok(1)` = first sighting, `Ok(n>1)` = known,
    /// `Err` = cache unreachable.
    fn test_orchestrator_with_counter(
        counter: noti_core::error::Result<i64>,
    ) -> (Arc<NotificationOrchestrator>, Sink) {
        let sink: Sink = Arc::new(Mutex::new(Vec::new()));
        let recorder = sink.clone();

        let mut repo = MockNotificationRepositoryTrait::new();
        repo.expect_create().times(..).returning(move |n| {
            recorder.lock().expect("sink lock").push(n.clone());
            Ok(n.clone())
        });

        let mut cache = MockCacheTrait::new();
        cache.expect_set_value().times(..).returning(|_, _, _| Ok(()));
        cache
            .expect_increment_with_ttl()
            .times(..)
            .returning(move |_, _| match &counter {
                Ok(n) => Ok(*n),
                Err(e) => Err(noti_core::error::NotiError::Internal(e.to_string())),
            });

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
            event_id: Some("evt-abc".to_string()),
        }
    }

    /// Context lacking a producer event id — exercises the `kafka:` coordinate
    /// fallback path of `MsgCtx::idem`.
    fn ctx_no_id() -> MsgCtx {
        MsgCtx {
            partition: 2,
            offset: 99,
            event_id: None,
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
        // Keyed on the producer's stable event id, not kafka coordinates.
        assert_eq!(n.idempotency_key.as_deref(), Some("email_verified:evt-abc"));
    }

    #[tokio::test]
    async fn idempotency_key_uses_event_id_for_verification_email() {
        let (orch, sink) = test_orchestrator();
        let data = serde_json::json!({
            "email": "bob@example.com",
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
        assert_eq!(
            queued[0].idempotency_key.as_deref(),
            Some("verify_email:evt-abc"),
            "verify email dedups on the stable event id (survives offset reset)"
        );
    }

    #[tokio::test]
    async fn idempotency_key_falls_back_to_kafka_coords_without_event_id() {
        let (orch, sink) = test_orchestrator();
        let data = serde_json::json!({ "email": "alice@example.com", "username": "alice" });

        dispatch(&orch, None, &ctx_no_id(), "EmailVerified", data)
            .await
            .expect("dispatch ok");

        let queued = sink.lock().expect("lock");
        assert_eq!(
            queued[0].idempotency_key.as_deref(),
            Some("kafka:email_verified:2:99"),
            "no event id → kafka coordinate fallback"
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
    async fn order_update_pushes_only_on_terminal_status() {
        for (status, expected) in [
            ("partially_filled", 1),
            ("filled", 2),
            ("cancelled", 2),
            ("expired", 2),
        ] {
            let (orch, sink) = test_orchestrator();
            let data = serde_json::json!({
                "id": Uuid::new_v4().to_string(),
                "user_id": Uuid::new_v4().to_string(),
                "filled_amount": "5.5",
                "status": status
            });

            dispatch(&orch, None, &ctx(), "OrderUpdate", data)
                .await
                .expect("dispatch ok");

            let queued = sink.lock().expect("lock");
            assert_eq!(
                queued.len(),
                expected,
                "status '{status}' should queue {expected} notification(s)"
            );
            assert_eq!(queued[0].template_id, "order_update.txt.tera");
            assert!(matches!(queued[0].channel, NotificationChannel::WebSocket));
        }
    }

    #[tokio::test]
    async fn order_update_skips_when_owner_is_absent() {
        let (orch, sink) = test_orchestrator();
        // Upstream could not resolve the owner — `user_id` serializes as null.
        let data = serde_json::json!({
            "id": Uuid::new_v4().to_string(),
            "user_id": null,
            "filled_amount": "5.5",
            "status": "filled"
        });

        dispatch(&orch, None, &ctx(), "OrderUpdate", data)
            .await
            .expect("dispatch ok");

        assert_eq!(sink.lock().expect("lock").len(), 0);
    }

    #[tokio::test]
    async fn meter_registered_notifies_owner_on_websocket() {
        let (orch, sink) = test_orchestrator();
        let uid = Uuid::new_v4();
        let meter_id = Uuid::new_v4();
        let data = serde_json::json!({
            "serial_number": "MTR-1",
            "meter_id": meter_id.to_string(),
            "user_id": uid.to_string(),
            "zone_id": 3,
            "status": "verified",
            "wallet_address": null
        });

        dispatch(&orch, None, &ctx(), "MeterRegistered", data)
            .await
            .expect("dispatch ok");

        let queued = sink.lock().expect("lock");
        assert_eq!(queued.len(), 1);
        let n = &queued[0];
        assert_eq!(n.template_id, "meter_registered.txt.tera");
        assert!(matches!(n.channel, NotificationChannel::WebSocket));
        assert_eq!(n.user_id, Some(uid));
        assert_eq!(n.variables["serial_number"], "MTR-1");
        assert_eq!(n.variables["zone_id"], "3");
        assert_eq!(
            n.idempotency_key.as_deref(),
            Some("meter_registered:evt-abc")
        );
    }

    #[tokio::test]
    async fn meter_registered_tolerates_absent_zone() {
        let (orch, sink) = test_orchestrator();
        let data = serde_json::json!({
            "serial_number": "MTR-2",
            "meter_id": Uuid::new_v4().to_string(),
            "user_id": Uuid::new_v4().to_string(),
            "zone_id": null,
            "status": "unverified"
        });

        dispatch(&orch, None, &ctx(), "MeterRegistered", data)
            .await
            .expect("dispatch ok");

        let queued = sink.lock().expect("lock");
        // Empty string, so the template's `{% if zone_id %}` row is omitted.
        assert_eq!(queued[0].variables["zone_id"], "");
    }

    #[tokio::test]
    async fn meter_updated_reuses_the_registration_template() {
        let (orch, sink) = test_orchestrator();
        let data = serde_json::json!({
            "serial_number": "MTR-3",
            "meter_id": Uuid::new_v4().to_string(),
            "user_id": Uuid::new_v4().to_string(),
            "status": "verified"
        });

        dispatch(&orch, None, &ctx(), "MeterUpdated", data)
            .await
            .expect("dispatch ok");

        assert_eq!(
            sink.lock().expect("lock")[0].template_id,
            "meter_registered.txt.tera"
        );
    }

    #[tokio::test]
    async fn order_created_acks_on_websocket_only() {
        let (orch, sink) = test_orchestrator();
        let uid = Uuid::new_v4();
        let order_id = Uuid::new_v4();
        let data = serde_json::json!({
            "id": order_id.to_string(),
            "user_id": uid.to_string(),
            "order_type": "limit",
            "side": "buy",
            // Decimals arrive as strings from the trading outbox.
            "energy_amount": "100.5",
            "price_per_kwh": "4.25",
            "status": "open"
        });

        dispatch(&orch, None, &ctx(), "OrderCreated", data)
            .await
            .expect("dispatch ok");

        let queued = sink.lock().expect("lock");
        assert_eq!(queued.len(), 1, "no push — the user is in the app");
        let n = &queued[0];
        assert_eq!(n.template_id, "order_created.txt.tera");
        assert!(matches!(n.channel, NotificationChannel::WebSocket));
        assert_eq!(n.user_id, Some(uid));
        assert_eq!(n.variables["order_id"], order_id.to_string());
        // `plain` strips the JSON quotes so the template renders 100.5, not "100.5".
        assert_eq!(n.variables["energy_amount"], "100.5");
        assert_eq!(n.idempotency_key.as_deref(), Some("order_created:evt-abc"));
    }

    #[tokio::test]
    async fn order_created_renders_numeric_decimals() {
        let (orch, sink) = test_orchestrator();
        let data = serde_json::json!({
            "id": Uuid::new_v4().to_string(),
            "user_id": Uuid::new_v4().to_string(),
            "side": "sell",
            "energy_amount": 100.5,
            "price_per_kwh": 4,
            "status": "open"
        });

        dispatch(&orch, None, &ctx(), "OrderCreated", data)
            .await
            .expect("dispatch ok");

        let queued = sink.lock().expect("lock");
        assert_eq!(queued[0].variables["energy_amount"], "100.5");
        assert_eq!(queued[0].variables["price_per_kwh"], "4");
    }

    #[tokio::test]
    async fn order_created_skips_non_uuid_user() {
        let (orch, sink) = test_orchestrator();
        let data = serde_json::json!({ "id": "o1", "user_id": "nope", "side": "buy" });

        dispatch(&orch, None, &ctx(), "OrderCreated", data)
            .await
            .expect("dispatch ok");

        assert_eq!(sink.lock().expect("lock").len(), 0);
    }

    #[tokio::test]
    async fn login_from_new_ip_alerts_on_ws_and_push() {
        let (orch, sink) = test_orchestrator_with_counter(Ok(1));
        let uid = Uuid::new_v4();
        let data = serde_json::json!({
            "user_id": uid.to_string(),
            "username": "alice",
            "ip_address": "203.0.113.7"
        });

        dispatch(&orch, None, &ctx(), "UserLoggedIn", data)
            .await
            .expect("dispatch ok");

        let queued = sink.lock().expect("lock");
        assert_eq!(queued.len(), 2, "WS + Push");
        let ws = queued
            .iter()
            .find(|n| matches!(n.channel, NotificationChannel::WebSocket))
            .expect("ws notification");
        assert_eq!(ws.template_id, "new_login.txt.tera");
        assert_eq!(ws.variables["ip_address"], "203.0.113.7");
        assert_eq!(ws.idempotency_key.as_deref(), Some("new_login:evt-abc"));
    }

    #[tokio::test]
    async fn login_from_known_ip_is_silent() {
        // Counter > 1 → this IP is already in the user's history.
        let (orch, sink) = test_orchestrator_with_counter(Ok(2));
        let data = serde_json::json!({
            "user_id": Uuid::new_v4().to_string(),
            "username": "alice",
            "ip_address": "203.0.113.7"
        });

        dispatch(&orch, None, &ctx(), "UserLoggedIn", data)
            .await
            .expect("dispatch ok");

        assert_eq!(sink.lock().expect("lock").len(), 0);
    }

    #[tokio::test]
    async fn login_alert_fails_closed_when_cache_is_down() {
        // With no IP history available every login would look new — alerting
        // then would storm the whole user base, so the handler stays silent.
        let (orch, sink) =
            test_orchestrator_with_counter(Err(noti_core::error::NotiError::Internal(
                "redis down".to_string(),
            )));
        let data = serde_json::json!({
            "user_id": Uuid::new_v4().to_string(),
            "username": "alice",
            "ip_address": "203.0.113.7"
        });

        dispatch(&orch, None, &ctx(), "UserLoggedIn", data)
            .await
            .expect("dispatch ok");

        assert_eq!(sink.lock().expect("lock").len(), 0);
    }

    #[tokio::test]
    async fn login_without_ip_is_silent() {
        let (orch, sink) = test_orchestrator();
        let data = serde_json::json!({
            "user_id": Uuid::new_v4().to_string(),
            "username": "alice",
            "ip_address": null
        });

        dispatch(&orch, None, &ctx(), "UserLoggedIn", data)
            .await
            .expect("dispatch ok");

        assert_eq!(sink.lock().expect("lock").len(), 0);
    }

    #[tokio::test]
    async fn wallet_unlinked_alerts_on_ws_and_push() {
        let (orch, sink) = test_orchestrator();
        let uid = Uuid::new_v4();
        let data = serde_json::json!({
            "user_id": uid.to_string(),
            "wallet_address": "9xQe"
        });

        dispatch(&orch, None, &ctx(), "UserWalletUnlinked", data)
            .await
            .expect("dispatch ok");

        let queued = sink.lock().expect("lock");
        assert_eq!(queued.len(), 2, "WS + Push");
        let ws = queued
            .iter()
            .find(|n| matches!(n.channel, NotificationChannel::WebSocket))
            .expect("ws notification");
        assert_eq!(ws.template_id, "security_alert.txt.tera");
        assert_eq!(ws.variables["headline"], "Wallet Unlinked");
        // Off-chain change: the on-chain rows are suppressed by empty values.
        assert_eq!(ws.variables["transaction_signature"], "");
        assert_eq!(ws.idempotency_key.as_deref(), Some("wallet_unlink:evt-abc"));

        let push = queued
            .iter()
            .find(|n| matches!(n.channel, NotificationChannel::Push))
            .expect("push notification");
        assert_eq!(
            push.idempotency_key.as_deref(),
            Some("wallet_unlink:push:evt-abc"),
            "push key is distinct from the WS key for the same event"
        );
    }

    #[tokio::test]
    async fn wallet_primary_changed_alerts_on_ws_and_push() {
        let (orch, sink) = test_orchestrator();
        let uid = Uuid::new_v4();
        let data = serde_json::json!({
            "user_id": uid.to_string(),
            "wallet_address": "9xQe",
            "is_primary": true
        });

        dispatch(&orch, None, &ctx(), "UserWalletPrimaryChanged", data)
            .await
            .expect("dispatch ok");

        let queued = sink.lock().expect("lock");
        assert_eq!(queued.len(), 2, "WS + Push");
        let ws = queued
            .iter()
            .find(|n| matches!(n.channel, NotificationChannel::WebSocket))
            .expect("ws notification");
        assert_eq!(ws.template_id, "security_alert.txt.tera");
        assert_eq!(ws.variables["headline"], "Primary Wallet Changed");
        assert_eq!(ws.variables["wallet_address"], "9xQe");
    }

    #[tokio::test]
    async fn wallet_events_skip_non_uuid_users() {
        let (orch, sink) = test_orchestrator();

        for event in ["UserWalletUnlinked", "UserWalletPrimaryChanged"] {
            dispatch(
                &orch,
                None,
                &ctx(),
                event,
                serde_json::json!({ "user_id": "not-a-uuid", "wallet_address": "9xQe" }),
            )
            .await
            .expect("dispatch ok");
        }

        assert_eq!(sink.lock().expect("lock").len(), 0);
    }

    #[tokio::test]
    async fn account_locked_emails_when_identifier_is_an_address() {
        let (orch, sink) = test_orchestrator();
        let data = serde_json::json!({
            "identifier": "dave@example.com",
            "lockout_secs": 900
        });

        dispatch(&orch, None, &ctx(), "AccountLocked", data)
            .await
            .expect("dispatch ok");

        let queued = sink.lock().expect("lock");
        assert_eq!(queued.len(), 1, "email only — payload carries no user id");
        let n = &queued[0];
        assert_eq!(n.template_id, "account_locked.html.tera");
        assert!(matches!(n.channel, NotificationChannel::Email));
        assert_eq!(n.recipient, "dave@example.com");
        assert_eq!(n.user_id, None);
        assert_eq!(n.variables["lockout_minutes"], 15);
        assert_eq!(n.idempotency_key.as_deref(), Some("account_locked:evt-abc"));
    }

    #[tokio::test]
    async fn account_locked_rounds_sub_minute_lockout_up() {
        let (orch, sink) = test_orchestrator();
        let data = serde_json::json!({ "identifier": "dave@example.com", "lockout_secs": 30 });

        dispatch(&orch, None, &ctx(), "AccountLocked", data)
            .await
            .expect("dispatch ok");

        assert_eq!(
            sink.lock().expect("lock")[0].variables["lockout_minutes"], 1,
            "a 30s lockout must not render as 0 minutes"
        );
    }

    #[tokio::test]
    async fn account_locked_skips_username_identifier() {
        let (orch, sink) = test_orchestrator();
        // IAM raises the lockout on the login identifier, which may be a
        // username — unresolvable to a mailbox from this service.
        let data = serde_json::json!({ "identifier": "dave", "lockout_secs": 900 });

        dispatch(&orch, None, &ctx(), "AccountLocked", data)
            .await
            .expect("dispatch ok");

        assert_eq!(sink.lock().expect("lock").len(), 0);
    }

    #[tokio::test]
    async fn wallet_link_requested_builds_confirm_link_from_token() {
        let (orch, sink) = test_orchestrator();
        let uid = Uuid::new_v4();
        let data = serde_json::json!({
            "user_id": uid.to_string(),
            "email": "carol@example.com",
            "username": "carol",
            "wallet_address": "9xQeWvG816bUx9EPjHmaT23yvVM2ZWbrrpZb9PusVFin",
            "token": "tok-w1"
        });

        dispatch(
            &orch,
            Some("https://app.gridtokenx.test/"),
            &ctx(),
            "WalletLinkRequested",
            data,
        )
        .await
        .expect("dispatch ok");

        let queued = sink.lock().expect("lock");
        assert_eq!(queued.len(), 1, "confirmation is email-only (pre-link)");
        let n = &queued[0];
        assert_eq!(n.template_id, "confirm_wallet.html.tera");
        assert!(matches!(n.channel, NotificationChannel::Email));
        assert_eq!(n.recipient, "carol@example.com");
        assert_eq!(n.user_id, Some(uid));
        assert_eq!(
            n.variables["confirmation_url"],
            "https://app.gridtokenx.test/wallet/confirm?token=tok-w1\
             &wallet=9xQeWvG816bUx9EPjHmaT23yvVM2ZWbrrpZb9PusVFin"
        );
        assert_eq!(n.idempotency_key.as_deref(), Some("wallet_confirm:evt-abc"));
    }

    #[tokio::test]
    async fn wallet_link_requested_rewrites_upstream_url() {
        let (orch, sink) = test_orchestrator();
        let data = serde_json::json!({
            "email": "carol@example.com",
            "username": "carol",
            "wallet_address": "9xQe",
            "confirmation_url": "http://localhost:4001/wallet/confirm?token=tok-w2"
        });

        dispatch(
            &orch,
            Some("https://app.gridtokenx.test/"),
            &ctx(),
            "WalletLinkRequested",
            data,
        )
        .await
        .expect("dispatch ok");

        let queued = sink.lock().expect("lock");
        assert_eq!(
            queued[0].variables["confirmation_url"],
            "https://app.gridtokenx.test/wallet/confirm?token=tok-w2"
        );
    }

    #[tokio::test]
    async fn wallet_link_requested_skips_incomplete_events() {
        let (orch, sink) = test_orchestrator();

        // No URL can be built (no frontend_url, no upstream url) → no broken email.
        dispatch(
            &orch,
            None,
            &ctx(),
            "WalletLinkRequested",
            serde_json::json!({
                "email": "carol@example.com",
                "wallet_address": "9xQe",
                "token": "tok-w3"
            }),
        )
        .await
        .expect("dispatch ok");

        // Missing wallet address → nothing to confirm.
        dispatch(
            &orch,
            Some("https://app.gridtokenx.test/"),
            &ctx(),
            "WalletLinkRequested",
            serde_json::json!({ "email": "carol@example.com", "token": "tok-w4" }),
        )
        .await
        .expect("dispatch ok");

        // Missing recipient.
        dispatch(
            &orch,
            Some("https://app.gridtokenx.test/"),
            &ctx(),
            "WalletLinkRequested",
            serde_json::json!({ "wallet_address": "9xQe", "token": "tok-w5" }),
        )
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
        // Each party gets a WebSocket + a Push notification → 4 total.
        assert_eq!(queued.len(), 4, "buyer + seller, each on WS + Push");

        let ws: Vec<_> = queued
            .iter()
            .filter(|n| matches!(n.channel, NotificationChannel::WebSocket))
            .collect();
        let push: Vec<_> = queued
            .iter()
            .filter(|n| matches!(n.channel, NotificationChannel::Push))
            .collect();
        assert_eq!(ws.len(), 2, "one WS per party");
        assert_eq!(push.len(), 2, "one Push per party");

        // Push recipient is the user_id (FcmProvider fans out to device tokens),
        // routed through the JSON push template.
        for n in &push {
            assert_eq!(n.template_id, "push_notification.txt.tera");
            assert_eq!(n.recipient, n.user_id.expect("push has user").to_string());
        }
        for uid in [buyer, seller] {
            assert!(ws.iter().any(|n| n.user_id == Some(uid)));
            assert!(push.iter().any(|n| n.user_id == Some(uid)));
        }
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
        // WebSocket + Push to the owner.
        assert_eq!(queued.len(), 2, "owner on WS + Push");
        assert!(queued.iter().all(|n| n.user_id == Some(owner)));
        let ws = queued
            .iter()
            .find(|n| matches!(n.channel, NotificationChannel::WebSocket))
            .expect("ws present");
        assert_eq!(ws.template_id, "price_alert_triggered.txt.tera");
        assert_eq!(ws.variables["condition"], "above");
        let push = queued
            .iter()
            .find(|n| matches!(n.channel, NotificationChannel::Push))
            .expect("push present");
        assert_eq!(push.template_id, "push_notification.txt.tera");
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
        // Each party: WebSocket + Push → 4 total.
        assert_eq!(queued.len(), 4, "buyer + seller, each on WS + Push");
        let ws: Vec<_> = queued
            .iter()
            .filter(|n| matches!(n.channel, NotificationChannel::WebSocket))
            .collect();
        let push: Vec<_> = queued
            .iter()
            .filter(|n| matches!(n.channel, NotificationChannel::Push))
            .collect();
        assert_eq!(ws.len(), 2);
        assert_eq!(push.len(), 2);
        assert!(ws.iter().all(|n| n.template_id == "settlement_processed.txt.tera"));
        assert!(push.iter().all(|n| n.template_id == "push_notification.txt.tera"));
        let buyer_n = ws
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
        // Only the buyer is notified, but on both WS + Push.
        assert_eq!(queued.len(), 2, "buyer only, WS + Push");
        assert!(queued.iter().all(|n| n.user_id == Some(buyer)));
        let ws = queued
            .iter()
            .find(|n| matches!(n.channel, NotificationChannel::WebSocket))
            .expect("ws present");
        // Empty upstream status falls back to "unknown".
        assert_eq!(ws.variables["status"], "unknown");
        assert!(
            queued.iter().any(|n| matches!(n.channel, NotificationChannel::Push)),
            "push present"
        );
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
        // WebSocket + Push (security-sensitive) to the owner.
        assert_eq!(queued.len(), 2, "owner on WS + Push");
        assert!(queued.iter().all(|n| n.user_id == Some(owner)));
        let ws = queued
            .iter()
            .find(|n| matches!(n.channel, NotificationChannel::WebSocket))
            .expect("ws present");
        assert_eq!(ws.template_id, "security_alert.txt.tera");
        assert_eq!(ws.variables["wallet_address"], "Wallet111");
        assert!(
            queued.iter().any(|n| matches!(n.channel, NotificationChannel::Push)),
            "push present"
        );
    }

    #[tokio::test]
    async fn user_onboarded_queues_websocket_to_owner() {
        let (orch, sink) = test_orchestrator();
        let owner = Uuid::new_v4();
        let data = serde_json::json!({
            "user_id": owner.to_string(),
            "user_account_pda": "Pda111",
            "transaction_signature": "sig-onboard"
        });

        dispatch(&orch, None, &ctx(), "UserOnboarded", data)
            .await
            .expect("dispatch ok");

        let queued = sink.lock().expect("lock");
        assert_eq!(queued.len(), 1, "one WS notification to the owner");
        let n = &queued[0];
        assert_eq!(n.user_id, Some(owner));
        assert!(matches!(n.channel, NotificationChannel::WebSocket));
        assert_eq!(n.recipient, owner.to_string());
        assert_eq!(n.template_id, "user_onboarded.txt.tera");
        assert_eq!(n.variables["user_account_pda"], "Pda111");
        assert_eq!(n.variables["transaction_signature"], "sig-onboard");
        assert_eq!(n.idempotency_key.as_deref(), Some("onboard:evt-abc"));
    }

    #[tokio::test]
    async fn user_onboarded_skips_without_user_uuid() {
        let (orch, sink) = test_orchestrator();
        let data = serde_json::json!({
            "user_account_pda": "Pda111",
            "transaction_signature": "sig-onboard"
        });

        dispatch(&orch, None, &ctx(), "UserOnboarded", data)
            .await
            .expect("dispatch ok");

        assert_eq!(sink.lock().expect("lock").len(), 0);
    }

    #[tokio::test]
    async fn meter_onboarded_queues_websocket_to_owner() {
        let (orch, sink) = test_orchestrator();
        let owner = Uuid::new_v4();
        let data = serde_json::json!({
            "user_id": owner.to_string(),
            "meter_id": "m-1",
            "meter_type": "smart",
            "transaction_signature": "sig-meter"
        });

        dispatch(&orch, None, &ctx(), "MeterOnboarded", data)
            .await
            .expect("dispatch ok");

        let queued = sink.lock().expect("lock");
        assert_eq!(queued.len(), 1, "one WS notification to the owner");
        let n = &queued[0];
        assert_eq!(n.user_id, Some(owner));
        assert!(matches!(n.channel, NotificationChannel::WebSocket));
        assert_eq!(n.recipient, owner.to_string());
        assert_eq!(n.template_id, "meter_onboarded.txt.tera");
        assert_eq!(n.variables["meter_id"], "m-1");
        assert_eq!(n.variables["meter_type"], "smart");
        assert_eq!(n.variables["transaction_signature"], "sig-meter");
        assert_eq!(n.idempotency_key.as_deref(), Some("meter:evt-abc"));
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
