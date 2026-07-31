//! Drift guard: every template the Kafka consumers route to must exist on disk
//! and render with the exact variable shape its handler supplies.
//!
//! The unit tests in `consumers::events` assert each handler picks the right
//! `template_id` and variables; the unit test in `noti-persistence::templating`
//! renders a *hand-maintained* list of (template, vars) pairs. That list can —
//! and did — fall out of sync with the handlers (`settlement_processed` and
//! `price_alert_triggered` were routed but never render-tested). A template that
//! references a variable the producer never supplies, or that is renamed/removed
//! on disk, would then only fail in production as a `PermanentFailure`.
//!
//! This test closes the gap by deriving the (template, vars) pairs from the
//! *real* dispatcher: it drives one representative payload through every handled
//! event type with a recording orchestrator (no broker, no DB), collects every
//! queued notification, then renders each through the real `TemplateEngine`
//! loaded from the repo `templates/` directory. No hand-maintained var list, so
//! it cannot drift from the handlers.

use std::collections::BTreeSet;
use std::sync::{Arc, Mutex};

use serde_json::{Value, json};
use uuid::Uuid;

use noti_core::domain::Notification;
use noti_core::traits::{
    MockCacheTrait, MockMessageQueueTrait, MockNotificationProviderTrait,
    MockNotificationRepositoryTrait, MockTemplateEngineTrait, TemplateEngineTrait,
};
use noti_logic::NotificationOrchestrator;
use noti_persistence::templating::TemplateEngine;
use noti_server::consumers::{MsgCtx, dispatch};

type Sink = Arc<Mutex<Vec<Notification>>>;

/// Orchestrator whose `repo.create` records every queued notification; cache
/// and MQ accept everything. The queue path never renders, so the mock template
/// engine here is never called — rendering happens below against the real one.
fn recording_orchestrator() -> (Arc<NotificationOrchestrator>, Sink) {
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

/// The real engine, loaded from the repo `templates/` dir (two levels up from
/// this crate). Construction also validates every template's Tera syntax.
fn template_engine() -> TemplateEngine {
    let dir = concat!(env!("CARGO_MANIFEST_DIR"), "/../../templates");
    TemplateEngine::new(dir).expect("template engine loads from templates/ dir")
}

/// One representative, *queue-producing* payload per handled event type. The
/// `frontend_url` leg matters for the email events that rewrite/derive links.
fn event_cases() -> Vec<(&'static str, Option<&'static str>, Value)> {
    let buyer = Uuid::new_v4().to_string();
    let seller = Uuid::new_v4().to_string();
    let user = Uuid::new_v4().to_string();
    let admin = Uuid::new_v4().to_string();
    let frontend = "https://app.gridtokenx.test/";

    vec![
        (
            "EmailVerified",
            Some(frontend),
            json!({ "email": "alice@example.com", "username": "alice" }),
        ),
        (
            "OrderMatched",
            None,
            json!({ "buyer_id": buyer, "seller_id": seller, "amount": 100, "price": 5 }),
        ),
        (
            "SettlementProcessed",
            None,
            json!({
                "status": "confirmed",
                "tx_signature": "sig-abc",
                "buyer_id": buyer,
                "seller_id": seller,
                "amount": 50,
                "price": 3
            }),
        ),
        (
            "ErcIssued",
            None,
            json!({ "recipient_email": "rec@example.com", "energy_amount": 1234 }),
        ),
        (
            "VppDispatched",
            None,
            json!({
                "cluster_id": "cluster-7",
                "target_kw": 250.5,
                "members_commanded": 12,
                "admin_user_id": admin,
                "user_id": user
            }),
        ),
        (
            "PriceAlertTriggered",
            None,
            json!({
                "alert_id": Uuid::new_v4().to_string(),
                "user_id": user,
                "target_price": "0.25",
                "triggered_price": "0.27",
                "condition": "above"
            }),
        ),
        (
            "PasswordResetRequested",
            Some(frontend),
            json!({
                "email": "bob@example.com",
                "reset_url": "https://upstream.invalid/reset-password?token=abc"
            }),
        ),
        (
            "VerificationEmailRequested",
            Some(frontend),
            json!({ "email": "bob@example.com", "username": "bob", "token": "tok-123" }),
        ),
        (
            "UserOnboarded",
            None,
            json!({
                "user_id": user,
                "user_account_pda": "PDA1",
                "transaction_signature": "SIG1"
            }),
        ),
        (
            "MeterOnboarded",
            None,
            json!({
                "user_id": user,
                "meter_id": "M1",
                "meter_type": "smart",
                "transaction_signature": "SIG2"
            }),
        ),
        (
            "UserWalletLinked",
            None,
            json!({
                "user_id": user,
                "wallet_address": "Wallet111",
                "shard_id": 3,
                "transaction_signature": "sig-link"
            }),
        ),
    ]
}

/// Every template a live handler routes to. If a handler is added, removed, or
/// re-pointed at a different template, this set must change in lockstep — the
/// assertion below fails loudly otherwise.
const EXPECTED_ROUTED_TEMPLATES: &[&str] = &[
    "welcome.html.tera",
    "trade_matched.txt.tera",
    // Shared by every Push-channel event (OrderMatched, SettlementProcessed,
    // PriceAlertTriggered, UserWalletLinked).
    "push_notification.txt.tera",
    "settlement_processed.txt.tera",
    "erc_issued.html.tera",
    "vpp_dispatched.txt.tera",
    "price_alert_triggered.txt.tera",
    "password_reset.html.tera",
    "verify_email.html.tera",
    "user_onboarded.txt.tera",
    "meter_onboarded.txt.tera",
    "security_alert.txt.tera",
];

#[tokio::test]
async fn every_routed_template_renders_with_handler_variables() {
    let (orch, sink) = recording_orchestrator();
    let ctx = MsgCtx {
        partition: 2,
        offset: 99,
        event_id: None,
    };

    for (event_type, frontend, data) in event_cases() {
        dispatch(&orch, frontend, &ctx, event_type, data)
            .await
            .unwrap_or_else(|e| panic!("dispatch '{event_type}' failed: {e}"));
    }

    let queued = sink.lock().expect("sink lock");
    assert!(
        !queued.is_empty(),
        "no notifications queued — event payloads stopped producing"
    );

    let engine = template_engine();
    let mut rendered: BTreeSet<String> = BTreeSet::new();

    for n in queued.iter() {
        let out = engine
            .render(&n.template_id, &n.variables)
            .unwrap_or_else(|e| {
                panic!(
                    "template '{}' failed to render with handler variables {}: {e}",
                    n.template_id, n.variables
                )
            });
        assert!(
            !out.trim().is_empty(),
            "template '{}' rendered empty",
            n.template_id
        );

        // SmtpProvider derives the email subject from the `<title>` element
        // (the Tera `subject` block). An HTML template that forgets
        // `{% block subject %}` inherits base.html.tera's "GridTokenX" default
        // and silently ships a generic subject — fail here instead.
        if n.template_id.ends_with(".html.tera") {
            let title = out
                .split_once("<title>")
                .and_then(|(_, rest)| rest.split_once("</title>"))
                .map_or_else(
                    || panic!("template '{}' has no <title>", n.template_id),
                    |(t, _)| t.trim(),
                );
            assert_ne!(
                title, "GridTokenX",
                "template '{}' missing `{{% block subject %}}` (got base default title)",
                n.template_id
            );
        }

        // The push template renders the JSON envelope FcmProvider parses
        // (`{title, body}`). A template that emits malformed JSON (e.g. an
        // unescaped value) would silently degrade to the plain-body fallback in
        // production — assert it is well-formed and carries both fields here.
        if n.template_id == "push_notification.txt.tera" {
            let parsed: Value = serde_json::from_str(out.trim()).unwrap_or_else(|e| {
                panic!("push template '{}' emitted invalid JSON ({e}): {out}", n.template_id)
            });
            assert!(
                parsed.get("title").and_then(Value::as_str).is_some(),
                "push template '{}' missing string `title`",
                n.template_id
            );
            assert!(
                parsed.get("body").and_then(Value::as_str).is_some_and(|b| !b.is_empty()),
                "push template '{}' missing non-empty `body`",
                n.template_id
            );
        }

        rendered.insert(n.template_id.clone());
    }

    let expected: BTreeSet<String> =
        EXPECTED_ROUTED_TEMPLATES.iter().map(|s| (*s).to_string()).collect();
    assert_eq!(
        rendered, expected,
        "routed templates drifted from the expected set: \
         a handler was added/removed/re-pointed without updating this guard"
    );
}

/// The notification list projects every row through its **text** template
/// (`noti_core::wire::text_template_id`) so an email row lists as prose instead
/// of a full HTML document. An email template shipped without a `.txt.tera`
/// sibling still lists — with an empty body — which reads to the user as a
/// blank row. Fail here instead.
#[tokio::test]
async fn every_routed_template_has_a_listable_text_body() {
    let (orch, sink) = recording_orchestrator();
    let ctx = MsgCtx {
        partition: 0,
        offset: 1,
        event_id: None,
    };

    for (event_type, frontend, data) in event_cases() {
        dispatch(&orch, frontend, &ctx, event_type, data)
            .await
            .unwrap_or_else(|e| panic!("dispatch '{event_type}' failed: {e}"));
    }

    let queued = sink.lock().expect("sink lock");
    let engine = template_engine();

    for n in queued.iter() {
        let text_id = noti_core::wire::text_template_id(&n.template_id);
        let out = engine.render(&text_id, &n.variables).unwrap_or_else(|e| {
            panic!(
                "'{}' has no listable text template ('{text_id}' failed to render): {e}",
                n.template_id
            )
        });

        let (title, message) = noti_core::wire::title_and_message(&text_id, &n.variables, &out);
        assert!(
            !title.is_empty(),
            "'{}' projects an empty list title",
            n.template_id
        );
        assert!(
            !message.trim().is_empty(),
            "'{}' projects an empty list body",
            n.template_id
        );
    }
}
