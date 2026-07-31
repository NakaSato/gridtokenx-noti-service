//! Tera-based template engine implementing `TemplateEngineTrait`.

use noti_core::error::{NotiError, Result};
use noti_core::traits::TemplateEngineTrait;
use serde_json::Value;
use tera::{Context, Tera};

pub struct TemplateEngine {
    tera: Tera,
}

impl TemplateEngine {
    /// # Errors
    ///
    /// Returns an error if the Tera engine cannot be initialized from the
    /// given templates directory path.
    pub fn new(templates_path: &str) -> std::result::Result<Self, anyhow::Error> {
        let mut tera = Tera::new(&format!("{templates_path}/**/*"))
            .map_err(|e| anyhow::anyhow!("Failed to initialize Tera engine: {e}"))?;

        // Templates are named `*.html.tera`, so the suffix to match is
        // `html.tera` (not `html`). Matching on `html` alone escaped nothing,
        // leaving HTML emails open to markup injection via user-supplied vars.
        tera.autoescape_on(vec!["html.tera"]);

        Ok(Self { tera })
    }

    /// Register a string-based template dynamically.
    ///
    /// # Errors
    ///
    /// Returns an error if the template name is duplicate or the content is
    /// invalid Tera syntax.
    pub fn add_raw_template(&mut self, name: &str, content: &str) -> Result<()> {
        self.tera
            .add_raw_template(name, content)
            .map_err(|e| NotiError::Template(e.to_string()))
    }
}

impl TemplateEngineTrait for TemplateEngine {
    fn render(&self, template_id: &str, variables: &Value) -> Result<String> {
        let context = Context::from_value(variables.clone())
            .map_err(|e| NotiError::Template(format!("context creation failed: {e}")))?;

        self.tera
            .render(template_id, &context)
            .map_err(|e| NotiError::Template(format!("render '{template_id}' failed: {e}")))
    }
}

#[cfg(test)]
mod tests {
    use super::TemplateEngine;
    use noti_core::traits::TemplateEngineTrait;
    use serde_json::json;

    fn engine() -> TemplateEngine {
        // Templates live at the repo root, two levels up from this crate.
        let dir = concat!(env!("CARGO_MANIFEST_DIR"), "/../../templates");
        TemplateEngine::new(dir).expect("template engine loads (also validates all template syntax)")
    }

    /// Every template the Kafka consumers route to, paired with the exact
    /// variable shape its handler passes (see `consumers/events.rs`).
    fn routed_template_cases() -> Vec<(&'static str, serde_json::Value)> {
        vec![
            ("welcome.html.tera", json!({ "name": "Alice" })),
            (
                "trade_matched.txt.tera",
                json!({ "role": "buyer", "amount": "10.5", "price": "2.30" }),
            ),
            ("erc_issued.html.tera", json!({ "amount": "100", "unit": "MWh" })),
            // No `unit` key: exercises the template's `default` filter, the path
            // taken by anything queuing this template outside the Kafka handler.
            ("erc_issued.html.tera", json!({ "amount": "100" })),
            (
                "vpp_dispatched.txt.tera",
                json!({ "cluster_id": "C1", "target_kw": "500", "members_count": 12 }),
            ),
            (
                "password_reset.html.tera",
                json!({ "reset_url": "https://app.example/reset?token=abc" }),
            ),
            (
                "verify_email.html.tera",
                json!({ "name": "Bob", "verification_url": "https://app.example/verify?token=xyz" }),
            ),
            // IAM sends `username` as a `#[serde(default)]` String, so an empty
            // name is reachable — the greeting must degrade to "Hello there".
            (
                "verify_email.html.tera",
                json!({ "name": "", "verification_url": "https://app.example/verify?token=xyz" }),
            ),
            (
                "user_onboarded.txt.tera",
                json!({ "user_account_pda": "PDA1", "transaction_signature": "SIG1" }),
            ),
            (
                "meter_onboarded.txt.tera",
                json!({ "meter_id": "M1", "meter_type": "smart", "transaction_signature": "SIG2" }),
            ),
            (
                "security_alert.txt.tera",
                json!({
                    "headline": "New Wallet Linked",
                    "summary": "A new Solana wallet has been linked to your account.",
                    "wallet_address": "0xabc",
                    "shard_id": "1",
                    "transaction_signature": "SIG3"
                }),
            ),
            // Off-chain variant: empty on-chain fields must omit their rows.
            (
                "security_alert.txt.tera",
                json!({
                    "headline": "Wallet Unlinked",
                    "summary": "A Solana wallet was removed from your account.",
                    "wallet_address": "0xabc",
                    "shard_id": "",
                    "transaction_signature": ""
                }),
            ),
            (
                "account_locked.html.tera",
                json!({ "identifier": "dave@example.com", "lockout_minutes": 15 }),
            ),
            (
                "new_login.txt.tera",
                json!({ "username": "dave", "ip_address": "203.0.113.7" }),
            ),
            (
                "order_update.txt.tera",
                json!({ "order_id": "ORD1", "filled_amount": "5.5", "status": "filled" }),
            ),
            (
                "meter_registered.txt.tera",
                json!({
                    "serial_number": "MTR-1",
                    "meter_id": "m1",
                    "zone_id": "3",
                    "status": "verified"
                }),
            ),
            (
                "order_created.txt.tera",
                json!({
                    "order_id": "ORD1",
                    "order_type": "limit",
                    "side": "buy",
                    "energy_amount": "100.5",
                    "price_per_kwh": "4.25",
                    "status": "open"
                }),
            ),
            (
                "confirm_wallet.html.tera",
                json!({
                    "name": "Carol",
                    "wallet_address": "9xQeWvG816bUx9EPjHmaT23yvVM2ZWbrrpZb9PusVFin",
                    "confirmation_url": "https://app.example/wallet/confirm?token=tok"
                }),
            ),
        ]
    }

    /// Templates that ship but no consumer routes to yet, plus the variable
    /// shapes their fallback branches exist for.
    fn unrouted_template_cases() -> Vec<(&'static str, serde_json::Value)> {
        vec![
            // The HTML security-alert email. Not routed from `consumers/events.rs`:
            // `UserLoggedIn` carries no recipient address (iam-core
            // `domain/identity/events.rs:70` sends user_id/username/ip_address
            // only), so the login handler stays WebSocket + Push. Covered here so
            // the markup and its `<title>` subject stay verified regardless.
            (
                "new_login.html.tera",
                json!({
                    "username": "dave",
                    "timestamp": "28 Jul 2026, 14:02 UTC+7",
                    "device": "Chrome 128 · macOS",
                    "location": "Bangkok, Thailand (approx.)",
                    "ip_address": "203.0.113.7",
                    "security_url": "https://app.example/security/sessions"
                }),
            ),
            // Bare payload: every field takes its fallback branch rather than
            // erroring on an undefined variable.
            ("new_login.html.tera", json!({})),
            // Empty strings are reachable wherever a producer field is
            // `#[serde(default)]`, and must take the same fallback branch.
            (
                "new_login.html.tera",
                json!({
                    "username": "",
                    "timestamp": "",
                    "device": "",
                    "location": "",
                    "ip_address": "",
                    "security_url": ""
                }),
            ),
        ]
    }

    /// Render every routed template. Guards against a template referencing a
    /// variable the producer never supplies.
    #[test]
    fn all_routed_templates_render() {
        let e = engine();

        for (name, vars) in routed_template_cases()
            .into_iter()
            .chain(unrouted_template_cases())
        {
            let out = e
                .render(name, &vars)
                .unwrap_or_else(|err| panic!("render '{name}' failed: {err}"));
            assert!(!out.trim().is_empty(), "template '{name}' rendered empty");

            // Shared components (templates/_components.html.tera macros) must
            // expand to real markup — if autoescaping ever applies to macro
            // output, buttons/lists ship as literal `&lt;table&gt;` text.
            assert!(
                !out.contains("&lt;table") && !out.contains("&lt;a href"),
                "template '{name}' contains escaped component markup"
            );

            // SmtpProvider derives the email subject from the `<title>` element
            // (the Tera `subject` block). An HTML template that forgets
            // `{% block subject %}` inherits base.html.tera's "GridTokenX"
            // default and silently ships a generic subject — fail here instead.
            if name.ends_with(".html.tera") {
                let title = out
                    .split_once("<title>")
                    .and_then(|(_, rest)| rest.split_once("</title>"))
                    .map_or_else(
                        || panic!("template '{name}' has no <title>"),
                        |(t, _)| t.trim(),
                    );
                assert_ne!(
                    title, "GridTokenX",
                    "template '{name}' missing `{{% block subject %}}` (got base default title)"
                );
            }
        }
    }
}
