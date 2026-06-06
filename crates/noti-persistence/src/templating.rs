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

        tera.autoescape_on(vec!["html"]);

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

    /// Render every template the Kafka consumers route to, with the exact
    /// variable shape each handler passes (see `consumers/events.rs`). Guards
    /// against a template referencing a variable the producer never supplies.
    #[test]
    fn all_routed_templates_render() {
        let e = engine();
        let cases: &[(&str, serde_json::Value)] = &[
            ("welcome.html.tera", json!({ "name": "Alice" })),
            (
                "trade_matched.txt.tera",
                json!({ "role": "buyer", "amount": "10.5", "price": "2.30" }),
            ),
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
                json!({ "wallet_address": "0xabc", "shard_id": "1", "transaction_signature": "SIG3" }),
            ),
        ];

        for (name, vars) in cases {
            let out = e
                .render(name, vars)
                .unwrap_or_else(|err| panic!("render '{name}' failed: {err}"));
            assert!(!out.trim().is_empty(), "template '{name}' rendered empty");
        }
    }
}
