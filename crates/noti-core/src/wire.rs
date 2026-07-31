//! Client-facing wire contract for a delivered notification.
//!
//! A [`Notification`] is a *delivery record*: it names a template and the
//! variables to render it with, but carries no title, no body, and no event
//! name. That is enough for the dispatch pipeline and useless to a UI, which
//! needs something to display and something to branch on.
//!
//! [`NotificationView`] is that projection, and both transports emit it — the
//! WebSocket frame and the REST list return the same JSON object, so a client
//! parses one shape regardless of how the notification arrived.

use serde::{Deserialize, Serialize};
use serde_json::Value;
use utoipa::ToSchema;

use crate::domain::Notification;

/// A notification as clients consume it: the stored record plus the rendered
/// text split into `title`/`message`, the canonical event `type`, and read
/// state.
#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub struct NotificationView {
    /// The stored delivery record (channel, status, template, variables, …).
    #[serde(flatten)]
    pub notification: Notification,
    /// Canonical event name — see [`event_type`]. Serialized as `type`.
    #[serde(rename = "type")]
    pub event_type: String,
    /// Short headline for a list row or toast.
    pub title: String,
    /// Rendered body, with the heading removed when the template had one.
    pub message: String,
    /// Whether the user has read it (`read_at` is set).
    pub is_read: bool,
}

impl NotificationView {
    /// Project a stored notification plus its already-rendered text.
    ///
    /// `rendered` must come from a **text** template — pass an HTML body and
    /// the markup lands verbatim in `message`. Use [`text_template_id`] to pick
    /// the right template when rendering for this view.
    #[must_use]
    pub fn new(notification: Notification, rendered: &str) -> Self {
        let (title, message) =
            title_and_message(&notification.template_id, &notification.variables, rendered);
        Self {
            event_type: event_type(&notification.template_id),
            is_read: notification.read_at.is_some(),
            notification,
            title,
            message,
        }
    }
}

/// The template file name without its `.txt.tera` / `.html.tera` suffix.
#[must_use]
pub fn template_stem(template_id: &str) -> &str {
    template_id
        .split_once('.')
        .map_or(template_id, |(stem, _)| stem)
}

/// The text-template counterpart of a template id.
///
/// Email notifications are queued against `*.html.tera`; a notification list
/// row wants the plain-text sibling, not a full HTML document. Ids that are
/// already text are returned unchanged.
#[must_use]
pub fn text_template_id(template_id: &str) -> String {
    match template_id.strip_suffix(".html.tera") {
        Some(stem) => format!("{stem}.txt.tera"),
        None => template_id.to_string(),
    }
}

/// Canonical event name for a template id — the value clients branch on.
///
/// Templates are an implementation detail (three wallet events share
/// `security_alert`, and `trade_matched` renders an `OrderMatched`), so the
/// wire name is the *upstream event* wherever the two differ. Everything else
/// is the `PascalCase` of the template stem.
///
/// `SecurityAlert` deliberately covers `UserWalletLinked` / `UserWalletUnlinked`
/// / `UserWalletPrimaryChanged`; `variables.headline` distinguishes them.
#[must_use]
pub fn event_type(template_id: &str) -> String {
    let stem = template_stem(template_id);
    match stem {
        "trade_matched" => "OrderMatched".to_string(),
        "welcome" => "EmailVerified".to_string(),
        "new_login" => "UserLoggedIn".to_string(),
        "password_reset" => "PasswordResetRequested".to_string(),
        "confirm_wallet" => "WalletLinkRequested".to_string(),
        "verify_email" => "VerificationEmailRequested".to_string(),
        other => pascal_case(other),
    }
}

/// Split rendered text into a display title and body.
///
/// Explicit variables win over the rendered text, because some templates render
/// a machine payload rather than prose — `push_notification.txt.tera` emits the
/// `{title, body}` JSON envelope FCM parses, and dumping that into a list row
/// would show a user raw JSON.
///
/// Title precedence:
/// 1. a `title` or `headline` variable (the shared `security_alert` template
///    names the specific event through `headline`),
/// 2. a first line underlined by `---` / `===` (the heading convention the text
///    templates use),
/// 3. the humanized template stem.
///
/// Message precedence: a `body` variable, else the rendered text with the
/// heading removed — otherwise every list row would repeat its own title.
#[must_use]
pub fn title_and_message(template_id: &str, variables: &Value, rendered: &str) -> (String, String) {
    let (heading, body) = split_heading(rendered.trim());

    let title = variable(variables, &["title", "headline"]).map_or_else(
        || {
            heading.map_or_else(|| humanize(template_stem(template_id)), ToString::to_string)
        },
        ToString::to_string,
    );

    let message = variable(variables, &["body"])
        .map_or_else(|| body.trim().to_string(), ToString::to_string);

    (title, message)
}

/// First of `keys` present as a non-empty string.
fn variable<'a>(variables: &'a Value, keys: &[&str]) -> Option<&'a str> {
    keys.iter()
        .find_map(|key| variables.get(key).and_then(Value::as_str))
        .map(str::trim)
        .filter(|s| !s.is_empty())
}

/// Detect the `heading` + `-----` underline convention, returning the heading
/// (when present) and the remaining body.
fn split_heading(text: &str) -> (Option<&str>, &str) {
    let Some((first, rest)) = text.split_once('\n') else {
        return (None, text);
    };
    let (second, body) = rest.split_once('\n').unwrap_or((rest, ""));

    let underline = second.trim();
    let is_underline = underline.len() >= 3
        && (underline.chars().all(|c| c == '-') || underline.chars().all(|c| c == '='));

    if is_underline {
        (Some(first.trim()), body)
    } else {
        (None, text)
    }
}

/// `order_created` → `OrderCreated`
fn pascal_case(stem: &str) -> String {
    stem.split('_').filter(|w| !w.is_empty()).map(capitalize).collect()
}

/// `order_created` → `Order Created`
fn humanize(stem: &str) -> String {
    stem.split('_')
        .filter(|w| !w.is_empty())
        .map(capitalize)
        .collect::<Vec<_>>()
        .join(" ")
}

fn capitalize(word: &str) -> String {
    let mut chars = word.chars();
    chars.next().map_or_else(String::new, |first| {
        first.to_uppercase().collect::<String>() + chars.as_str()
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn event_type_maps_templates_to_upstream_event_names() {
        assert_eq!(event_type("trade_matched.txt.tera"), "OrderMatched");
        assert_eq!(event_type("welcome.html.tera"), "EmailVerified");
        assert_eq!(event_type("new_login.txt.tera"), "UserLoggedIn");
        // No override: PascalCase of the stem.
        assert_eq!(event_type("order_created.txt.tera"), "OrderCreated");
        assert_eq!(event_type("settlement_processed.txt.tera"), "SettlementProcessed");
        assert_eq!(event_type("security_alert.txt.tera"), "SecurityAlert");
    }

    #[test]
    fn text_template_id_swaps_html_for_text() {
        assert_eq!(text_template_id("welcome.html.tera"), "welcome.txt.tera");
        assert_eq!(
            text_template_id("order_created.txt.tera"),
            "order_created.txt.tera"
        );
    }

    #[test]
    fn underlined_heading_becomes_the_title_and_leaves_the_body() {
        let rendered = "Trade Matched\n-------------\nYou have a new trade match.\n\nRole: buyer";
        let (title, message) = title_and_message("trade_matched.txt.tera", &json!({}), rendered);
        assert_eq!(title, "Trade Matched");
        assert_eq!(message, "You have a new trade match.\n\nRole: buyer");
    }

    #[test]
    fn headline_variable_wins_over_the_humanized_stem() {
        let rendered = "SECURITY ALERT: New Wallet Linked\n\nWallet Address: 0xabc";
        let (title, message) = title_and_message(
            "security_alert.txt.tera",
            &json!({ "headline": "New Wallet Linked" }),
            rendered,
        );
        assert_eq!(title, "New Wallet Linked");
        // No underline convention here, so the body stays whole.
        assert_eq!(message, rendered);
    }

    /// `push_notification.txt.tera` renders the FCM `{title, body}` envelope,
    /// so the rendered text is JSON. A list row must show the prose the caller
    /// supplied, never that payload.
    #[test]
    fn push_envelope_lists_from_its_variables_not_its_rendered_json() {
        let rendered = r#"{"title":"Trade Matched","body":"Your buyer order matched: 10 kWh @ 4.25"}"#;
        let (title, message) = title_and_message(
            "push_notification.txt.tera",
            &json!({ "title": "Trade Matched", "body": "Your buyer order matched: 10 kWh @ 4.25" }),
            rendered,
        );
        assert_eq!(title, "Trade Matched");
        assert_eq!(message, "Your buyer order matched: 10 kWh @ 4.25");
    }

    #[test]
    fn headingless_template_falls_back_to_the_humanized_stem() {
        let rendered = "Order placed: buy 100 kWh at 4.25 per kWh.\n\nOrder ID: ORD1";
        let (title, message) = title_and_message("order_created.txt.tera", &json!({}), rendered);
        assert_eq!(title, "Order Created");
        assert_eq!(message, rendered);
    }
}
