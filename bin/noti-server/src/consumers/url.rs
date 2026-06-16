//! URL helpers for rewriting upstream callback links to the configured
//! frontend base URL.

use std::fmt::Write as _;

/// Construct a URL using the configured frontend base URL.
/// Returns an empty string if `frontend_url` is not configured.
pub fn build_callback_url(frontend_url: Option<&str>, path: &str, query: &str) -> String {
    match frontend_url {
        Some(base) => {
            let base = base.trim_end_matches('/');
            let path = if path.starts_with('/') {
                path.to_string()
            } else {
                format!("/{path}")
            };
            if query.is_empty() {
                format!("{base}{path}")
            } else {
                format!("{base}{path}?{query}")
            }
        }
        None => String::new(),
    }
}

/// Percent-encode a string for safe use inside a URL query value.
/// Unreserved characters (RFC 3986 §2.3) pass through; everything else,
/// including `@` and `+` common in email addresses, is escaped.
pub fn urlencode(s: &str) -> String {
    let mut out = String::with_capacity(s.len());
    for byte in s.bytes() {
        match byte {
            b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'-' | b'.' | b'_' | b'~' => {
                out.push(byte as char);
            }
            // Writing to a String is infallible; the Result is intentionally
            // discarded (clippy::format_push_string — avoids a temp allocation).
            _ => {
                let _ = write!(out, "%{byte:02X}");
            }
        }
    }
    out
}

/// Rewrite the base URL (scheme + host + port) of `upstream_url` to use
/// `frontend_url` when configured. Preserves the original path and query string.
///
/// Example: `http://localhost:4001/verify?token=abc` with
///          `frontend_url = "https://trading-ui.gridtokenx-coresystem.orb.local/"`
///       → `https://trading-ui.gridtokenx-coresystem.orb.local/verify?token=abc`
pub fn rewrite_url(frontend_url: Option<&str>, upstream_url: &str) -> String {
    match frontend_url {
        Some(base) if !upstream_url.is_empty() => {
            let base = base.trim_end_matches('/');
            // Extract everything after the host: port → path?query
            let rest = upstream_url
                .find("://")
                .and_then(|scheme_end| {
                    upstream_url[scheme_end + 3..]
                        .find('/')
                        .map(|slash_pos| &upstream_url[scheme_end + 3 + slash_pos..])
                })
                .unwrap_or(upstream_url);
            format!("{base}{rest}")
        }
        _ => upstream_url.to_string(),
    }
}

#[cfg(test)]
mod tests {
    use super::{build_callback_url, rewrite_url, urlencode};

    #[test]
    fn urlencode_escapes_email_characters() {
        assert_eq!(urlencode("a+b@grx.test"), "a%2Bb%40grx.test");
        assert_eq!(urlencode("plain-user_1.x~y"), "plain-user_1.x~y");
    }

    #[test]
    fn build_callback_url_inserts_missing_leading_slash() {
        let url = build_callback_url(
            Some("https://app.gridtokenx.test/"),
            "reset-password",
            "token=abc",
        );

        assert_eq!(url, "https://app.gridtokenx.test/reset-password?token=abc");
    }

    #[test]
    fn rewrite_url_preserves_path_and_query() {
        let url = rewrite_url(
            Some("https://app.gridtokenx.test/"),
            "http://localhost:4001/verify-email?token=abc",
        );

        assert_eq!(url, "https://app.gridtokenx.test/verify-email?token=abc");
    }
}
