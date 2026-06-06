//! URL helpers for rewriting upstream callback links to the configured
//! frontend base URL.

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
    use super::{build_callback_url, rewrite_url};

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
