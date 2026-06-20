//! Live integration test for `SmtpProvider` against a real SMTP server
//! (closes gap #2).
//!
//! The inline unit tests in `providers::smtp` cover the Subject-injection
//! guards (`looks_like_html` / `extract_subject`) with no network. They never
//! exercise the part that only fails in production: actually building a
//! `lettre` transport, connecting, and delivering a message — including the
//! HTML→multipart split and the `<title>`→Subject derivation on the wire.
//!
//! This boots a [Mailpit](https://mailpit.axllent.org/) container (SMTP on
//! 1025, REST API on 8025), drives the *real* `SmtpProvider` in `none` TLS mode
//! against it, then reads the captured messages back over Mailpit's HTTP API to
//! assert each one arrived with the expected recipient and subject.
//!
//! Requires a Docker/OrbStack daemon — panics (does not skip) without one.

#![allow(clippy::expect_used, clippy::unwrap_used)]

use std::time::Duration;

use serde_json::Value;

use noti_core::traits::NotificationProviderTrait;
use noti_persistence::providers::smtp::SmtpProvider;

use testcontainers_modules::testcontainers::runners::AsyncRunner;
use testcontainers_modules::testcontainers::{ContainerAsync, GenericImage};

const FROM: &str = "no-reply@gridtokenx.test";

struct Mailpit {
    _container: ContainerAsync<GenericImage>,
    smtp_host: String,
    smtp_port: u16,
    api_base: String,
}

/// Boot Mailpit and wait until its REST API answers, so the assertions below
/// can read delivered mail back.
async fn start_mailpit() -> Mailpit {
    // No log-message wait: Mailpit's startup banner wording/stream varies across
    // versions and a wrong match wedges for the full startup timeout. Start as
    // soon as the container is running, then gate on the REST API poll below.
    let container = GenericImage::new("axllent/mailpit", "latest")
        .start()
        .await
        .expect("start mailpit container");

    let host = container.get_host().await.expect("mailpit host").to_string();
    let smtp_port = container
        .get_host_port_ipv4(1025)
        .await
        .expect("mailpit smtp port");
    let api_port = container
        .get_host_port_ipv4(8025)
        .await
        .expect("mailpit api port");
    let api_base = format!("http://{host}:{api_port}");

    // Belt-and-suspenders: poll the API until it serves, regardless of the exact
    // startup log wording.
    let client = reqwest::Client::new();
    let ready = tokio::time::timeout(Duration::from_secs(30), async {
        loop {
            if client
                .get(format!("{api_base}/api/v1/messages"))
                .send()
                .await
                .is_ok_and(|r| r.status().is_success())
            {
                break;
            }
            tokio::time::sleep(Duration::from_millis(200)).await;
        }
    })
    .await;
    ready.expect("mailpit API never became ready within 30s");

    Mailpit {
        _container: container,
        smtp_host: host,
        smtp_port,
        api_base,
    }
}

/// Fetch all messages currently captured by Mailpit.
async fn fetch_messages(api_base: &str) -> Vec<Value> {
    let raw = reqwest::Client::new()
        .get(format!("{api_base}/api/v1/messages"))
        .send()
        .await
        .expect("list messages")
        .text()
        .await
        .expect("read messages body");
    let body: Value = serde_json::from_str(&raw).expect("parse messages json");

    body["messages"].as_array().cloned().unwrap_or_default()
}

#[tokio::test]
async fn smtp_provider_delivers_html_and_plain_messages() {
    let mp = start_mailpit().await;

    // Real provider, no TLS (Mailpit speaks plain SMTP on 1025).
    let provider = SmtpProvider::new(
        &mp.smtp_host,
        mp.smtp_port,
        None,
        None,
        FROM.to_string(),
        Some("none"),
    )
    .expect("build SmtpProvider");

    // 1. An HTML message: the subject must be derived from the `<title>` and the
    //    body delivered as multipart/alternative.
    let html = "<!DOCTYPE html><html><head><title>Live SMTP Subject</title></head>\
                <body><h1>Hello</h1><p>Live body</p></body></html>";
    provider
        .send("html-recipient@example.com", html)
        .await
        .expect("send html email");

    // 2. A plain-text message: classified non-HTML, so it ships with the fixed
    //    default subject (and its content is never parsed for a `<title>`).
    provider
        .send("plain-recipient@example.com", "Your code is 123456")
        .await
        .expect("send plain email");

    // Read both back over the API (delivery is synchronous on `send().await`,
    // but allow a short settle for Mailpit to index them).
    let messages = tokio::time::timeout(Duration::from_secs(10), async {
        loop {
            let msgs = fetch_messages(&mp.api_base).await;
            if msgs.len() >= 2 {
                break msgs;
            }
            tokio::time::sleep(Duration::from_millis(200)).await;
        }
    })
    .await
    .expect("both messages never appeared in Mailpit within 10s");

    let find = |to: &str| -> Value {
        messages
            .iter()
            .find(|m| {
                m["To"]
                    .as_array()
                    .is_some_and(|tos| tos.iter().any(|t| t["Address"] == to))
            })
            .unwrap_or_else(|| panic!("no message delivered to {to}"))
            .clone()
    };

    let html_msg = find("html-recipient@example.com");
    assert_eq!(
        html_msg["Subject"], "Live SMTP Subject",
        "HTML subject not derived from <title>"
    );
    assert_eq!(html_msg["From"]["Address"], FROM, "From address mismatch");

    let plain_msg = find("plain-recipient@example.com");
    assert_eq!(
        plain_msg["Subject"], "GridTokenX Notification",
        "plain-text message must use the fixed default subject"
    );
}
