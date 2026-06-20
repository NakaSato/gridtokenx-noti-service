//! Offline integration test for `FcmProvider.send` against a **local HTTP stub**
//! standing in for both Google's `OAuth2` token endpoint and the FCM HTTP v1
//! `messages:send` API.
//!
//! `fcm_live.rs` proves the same path against the *real* Google API, but it
//! **skips** unless service-account secrets are present — so in normal CI the
//! provider's send path, per-platform envelope, and (most importantly) the
//! **self-healing token revoke** on `UNREGISTERED` never actually execute. The
//! inline unit tests cover only the pure helpers (`build_message`,
//! `is_invalid_token`, `parse_payload`).
//!
//! This closes that gap with **no network and no Docker**: a throwaway RSA
//! service-account key signs a real JWT assertion that the stub's `/token`
//! endpoint accepts, and the stub's `messages:send` route returns `200` for
//! healthy tokens and a `404 UNREGISTERED` for a planted dead one. It drives the
//! real `FcmProvider::send` fan-out across three devices (web / android / ios)
//! and asserts on the wire:
//!   - the `OAuth2` mint round-trip happened (token endpoint hit, bearer used),
//!   - each platform got its correct envelope block (webpush / android / apns),
//!   - the healthy tokens count as delivered, the dead one is **pruned and
//!     revoked in the registry** (self-heal), and
//!   - the overall result is terminal success (≥1 delivery), not a retry.

#![allow(clippy::expect_used, clippy::unwrap_used)]

use std::sync::Arc;

use async_trait::async_trait;
use chrono::Utc;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::sync::Mutex;
use uuid::Uuid;

use noti_core::domain::{DevicePlatform, DeviceToken};
use noti_core::error::Result;
use noti_core::traits::{DeviceTokenRepositoryTrait, NotificationProviderTrait};
use noti_persistence::providers::fcm::FcmProvider;

/// A minimal PKCS#8 RSA private key generated solely for this test. It signs the
/// JWT assertion the stub `/token` endpoint accepts — it is **not** a credential
/// for any real service and grants access to nothing.
const TEST_RSA_PRIVATE_KEY: &str = "-----BEGIN PRIVATE KEY-----\nMIIEvgIBADANBgkqhkiG9w0BAQEFAASCBKgwggSkAgEAAoIBAQDcpW8kTQiUoJSY\ngykivVmKUB6n/cnv9AZDhpAnOtjg2JldACT87AS2x0jZxbNeiQBRpPahKT2UPOnY\noni3y3yw6kV36Hwkxscr+KvUNVHu7XYORcF0ehfYRI0VCLZWWe0V5bsk0Dj82yYW\nvYLOld8KbwfcIFrD3oidWFdj99xdfPI9mSMi+VGOFR5+yePGJZKDNbOS1O1aLzQ0\nnt3KY8E+RpeUlLxvWju019ljUoWp4X7CzTeI5CDkXrPWLZ3ibRPDmZWXfUVTe83W\n3qT4ThY7kEpZhB9ZEvbBUTjWs5XNjawwMidZ+Efc5YxPJQZcmWtSckAi8pzEmH+N\nLLZxZKmjAgMBAAECggEAT3M4ioM8kDwkVaiA8vPono/MAiS2BrPBX6ZAGQgtGQWV\nb7ICH5qi9efbeSMhu+wsE7oJXq25cTvf3nRecJwSqaep3Qv3S8zR4ij4QoDyoEyc\nQnZmuxjNpj/E52qMMZrO7qAa254orxAAbpbN17KKrjidxWtXE4l5euLZEPOqw3Rz\nkMKvgwfYhgzl2ASkYcPdmOBGqxaiGZ+84ZzcKmATNuPimwcNqx8e1m7UNtIJfkj9\no6zHLaC86XMh4nWiFS7ilebrj7SoR5jz+Wyzy7Xcwmux1xJTO5nc2ceAK42kXIhO\nQWa7DXfhrE3p4x7vsY+0qIDdnyKqivazCntMLen9yQKBgQD0j/dX0GjCpayHVCes\n+IIxNeN5wLd96MIn9MaX+/dENxB1jH73wQ/YIZLLh9hOjp0OsMWQvagF7fPovTsV\ndNQrt+Tb6sOBq+tfOfpyhoY9Egi9QqcjWpGnErjNbeGg3zkAQExDxsgbGFjs6Nha\no3pzIY4azFtGhcS0P5HGHfjlSQKBgQDm9yGDDiQ7xM5IPIAt2WDuZrk3uoGSkQtu\nRPqSTg97F5dQ7HP83Incpp770hdNyrsZ3od6Esulx2JxK7WFwYvIXbOdHvs7pgg2\nC+jPf9weTiR6Q8+Lrb1YbfPwxUPBN0nEIFA9QSIt4hO3zVkT4bMWdH287MDmTrIO\nbfTGpR3TiwKBgGCki4+uEdfpdFY+ETevNHOR4gR4/YnJ8v+rINdqgHn6cIyjKoFp\nT4OPMN0xH29buADYJhpeeAlv0NUGAlUmR7nG/69QBFY3w9lrpeaf9mgnukBgGIBG\nCAzHvzOe2myiCXpp7jlSUj0yz+E+2lBnDbp1Zhx86QzjS6oW/NoXegXRAoGBALmp\nayz4jzPkjpYO3FL+7SZ3OOiNal8xbWjk1jAJw/QFEMQib1KSzdersR1o0wbbsu+m\nrGz68u1+i6nBoxe0b/NPL3VcVESswOkBRdKXS5Co7DXEkPANZ6nQKUogqMiG8ytP\ndnDnDNypYYRc9ABBbD7ewby+7Im2NPfYd+2/CWzlAoGBAK5Xm/8KMVMRedZJTEPX\nXbGb6ZioTOnMHPNoTN3PyP+l4Mp7kEG0eBg8yifldC6vk1MXLa8V40cmSwKUK84L\nlYAEIongiTh9yXNqfLIAq5rViZYRUFTlYKwr2Esks5zyDpeUGPjYw9ibFvZmDpnq\nDRmbNXHyveNnaAgPb2zhReeB\n-----END PRIVATE KEY-----\n";

/// Token planted to be rejected by the stub as `UNREGISTERED`, so the provider
/// must self-heal by revoking it.
const DEAD_TOKEN: &str = "dead-token-android";

/// In-memory device registry: hands back a fixed device set and records every
/// `revoke` so the test can assert self-healing.
struct StubRepo {
    devices: Vec<DeviceToken>,
    revoked: Mutex<Vec<String>>,
}

#[async_trait]
impl DeviceTokenRepositoryTrait for StubRepo {
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
        Ok(self.devices.clone())
    }

    async fn revoke(&self, token: &str) -> Result<()> {
        self.revoked.lock().await.push(token.to_string());
        Ok(())
    }
}

fn device(user_id: Uuid, token: &str, platform: DevicePlatform) -> DeviceToken {
    DeviceToken {
        id: Uuid::new_v4(),
        user_id,
        token: token.to_string(),
        platform,
        created_at: Utc::now(),
        last_seen_at: Utc::now(),
    }
}

/// Captured `messages:send` request: the raw JSON body and whether it carried a
/// bearer Authorization header.
#[derive(Clone)]
struct SendCapture {
    body: String,
    had_bearer: bool,
}

/// Shared stub state.
#[derive(Default)]
struct StubState {
    token_hits: usize,
    sends: Vec<SendCapture>,
}

/// Read a full HTTP/1.1 request (headers + `Content-Length` body) from a socket.
async fn read_request(sock: &mut tokio::net::TcpStream) -> Option<(String, String)> {
    let mut buf: Vec<u8> = Vec::with_capacity(4096);
    let mut chunk = [0u8; 4096];

    // Read until we have the full header block.
    let header_end = loop {
        if let Some(pos) = find_subslice(&buf, b"\r\n\r\n") {
            break pos + 4;
        }
        let n = sock.read(&mut chunk).await.ok()?;
        if n == 0 {
            return None;
        }
        buf.extend_from_slice(&chunk[..n]);
    };

    let head = String::from_utf8_lossy(&buf[..header_end]).into_owned();
    let content_length = head
        .lines()
        .find_map(|l| l.strip_prefix("Content-Length:").or_else(|| l.strip_prefix("content-length:")))
        .and_then(|v| v.trim().parse::<usize>().ok())
        .unwrap_or(0);

    // Read the remaining body bytes.
    while buf.len() < header_end + content_length {
        let n = sock.read(&mut chunk).await.ok()?;
        if n == 0 {
            break;
        }
        buf.extend_from_slice(&chunk[..n]);
    }

    let body = String::from_utf8_lossy(&buf[header_end..]).into_owned();
    Some((head, body))
}

fn find_subslice(haystack: &[u8], needle: &[u8]) -> Option<usize> {
    haystack.windows(needle.len()).position(|w| w == needle)
}

/// Spawn the stub server. Returns its base URL (`http://127.0.0.1:<port>`) and a
/// handle to the shared captured state.
async fn spawn_stub() -> (String, Arc<Mutex<StubState>>) {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind stub server");
    let port = listener.local_addr().expect("local addr").port();
    let state = Arc::new(Mutex::new(StubState::default()));

    let srv_state = state.clone();
    tokio::spawn(async move {
        loop {
            let Ok((mut sock, _)) = listener.accept().await else {
                break;
            };
            let st = srv_state.clone();
            tokio::spawn(async move {
                let Some((head, body)) = read_request(&mut sock).await else {
                    return;
                };
                let request_line = head.lines().next().unwrap_or_default();

                let response: String = if request_line.contains("/token") {
                    st.lock().await.token_hits += 1;
                    let json = r#"{"access_token":"stub-access-token","expires_in":3600}"#;
                    http_response(200, "application/json", json)
                } else if request_line.contains("messages:send") {
                    let had_bearer = head
                        .to_lowercase()
                        .contains("authorization: bearer stub-access-token");
                    st.lock().await.sends.push(SendCapture {
                        body: body.clone(),
                        had_bearer,
                    });
                    if body.contains(DEAD_TOKEN) {
                        // Planted dead token → FCM v1 shape the provider prunes on.
                        let err = r#"{"error":{"code":404,"status":"NOT_FOUND","details":[{"@type":"type.googleapis.com/google.firebase.fcm.v1.FcmError","errorCode":"UNREGISTERED"}]}}"#;
                        http_response(404, "application/json", err)
                    } else {
                        http_response(200, "application/json", r#"{"name":"projects/p/messages/1"}"#)
                    }
                } else {
                    http_response(404, "text/plain", "no route")
                };

                let _ = sock.write_all(response.as_bytes()).await;
                let _ = sock.flush().await;
            });
        }
    });

    (format!("http://127.0.0.1:{port}"), state)
}

fn http_response(status: u16, content_type: &str, body: &str) -> String {
    let reason = match status {
        200 => "OK",
        404 => "Not Found",
        _ => "Error",
    };
    format!(
        "HTTP/1.1 {status} {reason}\r\nContent-Type: {content_type}\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
        body.len()
    )
}

/// Write a throwaway service-account JSON whose `token_uri` points at the stub.
/// Returns the file path (caller best-effort removes it).
fn write_service_account(token_uri: &str) -> String {
    let path = std::env::temp_dir().join(format!(
        "fcm-stub-sa-{}-{}.json",
        std::process::id(),
        Uuid::new_v4()
    ));
    // private_key embeds real newlines via the \n escapes in the constant.
    let json = serde_json::json!({
        "type": "service_account",
        "project_id": "stub-project",
        "private_key_id": "stub-key-id",
        "private_key": TEST_RSA_PRIVATE_KEY,
        "client_email": "stub@stub-project.iam.gserviceaccount.com",
        "token_uri": token_uri,
    });
    std::fs::write(&path, serde_json::to_vec_pretty(&json).expect("serialize SA"))
        .expect("write service-account file");
    path.to_string_lossy().into_owned()
}

#[tokio::test]
async fn fcm_send_fans_out_and_self_heals_against_stub() {
    let (base_url, state) = spawn_stub().await;

    let user_id = Uuid::new_v4();
    let devices = vec![
        device(user_id, "good-token-web", DevicePlatform::Web),
        device(user_id, DEAD_TOKEN, DevicePlatform::Android),
        device(user_id, "good-token-ios", DevicePlatform::Ios),
    ];
    let repo = Arc::new(StubRepo {
        devices,
        revoked: Mutex::new(Vec::new()),
    });

    let sa_path = write_service_account(&format!("{base_url}/token"));
    let provider = FcmProvider::from_credentials_file(
        "stub-project".to_string(),
        &sa_path,
        repo.clone() as Arc<dyn DeviceTokenRepositoryTrait>,
    )
    .expect("build FcmProvider from stub credentials")
    .with_messaging_base_url(base_url.clone());

    let content = r#"{"title":"Trade Matched","body":"Your buyer order matched: 5 kWh @ 10"}"#;
    let result = provider
        .send(&user_id.to_string(), content)
        .await
        .expect("FCM send against stub should succeed (≥1 delivered)");

    // 2 healthy delivered, 1 dead pruned → terminal success, not a retry.
    assert_eq!(
        result, "fcm-delivered-2-pruned-1",
        "unexpected delivery summary: {result}"
    );

    // Self-heal: exactly the dead token was revoked in the registry.
    let revoked = repo.revoked.lock().await;
    assert_eq!(
        &*revoked,
        &[DEAD_TOKEN.to_string()],
        "self-heal should revoke exactly the dead token"
    );
    drop(revoked);

    let st = state.lock().await;
    // OAuth2 mint happened once (token cached for the three sends).
    assert_eq!(st.token_hits, 1, "expected exactly one token mint");
    assert_eq!(st.sends.len(), 3, "expected one send per device");
    assert!(
        st.sends.iter().all(|s| s.had_bearer),
        "every send must carry the minted bearer token"
    );

    // Per-platform envelope: each device's send body has its platform block.
    let bodies: Vec<&str> = st.sends.iter().map(|s| s.body.as_str()).collect();
    assert!(
        bodies.iter().any(|b| b.contains("\"webpush\"") && b.contains("good-token-web")),
        "web send missing webpush block"
    );
    assert!(
        bodies.iter().any(|b| b.contains("\"android\"") && b.contains(DEAD_TOKEN)),
        "android send missing android block"
    );
    assert!(
        bodies.iter().any(|b| b.contains("\"apns\"") && b.contains("good-token-ios")),
        "ios send missing apns block"
    );
    // Rendered notification content reached the wire.
    assert!(
        bodies.iter().all(|b| b.contains("Trade Matched")),
        "notification title missing from send body"
    );

    let _ = std::fs::remove_file(&sa_path);
}
