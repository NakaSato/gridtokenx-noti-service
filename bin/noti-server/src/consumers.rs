//! Background message consumers (`Kafka`, `RabbitMQ`).
//!
//! These live in the server crate because they need both `noti-persistence`
//! (for the consumer clients) and `noti-logic` (for the orchestrator),
//! which would create a circular dependency if placed in `noti-persistence`.

use std::sync::Arc;

use futures::StreamExt;
use lapin::options::{BasicAckOptions, BasicConsumeOptions, BasicNackOptions};
use lapin::types::FieldTable;
use rdkafka::consumer::{CommitMode, Consumer, StreamConsumer};
use rdkafka::message::{BorrowedMessage, Message};
use serde_json::Value;
use tokio_util::sync::CancellationToken;
use tracing::{error, info, warn};
use uuid::Uuid;

use noti_core::domain::NotificationChannel;
use noti_logic::NotificationOrchestrator;
use noti_persistence::messaging::rabbitmq::{NotificationTask, RabbitMQClient};

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

/// Construct a URL using the configured frontend base URL.
/// Returns an empty string if `frontend_url` is not configured.
fn build_callback_url(frontend_url: Option<&str>, path: &str, query: &str) -> String {
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
fn rewrite_url(frontend_url: Option<&str>, upstream_url: &str) -> String {
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

// ---------------------------------------------------------------------------
// Kafka consumer
// ---------------------------------------------------------------------------

/// # Errors
///
/// Returns an error if Kafka subscription fails.
pub async fn start_kafka_consumer(
    consumer: StreamConsumer,
    topics: Vec<String>,
    orchestrator: Arc<NotificationOrchestrator>,
    frontend_url: Option<String>,
    token: CancellationToken,
) -> anyhow::Result<()> {
    let topics_ref: Vec<&str> = topics.iter().map(String::as_str).collect();
    consumer.subscribe(&topics_ref)?;

    info!("🚀 Kafka consumer started for topics: {:?}", topics);

    loop {
        tokio::select! {
            () = token.cancelled() => {
                info!("🛑 Kafka consumer shutting down...");
                break;
            }
            result = consumer.recv() => {
                match result {
                    Ok(msg) => {
                        match handle_kafka_message(&orchestrator, frontend_url.as_deref(), &msg).await {
                            Ok(()) => {
                                if let Err(e) = consumer.commit_message(&msg, CommitMode::Async) {
                                    error!("Failed to commit Kafka message: {}", e);
                                }
                            }
                            Err(e) => {
                                error!("Failed to handle Kafka message: {}", e);
                            }
                        }
                    }
                    Err(e) => {
                        error!("Kafka receive error: {}", e);
                    }
                }
            }
        }
    }

    Ok(())
}

#[allow(clippy::too_many_lines)]
async fn handle_kafka_message(
    orchestrator: &Arc<NotificationOrchestrator>,
    frontend_url: Option<&str>,
    msg: &BorrowedMessage<'_>,
) -> anyhow::Result<()> {
    let payload = match msg.payload_view::<str>() {
        Some(Ok(s)) => s,
        Some(Err(e)) => return Err(anyhow::anyhow!("Invalid UTF-8 payload: {e}")),
        None => return Ok(()),
    };

    let event: Value = serde_json::from_str(payload)?;
    let event_type = event
        .get("event_type")
        .and_then(|v| v.as_str())
        .unwrap_or("unknown");

    info!(
        "Processing event: {} from topic: {}",
        event_type,
        msg.topic()
    );

    match event_type {
        "UserRegistered" => {
            let email = event["data"]["email"].as_str().unwrap_or_default();
            let username = event["data"]["username"].as_str().unwrap_or_default();

            if !email.is_empty() {
                orchestrator
                    .queue_notification(
                        None,
                        NotificationChannel::Email,
                        email.to_string(),
                        "welcome.html.tera".to_string(),
                        serde_json::json!({ "name": username }),
                        Some(format!(
                            "kafka:{}:{}:{}",
                            msg.topic(),
                            msg.partition(),
                            msg.offset()
                        )),
                    )
                    .await
                    .map_err(|e| anyhow::anyhow!(e))?;
            }
        }
        "OrderMatched" => {
            let buyer_id = event["data"]["buyer_id"]
                .as_str()
                .and_then(|id| Uuid::parse_str(id).ok());
            let seller_id = event["data"]["seller_id"]
                .as_str()
                .and_then(|id| Uuid::parse_str(id).ok());
            let amount = &event["data"]["amount"];
            let price = &event["data"]["price"];

            // Notify buyer
            if let Some(uid) = buyer_id {
                orchestrator
                    .queue_notification(
                        Some(uid),
                        NotificationChannel::WebSocket,
                        uid.to_string(),
                        "trade_matched.txt.tera".to_string(),
                        serde_json::json!({ "role": "buyer", "amount": amount, "price": price }),
                        Some(format!(
                            "kafka:matched:buy:{}:{}",
                            msg.partition(),
                            msg.offset()
                        )),
                    )
                    .await
                    .map_err(|e| anyhow::anyhow!(e))?;
            }

            // Notify seller
            if let Some(uid) = seller_id {
                orchestrator
                    .queue_notification(
                        Some(uid),
                        NotificationChannel::WebSocket,
                        uid.to_string(),
                        "trade_matched.txt.tera".to_string(),
                        serde_json::json!({ "role": "seller", "amount": amount, "price": price }),
                        Some(format!(
                            "kafka:matched:sell:{}:{}",
                            msg.partition(),
                            msg.offset()
                        )),
                    )
                    .await
                    .map_err(|e| anyhow::anyhow!(e))?;
            }
        }
        "SettlementProcessed" => {
            let status = event["data"]["status"].as_str().unwrap_or("unknown");
            let tx_sig = event["data"]["tx_signature"].as_str().unwrap_or("");

            // This usually goes to the involved parties, but we need their user_ids.
            // For now, logged as a system notification or specific user message if available.
            info!("Settlement {} processed with status: {}", tx_sig, status);
        }
        "ErcIssued" => {
            let user_id = event["data"]["user_id"]
                .as_str()
                .and_then(|id| Uuid::parse_str(id).ok());
            let email = event["data"]["email"]
                .as_str()
                .or_else(|| event["data"]["recipient_email"].as_str())
                .unwrap_or_default();
            let amount = &event["data"]["energy_amount"];

            if email.is_empty() {
                warn!("Skipping ErcIssued notification: missing recipient email");
            } else {
                orchestrator
                    .queue_notification(
                        user_id,
                        NotificationChannel::Email,
                        email.to_string(),
                        "erc_issued.html.tera".to_string(),
                        serde_json::json!({ "amount": amount }),
                        Some(format!("kafka:erc:{}:{}", msg.partition(), msg.offset())),
                    )
                    .await
                    .map_err(|e| anyhow::anyhow!(e))?;
            }
        }
        "VppDispatched" => {
            let cluster_id = event["data"]["cluster_id"].as_str().unwrap_or_default();
            let target_kw = event["data"]["target_kw"].as_f64().unwrap_or(0.0);
            let members_count = event["data"]["members_commanded"].as_u64().unwrap_or(0);

            // System-wide notification (e.g. to a dashboard or admin email)
            // In a real system, we might notify all involved prosumers.
            info!(
                "📢 VPP Dispatch Event: cluster={}, target={}kW, members={}",
                cluster_id, target_kw, members_count
            );

            let recipient_user_id = event["data"]["admin_user_id"]
                .as_str()
                .or_else(|| event["data"]["user_id"].as_str())
                .and_then(|id| Uuid::parse_str(id).ok());

            if let Some(uid) = recipient_user_id {
                orchestrator
                    .queue_notification(
                        Some(uid),
                        NotificationChannel::WebSocket,
                        uid.to_string(),
                        "vpp_dispatched.txt.tera".to_string(),
                        serde_json::json!({
                            "cluster_id": cluster_id,
                            "target_kw": target_kw,
                            "members_count": members_count
                        }),
                        Some(format!(
                            "kafka:vpp_dispatch:{}:{}",
                            msg.partition(),
                            msg.offset()
                        )),
                    )
                    .await
                    .map_err(|e| anyhow::anyhow!(e))?;
            } else {
                warn!("Skipping VppDispatched WebSocket notification: missing UUID recipient");
            }
        }
        "PasswordResetRequested" => {
            let user_id = event["data"]["user_id"]
                .as_str()
                .and_then(|id| Uuid::parse_str(id).ok());
            let email = event["data"]["email"].as_str().unwrap_or_default();
            let reset_url = event["data"]["reset_url"].as_str().unwrap_or_default();
            let token_value = event["data"]["token"].as_str().unwrap_or_default();

            // Rewrite upstream base URL to frontend_url when configured;
            // fall back to constructing from token when upstream provides nothing
            let reset_url = if !reset_url.is_empty() {
                rewrite_url(frontend_url, reset_url)
            } else if !token_value.is_empty() {
                build_callback_url(
                    frontend_url,
                    "reset-password",
                    &format!("token={token_value}"),
                )
            } else {
                reset_url.to_string()
            };

            if !email.is_empty() {
                orchestrator
                    .queue_notification(
                        user_id,
                        NotificationChannel::Email,
                        email.to_string(),
                        "password_reset.html.tera".to_string(),
                        serde_json::json!({ "reset_url": reset_url }),
                        Some(format!(
                            "kafka:pwd_reset:{}:{}",
                            msg.partition(),
                            msg.offset()
                        )),
                    )
                    .await
                    .map_err(|e| anyhow::anyhow!(e))?;
            }
        }
        "VerificationEmailRequested" => {
            let email = event["data"]["email"].as_str().unwrap_or_default();
            let username = event["data"]["username"].as_str().unwrap_or_default();
            let verification_url = event["data"]["verification_url"]
                .as_str()
                .unwrap_or_default();
            let token_value = event["data"]["token"].as_str().unwrap_or_default();

            // Rewrite upstream base URL to frontend_url when configured;
            // fall back to constructing from token when upstream provides nothing
            let verification_url = if !verification_url.is_empty() {
                rewrite_url(frontend_url, verification_url)
            } else if !token_value.is_empty() {
                build_callback_url(
                    frontend_url,
                    "verify-email",
                    &format!("token={token_value}&email={email}"),
                )
            } else {
                verification_url.to_string()
            };

            if !email.is_empty() {
                orchestrator
                    .queue_notification(
                        None,
                        NotificationChannel::Email,
                        email.to_string(),
                        "verify_email.html.tera".to_string(),
                        serde_json::json!({ "name": username, "verification_url": verification_url }),
                        Some(format!("kafka:verify_email:{}:{}", msg.partition(), msg.offset())),
                    )
                    .await
                    .map_err(|e| anyhow::anyhow!(e))?;
            }
        }
        "UserOnboarded" => {
            let user_id = event["data"]["user_id"]
                .as_str()
                .and_then(|id| Uuid::parse_str(id).ok());
            let pda = event["data"]["user_account_pda"]
                .as_str()
                .unwrap_or_default();
            let tx_sig = event["data"]["transaction_signature"]
                .as_str()
                .unwrap_or_default();

            if let Some(uid) = user_id {
                orchestrator
                    .queue_notification(
                        Some(uid),
                        NotificationChannel::WebSocket,
                        uid.to_string(),
                        "user_onboarded.txt.tera".to_string(),
                        serde_json::json!({ "user_account_pda": pda, "transaction_signature": tx_sig }),
                        Some(format!("kafka:onboard:{}:{}", msg.partition(), msg.offset())),
                    )
                    .await
                    .map_err(|e| anyhow::anyhow!(e))?;
            }
        }
        "MeterOnboarded" => {
            let user_id = event["data"]["user_id"]
                .as_str()
                .and_then(|id| Uuid::parse_str(id).ok());
            let meter_id = event["data"]["meter_id"].as_str().unwrap_or_default();
            let meter_type = event["data"]["meter_type"].as_str().unwrap_or_default();
            let tx_sig = event["data"]["transaction_signature"]
                .as_str()
                .unwrap_or_default();

            if let Some(uid) = user_id {
                orchestrator
                    .queue_notification(
                        Some(uid),
                        NotificationChannel::WebSocket,
                        uid.to_string(),
                        "meter_onboarded.txt.tera".to_string(),
                        serde_json::json!({
                            "meter_id": meter_id,
                            "meter_type": meter_type,
                            "transaction_signature": tx_sig
                        }),
                        Some(format!("kafka:meter:{}:{}", msg.partition(), msg.offset())),
                    )
                    .await
                    .map_err(|e| anyhow::anyhow!(e))?;
            }
        }
        "UserWalletLinked" => {
            let user_id = event["data"]["user_id"]
                .as_str()
                .and_then(|id| Uuid::parse_str(id).ok());
            let wallet_address = event["data"]["wallet_address"].as_str().unwrap_or_default();
            let shard_id = event["data"]["shard_id"].as_u64().unwrap_or(0);
            let tx_sig = event["data"]["transaction_signature"]
                .as_str()
                .unwrap_or_default();

            if let Some(uid) = user_id {
                orchestrator
                    .queue_notification(
                        Some(uid),
                        NotificationChannel::WebSocket,
                        uid.to_string(),
                        "security_alert.txt.tera".to_string(),
                        serde_json::json!({
                            "wallet_address": wallet_address,
                            "shard_id": shard_id,
                            "transaction_signature": tx_sig
                        }),
                        Some(format!(
                            "kafka:wallet_link:{}:{}",
                            msg.partition(),
                            msg.offset()
                        )),
                    )
                    .await
                    .map_err(|e| anyhow::anyhow!(e))?;
            }
        }
        _ => {
            warn!("Unhandled event type: {}", event_type);
        }
    }

    Ok(())
}

// ---------------------------------------------------------------------------
// RabbitMQ consumer
// ---------------------------------------------------------------------------

/// # Errors
///
/// Returns an error if the `RabbitMQ` consumer fails to start or encounters
/// a fatal delivery error.
pub async fn start_rabbitmq_consumer(
    mq_client: Arc<RabbitMQClient>,
    orchestrator: Arc<NotificationOrchestrator>,
    token: CancellationToken,
) -> anyhow::Result<()> {
    let mut consumer = mq_client
        .channel()
        .basic_consume(
            "noti.dispatch",
            "noti_service_consumer",
            BasicConsumeOptions::default(),
            FieldTable::default(),
        )
        .await?;

    info!("🚀 RabbitMQ consumer started");

    while let Some(delivery_res) = consumer.next().await {
        if token.is_cancelled() {
            break;
        }

        match delivery_res {
            Ok(delivery) => {
                let task: NotificationTask = match serde_json::from_slice(&delivery.data) {
                    Ok(t) => t,
                    Err(e) => {
                        error!("Failed to deserialize notification task: {}", e);
                        delivery.ack(BasicAckOptions::default()).await?;
                        continue;
                    }
                };

                info!("📥 Received task for notification {}", task.notification_id);

                match orchestrator.clone().dispatch(task.notification_id).await {
                    Ok(()) => {
                        delivery.ack(BasicAckOptions::default()).await?;
                    }
                    Err(e) => {
                        warn!(
                            "Failed to dispatch notification {}: {}",
                            task.notification_id, e
                        );
                        delivery
                            .nack(BasicNackOptions {
                                requeue: false,
                                multiple: false,
                            })
                            .await?;
                    }
                }
            }
            Err(e) => {
                error!("RabbitMQ consumer delivery error: {}", e);
            }
        }
    }

    Ok(())
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
