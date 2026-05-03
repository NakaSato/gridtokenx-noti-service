//! Background message consumers (Kafka, RabbitMQ).
//!
//! These live in the server crate because they need both `noti-persistence`
//! (for the consumer clients) and `noti-logic` (for the orchestrator),
//! which would create a circular dependency if placed in `noti-persistence`.

use std::sync::Arc;

use futures::StreamExt;
use lapin::options::*;
use lapin::types::FieldTable;
use rdkafka::consumer::{Consumer, StreamConsumer};
use rdkafka::message::{BorrowedMessage, Message};
use serde_json::Value;
use tokio_util::sync::CancellationToken;
use tracing::{error, info, warn};
use uuid::Uuid;

use noti_core::domain::NotificationChannel;
use noti_logic::NotificationOrchestrator;
use noti_persistence::messaging::rabbitmq::{NotificationTask, RabbitMQClient};

// ---------------------------------------------------------------------------
// Kafka consumer
// ---------------------------------------------------------------------------

pub async fn start_kafka_consumer(
    consumer: StreamConsumer,
    topics: Vec<String>,
    orchestrator: Arc<NotificationOrchestrator>,
    token: CancellationToken,
) -> anyhow::Result<()> {
    let topics_ref: Vec<&str> = topics.iter().map(String::as_str).collect();
    consumer.subscribe(&topics_ref)?;

    info!("🚀 Kafka consumer started for topics: {:?}", topics);

    loop {
        tokio::select! {
            _ = token.cancelled() => {
                info!("🛑 Kafka consumer shutting down...");
                break;
            }
            result = consumer.recv() => {
                match result {
                    Ok(msg) => {
                        if let Err(e) = handle_kafka_message(&orchestrator, &msg).await {
                            error!("Failed to handle Kafka message: {}", e);
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

async fn handle_kafka_message(
    orchestrator: &Arc<NotificationOrchestrator>,
    msg: &BorrowedMessage<'_>,
) -> anyhow::Result<()> {
    let payload = match msg.payload_view::<str>() {
        Some(Ok(s)) => s,
        Some(Err(e)) => return Err(anyhow::anyhow!("Invalid UTF-8 payload: {}", e)),
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
                        "welcome.txt.tera".to_string(),
                        serde_json::json!({ "name": username }),
                        Some(format!("kafka:{}:{}", msg.topic(), msg.offset())),
                    )
                    .await
                    .map_err(|e| anyhow::anyhow!(e))?;
            }
        }
        "OrderMatched" => {
            let buyer_id = event["data"]["buyer_id"].as_str().and_then(|id| Uuid::parse_str(id).ok());
            let seller_id = event["data"]["seller_id"].as_str().and_then(|id| Uuid::parse_str(id).ok());
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
                        Some(format!("kafka:matched:buy:{}", msg.offset())),
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
                        Some(format!("kafka:matched:sell:{}", msg.offset())),
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
            let user_id = event["data"]["user_id"].as_str().and_then(|id| Uuid::parse_str(id).ok());
            let amount = &event["data"]["energy_amount"];

            if let Some(uid) = user_id {
                orchestrator
                    .queue_notification(
                        Some(uid),
                        NotificationChannel::Email, // Certificates often go to email
                        "user@example.com".to_string(), // In reality, we'd look up the user's email
                        "erc_issued.txt.tera".to_string(),
                        serde_json::json!({ "amount": amount }),
                        Some(format!("kafka:erc:{}", msg.offset())),
                    )
                    .await
                    .map_err(|e| anyhow::anyhow!(e))?;
            }
        }
        "PasswordResetRequested" => {
            let user_id = event["data"]["user_id"].as_str().and_then(|id| Uuid::parse_str(id).ok());
            let email = event["data"]["email"].as_str().unwrap_or_default();
            let reset_url = event["data"]["reset_url"].as_str().unwrap_or_default();

            if !email.is_empty() {
                orchestrator
                    .queue_notification(
                        user_id,
                        NotificationChannel::Email,
                        email.to_string(),
                        "password_reset.txt.tera".to_string(),
                        serde_json::json!({ "reset_url": reset_url }),
                        Some(format!("kafka:pwd_reset:{}", msg.offset())),
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
