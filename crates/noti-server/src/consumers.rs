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
        "TradeSettled" => {
            // Handle trade settled notification
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
