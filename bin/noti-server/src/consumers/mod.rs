//! Background message consumers (`Kafka`, `RabbitMQ`).
//!
//! These live in the server crate because they need both `noti-persistence`
//! (for the consumer clients) and `noti-logic` (for the orchestrator),
//! which would create a circular dependency if placed in `noti-persistence`.

mod events;
mod url;

use std::sync::Arc;

use futures::StreamExt;
use lapin::options::{BasicAckOptions, BasicConsumeOptions, BasicNackOptions};
use lapin::types::FieldTable;
use rdkafka::consumer::{CommitMode, Consumer, StreamConsumer};
use rdkafka::message::{BorrowedMessage, Message};
use serde_json::Value;
use tokio_util::sync::CancellationToken;
use tracing::{error, info, warn};

use noti_logic::NotificationOrchestrator;
use noti_persistence::messaging::rabbitmq::{NotificationTask, RabbitMQClient};

use events::MsgCtx;

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

    let mut event: Value = serde_json::from_str(payload)?;
    let event_type = event
        .get("event_type")
        .and_then(Value::as_str)
        .unwrap_or("unknown")
        .to_string();

    info!(
        "Processing event: {} from topic: {}",
        event_type,
        msg.topic()
    );

    // Take the `data` object out of the envelope; default to an empty object
    // so per-event payload field defaults apply cleanly when it is absent.
    let data = event
        .get_mut("data")
        .map_or_else(|| Value::Object(serde_json::Map::new()), Value::take);

    let ctx = MsgCtx {
        topic: msg.topic().to_string(),
        partition: msg.partition(),
        offset: msg.offset(),
    };

    events::dispatch(orchestrator, frontend_url, &ctx, &event_type, data).await
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
