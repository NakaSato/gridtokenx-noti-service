//! Background message consumers (`Kafka`, `RabbitMQ`).
//!
//! These live in the server crate because they need both `noti-persistence`
//! (for the consumer clients) and `noti-logic` (for the orchestrator),
//! which would create a circular dependency if placed in `noti-persistence`.

mod events;
mod url;

use std::sync::Arc;
use std::time::Duration;

use futures::StreamExt;
use lapin::options::{BasicAckOptions, BasicConsumeOptions, BasicNackOptions};
use lapin::types::FieldTable;
use rdkafka::Offset;
use rdkafka::consumer::{CommitMode, Consumer, StreamConsumer};
use rdkafka::error::KafkaError;
use rdkafka::message::{BorrowedMessage, Message};
use rdkafka::topic_partition_list::TopicPartitionList;
use rdkafka::types::RDKafkaErrorCode;
use serde_json::Value;
use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;
use tracing::{error, info, warn};

use noti_core::health::{KafkaConsumerHealth, unix_now_secs};
use noti_logic::NotificationOrchestrator;
use noti_persistence::messaging::kafka;
use noti_persistence::messaging::rabbitmq::{NotificationTask, RabbitMQClient};

use events::MsgCtx;

// ---------------------------------------------------------------------------
// Kafka consumer
// ---------------------------------------------------------------------------

/// Hand-off buffer between the poll loop and the processing worker. Sized to
/// absorb a transient downstream stall (a `RabbitMQ` heartbeat blip lasting
/// seconds) without blocking `recv()`. A sustained outage eventually fills it
/// and backpressures the poll loop; the supervisor + readiness probe then
/// surface that rather than the consumer silently wedging.
const CHANNEL_CAP: usize = 512;
/// Upper bound on a single `recv()` wait. On timeout the loop records a
/// liveness heartbeat and polls again, so an idle topic never looks stale and
/// a vanished broker surfaces promptly as a recv error.
const POLL_TICK: Duration = Duration::from_secs(15);
/// Backoff bounds for rebuilding the consumer after a fatal session error.
const INITIAL_BACKOFF: Duration = Duration::from_secs(1);
const MAX_BACKOFF: Duration = Duration::from_secs(30);

/// An owned copy of a Kafka record, decoupled from the borrowed message so the
/// poll loop can hand it to the worker and immediately poll again.
struct OwnedRecord {
    payload: Option<String>,
    topic: String,
    partition: i32,
    offset: i64,
}

impl OwnedRecord {
    fn from_msg(msg: &BorrowedMessage<'_>) -> Self {
        let payload = msg
            .payload_view::<str>()
            .and_then(Result::ok)
            .map(str::to_string);
        Self {
            payload,
            topic: msg.topic().to_string(),
            partition: msg.partition(),
            offset: msg.offset(),
        }
    }
}

/// Run the Kafka consumer under a supervisor: each session subscribes, polls,
/// and hands records to a worker; on a fatal recv error (`AllBrokersDown`,
/// `PollExceeded`) the session ends, health flips down, and the supervisor
/// rebuilds the consumer after a bounded backoff. The loop exits only on the
/// cancellation token.
///
/// # Errors
///
/// Returns an error only if it exits abnormally; graceful shutdown via the
/// token returns `Ok(())`.
pub async fn start_kafka_consumer(
    brokers: String,
    group_id: String,
    topics: Vec<String>,
    orchestrator: Arc<NotificationOrchestrator>,
    frontend_url: Option<String>,
    health: Arc<KafkaConsumerHealth>,
    token: CancellationToken,
) -> anyhow::Result<()> {
    let mut backoff = INITIAL_BACKOFF;

    loop {
        if token.is_cancelled() {
            break;
        }

        match run_kafka_session(
            &brokers,
            &group_id,
            &topics,
            &orchestrator,
            frontend_url.as_deref(),
            &health,
            &token,
        )
        .await
        {
            Ok(()) => break, // graceful shutdown via the cancellation token
            Err(e) => {
                health.mark_down();
                error!("Kafka consumer session ended: {e}; restarting in {backoff:?}");
                tokio::select! {
                    () = token.cancelled() => break,
                    () = tokio::time::sleep(backoff) => {}
                }
                backoff = (backoff * 2).min(MAX_BACKOFF);
            }
        }
    }

    health.mark_down();
    info!("🛑 Kafka consumer stopped");
    Ok(())
}

/// One consumer session: build + subscribe, then poll until the token fires
/// (returns `Ok`) or a fatal broker error occurs (returns `Err`, triggering a
/// supervised restart).
async fn run_kafka_session(
    brokers: &str,
    group_id: &str,
    topics: &[String],
    orchestrator: &Arc<NotificationOrchestrator>,
    frontend_url: Option<&str>,
    health: &Arc<KafkaConsumerHealth>,
    token: &CancellationToken,
) -> anyhow::Result<()> {
    let consumer = Arc::new(kafka::create_consumer(brokers, group_id)?);
    let topics_ref: Vec<&str> = topics.iter().map(String::as_str).collect();
    consumer.subscribe(&topics_ref)?;

    info!("🚀 Kafka consumer started for topics: {:?}", topics);

    // Decouple poll from downstream send: a bounded channel hands owned records
    // to a single worker. Heavy downstream I/O (Redis + Postgres + RabbitMQ in
    // the orchestrator) runs in the worker, so a stalled downstream never
    // delays the next `recv()` and trips `max.poll.interval.ms` (the root cause
    // of the 2026-06 13h consumer wedge). One worker keeps offset commits
    // ordered and at-least-once.
    let (tx, mut rx) = mpsc::channel::<OwnedRecord>(CHANNEL_CAP);

    let worker_consumer = consumer.clone();
    let worker_orch = orchestrator.clone();
    let worker_frontend = frontend_url.map(str::to_string);
    let worker = tokio::spawn(async move {
        while let Some(rec) = rx.recv().await {
            match handle_record(&worker_orch, worker_frontend.as_deref(), &rec).await {
                Ok(()) => {
                    if let Err(e) =
                        commit_offset(&worker_consumer, &rec.topic, rec.partition, rec.offset)
                    {
                        error!("Failed to commit Kafka offset: {e}");
                    }
                }
                Err(e) => error!("Failed to handle Kafka message: {e}"),
            }
        }
    });

    let result = loop {
        tokio::select! {
            () = token.cancelled() => {
                info!("🛑 Kafka consumer shutting down...");
                break Ok(());
            }
            recv = tokio::time::timeout(POLL_TICK, consumer.recv()) => {
                match recv {
                    // Idle tick: no message within POLL_TICK. The loop is alive
                    // and polling — record a heartbeat so readiness stays green.
                    Err(_elapsed) => health.mark_progress(unix_now_secs()),
                    Ok(Ok(msg)) => {
                        health.mark_progress(unix_now_secs());
                        let rec = OwnedRecord::from_msg(&msg);
                        if tx.send(rec).await.is_err() {
                            break Err(anyhow::anyhow!("Kafka worker channel closed"));
                        }
                    }
                    Ok(Err(e)) => {
                        // Only a genuine broker-down state warrants tearing the
                        // session down for a supervised rebuild. Per-topic
                        // notices (e.g. a not-yet-created `trading.triggers`)
                        // and other transient consumption errors are logged
                        // while the loop keeps polling the topics that ARE
                        // available — matching librdkafka's own auto-recovery
                        // and avoiding a restart storm.
                        if is_fatal_kafka_error(&e) {
                            break Err(anyhow::anyhow!("Kafka recv error (fatal): {e}"));
                        }
                        warn!("Kafka recv error (non-fatal, continuing): {e}");
                        health.mark_progress(unix_now_secs());
                    }
                }
            }
        }
    };

    // Close the channel so the worker drains any buffered records and exits.
    drop(tx);
    if let Err(e) = worker.await {
        error!("Kafka worker task join error: {e}");
    }

    result
}

/// Whether a `recv()` error means the consumer can no longer make progress and
/// the session should be rebuilt. `AllBrokersDown` is the wedge signal from the
/// 2026-06 incident; everything else (unknown/unavailable topic, transient
/// consumption errors) is recoverable in place.
fn is_fatal_kafka_error(e: &KafkaError) -> bool {
    matches!(
        e,
        KafkaError::MessageConsumption(RDKafkaErrorCode::AllBrokersDown)
    )
}

/// Commit `offset + 1` (the next offset to read) for one partition.
fn commit_offset(
    consumer: &StreamConsumer,
    topic: &str,
    partition: i32,
    offset: i64,
) -> anyhow::Result<()> {
    let mut tpl = TopicPartitionList::new();
    tpl.add_partition_offset(topic, partition, Offset::Offset(offset + 1))?;
    consumer.commit(&tpl, CommitMode::Async)?;
    Ok(())
}

async fn handle_record(
    orchestrator: &Arc<NotificationOrchestrator>,
    frontend_url: Option<&str>,
    rec: &OwnedRecord,
) -> anyhow::Result<()> {
    let Some(payload) = rec.payload.as_deref() else {
        return Ok(());
    };

    let mut event: Value = serde_json::from_str(payload)?;
    let event_type = event
        .get("event_type")
        .and_then(Value::as_str)
        .unwrap_or("unknown")
        .to_string();

    info!(
        "Processing event: {} from topic: {}",
        event_type, rec.topic
    );

    // Take the `data` object out of the envelope; default to an empty object
    // so per-event payload field defaults apply cleanly when it is absent.
    let data = event
        .get_mut("data")
        .map_or_else(|| Value::Object(serde_json::Map::new()), Value::take);

    let ctx = MsgCtx {
        partition: rec.partition,
        offset: rec.offset,
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
