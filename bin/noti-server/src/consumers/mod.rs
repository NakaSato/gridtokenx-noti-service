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
    supervise(&health, &token, || {
        run_kafka_session(
            &brokers,
            &group_id,
            &topics,
            &orchestrator,
            frontend_url.as_deref(),
            &health,
            &token,
        )
    })
    .await;
    Ok(())
}

/// Supervise an async session: run it, and on a session error flip health down
/// and rebuild after a bounded backoff. Exits when the session returns `Ok`
/// (graceful shutdown) or the token is cancelled — including mid-backoff. The
/// session factory is injected so the supervision policy (backoff, cancel
/// handling, health signalling) is unit-testable without a real broker.
async fn supervise<F, Fut>(
    health: &Arc<KafkaConsumerHealth>,
    token: &CancellationToken,
    mut run_session: F,
) where
    F: FnMut() -> Fut,
    Fut: std::future::Future<Output = anyhow::Result<()>>,
{
    let mut backoff = INITIAL_BACKOFF;

    loop {
        if token.is_cancelled() {
            break;
        }

        match run_session().await {
            Ok(()) => break, // graceful shutdown via the cancellation token
            Err(e) => {
                health.mark_down();
                error!("Kafka consumer session ended: {e}; restarting in {backoff:?}");
                tokio::select! {
                    () = token.cancelled() => break,
                    () = tokio::time::sleep(backoff) => {}
                }
                backoff = next_backoff(backoff);
            }
        }
    }

    health.mark_down();
    info!("🛑 Kafka consumer stopped");
}

/// Double the backoff, capped at `MAX_BACKOFF`.
fn next_backoff(current: Duration) -> Duration {
    (current * 2).min(MAX_BACKOFF)
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

    loop {
        // Select on the token alongside the next delivery so cancellation takes
        // effect immediately even while the consumer is idle — without this the
        // task parks in `consumer.next()` and only notices shutdown when the
        // next message happens to arrive.
        let delivery_res = tokio::select! {
            () = token.cancelled() => break,
            next = consumer.next() => match next {
                Some(d) => d,
                None => break, // consumer stream closed
            },
        };

        match delivery_res {
            Ok(delivery) => match process_dispatch_payload(&orchestrator, &delivery.data).await {
                Disposition::Ack => delivery.ack(BasicAckOptions::default()).await?,
                Disposition::NackToDlx => {
                    delivery
                        .nack(BasicNackOptions {
                            requeue: false,
                            multiple: false,
                        })
                        .await?;
                }
            },
            Err(e) => {
                error!("RabbitMQ consumer delivery error: {}", e);
            }
        }
    }

    info!("🛑 RabbitMQ consumer stopped");
    Ok(())
}

/// Fate of a consumed `noti.dispatch` delivery once processed.
#[derive(Debug, PartialEq, Eq)]
enum Disposition {
    /// Remove from the queue: delivered, already terminal, or rescheduled for
    /// retry out-of-band by the orchestrator — and poison payloads that will
    /// never parse (drop rather than dead-letter them forever).
    Ack,
    /// Reject without requeue → routed to the `noti.retry` DLX for TTL-backoff
    /// redelivery (topology verified in `tests/rabbitmq_dlx.rs`).
    NackToDlx,
}

/// Decide the fate of one dispatch payload: deserialize it, run dispatch, and
/// map the outcome to an ack/nack disposition. Kept free of `lapin` so the
/// ack/nack policy is unit-testable without a broker.
async fn process_dispatch_payload(
    orchestrator: &Arc<NotificationOrchestrator>,
    data: &[u8],
) -> Disposition {
    let task: NotificationTask = match serde_json::from_slice(data) {
        Ok(t) => t,
        Err(e) => {
            // Poison message: re-delivery can't fix a parse failure, so ack to
            // drop it instead of dead-lettering the same bad bytes forever.
            error!("Failed to deserialize notification task: {e}");
            return Disposition::Ack;
        }
    };

    info!("📥 Received task for notification {}", task.notification_id);

    match orchestrator.clone().dispatch(task.notification_id).await {
        Ok(()) => Disposition::Ack,
        Err(e) => {
            warn!(
                "Failed to dispatch notification {}: {}",
                task.notification_id, e
            );
            Disposition::NackToDlx
        }
    }
}

#[cfg(test)]
mod tests {
    //! Regression tests for the supervised Kafka consumer (gap #2). The
    //! supervisor logic is exercised through a fake session so reconnect,
    //! backoff bounds, cancellation, and health signalling are verified
    //! without a real broker. Stems from the 2026-06 ~13h consumer wedge.

    use std::sync::Arc;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::time::Duration;

    use rdkafka::error::KafkaError;
    use rdkafka::types::RDKafkaErrorCode;
    use tokio_util::sync::CancellationToken;

    use noti_core::health::KafkaConsumerHealth;

    use super::{INITIAL_BACKOFF, MAX_BACKOFF, is_fatal_kafka_error, next_backoff, supervise};

    // --- fatal-error classification (the wedge signal) ---------------------

    #[test]
    fn all_brokers_down_is_fatal() {
        let e = KafkaError::MessageConsumption(RDKafkaErrorCode::AllBrokersDown);
        assert!(is_fatal_kafka_error(&e));
    }

    #[test]
    fn transient_consumption_errors_are_not_fatal() {
        // A not-yet-created topic and a transient transport blip must NOT tear
        // the session down — librdkafka auto-recovers; tearing down would cause
        // a restart storm.
        for code in [
            RDKafkaErrorCode::UnknownTopicOrPartition,
            RDKafkaErrorCode::BrokerTransportFailure,
            RDKafkaErrorCode::OperationTimedOut,
        ] {
            let e = KafkaError::MessageConsumption(code);
            assert!(!is_fatal_kafka_error(&e), "{code:?} must be non-fatal");
        }
    }

    #[test]
    fn non_consumption_errors_are_not_fatal() {
        let e = KafkaError::Subscription("bad topic".to_string());
        assert!(!is_fatal_kafka_error(&e));
    }

    // --- backoff bounds ----------------------------------------------------

    #[test]
    fn backoff_doubles_and_caps() {
        let mut b = INITIAL_BACKOFF;
        assert_eq!(b, Duration::from_secs(1));
        let expected = [2, 4, 8, 16, 30, 30, 30];
        for secs in expected {
            b = next_backoff(b);
            assert_eq!(b, Duration::from_secs(secs));
        }
        assert_eq!(next_backoff(MAX_BACKOFF), MAX_BACKOFF, "saturates at max");
    }

    // --- supervisor control flow (fake session, paused clock) --------------

    #[tokio::test(start_paused = true)]
    async fn skips_session_when_cancelled_before_start() {
        let health = Arc::new(KafkaConsumerHealth::new());
        let token = CancellationToken::new();
        token.cancel();

        let calls = Arc::new(AtomicUsize::new(0));
        let c = calls.clone();
        supervise(&health, &token, || {
            let c = c.clone();
            async move {
                c.fetch_add(1, Ordering::SeqCst);
                Ok(())
            }
        })
        .await;

        assert_eq!(calls.load(Ordering::SeqCst), 0, "pre-cancelled: no session");
    }

    #[tokio::test(start_paused = true)]
    async fn ok_session_exits_after_one_run() {
        let health = Arc::new(KafkaConsumerHealth::new());
        let token = CancellationToken::new();

        let calls = Arc::new(AtomicUsize::new(0));
        let c = calls.clone();
        supervise(&health, &token, || {
            let c = c.clone();
            async move {
                c.fetch_add(1, Ordering::SeqCst);
                Ok(())
            }
        })
        .await;

        assert_eq!(calls.load(Ordering::SeqCst), 1, "graceful Ok breaks loop");
    }

    #[tokio::test(start_paused = true)]
    async fn errored_session_retries_until_cancelled_and_marks_down() {
        let health = Arc::new(KafkaConsumerHealth::new());
        // Start "ready" so we can prove a session error flips readiness off.
        health.mark_progress(1_000);
        assert!(health.is_ready(1_000, 90));

        let token = CancellationToken::new();
        let stop_at = 3;
        let calls = Arc::new(AtomicUsize::new(0));
        let c = calls.clone();
        let tok = token.clone();
        supervise(&health, &token, || {
            let c = c.clone();
            let tok = tok.clone();
            async move {
                let n = c.fetch_add(1, Ordering::SeqCst) + 1;
                // Cancel from inside the session on the Nth failure so the
                // supervisor breaks out of its backoff sleep.
                if n >= stop_at {
                    tok.cancel();
                }
                Err::<(), _>(anyhow::anyhow!("simulated broker down"))
            }
        })
        .await;

        assert_eq!(
            calls.load(Ordering::SeqCst),
            stop_at,
            "retries each failure until cancellation lands"
        );
        assert!(
            !health.is_ready(1_000, 90),
            "session error must flip health to not-ready"
        );
    }

    // --- envelope decoding (handle_record, the JSON → dispatch glue) -------
    //
    // The per-event field mapping is covered in `events::tests`. These cover
    // the layer above it: unwrapping the Kafka record envelope (event_type +
    // data extraction, missing/garbage payload handling) and threading the
    // record's partition/offset into the idempotency key.

    use std::sync::Mutex;

    use noti_core::domain::{Notification, NotificationChannel};
    use noti_core::traits::{
        MockCacheTrait, MockMessageQueueTrait, MockNotificationProviderTrait,
        MockNotificationRepositoryTrait, MockTemplateEngineTrait,
    };
    use noti_logic::NotificationOrchestrator;

    use super::{OwnedRecord, handle_record};

    type Sink = Arc<Mutex<Vec<Notification>>>;

    /// Orchestrator whose `repo.create` records every queued notification.
    fn recording_orchestrator() -> (Arc<NotificationOrchestrator>, Sink) {
        let sink: Sink = Arc::new(Mutex::new(Vec::new()));
        let recorder = sink.clone();

        let mut repo = MockNotificationRepositoryTrait::new();
        repo.expect_create().times(..).returning(move |n| {
            recorder.lock().expect("sink lock").push(n.clone());
            Ok(n.clone())
        });

        let mut cache = MockCacheTrait::new();
        cache.expect_set_value().times(..).returning(|_, _, _| Ok(()));

        let mut mq = MockMessageQueueTrait::new();
        mq.expect_publish_dispatch().times(..).returning(|_| Ok(()));

        let orch = Arc::new(NotificationOrchestrator::new(
            Arc::new(repo),
            Arc::new(MockTemplateEngineTrait::new()),
            Arc::new(MockNotificationProviderTrait::new()),
            Arc::new(MockNotificationProviderTrait::new()),
            Arc::new(MockNotificationProviderTrait::new()),
            Arc::new(MockNotificationProviderTrait::new()),
            Arc::new(MockNotificationProviderTrait::new()),
            Arc::new(cache),
            Some(Arc::new(mq)),
        ));
        (orch, sink)
    }

    fn record(payload: Option<&str>) -> OwnedRecord {
        OwnedRecord {
            payload: payload.map(str::to_string),
            topic: "iam.user.events".to_string(),
            partition: 4,
            offset: 77,
        }
    }

    #[tokio::test]
    async fn handle_record_unwraps_envelope_and_routes_to_handler() {
        let (orch, sink) = recording_orchestrator();
        let payload = serde_json::json!({
            "event_type": "EmailVerified",
            "data": { "email": "alice@example.com", "username": "alice" }
        })
        .to_string();

        handle_record(&orch, Some("https://app.gridtokenx.test"), &record(Some(&payload)))
            .await
            .expect("handle_record ok");

        let queued = sink.lock().expect("lock");
        assert_eq!(queued.len(), 1, "envelope routed to EmailVerified handler");
        let n = &queued[0];
        assert!(matches!(n.channel, NotificationChannel::Email));
        assert_eq!(n.recipient, "alice@example.com");
        assert_eq!(
            n.idempotency_key.as_deref(),
            Some("kafka:email_verified:4:77"),
            "record partition/offset thread into the idempotency key"
        );
    }

    #[tokio::test]
    async fn handle_record_errors_on_malformed_json() {
        let (orch, sink) = recording_orchestrator();
        // Surfacing the error (not silently acking) lets the worker log it
        // and skip the offset commit, so the message isn't lost.
        handle_record(&orch, None, &record(Some("{not valid json")))
            .await
            .expect_err("malformed payload must surface as an error");
        assert_eq!(sink.lock().expect("lock").len(), 0);
    }

    #[tokio::test]
    async fn handle_record_ignores_empty_payload() {
        let (orch, sink) = recording_orchestrator();
        handle_record(&orch, None, &record(None))
            .await
            .expect("empty payload is a no-op, not an error");
        assert_eq!(sink.lock().expect("lock").len(), 0);
    }

    #[tokio::test]
    async fn handle_record_defaults_missing_data_to_empty_object() {
        // No `data` field → handler receives an empty object → its field
        // defaults apply (empty email) → graceful no-op, no panic, no error.
        let (orch, sink) = recording_orchestrator();
        let payload = serde_json::json!({ "event_type": "EmailVerified" }).to_string();

        handle_record(&orch, None, &record(Some(&payload)))
            .await
            .expect("missing data must not error");
        assert_eq!(sink.lock().expect("lock").len(), 0);
    }

    #[tokio::test]
    async fn handle_record_treats_missing_event_type_as_unknown() {
        let (orch, sink) = recording_orchestrator();
        let payload = serde_json::json!({ "data": { "email": "x@example.com" } }).to_string();

        handle_record(&orch, None, &record(Some(&payload)))
            .await
            .expect("missing event_type → 'unknown' → ignored");
        assert_eq!(sink.lock().expect("lock").len(), 0);
    }

    // --- RabbitMQ dispatch ack/nack policy (gap #7) ------------------------
    //
    // The lapin ack/nack plumbing + the DLX retry topology are integration-
    // tested in `tests/rabbitmq_dlx.rs`. These cover the broker-free decision:
    // which outcome acks (drop from queue) vs nacks (dead-letter for retry).

    use uuid::Uuid;

    use noti_core::domain::NotificationStatus;
    use noti_core::error::NotiError;

    use super::{Disposition, NotificationTask, process_dispatch_payload};

    fn pending(id: Uuid) -> Notification {
        let now = gridtokenx_telemetry::time::now();
        Notification {
            id,
            user_id: Some(Uuid::new_v4()),
            channel: NotificationChannel::Email,
            status: NotificationStatus::Pending,
            recipient: "user@example.com".into(),
            template_id: "welcome.html.tera".into(),
            variables: serde_json::json!({}),
            provider_id: None,
            provider_ref: None,
            retry_count: 0,
            next_retry_at: now,
            error_message: None,
            idempotency_key: None,
            created_at: now,
            updated_at: now,
            sent_at: None,
            read_at: None,
        }
    }

    /// Orchestrator whose `dispatch(id)` succeeds: the row is found pending,
    /// the template renders, and the email provider accepts the send.
    fn orchestrator_dispatch_ok() -> Arc<NotificationOrchestrator> {
        let mut repo = MockNotificationRepositoryTrait::new();
        repo.expect_get_by_id()
            .returning(|id| Ok(Some(pending(id))));
        repo.expect_update_status()
            .times(..)
            .returning(|_, _, _, _| Ok(()));

        let mut template = MockTemplateEngineTrait::new();
        template.expect_render().returning(|_, _| Ok("body".into()));

        let mut email = MockNotificationProviderTrait::new();
        email.expect_send().returning(|_, _| Ok("provider-ref".into()));
        email.expect_provider_id().returning(|| "mock-email");

        Arc::new(NotificationOrchestrator::new(
            Arc::new(repo),
            Arc::new(template),
            Arc::new(email),
            Arc::new(MockNotificationProviderTrait::new()),
            Arc::new(MockNotificationProviderTrait::new()),
            Arc::new(MockNotificationProviderTrait::new()),
            Arc::new(MockNotificationProviderTrait::new()),
            Arc::new(MockCacheTrait::new()),
            None,
        ))
    }

    /// Orchestrator whose `dispatch(id)` fails: the row is missing, so dispatch
    /// returns `NotFound` before touching anything else.
    fn orchestrator_dispatch_err() -> Arc<NotificationOrchestrator> {
        let mut repo = MockNotificationRepositoryTrait::new();
        repo.expect_get_by_id().returning(|_| Ok(None));

        Arc::new(NotificationOrchestrator::new(
            Arc::new(repo),
            Arc::new(MockTemplateEngineTrait::new()),
            Arc::new(MockNotificationProviderTrait::new()),
            Arc::new(MockNotificationProviderTrait::new()),
            Arc::new(MockNotificationProviderTrait::new()),
            Arc::new(MockNotificationProviderTrait::new()),
            Arc::new(MockNotificationProviderTrait::new()),
            Arc::new(MockCacheTrait::new()),
            None,
        ))
    }

    fn task_bytes(id: Uuid) -> Vec<u8> {
        serde_json::to_vec(&NotificationTask {
            notification_id: id,
            retry_count: 0,
        })
        .expect("serialize task")
    }

    #[tokio::test]
    async fn successful_dispatch_acks() {
        let orch = orchestrator_dispatch_ok();
        let disposition = process_dispatch_payload(&orch, &task_bytes(Uuid::new_v4())).await;
        assert_eq!(disposition, Disposition::Ack, "delivered → drop from queue");
    }

    #[tokio::test]
    async fn failed_dispatch_nacks_to_dlx() {
        let orch = orchestrator_dispatch_err();
        let disposition = process_dispatch_payload(&orch, &task_bytes(Uuid::new_v4())).await;
        assert_eq!(
            disposition,
            Disposition::NackToDlx,
            "dispatch error → dead-letter for TTL-backoff retry"
        );
    }

    #[tokio::test]
    async fn poison_payload_is_acked_not_dead_lettered() {
        // A payload that can never parse must be dropped, not dead-lettered in
        // a loop. dispatch must never run, so any orchestrator works.
        let orch = orchestrator_dispatch_err();
        let disposition = process_dispatch_payload(&orch, b"{ not a task }").await;
        assert_eq!(
            disposition,
            Disposition::Ack,
            "unparseable poison message → ack to drop"
        );
    }

    #[tokio::test]
    async fn dispatch_not_found_surfaces_as_nack() {
        // Guard the specific error mapping: NotFound is a dispatch Err and so
        // dead-letters (the row may yet be written by a lagging producer).
        let orch = orchestrator_dispatch_err();
        let missing = Uuid::new_v4();
        // Sanity: confirm the orchestrator really returns NotFound for the id.
        let direct = orch.clone().dispatch(missing).await;
        assert!(
            matches!(direct, Err(NotiError::NotFound(_))),
            "precondition: dispatch returns NotFound"
        );
        let disposition = process_dispatch_payload(&orch, &task_bytes(missing)).await;
        assert_eq!(disposition, Disposition::NackToDlx);
    }
}
