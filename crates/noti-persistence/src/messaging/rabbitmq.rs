//! RabbitMQ client implementing `MessageQueueTrait` for dispatch / retry queues.
//!
//! This module only handles **publishing**. Consumer logic lives in `noti-server`
//! to avoid a circular dependency with `noti-logic`.

use async_trait::async_trait;
use lapin::{
    BasicProperties, Connection, ConnectionProperties, ExchangeKind,
    options::*,
    types::{AMQPValue, FieldTable, ShortString},
};
use serde::{Deserialize, Serialize};
use tracing::info;
use uuid::Uuid;

use noti_core::error::{NotiError, Result};
use noti_core::traits::MessageQueueTrait;

/// Payload carried on the dispatch / retry queues.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NotificationTask {
    pub notification_id: Uuid,
    pub retry_count: i32,
}

#[derive(Clone)]
pub struct RabbitMQClient {
    channel: lapin::Channel,
}

impl RabbitMQClient {
    pub async fn new(url: &str) -> anyhow::Result<Self> {
        let connection = Connection::connect(url, ConnectionProperties::default()).await?;
        let channel = connection.create_channel().await?;

        info!("✅ Connected to RabbitMQ");

        // Declare main exchange
        channel
            .exchange_declare(
                "noti.exchange",
                ExchangeKind::Direct,
                ExchangeDeclareOptions::default(),
                FieldTable::default(),
            )
            .await?;

        // Declare DLX for retries
        let mut dlx_args = FieldTable::default();
        dlx_args.insert(
            "x-dead-letter-exchange".into(),
            AMQPValue::LongString("noti.exchange".into()),
        );
        dlx_args.insert(
            "x-dead-letter-routing-key".into(),
            AMQPValue::LongString("noti.dispatch".into()),
        );

        channel
            .queue_declare("noti.retry", QueueDeclareOptions::default(), dlx_args)
            .await?;

        // Declare main dispatch queue
        channel
            .queue_declare(
                "noti.dispatch",
                QueueDeclareOptions::default(),
                FieldTable::default(),
            )
            .await?;

        channel
            .queue_bind(
                "noti.dispatch",
                "noti.exchange",
                "noti.dispatch",
                QueueBindOptions::default(),
                FieldTable::default(),
            )
            .await?;

        Ok(Self { channel })
    }

    /// Returns a reference to the underlying lapin channel for consumer setup.
    pub fn channel(&self) -> &lapin::Channel {
        &self.channel
    }

    async fn publish_task(&self, task: &NotificationTask, delay_mins: i32) -> Result<()> {
        let payload = serde_json::to_vec(task).map_err(|e| NotiError::Internal(e.to_string()))?;

        if delay_mins > 0 {
            let expiration = (delay_mins * 60 * 1000).to_string();
            let mut props =
                BasicProperties::default().with_expiration(ShortString::from(expiration));

            let mut headers = FieldTable::default();
            headers.insert("x-retry-count".into(), AMQPValue::LongInt(task.retry_count));
            props = props.with_headers(headers);

            self.channel
                .basic_publish(
                    "",
                    "noti.retry",
                    BasicPublishOptions::default(),
                    &payload,
                    props,
                )
                .await
                .map_err(|e| NotiError::Internal(e.to_string()))?;

            info!(
                "⏳ Task {} queued for retry in {} mins",
                task.notification_id, delay_mins
            );
        } else {
            self.channel
                .basic_publish(
                    "noti.exchange",
                    "noti.dispatch",
                    BasicPublishOptions::default(),
                    &payload,
                    BasicProperties::default().with_delivery_mode(2),
                )
                .await
                .map_err(|e| NotiError::Internal(e.to_string()))?;
        }

        Ok(())
    }
}

#[async_trait]
impl MessageQueueTrait for RabbitMQClient {
    async fn publish_dispatch(&self, notification_id: Uuid) -> Result<()> {
        let task = NotificationTask {
            notification_id,
            retry_count: 0,
        };
        self.publish_task(&task, 0).await
    }

    async fn publish_retry(&self, notification_id: Uuid, delay_ms: u32) -> Result<()> {
        let task = NotificationTask {
            notification_id,
            retry_count: 1,
        };
        self.publish_task(&task, (delay_ms / 60000) as i32).await
    }
}
