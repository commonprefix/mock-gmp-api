use async_trait::async_trait;
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use tracing::debug;

use lapin::{
    BasicProperties, Channel, Connection, ConnectionProperties, Consumer, options::*,
    types::FieldTable,
};
#[async_trait]
pub trait QueueTrait {
    type Consumer;
    async fn publish(
        &self,
        item: &QueueItem,
        properties: Option<BasicProperties>,
    ) -> Result<(), anyhow::Error>;
    async fn consumer(&mut self, consumer_name: &str) -> Result<lapin::Consumer, anyhow::Error>;
}

#[derive(Clone)]
pub struct LapinConnection {
    channel: Channel,
    queue_name: String,
}

#[async_trait]
impl QueueTrait for LapinConnection {
    type Consumer = lapin::Consumer;

    async fn publish(
        &self,
        item: &QueueItem,
        properties: Option<BasicProperties>,
    ) -> Result<(), anyhow::Error> {
        let msg = serde_json::to_vec(item)?;

        let confirm = self
            .channel
            .basic_publish(
                "",
                &self.queue_name,
                BasicPublishOptions::default(),
                &msg,
                properties.unwrap_or(BasicProperties::default().with_delivery_mode(2)),
            )
            .await?
            .await?;

        if confirm.is_ack() {
            Ok(())
        } else {
            Err(anyhow::anyhow!("Failed to publish message"))
        }
    }

    async fn consumer(&mut self, consumer_name: &str) -> Result<Consumer, anyhow::Error> {
        let consumer = self
            .channel
            .basic_consume(
                &self.queue_name,
                consumer_name,
                BasicConsumeOptions::default(),
                FieldTable::default(),
            )
            .await?;
        Ok(consumer)
    }
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub enum QueueItem {
    VerifyMessages(VerifyMessagesItem),
    ConstructProof(ConstructProofItem),
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct VerifyMessagesItem {
    pub poll_id: String,
    pub contract_address: String,
    pub broadcast_created_at: DateTime<Utc>,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct ConstructProofItem {
    pub session_id: String,
    pub contract_address: String,
    pub broadcast_created_at: DateTime<Utc>,
}

impl LapinConnection {
    pub async fn new(addr: &str, queue_name: &str) -> Result<Self, anyhow::Error> {
        let conn = Connection::connect(addr, ConnectionProperties::default())
            .await
            .map_err(|e| anyhow::anyhow!(e.to_string()))?;

        debug!("CONNECTED");

        let channel = conn
            .create_channel()
            .await
            .map_err(|e| anyhow::anyhow!(e.to_string()))?;

        channel
            .confirm_select(ConfirmSelectOptions { nowait: false })
            .await?;

        let _queue = channel
            .queue_declare(
                queue_name,
                QueueDeclareOptions::default(),
                FieldTable::default(),
            )
            .await
            .map_err(|e| anyhow::anyhow!(e.to_string()))?;

        Ok(LapinConnection {
            channel,
            queue_name: queue_name.to_string(),
        })
    }

    // async fn republish(
    //     &self,
    //     item: &QueueItem,
    //     properties: Option<BasicProperties>,
    // ) -> Result<(), anyhow::Error> {

    //     if
    //     let msg = serde_json::to_vec(item)?;

    //     let confirm = self
    //         .channel
    //         .basic_publish(
    //             "",
    //             &self.queue_name,
    //             BasicPublishOptions::default(),
    //             &msg,
    //             properties.unwrap_or(BasicProperties::default().with_delivery_mode(2)),
    //         )
    //         .await?
    //         .await?;

    //     if confirm.is_ack() {
    //         Ok(())
    //     } else {
    //         Err(anyhow::anyhow!("Failed to publish message"))
    //     }
    // }
}
