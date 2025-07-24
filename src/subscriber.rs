use crate::{
    models::tasks::TasksModel,
    queue::{ConstructProofItem, QueueItem, QueueTrait, VerifyMessagesItem},
};
use futures::StreamExt;
use lapin::Consumer;
use tracing::error;

pub struct Subscriber<Q: QueueTrait> {
    queue: Q,
    database: TasksModel,
    chain_id: String,
    rpc: String,
}

impl<Q: QueueTrait> Subscriber<Q> {
    pub fn new(queue: Q, database: TasksModel, chain_id: String, rpc: String) -> Self {
        Self {
            queue,
            database,
            chain_id,
            rpc,
        }
    }
}

pub trait TransactionPoller<Q: QueueTrait> {
    fn make_database_item(&mut self) -> QueueItem;

    fn poll_transactions(&mut self) -> Result<(), anyhow::Error>;
}

//impl<Q: QueueTrait> TransactionPoller<Q> for Subscriber<Q> {}

impl<Q: QueueTrait> Subscriber<Q> {
    pub async fn run(&mut self) -> Result<(), anyhow::Error> {
        let mut consumer = self.queue.consumer("subscriber").await?;
        loop {
            self.work(&mut consumer).await?;
        }
    }

    pub async fn work(&self, consumer: &mut Consumer) -> Result<(), anyhow::Error> {
        match consumer.next().await {
            Some(Ok(delivery)) => {
                let message = String::from_utf8(delivery.data)?;
                let item: QueueItem = serde_json::from_str(&message)?;

                match item {
                    QueueItem::VerifyMessages(item) => {
                        self.handle_verify_messages(item).await?;
                    }
                    QueueItem::ConstructProof(item) => {
                        self.handle_construct_proof(item).await?;
                    }
                }
            }
            Some(Err(e)) => {
                error!("Error: {}", e);
            }
            None => {
                error!("Consumer closed");
            }
        }
        Ok(())
    }

    async fn handle_construct_proof(&self, item: ConstructProofItem) -> Result<(), anyhow::Error> {
        let axelard_execute_script_str = format!(
            "axelard query txs --events 'wasm-messages_poll_started._contract_address={}' --node {} --output json --limit 2 -h ",
            item.contract_address, self.rpc
        );
        Ok(())
    }

    async fn handle_verify_messages(&self, item: VerifyMessagesItem) -> Result<(), anyhow::Error> {
        let axelard_execute_script_str = format!(
            "axelard query txs --events 'wasm-messages_poll_started._contract_address={}' --node {} --output json --limit 2 -h ",
            item.contract_address, self.rpc
        );
        Ok(())
    }

    // TO BE CONTINUED
}
