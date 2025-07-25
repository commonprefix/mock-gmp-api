use crate::{
    models::tasks::TasksModel,
    queue::{ConstructProofItem, QueueItem, QueueTrait, VerifyMessagesItem},
};
use futures::StreamExt;
use lapin::{Consumer, options::BasicAckOptions};
use serde_json::Value;
use std::collections::HashMap;
use tracing::{debug, error, info};

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

pub enum DesiredEventType {
    QuorumReached,
    SigningCompleted,
}

impl DesiredEventType {
    pub fn event_type_name(&self) -> &str {
        match self {
            DesiredEventType::QuorumReached => "wasm-quorum_reached",
            DesiredEventType::SigningCompleted => "wasm-signing_completed",
        }
    }

    pub fn attribute_name(&self) -> &str {
        match self {
            DesiredEventType::QuorumReached => "poll_id",
            DesiredEventType::SigningCompleted => "session_id",
        }
    }
}

impl<Q: QueueTrait> Subscriber<Q> {
    pub async fn run(&mut self) -> Result<(), anyhow::Error> {
        let mut consumer = self.queue.consumer("subscriber").await?;
        loop {
            self.work(&mut consumer).await?;
        }
    }

    // TODO: (Issue) If I post a broadcast the subscriber tries to handle it immediately,
    // but the voting hasnt completed yet, so it falls into a rabbithole checking all the
    // history of the contract. If it waited a little before polling for the last page,
    // it would immediately find it (way faster than retrying after parsing all pages)
    // Maybe the solution is to only try to handle a delivery a few seconds after it was
    // submitted to the queue and not immediately.

    pub async fn work(&self, consumer: &mut Consumer) -> Result<(), anyhow::Error> {
        match consumer.next().await {
            Some(Ok(delivery)) => {
                let message = String::from_utf8(delivery.data.clone())?;
                let item: QueueItem = serde_json::from_str(&message)?;
                std::thread::sleep(std::time::Duration::from_secs(10));

                match item {
                    QueueItem::VerifyMessages(item) => {
                        info!("Got verify messages item: {:?}", item);
                        if let Err(e) = self.handle_verify_messages(item).await {
                            error!("Error: {}", e);
                        } else {
                            delivery.ack(BasicAckOptions::default()).await?;
                        }
                    }
                    QueueItem::ConstructProof(item) => {
                        info!("Got construct proof item: {:?}", item);
                        if let Err(e) = self.handle_construct_proof(item).await {
                            error!("Error: {}", e);
                        } else {
                            delivery.ack(BasicAckOptions::default()).await?;
                        }
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

    async fn handle_verify_messages(&self, item: VerifyMessagesItem) -> Result<(), anyhow::Error> {
        let initial_axelard_query_script_str = format!(
            "axelard query txs --events 'wasm-quorum_reached._contract_address={}' --node {} --output json --limit 1",
            item.contract_address, self.rpc
        );

        let total_pages =
            Self::get_total_page_number_from_query(initial_axelard_query_script_str).await?;

        // request for pages in reverse order to get the latest data first
        for page in (1..=total_pages).rev() {
            let axelard_query_script_str = format!(
                "axelard query txs --events 'wasm-quorum_reached._contract_address={}' --node {} --output json --limit 100 --page {}",
                item.contract_address, self.rpc, page
            );

            let maybe_quorum_reached_event = Self::get_event_from_script(
                axelard_query_script_str,
                DesiredEventType::QuorumReached,
                item.poll_id.clone(),
            )
            .await?;

            if let Some(quorum_reached_event) = maybe_quorum_reached_event {
                info!("Found quorum reached event: {:?}", quorum_reached_event);
                return Ok(());
            } else {
                error!("No quorum reached event found");
            }
        }

        Err(anyhow::anyhow!("No quorum reached event found"))
    }

    async fn handle_construct_proof(&self, item: ConstructProofItem) -> Result<(), anyhow::Error> {
        let initial_axelard_query_script_str = format!(
            "axelard query txs --events 'wasm-messages_signing_started._contract_address={}' --node {} --output json --limit 1",
            item.contract_address, self.rpc
        );

        let total_pages =
            Self::get_total_page_number_from_query(initial_axelard_query_script_str).await?;

        // request for pages in reverse order to get the latest data first
        for page in (1..=total_pages).rev() {
            let axelard_query_script_str = format!(
                "axelard query txs --events 'wasm-signing_completed._contract_address={}' --node {} --output json --limit 100 --page {}",
                item.contract_address, self.rpc, page
            );

            let maybe_signing_completed_event = Self::get_event_from_script(
                axelard_query_script_str,
                DesiredEventType::SigningCompleted,
                item.session_id.clone(),
            )
            .await?;

            if let Some(signing_completed_event) = maybe_signing_completed_event {
                info!(
                    "Found signing completed event: {:?}",
                    signing_completed_event
                );
                return Ok(());
            } else {
                error!("No signing completed event found");
            }
        }

        Err(anyhow::anyhow!("No signing completed event found"))
    }

    async fn get_total_page_number_from_query(
        axelard_query_script_str: String,
    ) -> Result<u32, anyhow::Error> {
        let axelard_query_result = tokio::process::Command::new("bash")
            .arg("-c")
            .arg(axelard_query_script_str.clone())
            .output()
            .await;

        match axelard_query_result {
            Ok(output) => {
                if output.status.success() {
                    info!(
                        "Query executed successfully: {}",
                        axelard_query_script_str.clone()
                    );

                    let output_str = String::from_utf8_lossy(&output.stdout);
                    let json_value = serde_json::from_str::<Value>(&output_str)?;

                    let total_count = json_value
                        .get("total_count")
                        .and_then(|v| {
                            // Try as number first, then as string
                            v.as_u64()
                                .or_else(|| v.as_str().and_then(|s| s.parse::<u64>().ok()))
                        })
                        .ok_or_else(|| {
                            error!("Total count not found in query result");
                            anyhow::anyhow!("Total count not found in query result")
                        })?;

                    // Each page has 100 entries e.g. for 399 entries, we need 4 pages
                    let total_pages =
                        (total_count / 100 + if total_count % 100 != 0 { 1 } else { 0 }) as u32;
                    Ok(total_pages)
                } else {
                    error!("Query failed: {}", axelard_query_script_str);
                    Err(anyhow::anyhow!(
                        "Query failed: {}",
                        axelard_query_script_str
                    ))
                }
            }
            Err(e) => {
                error!("Error: {}", e);
                Err(anyhow::anyhow!("Error: {}", e))
            }
        }
    }

    async fn get_event_from_script(
        axelard_query_script_str: String,
        desired_event_type: DesiredEventType,
        item_desired_id: String,
    ) -> Result<Option<Value>, anyhow::Error> {
        let event_type = desired_event_type.event_type_name();
        let desired_attribute = desired_event_type.attribute_name();
        let axelard_query_result = tokio::process::Command::new("bash")
            .arg("-c")
            .arg(axelard_query_script_str.clone())
            .output()
            .await;

        match axelard_query_result {
            Ok(output) => {
                if output.status.success() {
                    info!("Query executed successfully: {}", axelard_query_script_str);

                    let output_str = String::from_utf8_lossy(&output.stdout);
                    let json_value = serde_json::from_str::<Value>(&output_str)?;

                    if let Some(txs) = json_value.get("txs").and_then(|v| v.as_array()) {
                        debug!("Processing {} transactions", txs.len());
                        for tx in txs {
                            if let Some(logs) = tx.get("logs").and_then(|v| v.as_array()) {
                                for log in logs {
                                    if let Some(tx_events) =
                                        log.get("events").and_then(|v| v.as_array())
                                    {
                                        debug!("Transaction has {} events", tx_events.len());

                                        for event in tx_events {
                                            if let Some(event_type_val) =
                                                event.get("type").and_then(|v| v.as_str())
                                            {
                                                debug!("Found event type: {}", event_type_val);
                                                if event_type_val == event_type {
                                                    debug!(
                                                        "Event type matches! Looking for {} = '{}'...",
                                                        desired_attribute, item_desired_id
                                                    );
                                                    let attributes = event
                                                        .get("attributes")
                                                        .and_then(|v| v.as_array())
                                                        .ok_or_else(|| {
                                                            error!("Attributes not found in event");
                                                            anyhow::anyhow!(
                                                                "Attributes not found in event"
                                                            )
                                                        })?;

                                                    let event_id = attributes
                                                        .iter()
                                                        .find(|attr| {
                                                            attr.get("key")
                                                                .and_then(|v| v.as_str())
                                                                .unwrap_or("")
                                                                == desired_attribute
                                                        })
                                                        .and_then(|attr| {
                                                            attr.get("value")
                                                                .and_then(|v| v.as_str())
                                                        })
                                                        .unwrap_or("");

                                                    debug!(
                                                        "Found event id: '{}', looking for: '{}'",
                                                        event_id, item_desired_id
                                                    );

                                                    if event_id == item_desired_id {
                                                        info!("ID match found! Returning event.");
                                                        return Ok(Some(event.clone()));
                                                    }
                                                }
                                            }
                                        }
                                    }
                                }
                            }
                        }
                    } else {
                        return Err(anyhow::anyhow!("No event found"));
                    }
                } else {
                    error!("Query failed: {}", axelard_query_script_str);
                    return Err(anyhow::anyhow!(
                        "Query failed: {}",
                        axelard_query_script_str
                    ));
                }
            }

            Err(e) => {
                error!("Error: {}", e);
                return Err(anyhow::anyhow!("Error: {}", e));
            }
        }

        // if we fell through the match without finding or erroring, return None
        Ok(None)
    }
}
#[derive(Debug, Clone)]
pub struct EventData {
    pub event_type: String,
    pub attributes: HashMap<String, String>,
}
