use crate::{
    gmp_types::{
        CommonTaskFields, EventAttribute, GatewayTxTask, GatewayTxTaskFields, ReactToWasmEventTask,
        ReactToWasmEventTaskFields, WasmEvent,
    },
    models::tasks::TasksModel,
    queue::{ConstructProofItem, QueueItem, QueueTrait, VerifyMessagesItem},
};
use base64::{Engine as _, engine::general_purpose};
use chrono::{DateTime, Utc};
use futures::StreamExt;
use lapin::{
    Consumer,
    options::{BasicAckOptions, BasicNackOptions},
};
use serde_json::Value;
use std::collections::HashMap;
use tracing::{debug, error, info, warn};

pub struct Subscriber<Q: QueueTrait> {
    queue: Q,
    database: TasksModel,
    rpc: String,
}

impl<Q: QueueTrait> Subscriber<Q> {
    pub fn new(queue: Q, database: TasksModel, rpc: String) -> Self {
        Self {
            queue,
            database,
            rpc,
        }
    }
}

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

    pub async fn work(&self, consumer: &mut Consumer) -> Result<(), anyhow::Error> {
        info!("Waiting for message...");
        match consumer.next().await {
            Some(Ok(delivery)) => {
                let message = String::from_utf8(delivery.data.clone())?;
                let item: QueueItem = serde_json::from_str(&message)?;

                match item {
                    QueueItem::VerifyMessages(item) => {
                        info!("Got verify messages item: {:?}", item);
                        if let Err(e) = self.handle_verify_messages(item.clone()).await {
                            error!("Error: {}", e);
                            debug!("Republishing verify messages item");
                            delivery
                                .nack(BasicNackOptions {
                                    multiple: false,
                                    requeue: true,
                                })
                                .await?;
                        } else {
                            delivery.ack(BasicAckOptions::default()).await?;
                        }
                    }
                    QueueItem::ConstructProof(item) => {
                        info!("Got construct proof item: {:?}", item);
                        if let Err(e) = self.handle_construct_proof(item.clone()).await {
                            error!("Error: {}", e);
                            debug!("Republishing construct proof item");
                            self.queue
                                .publish(
                                    &QueueItem::ConstructProof(item),
                                    Some(delivery.properties.clone()),
                                )
                                .await?;
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

            let maybe_quorum_reached_event_fields = Self::get_event_fields_from_script(
                axelard_query_script_str,
                DesiredEventType::QuorumReached,
                item.poll_id.clone(),
                item.broadcast_created_at,
            )
            .await;

            match maybe_quorum_reached_event_fields {
                Ok(Some((quorum_reached_event, event_timestamp, block_height))) => {
                    info!("Found quorum reached event: {:?}", quorum_reached_event);

                    let mut attributes = Vec::new();
                    if let Some(attrs) = quorum_reached_event
                        .get("attributes")
                        .and_then(|v| v.as_array())
                    {
                        for attr in attrs {
                            if let (Some(key), Some(value)) = (
                                attr.get("key").and_then(|v| v.as_str()),
                                attr.get("value").and_then(|v| v.as_str()),
                            ) {
                                attributes.push(EventAttribute {
                                    key: key.to_string(),
                                    value: value.to_string(),
                                });
                            }
                        }
                    }

                    let react_to_wasm_quorum_reached_task = ReactToWasmEventTask {
                        common: CommonTaskFields {
                            id: uuid::Uuid::new_v4().to_string(),
                            chain: item.chain,
                            timestamp: event_timestamp.to_rfc3339(),
                            r#type: "REACT_TO_WASM_EVENT".to_string(),
                            meta: None,
                        },
                        task: ReactToWasmEventTaskFields {
                            event: WasmEvent {
                                attributes,
                                r#type: "wasm-quorum_reached".to_string(),
                            },
                            height: block_height.parse::<u64>().unwrap_or(0),
                        },
                    };

                    let task_json = serde_json::to_string(&react_to_wasm_quorum_reached_task)?;

                    self.database
                        .upsert(
                            &react_to_wasm_quorum_reached_task.common.id,
                            &react_to_wasm_quorum_reached_task.common.chain,
                            event_timestamp,
                            crate::gmp_types::TaskKind::ReactToWasmEvent,
                            Some(&task_json),
                        )
                        .await?;

                    info!(
                        "Inserted ReactToWasmEvent task with ID: {}",
                        react_to_wasm_quorum_reached_task.common.id
                    );

                    return Ok(());
                }
                Ok(None) => {
                    warn!("No quorum reached event found on page {}", page);
                }
                Err(e) => {
                    // Check if this is the "too old timestamp" error - if so, stop searching
                    if e.to_string()
                        .contains("Timestamp is less than message timestamp")
                    {
                        warn!(
                            "Reached transactions older than broadcast time, stopping search at page {}",
                            page
                        );
                        break;
                    } else {
                        return Err(e);
                    }
                }
            }
        }

        Err(anyhow::anyhow!("No quorum reached event found"))
    }

    async fn handle_construct_proof(&self, item: ConstructProofItem) -> Result<(), anyhow::Error> {
        let initial_axelard_query_script_str = format!(
            "axelard query txs --events 'wasm-signing_completed._contract_address={}' --node {} --output json --limit 1",
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

            let maybe_signing_completed_event_fields = Self::get_event_fields_from_script(
                axelard_query_script_str,
                DesiredEventType::SigningCompleted,
                item.session_id.clone(),
                item.broadcast_created_at,
            )
            .await;

            match maybe_signing_completed_event_fields {
                Ok(Some((signing_completed_event, event_timestamp, _block_height))) => {
                    info!(
                        "Found signing completed event: {:?}",
                        signing_completed_event
                    );

                    let axelard_query_command = format!(
                        "axelard query wasm contract-state smart {} '{}' --node {} --output json",
                        "axelar1ys83sedjffmqh70aksejmx3fy3q2d7twm3msurk7wn3l6nkwxp0sfelzhl",
                        format!(
                            "{{ \"proof\": {{ \"multisig_session_id\": \"{}\" }} }}",
                            item.session_id
                        ),
                        self.rpc,
                    );

                    let axelard_query_result = tokio::process::Command::new("bash")
                        .arg("-c")
                        .arg(axelard_query_command.clone())
                        .output()
                        .await;

                    match axelard_query_result {
                        Ok(output) => {
                            if output.status.success() {
                                let output_str = String::from_utf8_lossy(&output.stdout);
                                let json_value = serde_json::from_str::<Value>(&output_str)?;

                                info!("Query executed successfully: {}", output_str);

                                let execute_data = json_value
                                    .get("data")
                                    .and_then(|v| v.get("status"))
                                    .and_then(|v| v.get("completed"))
                                    .and_then(|v| v.get("execute_data"))
                                    .and_then(|v| v.as_str())
                                    .unwrap_or("");

                                if execute_data.is_empty() {
                                    warn!("No execute data found");
                                }

                                // Base64 encode the execute_data
                                let encoded_execute_data =
                                    general_purpose::STANDARD.encode(execute_data);

                                let gateway_tx_task = GatewayTxTask {
                                    common: CommonTaskFields {
                                        id: uuid::Uuid::new_v4().to_string(),
                                        chain: item.chain,
                                        timestamp: event_timestamp.to_rfc3339(),
                                        r#type: "GATEWAY_TX".to_string(),
                                        meta: None,
                                    },
                                    task: GatewayTxTaskFields {
                                        execute_data: encoded_execute_data,
                                    },
                                };

                                let task_json = serde_json::to_string(&gateway_tx_task)?;

                                self.database
                                    .upsert(
                                        &gateway_tx_task.common.id,
                                        &gateway_tx_task.common.chain,
                                        event_timestamp,
                                        crate::gmp_types::TaskKind::GatewayTx,
                                        Some(&task_json),
                                    )
                                    .await?;

                                info!(
                                    "Inserted GatewayTx task with ID: {}",
                                    gateway_tx_task.common.id
                                );

                                return Ok(());
                            } else {
                                error!("Query failed: {}", axelard_query_command);
                            }
                        }
                        Err(e) => {
                            error!("Error: {}", e);
                        }
                    }
                }
                Ok(None) => {
                    warn!("No signing completed event found on page {}", page);
                }
                Err(e) => {
                    if e.to_string()
                        .contains("Timestamp is less than message timestamp")
                    {
                        warn!(
                            "Reached transactions older than broadcast time, stopping search at page {}",
                            page
                        );
                        break;
                    } else {
                        return Err(e);
                    }
                }
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

    async fn get_event_fields_from_script(
        axelard_query_script_str: String,
        desired_event_type: DesiredEventType,
        item_desired_id: String,
        message_timestamp: DateTime<Utc>,
    ) -> Result<Option<(Value, DateTime<Utc>, String)>, anyhow::Error> {
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
                        let mut found_newer_tx = false;
                        for tx in txs {
                            if let Some(timestamp) = tx.get("timestamp").and_then(|v| v.as_str()) {
                                let tx_timestamp = DateTime::parse_from_rfc3339(timestamp)?;
                                // Allow 2 seconds tolerance for timestamp recording delays (different precision in local timestamps with blockchain ones))
                                let tolerance = chrono::Duration::seconds(2);
                                if tx_timestamp + tolerance < message_timestamp {
                                    continue; // Skip old transactions but keep checking the page
                                }
                                found_newer_tx = true;
                            }

                            let maybe_block_height = tx.get("height").and_then(|v| v.as_str());

                            if let Some(logs) = tx.get("logs").and_then(|v| v.as_array()) {
                                for log in logs {
                                    if let Some(tx_events) =
                                        log.get("events").and_then(|v| v.as_array())
                                    {
                                        for event in tx_events {
                                            if let Some(event_type_val) =
                                                event.get("type").and_then(|v| v.as_str())
                                            {
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

                                                    if event_id == item_desired_id {
                                                        info!("ID match found! Returning event.");
                                                        let tx_timestamp = if let Some(timestamp) =
                                                            tx.get("timestamp")
                                                                .and_then(|v| v.as_str())
                                                        {
                                                            DateTime::parse_from_rfc3339(timestamp)?
                                                                .into()
                                                        } else {
                                                            message_timestamp
                                                        };
                                                        return Ok(Some((
                                                            event.clone(),
                                                            tx_timestamp,
                                                            maybe_block_height
                                                                .unwrap_or("0")
                                                                .to_string(),
                                                        )));
                                                    }
                                                }
                                            }
                                        }
                                    }
                                }
                            }
                        }

                        // If we didn't find any transactions newer than broadcast time on this page,
                        // stop searching further pages (they'll be even older)
                        if !found_newer_tx {
                            return Err(anyhow::anyhow!(
                                "Timestamp is less than message timestamp"
                            ));
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

#[cfg(test)]
mod tests {
    use base64::{Engine, engine::general_purpose};
    use serde_json::Value;

    #[tokio::test]
    async fn test_proof_query() {
        let axelard_query_command = format!(
            "axelard query wasm contract-state smart {} '{}' --node {} --output json",
            "axelar1ys83sedjffmqh70aksejmx3fy3q2d7twm3msurk7wn3l6nkwxp0sfelzhl",
            format!(
                "{{ \"proof\": {{ \"multisig_session_id\": \"{}\" }} }}",
                "22128"
            ),
            "http://devnet-amplifier.axelar.dev:26657",
        );

        let axelard_query_result = tokio::process::Command::new("bash")
            .arg("-c")
            .arg(axelard_query_command.clone())
            .output()
            .await;

        println!("{:?}", axelard_query_result);
        assert!(axelard_query_result.is_ok());

        let json_value = serde_json::from_str::<Value>(
            r#"{"data":{"unsigned_tx_hash":"343b170eea170676f5db8381af07ea228edb5c7097b210de25bd9f382befe9b4","status":{"completed":{"execute_data":"12000022000000002400000000202900008085614000000000030d406840000000000026ac73008114aedcfbaf02217eb9265a4b67534354e4768363c3831466f26a03e4cd3442da9149468cfd898f788944c8f3e0107321030543653008b6a4eb09b34231bc1b800422053aa558e4088b141cfb632af40ad674473045022100db70c761ca6fccdc20ba1a96ba19e986535eef73ca61f4e12239b91b9191492a02205c91d55e4b1f349c2c3e7b72a43da8ce36e31ae97fb48ba1108e3af49db861d48114e9fd61a1ce3a3a9ec0e140c0d1f689953bcbca6ae1f1f9ea7c04747970657d0570726f6f66e1ea7c10756e7369676e65645f74785f686173687d4033343362313730656561313730363736663564623833383161663037656132323865646235633730393762323130646532356264396633383262656665396234e1ea7c0c736f757263655f636861696e7d066178656c6172e1ea7c0a6d6573736167655f69647d493078613932633237326330383735366530653965643664353063326466636164346234303439346433376466656235633236633236343439616562313730623430362d343732313238e1f1"}}}}"#,
        )
        .unwrap();

        let output = axelard_query_result.unwrap();
        let actual_json: Value = serde_json::from_slice(&output.stdout).unwrap();

        assert_eq!(json_value, actual_json);

        let execute_data = json_value
            .get("data")
            .and_then(|v| v.get("status"))
            .and_then(|v| v.get("completed"))
            .and_then(|v| v.get("execute_data"))
            .and_then(|v| v.as_str())
            .unwrap_or("");

        // Base64 encode the execute_data
        let encoded_execute_data = general_purpose::STANDARD.encode(execute_data);

        println!("Encoded execute data: {}", encoded_execute_data);
        assert!(!execute_data.is_empty());
    }
}
