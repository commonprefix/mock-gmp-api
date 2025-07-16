use actix_web::{App, Error, HttpResponse, HttpServer, error, get, post, web};
use chrono::{DateTime, Utc};
use futures::StreamExt;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::HashMap;
use std::fs;
use tracing::{debug, info, warn};
use uuid::Uuid;

use crate::{
    TasksModel,
    event_handler::handle_call_or_gas_credit_event,
    gmp_types::{Event, PostEventResponse, PostEventResult, Task},
    models::{
        broadcasts::{BroadcastStatus, BroadcastsModel},
        events::EventsModel,
    },
    utils::{execute_script_file, parse_task},
};

pub struct Server {
    pub port: u16,
    pub address: String,
    pub tasks_model: TasksModel,
    pub events_model: EventsModel,
    pub broadcasts_model: BroadcastsModel,
}

const MAX_SIZE: usize = 262_144; // max payload size is 256k

#[derive(Serialize, Deserialize, Debug)]
struct EventsRequest {
    events: Vec<Event>,
}

#[derive(Serialize, Deserialize, Debug)]
#[serde(untagged)]
enum BroadcastRequestType {
    TicketCreate {
        #[serde(rename = "TicketCreate")]
        ticket_create: serde_json::Value,
    },
    VerifyMessages {
        #[serde(rename = "VerifyMessages")]
        verify_messages: Vec<serde_json::Value>,
    },
    RouteIncomingMessages {
        #[serde(rename = "RouteIncomingMessages")]
        route_incoming_messages: Vec<serde_json::Value>,
    },
}

#[derive(Serialize, Deserialize, Debug)]
struct BroadcastResponse {
    #[serde(rename = "broadcastID")]
    broadcast_id: String,
}

#[post("/contracts/{contract_address}/broadcasts")]
async fn address_broadcast(
    contract_address: web::Path<String>,
    broadcasts_model: web::Data<BroadcastsModel>,
    mut payload: web::Payload,
) -> Result<HttpResponse, Error> {
    let mut body = web::BytesMut::new();
    while let Some(chunk) = payload.next().await {
        let chunk = chunk?;
        if (body.len() + chunk.len()) > MAX_SIZE {
            return Err(error::ErrorBadRequest("overflow"));
        }
        body.extend_from_slice(&chunk);
    }

    let broadcast_request: BroadcastRequestType = serde_json::from_slice(&body)
        .map_err(|e| error::ErrorBadRequest(format!("Invalid broadcast request: {}", e)))?;

    info!(
        "Broadcast request: {:?} to contract: {}",
        broadcast_request, contract_address
    );

    let broadcast_id = match &broadcast_request {
        BroadcastRequestType::TicketCreate { .. } => {
            format!("broadcast_ticket_{}", Uuid::new_v4().simple())
        }
        BroadcastRequestType::VerifyMessages { .. } => {
            format!("broadcast_verify_{}", Uuid::new_v4().simple())
        }
        BroadcastRequestType::RouteIncomingMessages { .. } => {
            format!("broadcast_route_{}", Uuid::new_v4().simple())
        }
    };

    // Store the broadcast with RECEIVED status
    let broadcast_json = serde_json::to_string(&broadcast_request).map_err(|e| {
        error::ErrorInternalServerError(format!("Failed to serialize broadcast: {}", e))
    })?;

    broadcasts_model
        .insert(&broadcast_id, &broadcast_json, BroadcastStatus::Received)
        .await
        .map_err(|e| error::ErrorInternalServerError(e.to_string()))?;

    let response = BroadcastResponse {
        broadcast_id: broadcast_id.clone(),
    };

    info!("Generated broadcast response: {:?}", response);
    Ok(HttpResponse::Ok().json(response))
}

#[get("/contracts/{contract_address}/broadcasts/{broadcast_id}")]
async fn get_broadcast(
    path: web::Path<(String, String)>,
    broadcasts_model: web::Data<BroadcastsModel>,
) -> Result<HttpResponse, Error> {
    let (contract_address, broadcast_id) = path.into_inner();
    let maybe_broadcast_with_tx = match broadcasts_model
        .find_with_tx_hash(broadcast_id.as_str())
        .await
    {
        Ok(result) => result,
        Err(e) => {
            let error_response = serde_json::json!({
                "error": format!("Database error: {}", e)
            });
            return Ok(HttpResponse::InternalServerError().json(error_response));
        }
    };

    if maybe_broadcast_with_tx.is_none() {
        let error_response = serde_json::json!({
            "error": "Broadcast not found"
        });
        return Ok(HttpResponse::NotFound().json(error_response));
    }

    let mut broadcast_with_tx = maybe_broadcast_with_tx.unwrap();

    let broadcast_tx_hash = broadcast_with_tx
        .tx_hash
        .as_deref()
        .unwrap_or("no_tx_hash_found");

    debug!(
        "Found broadcast with status: {:?}, tx_hash: {}",
        broadcast_with_tx.status, broadcast_tx_hash
    );

    // Only query blockchain if status is RECEIVED
    if broadcast_with_tx.status == BroadcastStatus::Received {
        info!(
            "Status is RECEIVED, querying blockchain for tx: {}",
            broadcast_tx_hash
        );

        match execute_script_file(
            "scripts/tx_status.sh",
            &[&broadcast_id, &contract_address, broadcast_tx_hash],
        )
        .await
        {
            Ok(output) => {
                info!("Script file output:\n{}", output);

                match fs::read_to_string("script_result.json") {
                    Ok(result_content) => match serde_json::from_str::<Value>(&result_content) {
                        Ok(script_result) => {
                            let script_status =
                                script_result["status"].as_str().unwrap_or("FAILED");

                            match script_status {
                                "SUCCESS" => {
                                    info!("Transaction successful, updating status to SUCCESS");
                                    if let Err(e) = broadcasts_model
                                        .update_status(&broadcast_id, BroadcastStatus::Success)
                                        .await
                                    {
                                        warn!("Failed to update status to SUCCESS: {}", e);
                                    } else {
                                        broadcast_with_tx.status = BroadcastStatus::Success;
                                    }
                                }
                                "FAILED" => {
                                    let error_msg = script_result["error"]
                                        .as_str()
                                        .unwrap_or("Transaction failed");
                                    info!(
                                        "Transaction failed: {}, updating status to FAILED",
                                        error_msg
                                    );
                                    if let Err(e) = broadcasts_model
                                        .update_status(&broadcast_id, BroadcastStatus::Failed)
                                        .await
                                    {
                                        warn!("Failed to update status to FAILED: {}", e);
                                    } else {
                                        broadcast_with_tx.status = BroadcastStatus::Failed;
                                        broadcast_with_tx.error = Some(error_msg.to_string());
                                    }
                                    if let Err(e) = broadcasts_model
                                        .update_error(&broadcast_id, error_msg)
                                        .await
                                    {
                                        warn!("Failed to update error: {}", e);
                                    }
                                }
                                "RECEIVED" => {
                                    info!("Transaction still pending, keeping status as RECEIVED");
                                    // Status remains RECEIVED - no database update needed
                                    // broadcast_with_tx.status is already RECEIVED
                                }
                                _ => {
                                    warn!(
                                        "Unknown script status: {}, treating as FAILED",
                                        script_status
                                    );
                                    let error_msg = "Unknown transaction status";
                                    if let Err(e) = broadcasts_model
                                        .update_status(&broadcast_id, BroadcastStatus::Failed)
                                        .await
                                    {
                                        warn!("Failed to update status to FAILED: {}", e);
                                    } else {
                                        broadcast_with_tx.status = BroadcastStatus::Failed;
                                        broadcast_with_tx.error = Some(error_msg.to_string());
                                    }
                                }
                            }
                        }
                        Err(e) => {
                            warn!("Failed to parse script result JSON: {}", e);
                        }
                    },
                    Err(e) => {
                        warn!("Failed to read script_result.json: {}", e);
                    }
                }
                let _ = fs::remove_file("script_result.json");
            }
            Err(e) => {
                warn!("Script file failed: {}", e);
            }
        }
    } else {
        info!(
            "Status is {:?}, returning current status without querying blockchain",
            broadcast_with_tx.status
        );
    }

    let response = match broadcast_with_tx.status {
        BroadcastStatus::Received => {
            serde_json::json!({
                "status": "RECEIVED"
            })
        }
        BroadcastStatus::Success => match &broadcast_with_tx.tx_hash {
            Some(tx_hash) => serde_json::json!({
                "status": "SUCCESS",
                "txHash": tx_hash
            }),
            None => serde_json::json!({
                "status": "FAILED",
                "error": "Success status but no transaction hash available"
            }),
        },
        BroadcastStatus::Failed => {
            if let Some(error_msg) = &broadcast_with_tx.error {
                serde_json::json!({
                    "status": "FAILED",
                    "error": error_msg
                })
            } else {
                serde_json::json!({
                    "status": "FAILED",
                    "error": "Transaction execution failed"
                })
            }
        }
    };

    Ok(HttpResponse::Ok().json(response))
}

#[post("/chains/{chain}/events")]
async fn post_events(
    chain: web::Path<String>,
    events_model: web::Data<EventsModel>,
    tasks_model: web::Data<TasksModel>,
    mut payload: web::Payload,
) -> Result<HttpResponse, Error> {
    let mut body = web::BytesMut::new();
    while let Some(chunk) = payload.next().await {
        let chunk = chunk?;
        if (body.len() + chunk.len()) > MAX_SIZE {
            return Err(error::ErrorBadRequest("overflow"));
        }
        body.extend_from_slice(&chunk);
    }

    let events_request: EventsRequest = serde_json::from_slice(&body)
        .map_err(|e| error::ErrorBadRequest(format!("Invalid JSON: {}", e)))?;

    debug!(
        "Received {} events for chain: {}",
        events_request.events.len(),
        chain
    );

    let mut results: Vec<PostEventResult> = Vec::new();

    for (index, event) in events_request.events.iter().enumerate() {
        debug!("Event {}: {:?}", index, event);

        let (event_id, event_type_str, timestamp) = event.common_fields();

        // Check that no other event with the same ID exists
        let maybe_event_with_same_id = events_model
            .find(event_id)
            .await
            .map_err(|e| error::ErrorInternalServerError(e.to_string()))?;
        if maybe_event_with_same_id.is_some() {
            results.push(PostEventResult {
                status: "ACCEPTED".to_string(),
                index,
                error: None,
                retriable: None,
            });
            warn!("Event with same ID already exists: {:?}", event);
            continue;
        }

        let event_json_str = match serde_json::to_string(event) {
            Ok(json_str) => json_str,
            Err(e) => {
                results.push(PostEventResult {
                    status: "error".to_string(),
                    index,
                    error: Some(format!("Failed to serialize event: {}", e)),
                    retriable: Some(true),
                });
                continue;
            }
        };

        let parsed_timestamp = match timestamp.parse::<DateTime<Utc>>() {
            Ok(ts) => ts,
            Err(e) => {
                results.push(PostEventResult {
                    status: "error".to_string(),
                    index,
                    error: Some(format!("Invalid timestamp format: {}", e)),
                    retriable: Some(true),
                });
                continue;
            }
        };

        if event_type_str == "CALL" || event_type_str == "GAS_CREDIT" {
            handle_call_or_gas_credit_event(
                event.clone(),
                &events_model,
                &tasks_model,
                &chain,
                event_type_str,
            )
            .await
            .map_err(|e| error::ErrorInternalServerError(e.to_string()))?;
        }

        // insert instead of upsert because we already checked that ID does not exist
        match events_model
            .insert(
                event_id,
                parsed_timestamp,
                event.event_type(),
                &event_json_str,
                &event.message_id(),
            )
            .await
        {
            Ok(_) => {
                results.push(PostEventResult {
                    status: "ACCEPTED".to_string(),
                    index,
                    error: None,
                    retriable: None,
                });
            }
            Err(e) => {
                results.push(PostEventResult {
                    status: "error".to_string(),
                    index,
                    error: Some(format!("Database error: {}", e)),
                    retriable: Some(true),
                });
            }
        }
    }

    let response = PostEventResponse { results };

    info!("Responding with: {:?}", response);
    Ok(HttpResponse::Ok().json(response))
}

#[post("/chains/{chain}/task")]
async fn post_task(
    db: web::Data<TasksModel>,
    mut payload: web::Payload,
) -> Result<HttpResponse, Error> {
    let mut body = web::BytesMut::new();
    while let Some(chunk) = payload.next().await {
        let chunk = chunk?;
        if (body.len() + chunk.len()) > MAX_SIZE {
            return Err(error::ErrorBadRequest("overflow"));
        }
        body.extend_from_slice(&chunk);
    }

    let json_value = serde_json::from_slice::<Value>(&body)?;
    let task = parse_task(&json_value).map_err(|e| error::ErrorBadRequest(e.to_string()))?;

    if matches!(task, Task::Unknown(_)) {
        return Err(error::ErrorBadRequest("Cannot store unknown tasks"));
    }

    let common = task.common();

    let timestamp = common
        .timestamp
        .parse::<DateTime<Utc>>()
        .map_err(|e| error::ErrorBadRequest(format!("Invalid timestamp format: {}", e)))?;

    db.upsert(
        &common.id,
        &common.chain,
        timestamp,
        task.kind(),
        Some(&serde_json::to_string(&json_value).unwrap()),
    )
    .await
    .map_err(|e| error::ErrorInternalServerError(e.to_string()))?;

    info!("task upserted: {:?}", task.id());

    Ok(HttpResponse::Ok().json(task))
}

#[get("/chains/{chain}/tasks")]
async fn get_tasks(
    db: web::Data<TasksModel>,
    chain: web::Path<String>,
    query: web::Query<HashMap<String, String>>,
) -> Result<HttpResponse, Error> {
    let after = query.get("after").map(|s| s.as_str());
    if after.is_some() {
        debug!("Requesting tasks after: {:?}", after.unwrap());
    } else {
        debug!("Requesting all tasks");
    }

    let raw_tasks = db
        .get_tasks(&chain, after)
        .await
        .map_err(|e| error::ErrorInternalServerError(e.to_string()))?;

    let response = serde_json::json!({
        "tasks": raw_tasks
    });

    info!("Returning {} tasks", raw_tasks.len());

    Ok(HttpResponse::Ok().json(response))
}

impl Server {
    pub fn new(
        port: u16,
        address: String,
        tasks_model: TasksModel,
        events_model: EventsModel,
        broadcasts_model: BroadcastsModel,
    ) -> Self {
        Self {
            port,
            address,
            tasks_model,
            events_model,
            broadcasts_model,
        }
    }

    pub async fn run(self) -> anyhow::Result<()> {
        let addr = format!("{}:{}", self.address, self.port);

        HttpServer::new(move || {
            App::new()
                .app_data(web::Data::new(self.tasks_model.clone()))
                .app_data(web::Data::new(self.events_model.clone()))
                .app_data(web::Data::new(self.broadcasts_model.clone()))
                .service(get_tasks)
                .service(post_task)
                .service(address_broadcast)
                .service(get_broadcast)
                .service(post_events)
        })
        .bind(addr)?
        .run()
        .await
        .map_err(|e| anyhow::anyhow!("Failed to start server: {}", e))?;

        Ok(())
    }
}
