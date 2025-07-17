use actix_web::{App, Error, HttpResponse, HttpServer, error, get, post, web};
use base64::{Engine as _, engine::general_purpose};
use chrono::{DateTime, Utc};
use futures::StreamExt;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use sha3::{Digest, Keccak256};
use std::collections::HashMap;
use tracing::{debug, info, warn};
use uuid::Uuid;

use crate::{
    TasksModel,
    event_handler::handle_call_or_gas_credit_event,
    gmp_types::{Event, PostEventResponse, PostEventResult, StorePayloadResult, Task},
    models::{
        broadcasts::{BroadcastStatus, BroadcastsModel},
        events::EventsModel,
        payloads::PayloadsModel,
        queries::QueriesModel,
    },
    utils::{execute_script_file, parse_task},
};

pub struct Server {
    pub port: u16,
    pub address: String,
    pub tasks_model: TasksModel,
    pub events_model: EventsModel,
    pub broadcasts_model: BroadcastsModel,
    pub payloads_model: PayloadsModel,
    pub queries_model: QueriesModel,
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

    // let broadcast_id = match &broadcast_request {
    //     BroadcastRequestType::TicketCreate { .. } => {
    //         format!("broadcast_ticket_{}", Uuid::new_v4().simple())
    //     }
    //     BroadcastRequestType::VerifyMessages { .. } => {
    //         format!("broadcast_verify_{}", Uuid::new_v4().simple())
    //     }
    //     BroadcastRequestType::RouteIncomingMessages { .. } => {
    //         format!("broadcast_route_{}", Uuid::new_v4().simple())
    //     }
    // };

    let broadcast_json = serde_json::to_string(&broadcast_request).map_err(|e| {
        error::ErrorInternalServerError(format!("Failed to serialize broadcast: {}", e))
    })?;

    let broadcast_id = Uuid::new_v4().simple().to_string();

    broadcasts_model
        .insert(&broadcast_id, &broadcast_json, BroadcastStatus::Received)
        .await
        .map_err(|e| error::ErrorInternalServerError(e.to_string()))?;

    let broadcast_id_clone = broadcast_id.clone();
    let contract_address_clone = contract_address.clone();
    let broadcast_json_clone = broadcast_json.clone();
    let broadcasts_model_clone = broadcasts_model.get_ref().clone();

    tokio::spawn(async move {
        info!("Executing broadcast transaction for {}", broadcast_id_clone);

        match execute_script_file(
            "scripts/execute_broadcast.sh",
            &[
                &broadcast_id_clone,
                &contract_address_clone,
                &broadcast_json_clone,
            ],
        )
        .await
        {
            Ok(output) => {
                info!(
                    "Transaction execution output for broadcast {}: {}",
                    broadcast_id_clone, output
                );

                if let Ok(script_result) = serde_json::from_str::<serde_json::Value>(&output) {
                    let script_status = script_result
                        .get("status")
                        .and_then(|v| v.as_str())
                        .unwrap_or("FAILED");

                    match script_status {
                        "SUCCESS" => {
                            let tx_hash = script_result
                                .get("tx_hash")
                                .and_then(|v| v.as_str())
                                .unwrap_or("");

                            info!(
                                "Transaction successful for broadcast {}, tx_hash: {}",
                                broadcast_id_clone, tx_hash
                            );

                            if !tx_hash.is_empty() {
                                if let Err(e) = broadcasts_model_clone
                                    .upsert(
                                        &broadcast_id_clone,
                                        &broadcast_json_clone,
                                        BroadcastStatus::Success,
                                        Some(tx_hash),
                                        None,
                                    )
                                    .await
                                {
                                    warn!("Failed to update transaction hash: {}", e);
                                }
                            } else {
                                warn!("Transaction successful but no tx hash found");
                            }
                        }
                        "FAILED" => {
                            let error_msg = script_result
                                .get("error")
                                .and_then(|v| v.as_str())
                                .unwrap_or("Transaction failed");

                            warn!(
                                "Transaction failed for broadcast {}: {}",
                                broadcast_id_clone, error_msg
                            );

                            if let Err(e) = broadcasts_model_clone
                                .upsert(
                                    &broadcast_id_clone,
                                    &broadcast_json_clone,
                                    BroadcastStatus::Failed,
                                    None,
                                    Some(error_msg),
                                )
                                .await
                            {
                                warn!("Failed to update broadcast status to FAILED: {}", e);
                            }
                        }
                        _ => {
                            warn!(
                                "Unknown script status for broadcast {}: {}",
                                broadcast_id_clone, script_status
                            );
                        }
                    }
                } else {
                    warn!(
                        "Failed to parse script output as JSON for broadcast {}: {}",
                        broadcast_id_clone, output
                    );

                    if let Err(e) = broadcasts_model_clone
                        .upsert(
                            &broadcast_id_clone,
                            &broadcast_json_clone,
                            BroadcastStatus::Failed,
                            None,
                            Some(&format!("Failed to parse script output as JSON")),
                        )
                        .await
                    {
                        warn!("Failed to update broadcast status to FAILED: {}", e);
                    }
                }
            }
            Err(e) => {
                warn!(
                    "Transaction execution script failed for broadcast {}: {}",
                    broadcast_id_clone, e
                );

                if let Err(e) = broadcasts_model_clone
                    .upsert(
                        &broadcast_id_clone,
                        &broadcast_json_clone,
                        BroadcastStatus::Failed,
                        None,
                        Some(&format!("Script execution failed: {}", e)),
                    )
                    .await
                {
                    warn!("Failed to update broadcast error: {}", e);
                }
            }
        }
    });

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
    let (_contract_address, broadcast_id) = path.into_inner();

    let broadcast_with_status = match broadcasts_model
        .find_with_status(broadcast_id.as_str())
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

    if broadcast_with_status.is_none() {
        let error_response = serde_json::json!({
            "error": "Broadcast not found"
        });
        return Ok(HttpResponse::NotFound().json(error_response));
    }

    let broadcast_with_status = broadcast_with_status.unwrap();

    let response = match broadcast_with_status.status {
        BroadcastStatus::Received => {
            serde_json::json!({
                "status": "RECEIVED"
            })
        }
        BroadcastStatus::Success => match &broadcast_with_status.tx_hash {
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
            if let Some(error_msg) = &broadcast_with_status.error {
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

#[post("/payloads")]
async fn post_payloads(
    payloads_model: web::Data<PayloadsModel>,
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

    let mut hasher = Keccak256::new();
    hasher.update(&body);
    let result = hasher.finalize();
    let hash = format!("0x{}", hex::encode(result));

    let payload_str = general_purpose::STANDARD.encode(&body);

    payloads_model
        .upsert(&hash, &payload_str)
        .await
        .map_err(|e| error::ErrorInternalServerError(e.to_string()))?;

    let response = StorePayloadResult { keccak256: hash };

    info!("Stored payload with hash: {}", response.keccak256);
    Ok(HttpResponse::Ok().json(response))
}

#[get("/payloads/0x{hash}")]
async fn get_payload(
    hash: web::Path<String>,
    payloads_model: web::Data<PayloadsModel>,
) -> Result<HttpResponse, Error> {
    let full_hash = format!("0x{}", hash);
    let payload_base64 = payloads_model
        .find(&full_hash)
        .await
        .map_err(|e| error::ErrorInternalServerError(e.to_string()))?;

    match payload_base64 {
        Some(payload_str) => {
            let payload_bytes = general_purpose::STANDARD
                .decode(&payload_str)
                .map_err(|e| {
                    error::ErrorInternalServerError(format!("Failed to decode payload: {}", e))
                })?;

            Ok(HttpResponse::Ok()
                .content_type("application/octet-stream")
                .body(payload_bytes))
        }
        None => Ok(HttpResponse::NotFound().json(serde_json::json!({
            "error": "Payload not found"
        }))),
    }
}

#[post("/contracts/{contract_address}/queries")]
async fn post_queries(
    contract_address: web::Path<String>,
    queries_model: web::Data<QueriesModel>,
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

    let query = serde_json::from_slice::<Value>(&body)?;

    queries_model
        .upsert(&contract_address, &serde_json::to_string(&query).unwrap())
        .await
        .map_err(|e| error::ErrorInternalServerError(e.to_string()))?;

    info!(
        "Stored query for contract: {}",
        contract_address.into_inner()
    );
    Ok(HttpResponse::Ok().json(serde_json::json!({})))
}

impl Server {
    pub fn new(
        port: u16,
        address: String,
        tasks_model: TasksModel,
        events_model: EventsModel,
        broadcasts_model: BroadcastsModel,
        payloads_model: PayloadsModel,
        queries_model: QueriesModel,
    ) -> Self {
        Self {
            port,
            address,
            tasks_model,
            events_model,
            broadcasts_model,
            payloads_model,
            queries_model,
        }
    }

    pub async fn run(self) -> anyhow::Result<()> {
        let addr = format!("{}:{}", self.address, self.port);

        HttpServer::new(move || {
            App::new()
                .app_data(web::Data::new(self.tasks_model.clone()))
                .app_data(web::Data::new(self.events_model.clone()))
                .app_data(web::Data::new(self.broadcasts_model.clone()))
                .app_data(web::Data::new(self.payloads_model.clone()))
                .app_data(web::Data::new(self.queries_model.clone()))
                .service(get_tasks)
                .service(post_task)
                .service(address_broadcast)
                .service(get_broadcast)
                .service(post_events)
                .service(post_payloads)
                .service(get_payload)
                .service(post_queries)
        })
        .bind(addr)?
        .run()
        .await
        .map_err(|e| anyhow::anyhow!("Failed to start server: {}", e))?;

        Ok(())
    }
}
