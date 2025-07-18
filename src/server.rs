use actix_web::{App, Error, HttpResponse, HttpServer, error, get, post, web};
use base64::{Engine as _, engine::general_purpose};
use chrono::{DateTime, Utc};
use futures::StreamExt;
use once_cell::sync::Lazy;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use sha3::{Digest, Keccak256};
use std::collections::HashMap;
use std::env;
use tokio::sync::Mutex;
use tracing::{debug, error, info, warn};
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
    utils::parse_task,
};

static AXELARD_LOCK: Lazy<Mutex<()>> = Lazy::new(|| Mutex::new(()));

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
struct BroadcastPostResponse {
    #[serde(rename = "broadcastID")]
    broadcast_id: String,
}

#[derive(Serialize, Deserialize, Debug)]
struct BroadcastGetResponse {
    status: BroadcastStatus,
    #[serde(rename = "txHash")]
    #[serde(skip_serializing_if = "Option::is_none")]
    tx_hash: Option<String>,
    #[serde(rename = "txEvents")]
    #[serde(skip_serializing_if = "Option::is_none")]
    tx_events: Option<Vec<String>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    error: Option<String>,
    #[serde(rename = "completedAt")]
    #[serde(skip_serializing_if = "Option::is_none")]
    completed_at: Option<DateTime<Utc>>,
    #[serde(rename = "receivedAt")]
    #[serde(skip_serializing_if = "Option::is_none")]
    received_at: Option<DateTime<Utc>>,
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

    let broadcast_request: Value = serde_json::from_slice(&body)
        .map_err(|e| error::ErrorBadRequest(format!("Invalid broadcast request: {}", e)))?;

    info!(
        "Broadcast request: {:?} to contract: {}",
        broadcast_request, contract_address
    );

    let broadcast_json = serde_json::to_string(&broadcast_request).map_err(|e| {
        error::ErrorInternalServerError(format!("Failed to serialize broadcast: {}", e))
    })?;

    let broadcast_id = Uuid::new_v4().simple().to_string();

    broadcasts_model
        .insert(
            &broadcast_id,
            &contract_address,
            &broadcast_json,
            BroadcastStatus::Received,
        )
        .await
        .map_err(|e| error::ErrorInternalServerError(e.to_string()))?;

    let broadcast_id_clone = broadcast_id.clone();
    let contract_address_clone = contract_address.clone();
    let broadcast_json_clone = broadcast_json.clone();
    let broadcasts_model_clone = broadcasts_model.clone();

    info!("Executing broadcast transaction for {}", broadcast_id_clone);

    let axelard_execute_script_str = format!(
        "axelard tx wasm execute {} \
        '{}' \
  --from {} \
  --keyring-backend test \
  --node {} \
  --chain-id {} \
  --gas-prices 0.00005uamplifier \
  --gas auto --gas-adjustment 1.5 --output json",
        contract_address_clone,
        broadcast_json_clone,
        env::var("AXELAR_KEY_NAME").unwrap(),
        env::var("AXELAR_RPC").unwrap(),
        env::var("CHAIN_ID").unwrap()
    );

    info!(
        "Waiting for axelard lock for broadcast {}",
        broadcast_id_clone
    );
    let _guard = AXELARD_LOCK.lock().await;

    let axelard_execute_script = tokio::process::Command::new("bash")
        .arg("-c")
        .arg(axelard_execute_script_str)
        .output()
        .await;

    match axelard_execute_script {
        Ok(output) => {
            let output_str = String::from_utf8_lossy(&output.stdout);
            info!(
                "Transaction execution output for broadcast {}: {}",
                broadcast_id_clone, output_str
            );

            if output.status.success() {
                if let Ok(script_result) = serde_json::from_str::<Value>(&output_str) {
                    let code = script_result
                        .get("code")
                        .and_then(|v| v.as_i64())
                        .unwrap_or(-1);

                    if code == 0 {
                        let tx_hash = script_result
                            .get("txhash")
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
                                    &contract_address_clone,
                                    &broadcast_json_clone,
                                    BroadcastStatus::Success,
                                    Some(tx_hash),
                                    None,
                                )
                                .await
                            {
                                error!("Failed to update transaction hash: {}", e);
                            }
                        } else {
                            warn!("Transaction successful but no tx hash found");
                        }
                    } else {
                        let raw_log = script_result
                            .get("raw_log")
                            .and_then(|v| v.as_str())
                            .unwrap_or(&output_str);
                        warn!(
                            "Transaction failed for broadcast {}: {}",
                            broadcast_id_clone, raw_log
                        );

                        if let Err(e) = broadcasts_model_clone
                            .upsert(
                                &broadcast_id_clone,
                                &contract_address_clone,
                                &broadcast_json_clone,
                                BroadcastStatus::Failed,
                                None,
                                Some(raw_log),
                            )
                            .await
                        {
                            error!("Failed to update broadcast status to FAILED: {}", e);
                        }
                    }
                } else {
                    error!(
                        "Transaction execution returned non-JSON output for broadcast {}: {}",
                        broadcast_id_clone, output_str
                    );

                    if let Err(e) = broadcasts_model_clone
                        .upsert(
                            &broadcast_id_clone,
                            &contract_address_clone,
                            &broadcast_json_clone,
                            BroadcastStatus::Failed,
                            None,
                            Some(&output_str),
                        )
                        .await
                    {
                        error!("Failed to update broadcast status to FAILED: {}", e);
                    }
                }
            } else {
                let error_str = String::from_utf8_lossy(&output.stderr);
                error!(
                    "Transaction failed for broadcast {}: {}",
                    broadcast_id_clone, error_str
                );

                if let Err(e) = broadcasts_model_clone
                    .upsert(
                        &broadcast_id_clone,
                        &contract_address_clone,
                        &broadcast_json_clone,
                        BroadcastStatus::Failed,
                        None,
                        Some(&error_str),
                    )
                    .await
                {
                    error!("Failed to update broadcast status to FAILED: {}", e);
                }
            }
        }
        Err(e) => {
            error!(
                "Transaction execution script failed for broadcast {}: {}",
                broadcast_id_clone, e
            );

            if let Err(e) = broadcasts_model_clone
                .upsert(
                    &broadcast_id_clone,
                    &contract_address_clone,
                    &broadcast_json_clone,
                    BroadcastStatus::Failed,
                    None,
                    Some(&format!("Script execution failed: {}", e)),
                )
                .await
            {
                error!("Failed to update broadcast error: {}", e);
            }
        }
    }

    let response = BroadcastPostResponse { broadcast_id };

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
        .find_with_status_and_hash(broadcast_id.as_str())
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

    let response: BroadcastGetResponse = match broadcast_with_status.status {
        BroadcastStatus::Received => BroadcastGetResponse {
            status: BroadcastStatus::Received,
            tx_hash: None,
            tx_events: None,
            error: None,
            completed_at: None,
            received_at: None,
        },
        BroadcastStatus::Success => match &broadcast_with_status.tx_hash {
            Some(tx_hash) => BroadcastGetResponse {
                status: BroadcastStatus::Success,
                tx_hash: Some(tx_hash.clone()),
                tx_events: None,
                error: None,
                completed_at: None,
                received_at: None,
            },
            None => BroadcastGetResponse {
                status: BroadcastStatus::Failed,
                tx_hash: None,
                tx_events: None,
                error: Some("Success status but no transaction hash available".to_string()),
                completed_at: None,
                received_at: None,
            },
        },
        BroadcastStatus::Failed => {
            if let Some(error_msg) = &broadcast_with_status.error {
                BroadcastGetResponse {
                    status: BroadcastStatus::Failed,
                    tx_hash: None,
                    tx_events: None,
                    error: Some(error_msg.clone()),
                    completed_at: None,
                    received_at: None,
                }
            } else {
                BroadcastGetResponse {
                    status: BroadcastStatus::Failed,
                    tx_hash: None,
                    tx_events: None,
                    error: Some("Transaction execution failed".to_string()),
                    completed_at: None,
                    received_at: None,
                }
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

#[derive(Serialize, Deserialize, Debug)]
struct QueryPostResponse {
    #[serde(rename = "queryID")]
    query_id: String,
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

    let query_request: Value = serde_json::from_slice(&body)
        .map_err(|e| error::ErrorBadRequest(format!("Invalid query request: {}", e)))?;

    info!(
        "Query request: {:?} to contract: {}",
        query_request, contract_address
    );

    let query_json = serde_json::to_string(&query_request).map_err(|e| {
        error::ErrorInternalServerError(format!("Failed to serialize query: {}", e))
    })?;

    let query_id = Uuid::new_v4().simple().to_string();

    queries_model
        .insert(&query_id, &contract_address, &query_json)
        .await
        .map_err(|e| error::ErrorInternalServerError(e.to_string()))?;

    let query_id_clone = query_id.clone();
    let contract_address_clone = contract_address.clone();
    let query_json_clone = query_json.clone();
    let queries_model_clone = queries_model.clone();

    info!("Executing query for {}", query_id_clone);

    let axelard_query_command = format!(
        "axelard query wasm contract-state smart {} \
            '{}' \
            --node {} \
            --chain-id {} \
            --output json",
        contract_address_clone,
        query_json_clone,
        env::var("AXELAR_RPC").unwrap(),
        env::var("CHAIN_ID").unwrap()
    );

    let axelard_query_result = tokio::process::Command::new("bash")
        .arg("-c")
        .arg(axelard_query_command)
        .output()
        .await;

    match axelard_query_result {
        Ok(output) => {
            let output_str = String::from_utf8_lossy(&output.stdout);
            info!(
                "Query execution output for {}: {}",
                query_id_clone, output_str
            );

            if output.status.success() && !output_str.trim().is_empty() {
                if let Err(e) = queries_model_clone
                    .update_result(&query_id_clone, &output_str, None)
                    .await
                {
                    error!("Failed to update query result: {}", e);
                }
            } else {
                let error_str = if !output_str.trim().is_empty() {
                    &output_str
                } else {
                    &String::from_utf8_lossy(&output.stderr)
                };

                error!("Query failed for {}: {}", query_id_clone, error_str);

                if let Err(e) = queries_model_clone
                    .update_result(&query_id_clone, "", Some(error_str))
                    .await
                {
                    error!("Failed to update query error: {}", e);
                }
            }
        }
        Err(e) => {
            error!(
                "Query execution script failed for {}: {}",
                query_id_clone, e
            );

            if let Err(e) = queries_model_clone
                .update_result(
                    &query_id_clone,
                    "",
                    Some(&format!("Script execution failed: {}", e)),
                )
                .await
            {
                error!("Failed to update query error: {}", e);
            }
        }
    }

    let response = QueryPostResponse { query_id };

    info!("Generated query response: {:?}", response);
    Ok(HttpResponse::Ok().json(response))
}

#[get("/contracts/{contract_address}/queries/{query_id}")]
async fn get_query(
    path: web::Path<(String, String)>,
    queries_model: web::Data<QueriesModel>,
) -> Result<HttpResponse, Error> {
    let (_contract_address, query_id) = path.into_inner();

    let query_result = match queries_model.find_result(&query_id).await {
        Ok(q) => q,
        Err(e) => {
            let err = format!("Database error: {}", e);
            return Ok(HttpResponse::InternalServerError()
                .content_type("text/plain")
                .body(err));
        }
    };

    match query_result {
        Some((result, error)) => {
            if let Some(err_msg) = error {
                return Ok(HttpResponse::BadRequest()
                    .content_type("text/plain")
                    .body(err_msg));
            }

            if !result.is_empty() {
                let mut body = result.clone();
                if body.contains("\"interchain_transfer\"") && !body.contains("\"token_id\"") {
                    if let Some(start) = body.find("\"interchain_transfer\"") {
                        if let Some(brace) = body[start..].find('{') {
                            let insert_at = start + brace + 1;
                            body = format!(
                                "{}\"token_id\":null,{}",
                                &body[..insert_at],
                                &body[insert_at..]
                            );
                        }
                    }
                }
                return Ok(HttpResponse::Ok().content_type("text/plain").body(body));
            }

            Ok(HttpResponse::Accepted()
                .content_type("text/plain")
                .body("Query processing"))
        }

        None => Ok(HttpResponse::NotFound()
            .content_type("text/plain")
            .body("Query not found")),
    }
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
                .service(get_query)
        })
        .bind(addr)?
        .run()
        .await
        .map_err(|e| anyhow::anyhow!("Failed to start server: {}", e))?;

        Ok(())
    }
}
