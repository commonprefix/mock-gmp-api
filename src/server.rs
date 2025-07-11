use actix_web::{App, Error, HttpResponse, HttpServer, error, get, post, web};
use chrono::{DateTime, Utc};
use futures::StreamExt;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::HashMap;
use tracing::{debug, info, warn};
use uuid::Uuid;

use crate::{
    TasksModel,
    gmp_types::{
        CommonTaskFields, Event, EventType, PostEventResponse, PostEventResult, Task, TaskMetadata,
        VerifyTask, VerifyTaskFields,
    },
    models::events::EventsModel,
    utils::parse_task,
};

pub struct Server {
    pub port: u16,
    pub address: String,
    pub tasks_model: TasksModel,
    pub events_model: EventsModel,
}

const MAX_SIZE: usize = 262_144; // max payload size is 256k

#[derive(Serialize, Deserialize, Debug)]
struct EventsRequest {
    events: Vec<Event>,
}

fn string_to_event_type(event_type: &str) -> EventType {
    match event_type {
        "CALL" => EventType::Call,
        "GAS_REFUNDED" => EventType::GasRefunded,
        "GAS_CREDIT" => EventType::GasCredit,
        "MESSAGE_EXECUTED" => EventType::MessageExecuted,
        "CANNOT_EXECUTE_MESSAGE_V2" => EventType::CannotExecuteMessageV2,
        "ITS_INTERCHAIN_TRANSFER" => EventType::ITSInterchainTransfer,
        _ => EventType::Call,
    }
}

#[post("/contracts/{contract_address}/broadcast")]
async fn address_broadcast(
    address: web::Path<String>,
    mut payload: web::Payload,
    _chain: web::Path<String>,
) -> Result<HttpResponse, Error> {
    let mut body = web::BytesMut::new();
    while let Some(chunk) = payload.next().await {
        let chunk = chunk?;
        if (body.len() + chunk.len()) > MAX_SIZE {
            return Err(error::ErrorBadRequest("overflow"));
        }
        body.extend_from_slice(&chunk);
    }

    let obj = serde_json::from_slice::<Value>(&body)?;
    info!("obj: {:?} to address: {}", obj, address);
    Ok(HttpResponse::Ok().json(obj))
}

#[post("/chains/{chain}/events")]
async fn post_events(
    chain: web::Path<String>,
    events_db: web::Data<EventsModel>,
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

        let (event_id, event_type, timestamp) = event.common_fields();
        let message_id = event.message_id();

        // Check that no other event with the same ID exists
        let maybe_event_with_same_id = events_db
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

        let maybe_corresponding_event = events_db
            .find_by_message_id(&message_id)
            .await
            .map_err(|e| error::ErrorInternalServerError(e.to_string()))?;
        if let Some(corresponding_event) = maybe_corresponding_event {
            let (_, corresponding_event_type, _) = corresponding_event.common_fields();
            // Check that no event exists with same message ID and type
            if corresponding_event_type == event_type {
                results.push(PostEventResult {
                    status: "ACCEPTED".to_string(),
                    index,
                    error: None,
                    retriable: None,
                });
                warn!(
                    "Event with same message ID and type already exists: {:?}",
                    event
                );
                continue;
            }
            // Check if both events arrived in order to create VERIFY task
            if corresponding_event_type == "CALL" && event_type == "GAS_CREDIT"
                || corresponding_event_type == "GAS_CREDIT" && event_type == "CALL"
            {
                // TODO: Refactor GMP Types for Events to be similar to Tasks enums
                let (gas_credit_event, call_event) = if corresponding_event_type == "GAS_CREDIT" {
                    (corresponding_event, event.clone())
                } else {
                    (event.clone(), corresponding_event)
                };
                debug!("Gas credit event: {:?}", gas_credit_event);
                debug!("Call event: {:?}", call_event);

                // attempt to not use json round-trip
                let (message, payload, meta) = match &call_event {
                    Event::Call {
                        message,
                        payload,
                        common,
                        ..
                    } => (message, payload, &common.meta),
                    _ => {
                        return Err(error::ErrorInternalServerError(
                            "Expected Call event".to_string(),
                        ));
                    }
                };

                //  TODO: As is, an event with a different ID but same message ID will
                //  trigger a second VerifyTask creation. There needs to be a check to avoid that

                let task = Task::Verify(VerifyTask {
                    common: CommonTaskFields {
                        id: Uuid::new_v4().to_string(),
                        chain: chain.clone(),
                        timestamp: Utc::now().to_rfc3339(),
                        r#type: "VERIFY".to_string(),
                        meta: meta.as_ref().map(|event_meta| TaskMetadata {
                            tx_id: event_meta.tx_id.clone(),
                            from_address: event_meta.from_address.clone(),
                            finalized: event_meta.finalized,
                            source_context: event_meta.source_context.clone(),
                            scoped_messages: None,
                        }),
                    },
                    task: VerifyTaskFields {
                        message: message.clone(),
                        payload: payload.clone(),
                    },
                });

                // let call_event_json_str = serde_json::to_string(&call_event).unwrap();
                // let call_event_json = serde_json::from_str::<Value>(&call_event_json_str).unwrap();
                // let call_event_message = call_event_json.get("message");
                // let call_event_payload = call_event_json.get("payload");
                // // TODO: is there a better way for this? Event meta and Task meta differ in one field
                // let call_event_meta = call_event_json.get("meta");
                // let call_event_meta_tx_id = call_event_meta.and_then(|meta| meta.get("txID"));
                // let call_event_meta_from_address =
                //     call_event_meta.and_then(|meta| meta.get("fromAddress"));
                // let call_event_meta_finalized =
                //     call_event_meta.and_then(|meta| meta.get("finalized"));
                // let call_event_meta_source_context =
                //     call_event_meta.and_then(|meta| meta.get("sourceContext"));

                // // Create VERIFY task
                // let task = Task::Verify(VerifyTask {
                //     common: CommonTaskFields {
                //         id: Uuid::new_v4().to_string(),
                //         chain: chain.clone(),
                //         timestamp: Utc::now().to_rfc3339(),
                //         r#type: "VERIFY".to_string(),
                //         meta: Some(TaskMetadata {
                //             tx_id: call_event_meta_tx_id.and_then(|v| {
                //                 if v.is_null() {
                //                     None
                //                 } else {
                //                     v.as_str().map(|s| s.to_string())
                //                 }
                //             }),
                //             from_address: call_event_meta_from_address.and_then(|v| {
                //                 if v.is_null() {
                //                     None
                //                 } else {
                //                     v.as_str().map(|s| s.to_string())
                //                 }
                //             }),
                //             finalized: call_event_meta_finalized
                //                 .and_then(|v| if v.is_null() { None } else { v.as_bool() }),
                //             source_context: call_event_meta_source_context.and_then(|v| {
                //                 if v.is_null() {
                //                     None
                //                 } else {
                //                     serde_json::from_value::<HashMap<String, String>>(v.clone())
                //                         .ok()
                //                 }
                //             }),
                //             scoped_messages: None,
                //         }),
                //     },
                //     task: VerifyTaskFields {
                //         message: serde_json::from_value(call_event_message.unwrap().clone())
                //             .map_err(|e| {
                //                 error::ErrorInternalServerError(format!(
                //                     "Failed to parse message: {}",
                //                     e
                //                 ))
                //             })?,
                //         payload: call_event_payload.unwrap().as_str().unwrap().to_string(),
                //     },
                // });

                info!("Created VERIFY task: {:?}", task);

                // TODO: POST VERIFY task so that distributor can pick it up
            }
        }

        // insert instead of upsert because we check that ID does not exist already
        match events_db
            .insert(
                event_id,
                parsed_timestamp,
                string_to_event_type(event_type),
                &event_json_str,
                &message_id,
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
    query: web::Query<HashMap<String, String>>,
) -> Result<HttpResponse, Error> {
    let after = query.get("after");
    if let Some(after_value) = after {
        debug!("Requesting tasks after: {}", after_value);
        // TODO: Implement filtering by 'after' parameter in database query
    }

    let raw_tasks = db
        .get_tasks()
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
    ) -> Self {
        Self {
            port,
            address,
            tasks_model,
            events_model,
        }
    }

    pub async fn run(self) -> anyhow::Result<()> {
        let addr = format!("{}:{}", self.address, self.port);

        HttpServer::new(move || {
            App::new()
                .app_data(web::Data::new(self.tasks_model.clone()))
                .app_data(web::Data::new(self.events_model.clone()))
                .service(get_tasks)
                .service(post_task)
                .service(address_broadcast)
                .service(post_events)
        })
        .bind(addr)?
        .run()
        .await
        .map_err(|e| anyhow::anyhow!("Failed to start server: {}", e))?;

        Ok(())
    }
}
