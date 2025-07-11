use actix_web::{App, Error, HttpResponse, HttpServer, error, get, post, web};
use futures::StreamExt;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::HashMap;
use tracing::{debug, info};

use crate::{
    TasksModel,
    gmp_types::{Event, EventType, PostEventResponse, PostEventResult, Task},
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

        let event_json = match serde_json::to_string(event) {
            Ok(json) => json,
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

        match events_db
            .upsert(
                event_id,
                timestamp,
                string_to_event_type(event_type),
                &event_json,
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

    db.upsert(
        &common.id,
        &common.chain,
        &common.timestamp,
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
