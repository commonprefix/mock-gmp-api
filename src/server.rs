use actix_web::{App, Error, HttpResponse, HttpServer, error, get, post, web};
use futures::StreamExt;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::HashMap;

use crate::{
    PostgresDB,
    gmp_types::{Event, PostEventResponse, PostEventResult, Task},
    utils::parse_task,
};

pub struct Server {
    pub port: u16,
    pub address: String,
    pub db: PostgresDB,
}

const MAX_SIZE: usize = 262_144; // max payload size is 256k

#[derive(Serialize, Deserialize, Debug)]
struct EventsRequest {
    events: Vec<Event>,
}

#[post("/{address}/broadcast")]
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
    println!("obj: {:?} to address: {}", obj, address);
    Ok(HttpResponse::Ok().json(obj))
}

#[post("/chains/{chain}/events")]
async fn events(
    chain: web::Path<String>,
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

    println!(
        "Received {} events for chain: {}",
        events_request.events.len(),
        chain
    );
    for (i, event) in events_request.events.iter().enumerate() {
        println!("Event {}: {:?}", i, event);
    }

    // Mock processing - create a successful result for each event
    let results: Vec<PostEventResult> = events_request
        .events
        .iter()
        .enumerate()
        .map(|(index, _event)| PostEventResult {
            status: "success".to_string(),
            index,
            error: None,
            retriable: None,
        })
        .collect();

    let response = PostEventResponse { results };

    println!("Responding with: {:?}", response);
    Ok(HttpResponse::Ok().json(response))
}

#[post("/task")]
async fn task(db: web::Data<PostgresDB>, mut payload: web::Payload) -> Result<HttpResponse, Error> {
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

    let task_json = json_value.get("task").unwrap();

    // without the macro t could not be different Task types
    macro_rules! get_common {
        ($t:expr) => {
            (&$t.common.chain, &$t.common.timestamp, &$t.common.meta)
        };
    }

    let (chain, timestamp, meta) = match &task {
        Task::Verify(t) => get_common!(t),
        Task::Execute(t) => get_common!(t),
        Task::GatewayTx(t) => get_common!(t),
        Task::ConstructProof(t) => get_common!(t),
        Task::ReactToWasmEvent(t) => get_common!(t),
        Task::Refund(t) => get_common!(t),
        Task::ReactToExpiredSigningSession(t) => get_common!(t),
        Task::ReactToRetriablePoll(t) => get_common!(t),
        Task::Unknown(_) => return Err(error::ErrorBadRequest("Cannot store unknown tasks")),
    };

    db.upsert(
        &task.id(),
        chain,
        timestamp,
        task.kind(),
        meta.as_ref()
            .map(|m| serde_json::to_string(m).unwrap())
            .as_deref(),
        Some(&serde_json::to_string(task_json).unwrap()),
    )
    .await
    .map_err(|e| error::ErrorInternalServerError(e.to_string()))?;

    println!("task upserted: {:?}", task.id());

    Ok(HttpResponse::Ok().json(task))
}

// attempting to mock call in relayer :  format!("{}/chains/{}/tasks", self.rpc_url, self.chain); in get_tasks
#[get("/chains/{chain}/tasks")]
async fn tasks(
    db: web::Data<PostgresDB>,
    query: web::Query<HashMap<String, String>>,
) -> Result<HttpResponse, Error> {
    let after = query.get("after");
    if let Some(after_value) = after {
        println!("Requesting tasks after: {}", after_value);
        // TODO: Implement filtering by 'after' parameter in database query
    }

    let raw_tasks = db
        .get_tasks()
        .await
        .map_err(|e| error::ErrorInternalServerError(e.to_string()))?;

    let response = serde_json::json!({
        "tasks": raw_tasks
    });

    println!("Returning {} tasks", raw_tasks.len());

    Ok(HttpResponse::Ok().json(response))
}

impl Server {
    pub fn new(port: u16, address: String, db: PostgresDB) -> Self {
        Self { port, address, db }
    }

    pub async fn run(self) -> anyhow::Result<()> {
        let addr = format!("{}:{}", self.address, self.port);

        HttpServer::new(move || {
            App::new()
                .app_data(web::Data::new(self.db.clone()))
                .service(tasks)
                .service(task)
                .service(address_broadcast)
                .service(events)
        })
        .bind(addr)?
        .run()
        .await
        .map_err(|e| anyhow::anyhow!("Failed to start server: {}", e))?;

        Ok(())
    }
}
