use actix_web::{App, Error, HttpResponse, HttpServer, error, get, post, web};
use futures::StreamExt;
use serde_json::Value;

use crate::{PostgresDB, gmp_types::Task, utils::parse_task};

pub struct Server {
    pub port: u16,
    pub address: String,
    pub db: PostgresDB,
}

const MAX_SIZE: usize = 262_144; // max payload size is 256k

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

#[post("/events")]
async fn events(mut payload: web::Payload) -> Result<HttpResponse, Error> {
    let mut body = web::BytesMut::new();
    while let Some(chunk) = payload.next().await {
        let chunk = chunk?;
        if (body.len() + chunk.len()) > MAX_SIZE {
            return Err(error::ErrorBadRequest("overflow"));
        }
        body.extend_from_slice(&chunk);
    }

    let obj = serde_json::from_slice::<Value>(&body)?;
    println!("obj: {:?}", obj);
    Ok(HttpResponse::Ok().json(obj))
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

    let maybe_json_task = serde_json::from_slice::<Value>(&body)?;
    let task = parse_task(&maybe_json_task).map_err(|e| error::ErrorBadRequest(e.to_string()))?;

    let task_json = maybe_json_task.get("task").unwrap();

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
async fn tasks(db: web::Data<PostgresDB>) -> Result<HttpResponse, Error> {
    let tasks = db
        .get_tasks()
        .await
        .map_err(|e| error::ErrorInternalServerError(e.to_string()))?;

    let response = serde_json::json!({
        "tasks": tasks
    });

    println!("tasks: {:?}", response);

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
