use actix_web::{App, Error, HttpResponse, HttpServer, Responder, error, get, post, web};
use futures::StreamExt;
use serde::{Deserialize, Serialize};

use crate::PostgresDB;

pub struct Server {
    pub port: u16,
    pub address: String,
    pub db: PostgresDB,
}

const MAX_SIZE: usize = 262_144; // max payload size is 256k

#[derive(Serialize, Deserialize, Debug)]
struct MyObj {
    name: String,
    number: i32,
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

    let obj = serde_json::from_slice::<MyObj>(&body)?;
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

    let obj = serde_json::from_slice::<MyObj>(&body)?;
    println!("obj: {:?}", obj);
    Ok(HttpResponse::Ok().json(obj))
}

#[get("/tasks")]
async fn tasks() -> impl Responder {
    format!("Hello, getting tasks")
}

impl Server {
    pub fn new(port: u16, address: String, db: PostgresDB) -> Self {
        Self { port, address, db }
    }

    pub async fn run(&self) -> anyhow::Result<()> {
        let addr = format!("{}:{}", self.address, self.port);

        HttpServer::new(|| {
            App::new()
                .service(tasks)
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
