use mock_gmp_api::{
    Server, TasksModel,
    models::{broadcasts::BroadcastsModel, events::EventsModel, payloads::PayloadsModel},
    queue::LapinConnection,
    utils::setup_logging,
};
use tracing::error;

#[actix_web::main]
async fn main() -> anyhow::Result<()> {
    dotenv::dotenv().ok();
    setup_logging();

    let tasks_model = TasksModel::new(&std::env::var("POSTGRES_URL").unwrap()).await?;
    let events_model = EventsModel::new(&std::env::var("POSTGRES_URL").unwrap()).await?;
    let broadcasts_model = BroadcastsModel::new(&std::env::var("POSTGRES_URL").unwrap()).await?;
    let payloads_model = PayloadsModel::new(&std::env::var("POSTGRES_URL").unwrap()).await?;
    let addr =
        std::env::var("QUEUE_ADDRESS").unwrap_or_else(|_| "amqp://127.0.0.1:5672/%2f".into());
    let queue = LapinConnection::new(&addr, "mock_gmp_api").await?;
    let server = Server::new(
        std::env::var("SERVER_PORT")
            .unwrap()
            .parse::<u16>()
            .unwrap(),
        std::env::var("SERVER_ADDRESS").unwrap(),
        tasks_model,
        events_model,
        broadcasts_model,
        payloads_model,
        queue,
    );
    if let Err(e) = server.run().await {
        error!("Error: {}", e);
        std::process::exit(1);
    }
    Ok(())
}
