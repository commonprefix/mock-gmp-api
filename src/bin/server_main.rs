use mock_gmp_api::{Server, TasksModel, models::events::EventsModel};

#[actix_web::main]
async fn main() -> anyhow::Result<()> {
    dotenv::dotenv().ok();
    let tasks_model = TasksModel::new(&std::env::var("POSTGRES_URL").unwrap()).await?;
    let events_model = EventsModel::new(&std::env::var("POSTGRES_URL").unwrap()).await?;
    let server = Server::new(
        std::env::var("SERVER_PORT")
            .unwrap()
            .parse::<u16>()
            .unwrap(),
        std::env::var("SERVER_ADDRESS").unwrap(),
        tasks_model,
        events_model,
    );
    if let Err(e) = server.run().await {
        eprintln!("Error: {}", e);
        std::process::exit(1);
    }
    Ok(())
}
