use mock_gmp_api::{PostgresDB, Server};

#[actix_web::main]
async fn main() -> anyhow::Result<()> {
    dotenv::dotenv().ok();
    let postgres_db = PostgresDB::new(&std::env::var("POSTGRES_URL").unwrap()).await?;
    let server = Server::new(
        std::env::var("SERVER_PORT")
            .unwrap()
            .parse::<u16>()
            .unwrap(),
        std::env::var("SERVER_ADDRESS").unwrap(),
        postgres_db,
    );
    if let Err(e) = server.run().await {
        eprintln!("Error: {}", e);
        std::process::exit(1);
    }
    Ok(())
}
