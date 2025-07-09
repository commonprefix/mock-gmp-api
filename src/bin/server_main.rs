use mock_gmp_api::Server;

#[actix_web::main]
async fn main() -> anyhow::Result<()> {
    dotenv::dotenv().ok();
    let server = Server::new(
        std::env::var("SERVER_PORT")
            .unwrap()
            .parse::<u16>()
            .unwrap(),
        std::env::var("SERVER_ADDRESS").unwrap(),
    );
    if let Err(e) = server.run().await {
        eprintln!("Error: {}", e);
        std::process::exit(1);
    }
    Ok(())
}
