use mock_gmp_api::Server;

#[actix_web::main]
async fn main() -> anyhow::Result<()> {
    let server = Server::new(8080, "127.0.0.1".to_string());
    if let Err(e) = server.run().await {
        eprintln!("Error: {}", e);
        std::process::exit(1);
    }
    Ok(())
}
