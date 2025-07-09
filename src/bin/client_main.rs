use mock_gmp_api::Client;

#[tokio::main]
async fn main() -> Result<(), anyhow::Error> {
    dotenv::dotenv().ok();
    let server_address = std::env::var("SERVER_ADDRESS").unwrap();
    let server_port = std::env::var("SERVER_PORT")
        .unwrap()
        .parse::<u16>()
        .unwrap();
    let client = Client::new(format!("http://{}:{}", server_address, server_port));

    match client.get_tasks().await {
        Ok(response) => println!("Success: {}", response),
        Err(e) => println!("Error: {}", e),
    }

    Ok(())
}
