use mock_gmp_api::{
    models::tasks::TasksModel, queue::LapinConnection, subscriber::Subscriber, utils::setup_logging,
};
use tracing::error;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    dotenv::dotenv().ok();
    setup_logging();

    let addr =
        std::env::var("QUEUE_ADDRESS").unwrap_or_else(|_| "amqp://127.0.0.1:5672/%2f".into());

    let queue = LapinConnection::new(&addr, "mock_gmp_api").await?;
    let database = TasksModel::new(&std::env::var("POSTGRES_URL").unwrap()).await?;
    let chain_id = std::env::var("CHAIN_ID").unwrap();
    let rpc = std::env::var("AXELAR_RPC").unwrap();

    let mut subscriber = Subscriber::new(queue, database, chain_id, rpc);

    subscriber.run().await?;

    Ok(())
}
