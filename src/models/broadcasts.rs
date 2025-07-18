use serde::{Deserialize, Serialize};
use serde_json::{self, Value};
use sqlx::{PgPool, Row};
use tracing::error;
const PG_TABLE_NAME: &str = "broadcasts";

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct BroadcastWithTxHash {
    pub broadcast: Value,
    pub tx_hash: Option<String>,
    pub status: BroadcastStatus,
    pub error: Option<String>,
}

#[derive(Clone, Debug)]
pub struct BroadcastsModel {
    pool: PgPool,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize, sqlx::Type)]
#[sqlx(type_name = "broadcast_status")]
#[serde(rename_all = "UPPERCASE")]
pub enum BroadcastStatus {
    #[sqlx(rename = "RECEIVED")]
    Received,
    #[sqlx(rename = "SUCCESS")]
    Success,
    #[sqlx(rename = "FAILED")]
    Failed,
}

impl BroadcastsModel {
    pub async fn new(url: &str) -> Result<Self, anyhow::Error> {
        let pool = PgPool::connect(url).await?;
        Ok(Self { pool })
    }

    pub async fn find(&self, id: &str) -> Result<Option<Value>, anyhow::Error> {
        let query = format!("SELECT broadcast FROM {} WHERE id = $1", PG_TABLE_NAME);
        let row = sqlx::query(&query)
            .bind(id)
            .fetch_optional(&self.pool)
            .await?;

        let broadcast = row.and_then(|row| {
            let broadcast_text: String = row.get("broadcast");

            match serde_json::from_str::<Value>(&broadcast_text) {
                Ok(broadcast) => Some(broadcast),
                Err(e) => {
                    error!("Failed to parse broadcast JSON: {:?}", e);
                    None
                }
            }
        });

        Ok(broadcast)
    }

    pub async fn find_with_status_and_hash(
        &self,
        id: &str,
    ) -> Result<Option<BroadcastWithTxHash>, anyhow::Error> {
        let query = format!(
            "SELECT broadcast, tx_hash, status, error FROM {} WHERE id = $1",
            PG_TABLE_NAME
        );
        let row = sqlx::query(&query)
            .bind(id)
            .fetch_optional(&self.pool)
            .await?;

        let result = row.and_then(|row| {
            let broadcast_text: String = row.get("broadcast");
            let tx_hash: Option<String> = row.get("tx_hash");
            let status: BroadcastStatus = row.get("status");
            let error: Option<String> = row.get("error");

            match serde_json::from_str::<Value>(&broadcast_text) {
                Ok(broadcast) => Some(BroadcastWithTxHash {
                    broadcast,
                    tx_hash,
                    status,
                    error,
                }),
                Err(e) => {
                    error!("Failed to parse broadcast JSON: {:?}", e);
                    None
                }
            }
        });

        Ok(result)
    }

    pub async fn insert(
        &self,
        id: &str,
        contract_address: &str,
        broadcast: &str,
        status: BroadcastStatus,
    ) -> Result<(), anyhow::Error> {
        let query = format!(
            "INSERT INTO {} (id, contract_address, broadcast, status) VALUES ($1, $2, $3, $4)",
            PG_TABLE_NAME
        );

        sqlx::query(&query)
            .bind(id)
            .bind(contract_address)
            .bind(broadcast)
            .bind(status)
            .execute(&self.pool)
            .await?;

        Ok(())
    }

    pub async fn upsert(
        &self,
        id: &str,
        contract_address: &str,
        broadcast: &str,
        status: BroadcastStatus,
        tx_hash: Option<&str>,
        error: Option<&str>,
    ) -> Result<(), anyhow::Error> {
        let query = format!(
            "INSERT INTO {} (id, contract_address, broadcast, status, tx_hash, error) VALUES ($1, $2, $3, $4, $5, $6) ON CONFLICT (id) DO UPDATE SET contract_address = $2, broadcast = $3, status = $4, tx_hash = $5, error = $6",
            PG_TABLE_NAME
        );

        sqlx::query(&query)
            .bind(id)
            .bind(contract_address)
            .bind(broadcast)
            .bind(status)
            .bind(tx_hash)
            .bind(error)
            .execute(&self.pool)
            .await?;

        Ok(())
    }

    pub async fn delete(&self, id: &str) -> Result<(), anyhow::Error> {
        let query = format!("DELETE FROM {} WHERE id = $1", PG_TABLE_NAME);
        sqlx::query(&query).bind(id).execute(&self.pool).await?;

        Ok(())
    }
}
