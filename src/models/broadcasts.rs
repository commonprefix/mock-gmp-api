use crate::gmp_types::BroadcastRequest;
use serde_json;
use sqlx::{PgPool, Row};
use tracing::error;
const PG_TABLE_NAME: &str = "broadcasts";

#[derive(Clone, Debug)]
pub struct BroadcastsModel {
    pool: PgPool,
}

#[derive(Clone, Debug, PartialEq, sqlx::Type)]
#[sqlx(type_name = "broadcast_status")]
pub enum BroadcastStatus {
    Received,
    Success,
    Failed,
}

impl BroadcastsModel {
    pub async fn new(url: &str) -> Result<Self, anyhow::Error> {
        let pool = PgPool::connect(url).await?;
        Ok(Self { pool })
    }

    pub async fn find(&self, id: &str) -> Result<Option<BroadcastRequest>, anyhow::Error> {
        let query = format!("SELECT broadcast FROM {} WHERE id = $1", PG_TABLE_NAME);
        let row = sqlx::query(&query)
            .bind(id)
            .fetch_optional(&self.pool)
            .await?;

        let broadcast = row.and_then(|row| {
            let broadcast_text: String = row.get("broadcast");

            match serde_json::from_str(&broadcast_text) {
                Ok(broadcast) => Some(broadcast),
                Err(e) => {
                    error!("Failed to parse broadcast JSON: {:?}", e);
                    None
                }
            }
        });

        Ok(broadcast)
    }

    pub async fn insert(
        &self,
        id: &str,
        broadcast: &str,
        status: BroadcastStatus,
    ) -> Result<(), anyhow::Error> {
        let query = format!(
            "INSERT INTO {} (id, broadcast, status) VALUES ($1, $2, $3)",
            PG_TABLE_NAME
        );

        sqlx::query(&query)
            .bind(id)
            .bind(broadcast)
            .bind(status)
            .execute(&self.pool)
            .await?;

        Ok(())
    }

    pub async fn upsert(
        &self,
        id: &str,
        broadcast: &str,
        status: BroadcastStatus,
    ) -> Result<(), anyhow::Error> {
        let query = format!(
            "INSERT INTO {} (id, broadcast, status) VALUES ($1, $2, $3) ON CONFLICT (id) DO UPDATE SET broadcast = $2, status = $3",
            PG_TABLE_NAME
        );

        sqlx::query(&query)
            .bind(id)
            .bind(broadcast)
            .bind(status)
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
