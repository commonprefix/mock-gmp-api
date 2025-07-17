use sqlx::{PgPool, Row};

#[derive(Clone, Debug)]
pub struct PayloadsModel {
    pool: PgPool,
}

impl PayloadsModel {
    pub async fn new(url: &str) -> Result<Self, anyhow::Error> {
        let pool = PgPool::connect(url).await?;
        Ok(Self { pool })
    }

    pub async fn insert(&self, id: &str, payload: &str) -> Result<(), anyhow::Error> {
        sqlx::query("INSERT INTO payloads (id, payload) VALUES ($1, $2)")
            .bind(id)
            .bind(payload)
            .execute(&self.pool)
            .await?;
        Ok(())
    }

    pub async fn find(&self, id: &str) -> Result<Option<String>, anyhow::Error> {
        let row = sqlx::query("SELECT payload FROM payloads WHERE id = $1")
            .bind(id)
            .fetch_optional(&self.pool)
            .await?;
        Ok(row.map(|row| row.get("payload")))
    }

    pub async fn delete(&self, id: &str) -> Result<(), anyhow::Error> {
        sqlx::query("DELETE FROM payloads WHERE id = $1")
            .bind(id)
            .execute(&self.pool)
            .await?;
        Ok(())
    }

    pub async fn upsert(&self, id: &str, payload: &str) -> Result<(), anyhow::Error> {
        sqlx::query("INSERT INTO payloads (id, payload) VALUES ($1, $2) ON CONFLICT (id) DO UPDATE SET payload = $2")
            .bind(id)
            .bind(payload)
            .execute(&self.pool)
            .await?;
        Ok(())
    }
}
