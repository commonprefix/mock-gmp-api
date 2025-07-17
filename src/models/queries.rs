use sqlx::{PgPool, Row};

#[derive(Clone, Debug)]
pub struct QueriesModel {
    pool: PgPool,
}

impl QueriesModel {
    pub async fn new(url: &str) -> Result<Self, anyhow::Error> {
        let pool = PgPool::connect(url).await?;
        Ok(Self { pool })
    }

    pub async fn insert(&self, id: &str, query: &str) -> Result<(), anyhow::Error> {
        sqlx::query("INSERT INTO queries (id, query) VALUES ($1, $2)")
            .bind(id)
            .bind(query)
            .execute(&self.pool)
            .await?;
        Ok(())
    }

    pub async fn find(&self, id: &str) -> Result<Option<String>, anyhow::Error> {
        let row = sqlx::query("SELECT query FROM queries WHERE id = $1")
            .bind(id)
            .fetch_optional(&self.pool)
            .await?;
        Ok(row.map(|row| row.get("query")))
    }

    pub async fn delete(&self, id: &str) -> Result<(), anyhow::Error> {
        sqlx::query("DELETE FROM queries WHERE id = $1")
            .bind(id)
            .execute(&self.pool)
            .await?;
        Ok(())
    }

    pub async fn upsert(&self, id: &str, query: &str) -> Result<(), anyhow::Error> {
        sqlx::query("INSERT INTO queries (id, query) VALUES ($1, $2) ON CONFLICT (id) DO UPDATE SET query = $2")
            .bind(id)
            .bind(query)
            .execute(&self.pool)
            .await?;
        Ok(())
    }
}
