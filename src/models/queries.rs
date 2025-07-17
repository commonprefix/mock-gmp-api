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

    pub async fn insert(
        &self,
        id: &str,
        contract_address: &str,
        query: &str,
    ) -> Result<(), anyhow::Error> {
        sqlx::query("INSERT INTO queries (id, contract_address, query) VALUES ($1, $2, $3)")
            .bind(id)
            .bind(contract_address)
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

    pub async fn find_result(
        &self,
        id: &str,
    ) -> Result<Option<(String, Option<String>)>, anyhow::Error> {
        let row = sqlx::query("SELECT result, error FROM queries WHERE id = $1")
            .bind(id)
            .fetch_optional(&self.pool)
            .await?;
        Ok(row.map(|row| {
            let result: String = row.get("result");
            let error: Option<String> = row.get("error");
            (result, error)
        }))
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

    pub async fn update_result(
        &self,
        id: &str,
        result: &str,
        error: Option<&str>,
    ) -> Result<(), anyhow::Error> {
        sqlx::query("UPDATE queries SET result = $1, error = $2 WHERE id = $3")
            .bind(result)
            .bind(error)
            .bind(id)
            .execute(&self.pool)
            .await?;
        Ok(())
    }
}
