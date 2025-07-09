use serde::{Deserialize, Serialize};
use sqlx::{PgPool, Row};

#[derive(Clone, Debug)]
pub struct PostgresDB {
    pool: PgPool,
}

#[derive(Debug, sqlx::FromRow, Serialize, Deserialize)]
pub struct Task {
    pub id: i64,
    pub name: String,
    pub description: String,
}

impl PostgresDB {
    pub async fn new(url: &str) -> Result<Self, anyhow::Error> {
        let pool = PgPool::connect(url).await?;
        Ok(Self { pool })
    }

    pub async fn get_tasks(&self) -> Result<Vec<Task>, anyhow::Error> {
        let query = "SELECT * FROM tasks";
        let tasks = sqlx::query(query).fetch_all(&self.pool).await?;
        println!("tasks: {:?}", tasks);
        let tasks = tasks
            .into_iter()
            .map(|row| Task {
                id: row.get("id"),
                name: row.get("name"),
                description: row.get("description"),
            })
            .collect();
        Ok(tasks)
    }
}
