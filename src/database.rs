use crate::{gmp_types::Task, utils::parse_task};
use sqlx::{PgPool, Row};

#[derive(Clone, Debug)]
pub struct PostgresDB {
    pool: PgPool,
}

impl PostgresDB {
    pub async fn new(url: &str) -> Result<Self, anyhow::Error> {
        let pool = PgPool::connect(url).await?;
        Ok(Self { pool })
    }

    pub async fn get_tasks(&self) -> Result<Vec<Task>, anyhow::Error> {
        let query = "SELECT id, chain, timestamp, type, meta, task FROM tasks";
        let rows = sqlx::query(query).fetch_all(&self.pool).await?;

        Ok(rows
            .iter()
            .filter_map(|row| match parse_task(&row.get("task")) {
                Ok(task) => Some(task),
                Err(e) => {
                    println!("Failed to parse task: {:?}", e);
                    None
                }
            })
            .collect::<Vec<_>>())
    }
}
