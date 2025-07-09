use crate::{gmp_types::Task, utils::parse_task};
use serde_json;
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
            .filter_map(|row| {
                let task_text: String = row.get("task");
                let task_json = serde_json::from_str(&task_text).unwrap_or_default();
                match parse_task(&task_json) {
                    Ok(task) => Some(task),
                    Err(e) => {
                        println!("Failed to parse task: {:?}", e);
                        None
                    }
                }
            })
            .collect::<Vec<_>>())
    }
}
