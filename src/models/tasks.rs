use crate::{
    gmp_types::{Task, TaskKind},
    utils::parse_task,
};
use serde_json;
use sqlx::{PgPool, Row};

const PG_TABLE_NAME: &str = "tasks";

#[derive(Clone, Debug)]
pub struct TasksModel {
    pool: PgPool,
}

impl TasksModel {
    pub async fn new(url: &str) -> Result<Self, anyhow::Error> {
        let pool = PgPool::connect(url).await?;
        Ok(Self { pool })
    }

    pub async fn find(&self, id: &str) -> Result<Option<Task>, anyhow::Error> {
        let query = format!("SELECT task FROM {} WHERE id = $1", PG_TABLE_NAME);
        let row = sqlx::query(&query)
            .bind(id)
            .fetch_optional(&self.pool)
            .await?;

        let task = row.and_then(|row| {
            let task_text: String = row.get("task");

            let task_json: serde_json::Value = match serde_json::from_str(&task_text) {
                Ok(value) => value,
                Err(e) => {
                    println!("Failed to parse task JSON: {:?}", e);
                    return None;
                }
            };

            match parse_task(&task_json) {
                Ok(task) => Some(task),
                Err(e) => {
                    println!("Failed to parse task: {:?}", e);
                    None
                }
            }
        });

        Ok(task)
    }

    pub async fn upsert(
        &self,
        id: &str,
        chain: &str,
        timestamp: &str,
        task_type: TaskKind,
        task: Option<&str>,
    ) -> Result<(), anyhow::Error> {
        let query = format!(
            "INSERT INTO {} (id, chain, timestamp, type, task) VALUES ($1, $2, $3, $4, $5) ON CONFLICT (id) DO UPDATE SET chain = $2, timestamp = $3, type = $4, task = $5 RETURNING *",
            PG_TABLE_NAME
        );

        sqlx::query(&query)
            .bind(id)
            .bind(chain)
            .bind(timestamp)
            .bind(task_type)
            .bind(task)
            .execute(&self.pool)
            .await?;

        Ok(())
    }

    pub async fn delete(&self, id: &str) -> Result<(), anyhow::Error> {
        let query = format!("DELETE FROM {} WHERE id = $1", PG_TABLE_NAME);
        sqlx::query(&query).bind(id).execute(&self.pool).await?;

        Ok(())
    }

    pub async fn get_tasks(&self) -> Result<Vec<serde_json::Value>, anyhow::Error> {
        let query = format!("SELECT task FROM {}", PG_TABLE_NAME);
        let rows = sqlx::query(&query).fetch_all(&self.pool).await?;

        Ok(rows
            .iter()
            .filter_map(|row| {
                let task_text: String = row.get("task");

                match serde_json::from_str(&task_text) {
                    Ok(value) => Some(value),
                    Err(e) => {
                        println!("Failed to parse task JSON: {:?}", e);
                        None
                    }
                }
            })
            .collect::<Vec<_>>())
    }
}

#[cfg(test)]
mod tests {

    use testcontainers::{ContainerAsync, runners::AsyncRunner};
    use testcontainers_modules::postgres;

    use crate::{
        gmp_types::{ExecuteTask, GatewayTxTask, Task, TaskKind, VerifyTask},
        models::tasks::TasksModel,
    };

    async fn setup_test_container() -> (TasksModel, ContainerAsync<postgres::Postgres>) {
        let container = postgres::Postgres::default()
            .with_init_sql(
                include_str!("../../migrations/0001_tasks.sql")
                    .to_string()
                    .into_bytes(),
            )
            .start()
            .await
            .unwrap();
        let connection_string = format!(
            "postgres://postgres:postgres@{}:{}/postgres",
            container.get_host().await.unwrap(),
            container.get_host_port_ipv4(5432).await.unwrap()
        );
        let model = TasksModel::new(&connection_string).await.unwrap();
        // we need to return the container too otherwise it will be dropped and the test will run forever
        (model, container)
    }

    #[tokio::test]
    async fn test_upsert_and_get_tasks() {
        let (db, _container) = setup_test_container().await;
        let valid_verify_tasks_dir = "testdata/xrpl_tasks/valid_tasks/VerifyTask.json";
        let valid_verify_tasks_json = std::fs::read_to_string(valid_verify_tasks_dir).unwrap();
        let valid_verify_tasks: Vec<VerifyTask> =
            serde_json::from_str(&valid_verify_tasks_json).unwrap();
        let valid_verify_task = valid_verify_tasks[1].clone();

        // Create complete task JSON as the server would store it
        let complete_task_json = serde_json::json!({
            "id": valid_verify_task.common.id,
            "chain": valid_verify_task.common.chain,
            "timestamp": valid_verify_task.common.timestamp,
            "type": "VERIFY",
            "meta": valid_verify_task.common.meta,
            "task": valid_verify_task.task
        });

        db.upsert(
            valid_verify_task.common.id.as_str(),
            valid_verify_task.common.chain.as_str(),
            valid_verify_task.common.timestamp.as_str(),
            TaskKind::Verify,
            Some(&serde_json::to_string(&complete_task_json).unwrap()),
        )
        .await
        .unwrap();
        let raw_tasks = db.get_tasks().await.unwrap();
        assert_eq!(raw_tasks.len(), 1);

        let task = crate::utils::parse_task(&raw_tasks[0]).unwrap();
        let task = match &task {
            Task::Verify(verify_task) => verify_task,
            _ => panic!("Expected VerifyTask, got different task type"),
        };

        assert_eq!(task, &valid_verify_task);

        let valid_execute_tasks_dir = "testdata/xrpl_tasks/valid_tasks/ExecuteTask.json";
        let valid_execute_tasks_json = std::fs::read_to_string(valid_execute_tasks_dir).unwrap();
        let valid_execute_tasks: Vec<ExecuteTask> =
            serde_json::from_str(&valid_execute_tasks_json).unwrap();
        let valid_execute_task = valid_execute_tasks[0].clone();

        // Create complete task JSON as the server would store it
        let complete_execute_task_json = serde_json::json!({
            "id": valid_execute_task.common.id,
            "chain": valid_execute_task.common.chain,
            "timestamp": valid_execute_task.common.timestamp,
            "type": "EXECUTE",
            "meta": valid_execute_task.common.meta,
            "task": valid_execute_task.task
        });

        db.upsert(
            valid_execute_task.common.id.as_str(),
            valid_execute_task.common.chain.as_str(),
            valid_execute_task.common.timestamp.as_str(),
            TaskKind::Execute,
            Some(&serde_json::to_string(&complete_execute_task_json).unwrap()),
        )
        .await
        .unwrap();
        let raw_tasks = db.get_tasks().await.unwrap();
        assert_eq!(raw_tasks.len(), 2);

        let task = crate::utils::parse_task(&raw_tasks[1]).unwrap();
        let task = match &task {
            Task::Execute(execute_task) => execute_task,
            _ => panic!("Expected ExecuteTask, got different task type"),
        };

        assert_eq!(task, &valid_execute_task);

        let valid_gateway_tx_tasks_dir = "testdata/xrpl_tasks/valid_tasks/GatewayTxTask.json";
        let valid_gateway_tx_tasks_json =
            std::fs::read_to_string(valid_gateway_tx_tasks_dir).unwrap();
        let valid_gateway_tx_tasks: Vec<GatewayTxTask> =
            serde_json::from_str(&valid_gateway_tx_tasks_json).unwrap();
        let valid_gateway_tx_task = valid_gateway_tx_tasks[1].clone();

        // Create complete task JSON as the server would store it
        let complete_gateway_tx_task_json = serde_json::json!({
            "id": valid_gateway_tx_task.common.id,
            "chain": valid_gateway_tx_task.common.chain,
            "timestamp": valid_gateway_tx_task.common.timestamp,
            "type": "GATEWAY_TX",
            "meta": valid_gateway_tx_task.common.meta,
            "task": valid_gateway_tx_task.task
        });

        db.upsert(
            valid_gateway_tx_task.common.id.as_str(),
            valid_gateway_tx_task.common.chain.as_str(),
            valid_gateway_tx_task.common.timestamp.as_str(),
            TaskKind::GatewayTx,
            Some(&serde_json::to_string(&complete_gateway_tx_task_json).unwrap()),
        )
        .await
        .unwrap();

        let raw_tasks = db.get_tasks().await.unwrap();
        assert_eq!(raw_tasks.len(), 3);

        let task = crate::utils::parse_task(&raw_tasks[2]).unwrap();
        let task = match &task {
            Task::GatewayTx(gateway_tx_task) => gateway_tx_task,
            _ => panic!("Expected GatewayTxTask, got different task type"),
        };

        assert_eq!(task, &valid_gateway_tx_task);
    }
}
