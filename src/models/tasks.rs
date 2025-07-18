use crate::{
    gmp_types::{Task, TaskKind},
    utils::parse_task,
};
use chrono::{DateTime, Utc};
use serde_json;
use sqlx::{PgPool, Row};
use tracing::error;

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
                    error!("Failed to parse task JSON: {:?}", e);
                    return None;
                }
            };

            match parse_task(&task_json) {
                Ok(task) => Some(task),
                Err(e) => {
                    error!("Failed to parse task: {:?}", e);
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
        timestamp: DateTime<Utc>,
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

    pub async fn get_tasks(
        &self,
        chain: &str,
        after: Option<&str>,
    ) -> Result<Vec<serde_json::Value>, anyhow::Error> {
        let after_task = match after {
            Some(id) => self.find(id).await?,
            None => None,
        };
        let after_task_timestamp: String = match after_task {
            Some(task) => task.common().timestamp.clone(),
            None => "1970-01-01T00:00:00Z".to_string(),
        };
        let query = format!(
            "SELECT task FROM {} WHERE chain = $1 AND timestamp > $2",
            PG_TABLE_NAME
        );
        let rows = sqlx::query(&query)
            .bind(chain)
            .bind(after_task_timestamp.parse::<DateTime<Utc>>().unwrap())
            .fetch_all(&self.pool)
            .await?;

        Ok(rows
            .iter()
            .filter_map(|row| {
                let task_text: String = row.get("task");

                match serde_json::from_str(&task_text) {
                    Ok(value) => Some(value),
                    Err(e) => {
                        error!("Failed to parse task JSON: {:?}", e);
                        None
                    }
                }
            })
            .collect::<Vec<_>>())
    }
}

#[cfg(test)]
mod tests {

    use chrono::{DateTime, Utc};
    use testcontainers::{ContainerAsync, runners::AsyncRunner};
    use testcontainers_modules::postgres;

    use crate::{
        gmp_types::{ExecuteTask, GatewayTxTask, Task, TaskKind, VerifyTask},
        models::tasks::TasksModel,
        utils::parse_task,
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
        let mut expected_tasks = Vec::new();

        // Test all VerifyTasks
        let valid_verify_tasks_dir = "testdata/gmp_tasks/valid_tasks/VerifyTask.json";
        let valid_verify_tasks_json = std::fs::read_to_string(valid_verify_tasks_dir).unwrap();
        let valid_verify_tasks: Vec<VerifyTask> =
            serde_json::from_str(&valid_verify_tasks_json).unwrap();

        for valid_verify_task in valid_verify_tasks {
            let complete_task_json = serde_json::json!({
                "id": valid_verify_task.common.id,
                "chain": valid_verify_task.common.chain,
                "timestamp": valid_verify_task.common.timestamp,
                "type": "VERIFY",
                "meta": valid_verify_task.common.meta,
                "task": valid_verify_task.task
            });

            let timestamp = valid_verify_task
                .common
                .timestamp
                .parse::<DateTime<Utc>>()
                .expect("Failed to parse timestamp");

            db.upsert(
                valid_verify_task.common.id.as_str(),
                valid_verify_task.common.chain.as_str(),
                timestamp,
                TaskKind::Verify,
                Some(&serde_json::to_string(&complete_task_json).unwrap()),
            )
            .await
            .unwrap();

            expected_tasks.push(Task::Verify(valid_verify_task));
        }

        // Test all ExecuteTasks
        let valid_execute_tasks_dir = "testdata/gmp_tasks/valid_tasks/ExecuteTask.json";
        let valid_execute_tasks_json = std::fs::read_to_string(valid_execute_tasks_dir).unwrap();
        let valid_execute_tasks: Vec<ExecuteTask> =
            serde_json::from_str(&valid_execute_tasks_json).unwrap();

        for valid_execute_task in valid_execute_tasks {
            let complete_execute_task_json = serde_json::json!({
                "id": valid_execute_task.common.id,
                "chain": valid_execute_task.common.chain,
                "timestamp": valid_execute_task.common.timestamp,
                "type": "EXECUTE",
                "meta": valid_execute_task.common.meta,
                "task": valid_execute_task.task
            });

            let timestamp = valid_execute_task
                .common
                .timestamp
                .parse::<DateTime<Utc>>()
                .expect("Failed to parse timestamp");

            db.upsert(
                valid_execute_task.common.id.as_str(),
                valid_execute_task.common.chain.as_str(),
                timestamp,
                TaskKind::Execute,
                Some(&serde_json::to_string(&complete_execute_task_json).unwrap()),
            )
            .await
            .unwrap();

            expected_tasks.push(Task::Execute(valid_execute_task));
        }

        // Test all GatewayTxTasks
        let valid_gateway_tx_tasks_dir = "testdata/gmp_tasks/valid_tasks/GatewayTxTask.json";
        let valid_gateway_tx_tasks_json =
            std::fs::read_to_string(valid_gateway_tx_tasks_dir).unwrap();
        let valid_gateway_tx_tasks: Vec<GatewayTxTask> =
            serde_json::from_str(&valid_gateway_tx_tasks_json).unwrap();

        for valid_gateway_tx_task in valid_gateway_tx_tasks {
            let complete_gateway_tx_task_json = serde_json::json!({
                "id": valid_gateway_tx_task.common.id,
                "chain": valid_gateway_tx_task.common.chain,
                "timestamp": valid_gateway_tx_task.common.timestamp,
                "type": "GATEWAY_TX",
                "meta": valid_gateway_tx_task.common.meta,
                "task": valid_gateway_tx_task.task
            });

            let timestamp = valid_gateway_tx_task
                .common
                .timestamp
                .parse::<DateTime<Utc>>()
                .expect("Failed to parse timestamp");

            db.upsert(
                valid_gateway_tx_task.common.id.as_str(),
                valid_gateway_tx_task.common.chain.as_str(),
                timestamp,
                TaskKind::GatewayTx,
                Some(&serde_json::to_string(&complete_gateway_tx_task_json).unwrap()),
            )
            .await
            .unwrap();

            expected_tasks.push(Task::GatewayTx(valid_gateway_tx_task));
        }

        let raw_tasks = db.get_tasks("xrpl", None).await.unwrap();
        assert_eq!(raw_tasks.len(), expected_tasks.len());

        let parsed_tasks: Vec<Task> = raw_tasks
            .iter()
            .map(|task_json| parse_task(task_json).unwrap())
            .collect();

        let expected_task_ids: Vec<String> = expected_tasks.iter().map(|task| task.id()).collect();

        let parsed_task_ids: Vec<String> = parsed_tasks.iter().map(|task| task.id()).collect();

        assert_eq!(expected_task_ids, parsed_task_ids, "Task IDs should match");

        for expected_task in &expected_tasks {
            let found = parsed_tasks
                .iter()
                .any(|parsed_task| match (expected_task, parsed_task) {
                    (Task::Verify(expected), Task::Verify(parsed)) => expected == parsed,
                    (Task::Execute(expected), Task::Execute(parsed)) => expected == parsed,
                    (Task::GatewayTx(expected), Task::GatewayTx(parsed)) => expected == parsed,
                    _ => false,
                });
            assert!(
                found,
                "Expected task not found in parsed tasks: {:?}",
                expected_task.id()
            );
        }
    }

    #[tokio::test]
    async fn test_get_tasks_after() {
        let (db, _container) = setup_test_container().await;

        let task1_json = serde_json::json!({
            "id": "0197a679-9cf6-785c-8666-a2cf0c84c984",
            "chain": "xrpl",
            "timestamp": "2024-01-01T00:00:00Z",
            "type": "GATEWAY_TX",
            "meta": null,
            "task": {
                "executeData": "data1"
            }
        });

        let task2_json = serde_json::json!({
            "id": "0197a679-9cf6-785c-8666-a2cf0c84c985",
            "chain": "xrpl",
            "timestamp": "2024-02-01T00:00:00Z",
            "type": "GATEWAY_TX",
            "meta": null,
            "task": {
                "executeData": "data2"
            }
        });

        let task3_json = serde_json::json!({
            "id": "0197a679-9cf6-785c-8666-a2cf0c84c986",
            "chain": "xrpl",
            "timestamp": "2024-03-01T00:00:00Z",
            "type": "GATEWAY_TX",
            "meta": null,
            "task": {
                "executeData": "data3"
            }
        });

        db.upsert(
            "0197a679-9cf6-785c-8666-a2cf0c84c984",
            "xrpl",
            "2024-01-01T00:00:00Z".parse::<DateTime<Utc>>().unwrap(),
            TaskKind::GatewayTx,
            Some(&serde_json::to_string(&task1_json).unwrap()),
        )
        .await
        .unwrap();

        db.upsert(
            "0197a679-9cf6-785c-8666-a2cf0c84c985",
            "xrpl",
            "2024-02-01T00:00:00Z".parse::<DateTime<Utc>>().unwrap(),
            TaskKind::GatewayTx,
            Some(&serde_json::to_string(&task2_json).unwrap()),
        )
        .await
        .unwrap();

        db.upsert(
            "0197a679-9cf6-785c-8666-a2cf0c84c986",
            "xrpl",
            "2024-03-01T00:00:00Z".parse::<DateTime<Utc>>().unwrap(),
            TaskKind::GatewayTx,
            Some(&serde_json::to_string(&task3_json).unwrap()),
        )
        .await
        .unwrap();

        // Get tasks after second task
        let tasks = db
            .get_tasks("xrpl", Some("0197a679-9cf6-785c-8666-a2cf0c84c985"))
            .await
            .unwrap();

        // Should only return one task which has a greater timestamp
        assert_eq!(tasks.len(), 1);

        let tasks = db
            .get_tasks("xrpl", Some("0197a679-9cf6-785c-8666-a2cf0c84c984"))
            .await
            .unwrap();

        assert_eq!(tasks.len(), 2);

        let tasks = db
            .get_tasks("xrpl", Some("0197a679-9cf6-785c-8666-a2cf0c84c986"))
            .await
            .unwrap();

        assert_eq!(tasks.len(), 0);
    }
}
