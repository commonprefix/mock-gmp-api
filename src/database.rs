use crate::{
    gmp_types::{Task, TaskKind},
    utils::parse_task,
};
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

    // pub async fn get_tasks(&self) -> Result<Vec<serde_json::Value>, anyhow::Error> {
    //     let query = "SELECT id, chain, timestamp, type, meta, task FROM tasks";
    //     let rows = sqlx::query(query).fetch_all(&self.pool).await?;

    //     Ok(rows
    //         .iter()
    //         .filter_map(|row| {
    //             let id: String = row.get("id");
    //             let chain: String = row.get("chain");
    //             let timestamp: String = row.get("timestamp");
    //             let task_kind: TaskKind = row.get("type");
    //             let meta: Option<String> = row.get("meta");
    //             let task_text: String = row.get("task");

    //             let task_type = match task_kind {
    //                 TaskKind::Verify => "VERIFY",
    //                 TaskKind::Execute => "EXECUTE",
    //                 TaskKind::GatewayTx => "GATEWAY_TX",
    //                 TaskKind::ConstructProof => "CONSTRUCT_PROOF",
    //                 TaskKind::ReactToWasmEvent => "REACT_TO_WASM_EVENT",
    //                 TaskKind::Refund => "REFUND",
    //                 TaskKind::ReactToExpiredSigningSession => "REACT_TO_EXPIRED_SIGNING_SESSION",
    //                 TaskKind::ReactToRetriablePoll => "REACT_TO_RETRIABLE_POLL",
    //                 TaskKind::Unknown => "UNKNOWN",
    //             };

    //             let task_fields: serde_json::Value = match serde_json::from_str(&task_text) {
    //                 Ok(value) => value,
    //                 Err(e) => {
    //                     println!("Failed to parse task fields: {:?}", e);
    //                     return None;
    //                 }
    //             };

    //             let meta_value: Option<serde_json::Value> =
    //                 meta.and_then(|m| serde_json::from_str(&m).ok());

    //             serde_json::json!({
    //                 "id": id,
    //                 "chain": chain,
    //                 "timestamp": timestamp,
    //                 "type": task_type,
    //                 "meta": meta_value,
    //                 "task": task_fields
    //             })
    //         })
    //         .filter(|v| !v.is_null()) // Filter out failed parses
    //         .collect::<Vec<_>>())
    // }

    pub async fn find(&self, id: &str) -> Result<Option<Task>, anyhow::Error> {
        let query = "SELECT id, chain, timestamp, type, meta, task FROM tasks WHERE id = $1";
        let row = sqlx::query(query)
            .bind(id)
            .fetch_optional(&self.pool)
            .await?;

        let task = row.and_then(|row| {
            let id: String = row.get("id");
            let chain: String = row.get("chain");
            let timestamp: String = row.get("timestamp");
            let task_kind: TaskKind = row.get("type");
            let meta: Option<String> = row.get("meta");
            let task_text: String = row.get("task");

            let task_type = match task_kind {
                TaskKind::Verify => "VERIFY",
                TaskKind::Execute => "EXECUTE",
                TaskKind::GatewayTx => "GATEWAY_TX",
                TaskKind::ConstructProof => "CONSTRUCT_PROOF",
                TaskKind::ReactToWasmEvent => "REACT_TO_WASM_EVENT",
                TaskKind::Refund => "REFUND",
                TaskKind::ReactToExpiredSigningSession => "REACT_TO_EXPIRED_SIGNING_SESSION",
                TaskKind::ReactToRetriablePoll => "REACT_TO_RETRIABLE_POLL",
                TaskKind::Unknown => "UNKNOWN",
            };

            let task_fields: serde_json::Value = match serde_json::from_str(&task_text) {
                Ok(value) => value,
                Err(e) => {
                    println!("Failed to parse task fields: {:?}", e);
                    return None;
                }
            };

            let meta_value: Option<serde_json::Value> =
                meta.and_then(|m| serde_json::from_str(&m).ok());

            let complete_task_json = serde_json::json!({
                "id": id,
                "chain": chain,
                "timestamp": timestamp,
                "type": task_type,
                "meta": meta_value,
                "task": task_fields
            });

            match parse_task(&complete_task_json) {
                Ok(task) => Some(task),
                Err(e) => {
                    println!("Failed to parse complete task: {:?}", e);
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
        meta: Option<&str>,
        task: Option<&str>,
    ) -> Result<(), anyhow::Error> {
        let query = "INSERT INTO tasks (id, chain, timestamp, type, meta, task) VALUES ($1, $2, $3, $4, $5, $6) ON CONFLICT (id) DO UPDATE SET chain = $2, timestamp = $3, type = $4, meta = $5, task = $6 RETURNING *";

        sqlx::query(query)
            .bind(id)
            .bind(chain)
            .bind(timestamp)
            .bind(task_type)
            .bind(meta)
            .bind(task)
            .execute(&self.pool)
            .await?;

        Ok(())
    }

    pub async fn delete(&self, id: &str) -> Result<(), anyhow::Error> {
        let query = "DELETE FROM tasks WHERE id = $1";
        sqlx::query(query).bind(id).execute(&self.pool).await?;

        Ok(())
    }

    pub async fn get_tasks(&self) -> Result<Vec<serde_json::Value>, anyhow::Error> {
        let query = "SELECT id, chain, timestamp, type, meta, task FROM tasks";
        let rows = sqlx::query(query).fetch_all(&self.pool).await?;

        Ok(rows
            .iter()
            .map(|row| {
                let id: String = row.get("id");
                let chain: String = row.get("chain");
                let timestamp: String = row.get("timestamp");
                let task_kind: TaskKind = row.get("type");
                let meta: Option<String> = row.get("meta");
                let task_text: String = row.get("task");

                let task_type = match task_kind {
                    TaskKind::Verify => "VERIFY",
                    TaskKind::Execute => "EXECUTE",
                    TaskKind::GatewayTx => "GATEWAY_TX",
                    TaskKind::ConstructProof => "CONSTRUCT_PROOF",
                    TaskKind::ReactToWasmEvent => "REACT_TO_WASM_EVENT",
                    TaskKind::Refund => "REFUND",
                    TaskKind::ReactToExpiredSigningSession => "REACT_TO_EXPIRED_SIGNING_SESSION",
                    TaskKind::ReactToRetriablePoll => "REACT_TO_RETRIABLE_POLL",
                    TaskKind::Unknown => "UNKNOWN",
                };

                let task_fields: serde_json::Value = match serde_json::from_str(&task_text) {
                    Ok(value) => value,
                    Err(e) => {
                        println!("Failed to parse task fields: {:?}", e);
                        return serde_json::Value::Null;
                    }
                };

                let meta_value: Option<serde_json::Value> =
                    meta.and_then(|m| serde_json::from_str(&m).ok());

                // Return the raw JSON structure that get_tasks_action expects
                serde_json::json!({
                    "id": id,
                    "chain": chain,
                    "timestamp": timestamp,
                    "type": task_type,
                    "meta": meta_value,
                    "task": task_fields
                })
            })
            .filter(|v| !v.is_null()) // Filter out failed parses
            .collect::<Vec<_>>())
    }
}

#[cfg(test)]
mod tests {

    use testcontainers::{ContainerAsync, runners::AsyncRunner};
    use testcontainers_modules::postgres;

    use crate::{
        PostgresDB,
        gmp_types::{ExecuteTask, GatewayTxTask, Task, TaskKind, VerifyTask},
    };

    async fn setup_test_container() -> (PostgresDB, ContainerAsync<postgres::Postgres>) {
        let container = postgres::Postgres::default()
            .with_init_sql(
                include_str!("../migrations/0001_tasks.sql")
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
        let model = PostgresDB::new(&connection_string).await.unwrap();
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

        db.upsert(
            valid_verify_task.common.id.as_str(),
            valid_verify_task.common.chain.as_str(),
            valid_verify_task.common.timestamp.as_str(),
            TaskKind::Verify,
            valid_verify_task
                .common
                .meta
                .as_ref()
                .map(|meta| serde_json::to_string(meta).unwrap())
                .as_deref(),
            Some(&serde_json::to_string(&valid_verify_task.task).unwrap()),
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

        db.upsert(
            valid_execute_task.common.id.as_str(),
            valid_execute_task.common.chain.as_str(),
            valid_execute_task.common.timestamp.as_str(),
            TaskKind::Execute,
            valid_execute_task
                .common
                .meta
                .as_ref()
                .map(|meta| serde_json::to_string(meta).unwrap())
                .as_deref(),
            Some(&serde_json::to_string(&valid_execute_task.task).unwrap()),
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

        db.upsert(
            valid_gateway_tx_task.common.id.as_str(),
            valid_gateway_tx_task.common.chain.as_str(),
            valid_gateway_tx_task.common.timestamp.as_str(),
            TaskKind::GatewayTx,
            valid_gateway_tx_task
                .common
                .meta
                .as_ref()
                .map(|meta| serde_json::to_string(meta).unwrap())
                .as_deref(),
            Some(&serde_json::to_string(&valid_gateway_tx_task.task).unwrap()),
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
