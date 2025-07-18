use crate::gmp_types::Event;
use crate::gmp_types::EventType;
use chrono::{DateTime, Utc};
use serde_json;
use sqlx::{PgPool, Row};
use tracing::error;

const PG_TABLE_NAME: &str = "events";

#[derive(Clone, Debug)]
pub struct EventsModel {
    pool: PgPool,
}

impl EventsModel {
    pub async fn new(url: &str) -> Result<Self, anyhow::Error> {
        let pool = PgPool::connect(url).await?;
        Ok(Self { pool })
    }

    pub async fn find(&self, id: &str) -> Result<Option<Event>, anyhow::Error> {
        let query = format!("SELECT event FROM {} WHERE id = $1", PG_TABLE_NAME);
        let row = sqlx::query(&query)
            .bind(id)
            .fetch_optional(&self.pool)
            .await?;

        let event = row.and_then(|row| {
            let event_text: String = row.get("event");

            match serde_json::from_str(&event_text) {
                Ok(event) => Some(event),
                Err(e) => {
                    error!("Failed to parse event JSON: {:?}", e);
                    None
                }
            }
        });

        Ok(event)
    }

    pub async fn insert(
        &self,
        id: &str,
        timestamp: DateTime<Utc>,
        event_type: EventType,
        event: &str,
        message_id: &str,
    ) -> Result<(), anyhow::Error> {
        let query = format!(
            "INSERT INTO {} (id, timestamp, type, event, message_id) VALUES ($1, $2, $3, $4, $5)",
            PG_TABLE_NAME
        );

        sqlx::query(&query)
            .bind(id)
            .bind(timestamp)
            .bind(event_type)
            .bind(event)
            .bind(message_id)
            .execute(&self.pool)
            .await?;

        Ok(())
    }

    pub async fn upsert(
        &self,
        id: &str,
        timestamp: DateTime<Utc>,
        event_type: EventType,
        event: &str,
        message_id: &str,
    ) -> Result<(), anyhow::Error> {
        let query = format!(
            "INSERT INTO {} (id, timestamp, type, event, message_id) VALUES ($1, $2, $3, $4, $5) ON CONFLICT (id) DO UPDATE SET timestamp = $2, type = $3, event = $4, message_id = $5",
            PG_TABLE_NAME
        );

        sqlx::query(&query)
            .bind(id)
            .bind(timestamp)
            .bind(event_type)
            .bind(event)
            .bind(message_id)
            .execute(&self.pool)
            .await?;

        Ok(())
    }

    pub async fn delete(&self, id: &str) -> Result<(), anyhow::Error> {
        let query = format!("DELETE FROM {} WHERE id = $1", PG_TABLE_NAME);
        sqlx::query(&query).bind(id).execute(&self.pool).await?;

        Ok(())
    }

    pub async fn get_events(&self) -> Result<Vec<serde_json::Value>, anyhow::Error> {
        let query = format!("SELECT event FROM {}", PG_TABLE_NAME);
        let rows = sqlx::query(&query).fetch_all(&self.pool).await?;

        Ok(rows
            .iter()
            .filter_map(|row| {
                let event_text: String = row.get("event");

                match serde_json::from_str(&event_text) {
                    Ok(value) => Some(value),
                    Err(e) => {
                        error!("Failed to parse event JSON: {:?}", e);
                        None
                    }
                }
            })
            .collect::<Vec<_>>())
    }

    pub async fn find_by_message_id(
        &self,
        message_id: &str,
    ) -> Result<Option<Event>, anyhow::Error> {
        let query = format!("SELECT event FROM {} WHERE message_id = $1", PG_TABLE_NAME);
        let row = sqlx::query(&query)
            .bind(message_id)
            .fetch_optional(&self.pool)
            .await?;
        let event = row.and_then(|row| {
            let event_text: String = row.get("event");

            match serde_json::from_str(&event_text) {
                Ok(event) => Some(event),
                Err(e) => {
                    error!("Failed to parse event JSON: {:?}", e);
                    None
                }
            }
        });

        Ok(event)
    }

    pub async fn find_event_by_type_and_message_id(
        &self,
        event_type: EventType,
        message_id: &str,
    ) -> Result<Option<Event>, anyhow::Error> {
        let query = format!(
            "SELECT event FROM {} WHERE type = $1 AND message_id = $2",
            PG_TABLE_NAME
        );
        let row = sqlx::query(&query)
            .bind(event_type)
            .bind(message_id)
            .fetch_optional(&self.pool)
            .await?;
        let event = row.and_then(|row| {
            let event_text: String = row.get("event");

            match serde_json::from_str(&event_text) {
                Ok(event) => Some(event),
                Err(e) => {
                    error!("Failed to parse event JSON: {:?}", e);
                    None
                }
            }
        });

        Ok(event)
    }
}

#[cfg(test)]
mod tests {

    use chrono::{DateTime, Utc};
    use testcontainers::{ContainerAsync, runners::AsyncRunner};
    use testcontainers_modules::postgres;

    use crate::{
        gmp_types::{Event, EventType},
        models::events::EventsModel,
    };

    async fn setup_test_container() -> (EventsModel, ContainerAsync<postgres::Postgres>) {
        let container = postgres::Postgres::default()
            .with_init_sql(
                include_str!("../../migrations/0002_events.sql")
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
        let model = EventsModel::new(&connection_string).await.unwrap();
        // we need to return the container too otherwise it will be dropped and the test will run forever
        (model, container)
    }

    #[tokio::test]
    async fn test_upsert_and_get_events() {
        let (db, _container) = setup_test_container().await;

        let mut expected_events = Vec::new();

        // Test all CallEvents
        let call_events_dir = "testdata/events/CallEvent.json";
        let call_events_json = std::fs::read_to_string(call_events_dir).unwrap();
        let call_events: Vec<Event> = serde_json::from_str(&call_events_json).unwrap();

        for call_event in call_events {
            let (event_id, _event_type, timestamp_str) = call_event.common_fields();
            let message_id = call_event.message_id();

            let timestamp = timestamp_str
                .parse::<DateTime<Utc>>()
                .expect("Failed to parse timestamp");

            db.upsert(
                event_id,
                timestamp,
                EventType::Call,
                &serde_json::to_string(&call_event).unwrap(),
                &message_id,
            )
            .await
            .unwrap();

            expected_events.push(call_event);
        }

        // Test all GasCreditEvents
        let gas_credit_events_dir = "testdata/events/GasCreditEvent.json";
        let gas_credit_events_json = std::fs::read_to_string(gas_credit_events_dir).unwrap();
        let gas_credit_events: Vec<Event> = serde_json::from_str(&gas_credit_events_json).unwrap();

        for gas_credit_event in gas_credit_events {
            let (event_id, _event_type, timestamp_str) = gas_credit_event.common_fields();
            let message_id = gas_credit_event.message_id();
            let timestamp = timestamp_str
                .parse::<DateTime<Utc>>()
                .expect("Failed to parse timestamp");

            db.upsert(
                event_id,
                timestamp,
                EventType::GasCredit,
                &serde_json::to_string(&gas_credit_event).unwrap(),
                &message_id,
            )
            .await
            .unwrap();

            expected_events.push(gas_credit_event);
        }

        let raw_events = db.get_events().await.unwrap();
        assert_eq!(raw_events.len(), expected_events.len());

        let parsed_events: Vec<Event> = raw_events
            .iter()
            .map(|event_json| serde_json::from_value(event_json.clone()).unwrap())
            .collect();

        let expected_event_jsons: Vec<String> = expected_events
            .iter()
            .map(|event| serde_json::to_string(event).unwrap())
            .collect();

        let parsed_event_jsons: Vec<String> = parsed_events
            .iter()
            .map(|event| serde_json::to_string(event).unwrap())
            .collect();

        for expected_json in &expected_event_jsons {
            assert!(
                parsed_event_jsons.contains(expected_json),
                "Expected event not found in parsed events: {}",
                expected_json
            );
        }
    }
}
