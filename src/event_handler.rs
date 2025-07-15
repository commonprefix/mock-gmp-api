use chrono::{DateTime, Utc};
use tracing::{debug, info, warn};
use uuid::Uuid;

use crate::{
    TasksModel,
    gmp_types::{
        CommonTaskFields, Event, EventType, Task, TaskKind, TaskMetadata, VerifyTask,
        VerifyTaskFields,
    },
    models::events::EventsModel,
};

pub async fn handle_call_or_gas_credit_event(
    event: Event,
    events_model: &EventsModel,
    tasks_model: &TasksModel,
    chain: &str,
    event_type_str: &str,
) -> Result<(), anyhow::Error> {
    // Check that no call or gas credit event exists with same type and message ID
    let maybe_event_with_same_type_and_message_id = events_model
        .find_event_by_type_and_message_id(event.event_type(), &event.message_id())
        .await
        .map_err(|e| anyhow::anyhow!(e.to_string()))?;
    if maybe_event_with_same_type_and_message_id.is_some() {
        warn!(
            "Event with same type and message ID already exists: {:?}",
            event
        );
        Ok(())
    } else {
        let desired_corresponding_event_type = if event_type_str == "CALL" {
            EventType::GasCredit
        } else {
            EventType::Call
        };

        let maybe_corresponding_event = events_model
            .find_event_by_type_and_message_id(
                desired_corresponding_event_type,
                &event.message_id(),
            )
            .await
            .map_err(|e| anyhow::anyhow!(e.to_string()))?;
        if let Some(corresponding_event) = maybe_corresponding_event {
            // We can proceed with creating a VERIFY task
            // TODO: Refactor GMP Types for Events to be similar to Tasks enums
            let (gas_credit_event, call_event) = if event.event_type() == EventType::Call {
                (corresponding_event, event.clone())
            } else {
                (event.clone(), corresponding_event)
            };
            debug!("Call event: {:?}", call_event);
            debug!("Gas credit event: {:?}", gas_credit_event);

            let (message, payload, meta) = match &call_event {
                Event::Call {
                    message,
                    payload,
                    common,
                    ..
                } => (message, payload, &common.meta),
                // should never happen
                _ => {
                    return Err(anyhow::anyhow!("Expected Call event"));
                }
            };

            let task = VerifyTask {
                common: CommonTaskFields {
                    id: Uuid::new_v4().to_string(),
                    chain: chain.to_string(),
                    timestamp: Utc::now().to_rfc3339(),
                    r#type: "VERIFY".to_string(),
                    meta: meta.as_ref().map(|event_meta| TaskMetadata {
                        tx_id: event_meta.tx_id.clone(),
                        from_address: event_meta.from_address.clone(),
                        finalized: event_meta.finalized,
                        source_context: event_meta.source_context.clone(),
                        scoped_messages: None,
                    }),
                },
                task: VerifyTaskFields {
                    message: message.clone(),
                    payload: payload.clone(),
                },
            };

            let db_write_result = tasks_model
                .upsert(
                    &task.common.id,
                    &task.common.chain,
                    task.common.timestamp.parse::<DateTime<Utc>>().unwrap(),
                    TaskKind::Verify,
                    Some(&serde_json::to_string(&task).unwrap()),
                )
                .await
                .map_err(|e| anyhow::anyhow!(e.to_string()));

            if let Err(e) = db_write_result {
                // TODO: If the task is not written it cannot be retried since the events stay in the database and duplicate check will fail
                // Maybe here we delete the corresponding events from the DB?
                warn!("Failed to write VERIFY task to database: {:?}", e);
            } else {
                info!("Created VERIFY task: {:?}", task);
            }
        }
        Ok(())
    }
}
