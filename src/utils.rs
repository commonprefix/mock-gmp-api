use serde::de::DeserializeOwned;
use serde_json::Value;
use tracing::{debug, error, info, level_filters::LevelFilter};
use tracing_subscriber::{Registry, fmt, prelude::*};

use crate::gmp_types::{
    CommonTaskFields, ConstructProofTask, ExecuteTask, GatewayTxTask,
    ReactToExpiredSigningSessionTask, ReactToRetriablePollTask, ReactToWasmEventTask, RefundTask,
    Task, UnknownTask, VerifyTask,
};

fn parse_as<T: DeserializeOwned>(value: &Value) -> Result<T, anyhow::Error> {
    serde_json::from_value(value.clone()).map_err(|e| anyhow::anyhow!(e.to_string()))
}

pub fn parse_task(task_json: &Value) -> Result<Task, anyhow::Error> {
    let task_headers: CommonTaskFields =
        serde_json::from_value(task_json.clone()).map_err(|e| anyhow::anyhow!(e.to_string()))?;

    match task_headers.r#type.as_str() {
        "CONSTRUCT_PROOF" => {
            let task: ConstructProofTask = parse_as(task_json)?;
            Ok(Task::ConstructProof(task))
        }
        "GATEWAY_TX" => {
            let task: GatewayTxTask = parse_as(task_json)?;
            Ok(Task::GatewayTx(task))
        }
        "VERIFY" => {
            let task: VerifyTask = parse_as(task_json)?;
            Ok(Task::Verify(task))
        }
        "EXECUTE" => {
            let task: ExecuteTask = parse_as(task_json)?;
            Ok(Task::Execute(task))
        }
        "REFUND" => {
            let task: RefundTask = parse_as(task_json)?;
            Ok(Task::Refund(task))
        }
        "REACT_TO_WASM_EVENT" => {
            let task: ReactToWasmEventTask = parse_as(task_json)?;
            Ok(Task::ReactToWasmEvent(task))
        }
        "REACT_TO_RETRIABLE_POLL" => {
            let task: ReactToRetriablePollTask = parse_as(task_json)?;
            Ok(Task::ReactToRetriablePoll(task))
        }
        "REACT_TO_EXPIRED_SIGNING_SESSION" => {
            let task: ReactToExpiredSigningSessionTask = parse_as(task_json)?;
            Ok(Task::ReactToExpiredSigningSession(task))
        }
        _ => {
            let task: UnknownTask = parse_as(task_json)?;
            Ok(Task::Unknown(task))
        }
    }
}

pub fn setup_logging() {
    let fmt_layer = fmt::layer()
        .with_target(true)
        .with_filter(LevelFilter::DEBUG);

    let gmp_api = Registry::default().with(fmt_layer);

    tracing::subscriber::set_global_default(gmp_api)
        .expect("Failed to set global tracing subscriber");
}

pub fn extract_id_and_contract_address(
    script_result: &Value,
    event_type: &str,
) -> Result<Option<(String, String)>, anyhow::Error> {
    let logs = script_result
        .get("logs")
        .and_then(|v| v.as_array())
        .ok_or_else(|| {
            error!("Logs not found in verify messages");
            anyhow::anyhow!("Logs not found in verify messages")
        })?;
    for log in logs {
        let events = log
            .get("events")
            .and_then(|v| v.as_array())
            .ok_or_else(|| {
                error!("Events not found in log");
                anyhow::anyhow!("Events not found in log")
            })?;

        for event in events {
            let parsed_event_type = event.get("type").and_then(|v| v.as_str()).unwrap_or("");
            if parsed_event_type == event_type {
                debug!("Poll started event found");
                debug!("Event: {:?}", event);
                let attributes = event
                    .get("attributes")
                    .and_then(|v| v.as_array())
                    .ok_or_else(|| {
                        error!("Attributes not found in event");
                        anyhow::anyhow!("Attributes not found in event")
                    })?;
                let poll_id = attributes
                    .iter()
                    .find(|attr| {
                        attr.get("key").and_then(|v| v.as_str()).unwrap_or("") == "poll_id"
                    })
                    .and_then(|attr| attr.get("value").and_then(|v| v.as_str()))
                    .unwrap_or("");
                let contract_address = attributes
                    .iter()
                    .find(|attr| {
                        attr.get("key").and_then(|v| v.as_str()).unwrap_or("")
                            == "_contract_address"
                    })
                    .and_then(|attr| attr.get("value").and_then(|v| v.as_str()))
                    .unwrap_or("");
                if poll_id != "" && contract_address != "" {
                    return Ok(Some((poll_id.to_string(), contract_address.to_string())));
                }
            }
        }
    }
    Ok(None)
}
