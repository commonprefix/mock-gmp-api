use serde::de::DeserializeOwned;
use serde_json::Value;
use tracing::level_filters::LevelFilter;
use tracing_subscriber::{Registry, fmt, prelude::*};

use crate::gmp_types::{
    CommonTaskFields, ConstructProofTask, ExecuteTask, GatewayTxTask,
    ReactToExpiredSigningSessionTask, ReactToRetriablePollTask, ReactToWasmEventTask, RefundTask,
    Task, UnknownTask, VerifyTask,
};

use std::collections::HashMap;
use tokio::process::Command;

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

/// Execute a simple bash command and return the output
pub async fn execute_command(command: &str, args: &[&str]) -> Result<String, anyhow::Error> {
    let output = Command::new(command).args(args).output().await?;

    if !output.status.success() {
        let stderr = String::from_utf8_lossy(&output.stderr);
        return Err(anyhow::anyhow!("Command failed: {}", stderr));
    }

    let stdout = String::from_utf8_lossy(&output.stdout);
    Ok(stdout.trim().to_string())
}

/// Execute a bash script with environment variables
pub async fn execute_bash_script(
    script: &str,
    env_vars: Option<HashMap<String, String>>,
) -> Result<String, anyhow::Error> {
    let mut cmd = Command::new("bash");
    cmd.arg("-c").arg(script);

    // Set environment variables if provided
    if let Some(vars) = env_vars {
        for (key, value) in vars {
            cmd.env(key, value);
        }
    }

    let output = cmd.output().await?;

    if !output.status.success() {
        let stderr = String::from_utf8_lossy(&output.stderr);
        return Err(anyhow::anyhow!("Script failed: {}", stderr));
    }

    let stdout = String::from_utf8_lossy(&output.stdout);
    Ok(stdout.trim().to_string())
}

/// Execute a bash script from a file
pub async fn execute_script_file(
    script_path: &str,
    args: &[&str],
) -> Result<String, anyhow::Error> {
    let mut cmd_args = vec![script_path];
    cmd_args.extend_from_slice(args);

    let output = Command::new("bash").args(&cmd_args).output().await?;

    if !output.status.success() {
        let stderr = String::from_utf8_lossy(&output.stderr);
        return Err(anyhow::anyhow!("Script file failed: {}", stderr));
    }

    let stdout = String::from_utf8_lossy(&output.stdout);
    Ok(stdout.trim().to_string())
}
