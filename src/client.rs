use reqwest;
use serde_json::Value;
use std::error::Error;
use tracing::{debug, error, info};

use crate::gmp_types::StorePayloadResult;

pub struct Client {
    pub base_url: String,
    pub client: reqwest::Client,
}

impl Client {
    pub fn new(base_url: String) -> Self {
        Self {
            base_url,
            client: reqwest::Client::new(),
        }
    }

    pub async fn get_tasks(&self) -> Result<String, Box<dyn Error>> {
        let url = format!("{}/chains/xrpl/tasks", self.base_url);

        debug!("Making GET request to: {}", url);

        let response = self.client.get(&url).send().await?;

        if response.status().is_success() {
            let body = response.text().await?;
            info!("Response: {}", body);
            Ok(body)
        } else {
            let error_msg = format!("Request failed with status: {}", response.status());
            error!("{}", error_msg);
            Err(error_msg.into())
        }
    }

    pub async fn post_task(&self, task: Value) -> Result<String, anyhow::Error> {
        let url = format!("{}/chains/xrpl/task", self.base_url);
        let response = self.client.post(&url).json(&task).send().await?;

        Ok(response.text().await?)
    }

    pub async fn post_events(&self, events: Value) -> Result<String, anyhow::Error> {
        let url = format!("{}/chains/xrpl/events", self.base_url);
        let response = self.client.post(&url).json(&events).send().await?;

        Ok(response.text().await?)
    }

    /// Post binary payload and get keccak256 hash
    pub async fn post_payload(&self, payload: &[u8]) -> Result<String, Box<dyn Error>> {
        let url = format!("{}/payloads", self.base_url);

        debug!("Posting payload of {} bytes to: {}", payload.len(), url);

        let response = self
            .client
            .post(&url)
            .header("Content-Type", "application/octet-stream")
            .body(payload.to_vec())
            .send()
            .await?;

        if response.status().is_success() {
            let result: StorePayloadResult = response.json().await?;
            info!("Payload stored with hash: {}", result.keccak256);
            Ok(result.keccak256)
        } else {
            let error_msg = format!("Payload upload failed with status: {}", response.status());
            error!("{}", error_msg);
            Err(error_msg.into())
        }
    }

    /// Get payload by keccak256 hash (with 0x prefix)
    pub async fn get_payload(&self, hash: &str) -> Result<Vec<u8>, Box<dyn Error>> {
        let url = format!("{}/payloads/{}", self.base_url, hash);

        debug!("Getting payload from: {}", url);

        let response = self.client.get(&url).send().await?;

        if response.status().is_success() {
            let bytes = response.bytes().await?;
            info!("Retrieved payload of {} bytes", bytes.len());
            Ok(bytes.to_vec())
        } else if response.status() == 404 {
            let error_msg = format!("Payload not found for hash: {}", hash);
            error!("{}", error_msg);
            Err(error_msg.into())
        } else {
            let error_msg = format!(
                "Payload retrieval failed with status: {}",
                response.status()
            );
            error!("{}", error_msg);
            Err(error_msg.into())
        }
    }

    /// Test method: post payload, get it back, and verify they match
    pub async fn test_payload_roundtrip(&self, test_data: &[u8]) -> Result<bool, Box<dyn Error>> {
        info!("Testing payload roundtrip with {} bytes", test_data.len());

        // Post the payload
        let hash = self.post_payload(test_data).await?;
        info!("Posted payload, got hash: {}", hash);

        // Get the payload back
        let retrieved_data = self.get_payload(&hash).await?;
        info!("Retrieved payload of {} bytes", retrieved_data.len());

        // Verify they match
        let matches = test_data == retrieved_data.as_slice();
        if matches {
            info!("Payload roundtrip test PASSED - data matches!");
        } else {
            error!("Payload roundtrip test FAILED - data mismatch!");
        }

        Ok(matches)
    }
}
