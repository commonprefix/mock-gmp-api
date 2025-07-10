use reqwest;
use serde_json::Value;
use std::error::Error;

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

        println!("Making GET request to: {}", url);

        let response = self.client.get(&url).send().await?;

        if response.status().is_success() {
            let body = response.text().await?;
            println!("Response: {}", body);
            Ok(body)
        } else {
            let error_msg = format!("Request failed with status: {}", response.status());
            println!("{}", error_msg);
            Err(error_msg.into())
        }
    }

    pub async fn post_task(&self, task: Value) -> Result<String, anyhow::Error> {
        let url = format!("{}/task", self.base_url);
        let response = self.client.post(&url).json(&task).send().await?;

        Ok(response.text().await?)
    }

    pub async fn post_events(&self, events: Value) -> Result<String, anyhow::Error> {
        let url = format!("{}/chains/xrpl/events", self.base_url);
        let response = self.client.post(&url).json(&events).send().await?;

        Ok(response.text().await?)
    }
}
