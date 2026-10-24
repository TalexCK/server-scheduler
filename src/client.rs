use std::time::Duration;

use reqwest::{Client, Response, StatusCode};
use serde::{Serialize, de::DeserializeOwned};
use thiserror::Error;

use crate::{
  config::LoadedConfig,
  protocol::{CommandRequest, CommandResponse, InstanceInfo, LaunchRequest, LogResponse},
};

#[derive(Clone)]
pub struct SchedulerClient {
  client: Client,
  base_url: String,
  token: String,
}

impl SchedulerClient {
  pub fn new(config: &LoadedConfig) -> Result<Self, ClientError> {
    Ok(Self {
      client: Client::builder()
        .no_proxy()
        .timeout(Duration::from_secs(10))
        .build()?,
      base_url: format!("http://127.0.0.1:{}", config.value.api.port),
      token: config.value.api.token.clone(),
    })
  }

  pub async fn healthy(&self) -> bool {
    self
      .client
      .get(format!("{}/health", self.base_url))
      .send()
      .await
      .is_ok_and(|response| response.status().is_success())
  }

  pub async fn launch(
    &self,
    server_id: &str,
    players: Vec<String>,
  ) -> Result<InstanceInfo, ClientError> {
    self
      .post_json(
        &format!("/admin/v1/servers/{server_id}/launch"),
        &LaunchRequest { players },
      )
      .await
  }

  pub async fn terminate(&self, server_id: &str) -> Result<InstanceInfo, ClientError> {
    self
      .post_empty_json(&format!("/admin/v1/servers/{server_id}/terminate"))
      .await
  }

  pub async fn list(&self) -> Result<Vec<InstanceInfo>, ClientError> {
    let response = self
      .client
      .get(format!("{}/admin/v1/servers", self.base_url))
      .bearer_auth(&self.token)
      .send()
      .await?;
    decode(response).await
  }

  pub async fn logs(&self, server_id: &str, lines: usize) -> Result<LogResponse, ClientError> {
    let response = self
      .client
      .get(format!(
        "{}/admin/v1/servers/{server_id}/logs",
        self.base_url
      ))
      .query(&[("lines", lines)])
      .bearer_auth(&self.token)
      .send()
      .await?;
    decode(response).await
  }

  pub async fn command(
    &self,
    server_id: &str,
    command: String,
  ) -> Result<CommandResponse, ClientError> {
    self
      .post_json(
        &format!("/admin/v1/servers/{server_id}/command"),
        &CommandRequest { command },
      )
      .await
  }

  pub async fn shutdown(&self) -> Result<(), ClientError> {
    let response = self
      .client
      .post(format!("{}/admin/v1/system/shutdown", self.base_url))
      .bearer_auth(&self.token)
      .send()
      .await?;
    ensure_success(response).await.map(|_| ())
  }

  async fn post_json<I: Serialize, O: DeserializeOwned>(
    &self,
    path: &str,
    value: &I,
  ) -> Result<O, ClientError> {
    let response = self
      .client
      .post(format!("{}{}", self.base_url, path))
      .bearer_auth(&self.token)
      .json(value)
      .send()
      .await?;
    decode(response).await
  }

  async fn post_empty_json<O: DeserializeOwned>(&self, path: &str) -> Result<O, ClientError> {
    let response = self
      .client
      .post(format!("{}{}", self.base_url, path))
      // Lifecycle handlers wait for process exit and durable world cleanup.
      .timeout(Duration::from_secs(180))
      .bearer_auth(&self.token)
      .header("content-type", "application/json")
      .body("{}")
      .send()
      .await?;
    decode(response).await
  }
}

async fn decode<T: DeserializeOwned>(response: Response) -> Result<T, ClientError> {
  ensure_success(response)
    .await?
    .json()
    .await
    .map_err(ClientError::Http)
}

async fn ensure_success(response: Response) -> Result<Response, ClientError> {
  let status = response.status();
  if status.is_success() {
    return Ok(response);
  }
  let body = response.text().await.unwrap_or_default();
  Err(ClientError::Status { status, body })
}

#[derive(Debug, Error)]
pub enum ClientError {
  #[error("HTTP request failed: {0}")]
  Http(#[from] reqwest::Error),
  #[error("scheduler returned HTTP {status}: {body}")]
  Status { status: StatusCode, body: String },
}

#[cfg(test)]
mod tests {
  use super::*;
  use crate::protocol::InstanceState;

  #[tokio::test]
  async fn terminate_waits_beyond_the_normal_query_timeout() {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let app = axum::Router::new().route(
      "/admin/v1/servers/game/terminate",
      axum::routing::post(|| async {
        tokio::time::sleep(Duration::from_secs(11)).await;
        axum::Json(InstanceInfo {
          server_id: "game".into(),
          instance_id: uuid::Uuid::new_v4(),
          state: InstanceState::Exited,
          pid: 123,
          port: 50002,
          work_dir: "runtime/game".into(),
          log_file: "logs/game.log".into(),
          started_at: 0,
          last_heartbeat: None,
          exit_code: Some(0),
        })
      }),
    );
    let server = tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
    let client = SchedulerClient {
      client: Client::builder()
        .no_proxy()
        .timeout(Duration::from_secs(10))
        .build()
        .unwrap(),
      base_url: format!("http://{address}"),
      token: "test-token".into(),
    };
    let result = client.terminate("game").await;
    server.abort();
    assert_eq!(result.unwrap().state, InstanceState::Exited);
  }
}
