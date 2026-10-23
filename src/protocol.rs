use std::path::PathBuf;

use serde::{Deserialize, Serialize};
use uuid::Uuid;

use crate::config::{SoloMode, SoloStartup};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LaunchRequest {
  #[serde(default)]
  pub players: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CommandRequest {
  pub command: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CommandResponse {
  pub output: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LogResponse {
  pub lines: Vec<String>,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "lowercase")]
pub enum InstanceState {
  Starting,
  Ready,
  Stopping,
  Exited,
  Failed,
}

impl InstanceState {
  pub fn active(self) -> bool {
    matches!(self, Self::Starting | Self::Ready | Self::Stopping)
  }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct InstanceInfo {
  pub server_id: String,
  pub instance_id: Uuid,
  pub state: InstanceState,
  pub pid: u32,
  pub port: u16,
  pub work_dir: PathBuf,
  pub log_file: PathBuf,
  pub started_at: u64,
  pub last_heartbeat: Option<u64>,
  pub exit_code: Option<i32>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct ReadyForm {
  pub server_id: String,
  pub instance_id: Uuid,
}

#[derive(Debug, Clone, Deserialize)]
pub struct HeartbeatForm {
  pub server_id: String,
  pub instance_id: Uuid,
}

#[derive(Debug, Clone, Deserialize)]
pub struct IdleForm {
  pub server_id: String,
  pub instance_id: Uuid,
}

#[derive(Debug, Clone, Deserialize)]
pub struct PlayersForm {
  #[serde(default)]
  pub players: String,
}

#[derive(Debug, Clone, Deserialize)]
pub struct PlayerListForm {
  #[serde(default)]
  pub players: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct PlayerPresence {
  pub uuid: Uuid,
  pub username: String,
  pub ping_ms: i64,
  pub server_id: Option<String>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct TransferResultForm {
  pub transfer_id: Uuid,
  pub status: TransferResultStatus,
  #[serde(default)]
  pub message: String,
}

#[derive(Debug, Clone, Copy, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum TransferResultStatus {
  Success,
  Failure,
}

#[derive(Debug, Clone)]
pub struct PendingTransfer {
  pub transfer_id: Uuid,
  pub server_id: String,
  pub player: String,
  pub address: String,
}

#[derive(Debug, Clone, Deserialize)]
pub struct SoloSessionCreateRequest {
  pub game_id: String,
  pub owner: Uuid,
  #[serde(default)]
  pub players: Vec<Uuid>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct SoloLaunchForm {
  pub owner: Uuid,
  #[serde(default)]
  pub players: String,
}

#[derive(Debug, Clone, Deserialize)]
pub struct SoloDestroyForm {
  pub player: Uuid,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SoloSessionInfo {
  pub session_id: String,
  pub game_id: String,
  pub server_id: String,
  pub backend_id: String,
  pub owner: Uuid,
  pub players: Vec<Uuid>,
  pub mode: SoloMode,
  pub startup: SoloStartup,
  pub created: u64,
  pub updated: u64,
  pub expires: Option<u64>,
  pub instance: Option<InstanceInfo>,
}
