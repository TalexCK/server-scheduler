use std::{
  collections::{BTreeMap, BTreeSet},
  path::{Component, Path, PathBuf},
};

use serde::{Deserialize, Serialize};
use thiserror::Error;
use tokio::fs;

const DISALLOWED_JVM_OPTION_PREFIXES: [&str; 2] = ["-Dusing.aikars.flags=", "-Daikars.new.flags="];

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
pub enum JavaVersion {
  #[serde(rename = "21")]
  Java21,
  #[serde(rename = "25")]
  Java25,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SchedulerConfig {
  pub api: ApiConfig,
  pub bridge: BridgeConfig,
  pub postgresql: PostgresConfig,
  pub ports: PortConfig,
  #[serde(default, rename = "enable-frpc")]
  pub enable_frpc: bool,
  #[serde(default = "default_frpc_core", rename = "frpc-core")]
  pub frpc_core: PathBuf,
  pub java: JavaHomes,
  pub paths: PathsConfig,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ApiConfig {
  pub port: u16,
  pub token: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BridgeConfig {
  pub token: String,
  #[serde(default = "default_heartbeat_timeout")]
  pub heartbeat_timeout_seconds: u64,
  #[serde(default = "default_transfer_retry")]
  pub transfer_retry_seconds: u64,
  #[serde(default = "default_transfer_timeout")]
  pub transfer_timeout_seconds: u64,
  #[serde(default = "default_transfer_max_attempts")]
  pub transfer_max_attempts: u32,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PostgresConfig {
  pub home: PathBuf,
  pub host: String,
  pub port: u16,
  pub username: String,
  pub password: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PortConfig {
  pub velocity: u16,
  pub start: u16,
  pub end: u16,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct JavaHomes {
  #[serde(rename = "21_home")]
  pub java21_home: PathBuf,
  #[serde(rename = "25_home")]
  pub java25_home: PathBuf,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PathsConfig {
  pub cores: PathBuf,
  pub plugins: PathBuf,
  pub mods: PathBuf,
  #[serde(default = "default_dependencies_path")]
  pub dependencies: PathBuf,
  #[serde(default = "default_files_path")]
  pub files: PathBuf,
  #[serde(default = "default_network_path")]
  pub network: PathBuf,
  pub servers: PathBuf,
  pub worlds: PathBuf,
  pub runtime: PathBuf,
  pub logs: PathBuf,
  pub pid_file: PathBuf,
  pub scheduler_log: PathBuf,
}

#[derive(Debug, Clone)]
pub struct LoadedConfig {
  pub file: PathBuf,
  pub root: PathBuf,
  pub value: SchedulerConfig,
}

impl LoadedConfig {
  pub async fn load(path: impl AsRef<Path>) -> Result<Self, ConfigError> {
    let file = absolute(path.as_ref())?;
    let bytes = fs::read(&file).await.map_err(|source| ConfigError::Read {
      path: file.clone(),
      source,
    })?;
    let value =
      serde_json::from_slice::<SchedulerConfig>(&bytes).map_err(|source| ConfigError::Parse {
        path: file.clone(),
        source,
      })?;
    let root = file.parent().unwrap_or(Path::new(".")).to_path_buf();
    value.validate()?;
    Ok(Self { file, root, value })
  }

  pub fn resolve(&self, path: &Path) -> PathBuf {
    if path.is_absolute() {
      path.to_path_buf()
    } else {
      self.root.join(path)
    }
  }

  pub fn java_bin(&self, version: JavaVersion) -> PathBuf {
    let home = match version {
      JavaVersion::Java21 => &self.value.java.java21_home,
      JavaVersion::Java25 => &self.value.java.java25_home,
    };
    self
      .resolve(home)
      .join("bin")
      .join(if cfg!(windows) { "java.exe" } else { "java" })
  }

  pub fn server_file(&self, id: &str) -> Result<PathBuf, ConfigError> {
    validate_id(id)?;
    Ok(
      self
        .resolve(&self.value.paths.servers)
        .join(format!("{id}.json")),
    )
  }

  pub async fn load_server(&self, id: &str) -> Result<ServerDefinition, ConfigError> {
    let path = self.server_file(id)?;
    let bytes = fs::read(&path).await.map_err(|source| ConfigError::Read {
      path: path.clone(),
      source,
    })?;
    let definition =
      serde_json::from_slice::<ServerDefinition>(&bytes).map_err(|source| ConfigError::Parse {
        path: path.clone(),
        source,
      })?;
    definition.validate()?;
    Ok(definition)
  }

  pub async fn server_ids(&self) -> Result<Vec<String>, ConfigError> {
    let directory = self.resolve(&self.value.paths.servers);
    let mut entries = fs::read_dir(&directory)
      .await
      .map_err(|source| ConfigError::Read {
        path: directory.clone(),
        source,
      })?;
    let mut ids = Vec::new();
    while let Some(entry) = entries
      .next_entry()
      .await
      .map_err(|source| ConfigError::Read {
        path: directory.clone(),
        source,
      })?
    {
      let path = entry.path();
      if !entry
        .file_type()
        .await
        .map_err(|source| ConfigError::Read {
          path: path.clone(),
          source,
        })?
        .is_file()
        || path.extension().and_then(|value| value.to_str()) != Some("json")
      {
        continue;
      }
      if let Some(id) = path.file_stem().and_then(|value| value.to_str()) {
        validate_id(id)?;
        ids.push(id.to_string());
      }
    }
    ids.sort();
    Ok(ids)
  }

  pub async fn game_definitions(&self) -> Result<Vec<(String, GameVotingDefinition)>, ConfigError> {
    let mut games = Vec::new();
    let mut game_ids = BTreeSet::new();
    for server_id in self.server_ids().await? {
      let definition = self.load_server(&server_id).await?;
      let Some(game) = definition.gamevoting else {
        continue;
      };
      if !game_ids.insert(game.id.to_lowercase()) {
        return Err(ConfigError::Validation(format!(
          "duplicate GameVoting game ID: {}",
          game.id
        )));
      }
      games.push((server_id, game));
    }
    games.sort_by(|left, right| {
      left
        .1
        .order
        .cmp(&right.1.order)
        .then_with(|| left.1.id.to_lowercase().cmp(&right.1.id.to_lowercase()))
    });
    Ok(games)
  }
}

impl SchedulerConfig {
  fn validate(&self) -> Result<(), ConfigError> {
    if self.api.token.len() < 32 || self.bridge.token.len() < 32 {
      return Err(ConfigError::Validation(
        "API and bridge tokens must contain at least 32 characters".into(),
      ));
    }
    if self.api.port == 0 {
      return Err(ConfigError::Validation(
        "API port must be greater than zero".into(),
      ));
    }
    if self.bridge.transfer_retry_seconds == 0
      || self.bridge.transfer_timeout_seconds <= self.bridge.transfer_retry_seconds
      || self.bridge.transfer_max_attempts == 0
    {
      return Err(ConfigError::Validation(
        "bridge transfer retry limits are invalid".into(),
      ));
    }
    if self.postgresql.host.trim().is_empty() || self.postgresql.host.contains(['\r', '\n']) {
      return Err(ConfigError::Validation(
        "PostgreSQL host must contain a single non-empty line".into(),
      ));
    }
    if self.postgresql.port == 0 {
      return Err(ConfigError::Validation(
        "PostgreSQL port must be greater than zero".into(),
      ));
    }
    validate_environment_name(&self.postgresql.username).map_err(|_| {
      ConfigError::Validation("PostgreSQL username contains invalid characters".into())
    })?;
    if self.postgresql.password.is_empty()
      || !self
        .postgresql
        .password
        .chars()
        .all(|character| character.is_ascii_alphanumeric() || "._~!@#$%^&*+=-".contains(character))
    {
      return Err(ConfigError::Validation(
        "PostgreSQL password contains invalid characters".into(),
      ));
    }
    if self.ports.velocity == 0 || self.ports.start == 0 || self.ports.end < self.ports.start {
      return Err(ConfigError::Validation(
        "server port allocation range is invalid".into(),
      ));
    }
    if (self.ports.start..=self.ports.end).contains(&self.ports.velocity) {
      return Err(ConfigError::Validation(
        "Velocity port must be outside the child server port range".into(),
      ));
    }
    if self.frpc_core.as_os_str().is_empty() {
      return Err(ConfigError::Validation(
        "frpc-core must not be empty".into(),
      ));
    }
    validate_relative_path(&self.frpc_core)?;
    Ok(())
  }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ServerDefinition {
  pub core: CoreDefinition,
  pub memory: MemoryDefinition,
  #[serde(default)]
  pub autostart: bool,
  #[serde(default = "default_restart_delay")]
  pub restart_delay_seconds: u64,
  #[serde(default)]
  pub daily_restart: Option<DailyRestartDefinition>,
  #[serde(default)]
  pub idle_timeout_seconds: Option<u64>,
  #[serde(default)]
  pub operator_players: bool,
  #[serde(default)]
  pub gamevoting: Option<GameVotingDefinition>,
  #[serde(default)]
  pub plugins: Vec<ArtifactDefinition>,
  #[serde(default)]
  pub mods: Vec<ModDefinition>,
  #[serde(default)]
  pub world: WorldDefinition,
  #[serde(default)]
  pub files: Vec<FileDefinition>,
  #[serde(default)]
  pub render_files: Vec<PathBuf>,
  #[serde(default)]
  pub properties: BTreeMap<String, String>,
  #[serde(default)]
  pub environment: BTreeMap<String, String>,
  #[serde(default)]
  pub jvm_args: Vec<String>,
  #[serde(default)]
  pub server_args: Vec<String>,
  #[serde(default = "default_nogui")]
  pub nogui: bool,
  #[serde(default = "default_startup_timeout")]
  pub startup_timeout_seconds: u64,
}

impl ServerDefinition {
  fn validate(&self) -> Result<(), ConfigError> {
    validate_id(&self.core.kind)?;
    if self.core.minecraft_version.trim().is_empty() {
      return Err(ConfigError::Validation(
        "minecraft_version must not be empty".into(),
      ));
    }
    validate_version(&self.core.minecraft_version)?;
    if let Some(loader) = &self.core.loader_version {
      validate_version(loader)?;
    }
    if let Some(jar) = &self.core.jar {
      validate_relative_path(Path::new(jar))?;
    }
    if self.memory.initial_mib == 0 || self.memory.maximum_mib < self.memory.initial_mib {
      return Err(ConfigError::Validation("memory limits are invalid".into()));
    }
    if self.idle_timeout_seconds == Some(0) {
      return Err(ConfigError::Validation(
        "idle_timeout_seconds must be greater than zero or null".into(),
      ));
    }
    if let Some(daily_restart) = &self.daily_restart {
      daily_restart.validate()?;
    }
    if let Some(game) = &self.gamevoting {
      game.validate()?;
      if game.solo_mode == SoloMode::PlayerWorld {
        if !game.solo {
          return Err(ConfigError::Validation(
            "player_world solo_mode requires solo to be enabled".into(),
          ));
        }
        if game.solo_startup != SoloStartup::OnDemand {
          return Err(ConfigError::Validation(
            "player_world solo_mode requires on_demand startup".into(),
          ));
        }
        if self.autostart {
          return Err(ConfigError::Validation(
            "player_world solo_mode cannot use server autostart".into(),
          ));
        }
        if self
          .properties
          .get("level-name")
          .is_some_and(|value| value != "world")
        {
          return Err(ConfigError::Validation(
            "player_world solo_mode requires level-name to be world".into(),
          ));
        }
      }
    }
    for artifact in &self.plugins {
      validate_id(&artifact.id)?;
      if let Some(version) = &artifact.version {
        validate_version(version)?;
      }
      if let Some(config) = &artifact.config {
        validate_id(config)?;
      }
      if let Some(target) = &artifact.config_target {
        validate_relative_path(target)?;
      }
    }
    for artifact in &self.mods {
      validate_id(&artifact.id)?;
      if let Some(version) = &artifact.version {
        validate_version(version)?;
      }
      if let Some(config) = &artifact.config {
        validate_id(config)?;
      }
      if let Some(target) = &artifact.config_path {
        validate_relative_path(target)?;
      }
    }
    if let WorldDefinition::Template { id } = &self.world {
      validate_id(id)?;
    }
    for layer in &self.files {
      validate_relative_path(&layer.source)?;
      validate_optional_relative_path(&layer.target)?;
    }
    for path in &self.render_files {
      validate_relative_path(path)?;
    }
    for name in self.environment.keys() {
      validate_environment_name(name)?;
    }
    if self.jvm_args.iter().any(|value| {
      DISALLOWED_JVM_OPTION_PREFIXES
        .iter()
        .any(|prefix| value.starts_with(prefix))
    }) {
      return Err(ConfigError::Validation(
        "Aikar JVM marker options are not allowed".into(),
      ));
    }
    Ok(())
  }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct DailyRestartDefinition {
  pub hour: u8,
  #[serde(default)]
  pub minute: u8,
  #[serde(default = "default_daily_restart_empty_grace")]
  pub empty_grace_seconds: u64,
}

impl DailyRestartDefinition {
  fn validate(&self) -> Result<(), ConfigError> {
    if self.hour > 23 || self.minute > 59 {
      return Err(ConfigError::Validation(
        "daily_restart time is invalid".into(),
      ));
    }
    if self.empty_grace_seconds == 0 {
      return Err(ConfigError::Validation(
        "daily_restart empty_grace_seconds must be greater than zero".into(),
      ));
    }
    Ok(())
  }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GameVotingDefinition {
  #[serde(default)]
  pub order: u32,
  pub id: String,
  pub name: String,
  #[serde(default)]
  pub description: Vec<String>,
  #[serde(default)]
  pub rules: Vec<String>,
  #[serde(default)]
  pub aliases: Vec<String>,
  pub material: String,
  #[serde(default)]
  pub custom_model_data: u32,
  #[serde(default)]
  pub version: Option<String>,
  #[serde(default)]
  pub min_version: Option<String>,
  #[serde(default)]
  pub max_version: Option<String>,
  #[serde(default = "default_game_min_players")]
  pub min_players: u32,
  #[serde(default = "default_game_max_players")]
  pub max_players: u32,
  #[serde(default)]
  pub solo: bool,
  #[serde(default)]
  pub solo_mode: SoloMode,
  #[serde(default)]
  pub solo_startup: SoloStartup,
  #[serde(default = "default_solo_max_players")]
  pub solo_max_players: u32,
  #[serde(default = "default_solo_retention_days")]
  pub solo_retention_days: u32,
}

impl GameVotingDefinition {
  fn validate(&self) -> Result<(), ConfigError> {
    validate_id(&self.id)?;
    if self.name.trim().is_empty() {
      return Err(ConfigError::Validation(
        "GameVoting name must not be empty".into(),
      ));
    }
    for alias in &self.aliases {
      validate_id(alias)?;
    }
    if self.material.is_empty()
      || !self
        .material
        .chars()
        .all(|value| value.is_ascii_uppercase() || value.is_ascii_digit() || value == '_')
    {
      return Err(ConfigError::Validation(
        "GameVoting material must be an uppercase Minecraft material ID".into(),
      ));
    }
    for version in [
      self.version.as_deref(),
      self.min_version.as_deref(),
      self.max_version.as_deref(),
    ]
    .into_iter()
    .flatten()
    {
      if !version.eq_ignore_ascii_case("any") {
        validate_version(version)?;
      }
    }
    if self.min_players == 0 || self.max_players < self.min_players {
      return Err(ConfigError::Validation(
        "GameVoting player limits are invalid".into(),
      ));
    }
    if self.solo_max_players == 0 || self.solo_retention_days == 0 {
      return Err(ConfigError::Validation(
        "GameVoting solo limits must be greater than zero".into(),
      ));
    }
    if self.solo
      && (self.solo_max_players < self.min_players || self.solo_max_players > self.max_players)
    {
      return Err(ConfigError::Validation(
        "GameVoting solo_max_players is outside the game player range".into(),
      ));
    }
    if self.solo_mode == SoloMode::PlayerWorld && self.solo_max_players > 2 {
      return Err(ConfigError::Validation(
        "player_world solo_max_players must be 1 or 2".into(),
      ));
    }
    let java_int_max = i32::MAX as u32;
    if self.order > java_int_max
      || self.custom_model_data > java_int_max
      || self.min_players > java_int_max
      || self.max_players > java_int_max
      || self.solo_max_players > java_int_max
      || self.solo_retention_days > java_int_max
    {
      return Err(ConfigError::Validation(
        "GameVoting numeric values exceed the Java integer range".into(),
      ));
    }
    Ok(())
  }
}

#[derive(Debug, Clone, Copy, Default, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum SoloMode {
  #[default]
  Shared,
  PlayerWorld,
}

impl SoloMode {
  pub fn as_str(self) -> &'static str {
    match self {
      Self::Shared => "shared",
      Self::PlayerWorld => "player_world",
    }
  }
}

#[derive(Debug, Clone, Copy, Default, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum SoloStartup {
  Always,
  #[default]
  OnDemand,
}

impl SoloStartup {
  pub fn as_str(self) -> &'static str {
    match self {
      Self::Always => "always",
      Self::OnDemand => "on_demand",
    }
  }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CoreDefinition {
  pub kind: String,
  pub minecraft_version: String,
  #[serde(default)]
  pub loader_version: Option<String>,
  pub java: JavaVersion,
  #[serde(default)]
  pub jar: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MemoryDefinition {
  pub initial_mib: u32,
  pub maximum_mib: u32,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ArtifactDefinition {
  pub id: String,
  #[serde(default)]
  pub version: Option<String>,
  #[serde(default)]
  pub config: Option<String>,
  #[serde(default)]
  pub config_target: Option<PathBuf>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ModDefinition {
  pub id: String,
  #[serde(default)]
  pub version: Option<String>,
  #[serde(default)]
  pub config: Option<String>,
  #[serde(default)]
  pub config_target: ModConfigTarget,
  #[serde(default)]
  pub config_path: Option<PathBuf>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FileDefinition {
  pub source: PathBuf,
  #[serde(default)]
  pub target: PathBuf,
}

#[derive(Debug, Clone, Copy, Default, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum ModConfigTarget {
  #[default]
  Config,
  #[serde(alias = "mod")]
  Mods,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(tag = "mode", rename_all = "lowercase")]
pub enum WorldDefinition {
  #[default]
  Generated,
  Template {
    id: String,
  },
}

#[derive(Debug, Error)]
pub enum ConfigError {
  #[error("failed to resolve path {path}: {source}")]
  Resolve {
    path: PathBuf,
    source: std::io::Error,
  },
  #[error("failed to read {path}: {source}")]
  Read {
    path: PathBuf,
    source: std::io::Error,
  },
  #[error("failed to parse {path}: {source}")]
  Parse {
    path: PathBuf,
    source: serde_json::Error,
  },
  #[error("invalid configuration: {0}")]
  Validation(String),
}

pub fn validate_id(id: &str) -> Result<(), ConfigError> {
  if id.is_empty()
    || !id
      .chars()
      .all(|character| character.is_ascii_alphanumeric() || character == '-' || character == '_')
  {
    return Err(ConfigError::Validation(format!("invalid identifier: {id}")));
  }
  Ok(())
}

fn validate_version(version: &str) -> Result<(), ConfigError> {
  if version.is_empty()
    || version == "."
    || version == ".."
    || !version.chars().all(|character| {
      character.is_ascii_alphanumeric()
        || character == '-'
        || character == '_'
        || character == '.'
        || character == '+'
    })
  {
    return Err(ConfigError::Validation(format!(
      "invalid version: {version}"
    )));
  }
  Ok(())
}

fn validate_relative_path(path: &Path) -> Result<(), ConfigError> {
  if path.is_absolute()
    || path
      .components()
      .any(|component| !matches!(component, Component::Normal(_)))
  {
    return Err(ConfigError::Validation(format!(
      "invalid relative path: {}",
      path.display()
    )));
  }
  Ok(())
}

fn validate_optional_relative_path(path: &Path) -> Result<(), ConfigError> {
  if path.as_os_str().is_empty() || path == Path::new(".") {
    return Ok(());
  }
  validate_relative_path(path)
}

fn validate_environment_name(name: &str) -> Result<(), ConfigError> {
  if name.is_empty()
    || !name.chars().enumerate().all(|(index, character)| {
      character == '_'
        || character.is_ascii_alphanumeric() && (index > 0 || !character.is_ascii_digit())
    })
  {
    return Err(ConfigError::Validation(format!(
      "invalid environment variable name: {name}"
    )));
  }
  Ok(())
}

fn absolute(path: &Path) -> Result<PathBuf, ConfigError> {
  if path.is_absolute() {
    Ok(path.to_path_buf())
  } else {
    std::env::current_dir()
      .map(|directory| directory.join(path))
      .map_err(|source| ConfigError::Resolve {
        path: path.to_path_buf(),
        source,
      })
  }
}

fn default_heartbeat_timeout() -> u64 {
  45
}
fn default_transfer_retry() -> u64 {
  30
}
fn default_transfer_timeout() -> u64 {
  45
}
fn default_transfer_max_attempts() -> u32 {
  2
}
fn default_startup_timeout() -> u64 {
  120
}
fn default_restart_delay() -> u64 {
  10
}
fn default_daily_restart_empty_grace() -> u64 {
  300
}
fn default_files_path() -> PathBuf {
  PathBuf::from("files")
}
fn default_dependencies_path() -> PathBuf {
  PathBuf::from("dependencies")
}
fn default_network_path() -> PathBuf {
  PathBuf::from("network")
}
fn default_frpc_core() -> PathBuf {
  PathBuf::from("frpc")
}
fn default_nogui() -> bool {
  true
}
fn default_game_min_players() -> u32 {
  1
}
fn default_game_max_players() -> u32 {
  50
}
fn default_solo_max_players() -> u32 {
  1
}
fn default_solo_retention_days() -> u32 {
  10
}

#[cfg(test)]
mod tests {
  use super::*;

  #[test]
  fn identifiers_reject_path_segments() {
    assert!(validate_id("paper-1").is_ok());
    assert!(validate_id("../paper").is_err());
  }

  #[test]
  fn combined_server_definition_parses() {
    let value = serde_json::json!({
        "core": {"kind":"paper","minecraft_version":"1.21.11","java":"21"},
        "memory": {"initial_mib":1024,"maximum_mib":4096},
        "plugins":[{"id":"bridge"}],
        "mods":[{"id":"voicechat","config_target":"mods"}],
        "world":{"mode":"generated"}
    });
    let parsed: ServerDefinition = serde_json::from_value(value).unwrap();
    assert_eq!(parsed.core.java, JavaVersion::Java21);
    assert!(matches!(
      parsed.mods[0].config_target,
      ModConfigTarget::Mods
    ));
  }

  #[test]
  fn server_definition_rejects_aikar_marker_options() {
    for value in [
      "-Dusing.aikars.flags=https://invalid.example",
      "-Daikars.new.flags=true",
    ] {
      let definition: ServerDefinition = serde_json::from_value(serde_json::json!({
        "core": {"kind":"paper","minecraft_version":"1.21.11","java":"21"},
        "memory": {"initial_mib":1024,"maximum_mib":4096},
        "jvm_args": [value]
      }))
      .unwrap();
      assert!(definition.validate().is_err());
    }
  }

  #[test]
  fn server_definition_rejects_zero_idle_timeout() {
    let definition: ServerDefinition = serde_json::from_value(serde_json::json!({
      "core": {"kind":"paper","minecraft_version":"1.21.11","java":"21"},
      "memory": {"initial_mib":1024,"maximum_mib":4096},
      "idle_timeout_seconds": 0
    }))
    .unwrap();
    assert!(definition.validate().is_err());
  }

  #[test]
  fn server_definition_parses_daily_restart() {
    let definition: ServerDefinition = serde_json::from_value(serde_json::json!({
      "core": {"kind":"fabric","minecraft_version":"26.2","java":"25"},
      "memory": {"initial_mib":1024,"maximum_mib":4096},
      "daily_restart": {
        "hour": 4
      }
    }))
    .unwrap();
    assert!(definition.validate().is_ok());
    assert_eq!(
      definition.daily_restart,
      Some(DailyRestartDefinition {
        hour: 4,
        minute: 0,
        empty_grace_seconds: 300
      })
    );
  }

  #[test]
  fn server_definition_rejects_invalid_daily_restart() {
    for daily_restart in [
      serde_json::json!({"hour": 24}),
      serde_json::json!({"hour": 4, "minute": 60}),
      serde_json::json!({"hour": 4, "empty_grace_seconds": 0}),
    ] {
      let definition: ServerDefinition = serde_json::from_value(serde_json::json!({
        "core": {"kind":"fabric","minecraft_version":"26.2","java":"25"},
        "memory": {"initial_mib":1024,"maximum_mib":4096},
        "daily_restart": daily_restart
      }))
      .unwrap();
      assert!(definition.validate().is_err());
    }
  }

  #[test]
  fn server_definition_parses_gamevoting_catalog_entry() {
    let definition: ServerDefinition = serde_json::from_value(serde_json::json!({
      "core": {"kind":"fabric","minecraft_version":"1.21.11","java":"21"},
      "memory": {"initial_mib":1024,"maximum_mib":4096},
      "gamevoting": {
        "order": 10,
        "id": "bedwar",
        "name": "BedWars",
        "description": ["Protect the bed"],
        "rules": ["Destroy every enemy bed"],
        "aliases": ["bedwars"],
        "material": "BLUE_BED",
        "min_version": "1.21.11",
        "max_version": "1.21.11",
        "min_players": 2,
        "max_players": 8
      }
    }))
    .unwrap();
    assert!(definition.validate().is_ok());
    let game = definition.gamevoting.unwrap();
    assert_eq!(game.max_players, 8);
    assert_eq!(game.rules, ["Destroy every enemy bed"]);
    assert_eq!(game.aliases, ["bedwars"]);
    assert!(!game.solo);
    assert_eq!(game.solo_mode, SoloMode::Shared);
    assert_eq!(game.solo_startup, SoloStartup::OnDemand);
    assert_eq!(game.solo_max_players, 1);
    assert_eq!(game.solo_retention_days, 10);
    assert!(!definition.operator_players);
  }

  #[test]
  fn server_definition_parses_operator_players() {
    let definition: ServerDefinition = serde_json::from_value(serde_json::json!({
      "core": {"kind":"fabric","minecraft_version":"1.21.11","java":"21"},
      "memory": {"initial_mib":1024,"maximum_mib":4096},
      "operator_players": true
    }))
    .unwrap();
    assert!(definition.validate().is_ok());
    assert!(definition.operator_players);
  }

  #[test]
  fn server_definition_validates_player_world_solo_mode() {
    let valid: ServerDefinition = serde_json::from_value(serde_json::json!({
      "core": {"kind":"fabric","minecraft_version":"26.2","java":"25"},
      "memory": {"initial_mib":1024,"maximum_mib":4096},
      "gamevoting": {
        "id": "puzzle",
        "name": "Puzzle",
        "material": "COMPASS",
        "min_players": 1,
        "max_players": 2,
        "solo": true,
        "solo_mode": "player_world",
        "solo_startup": "on_demand",
        "solo_max_players": 2
      }
    }))
    .unwrap();
    assert!(valid.validate().is_ok());

    for invalid in [
      serde_json::json!({"solo_startup": "always"}),
      serde_json::json!({"solo_max_players": 3}),
      serde_json::json!({"solo": false}),
    ] {
      let mut definition_value = serde_json::to_value(&valid).unwrap();
      for (key, override_value) in invalid.as_object().unwrap() {
        value_pointer_mut(&mut definition_value, "/gamevoting")
          .insert(key.clone(), override_value.clone());
      }
      let definition: ServerDefinition = serde_json::from_value(definition_value).unwrap();
      assert!(definition.validate().is_err());
    }

    let mut invalid_level = valid;
    invalid_level
      .properties
      .insert("level-name".into(), "other".into());
    assert!(invalid_level.validate().is_err());
  }

  fn value_pointer_mut<'a>(
    value: &'a mut serde_json::Value,
    pointer: &str,
  ) -> &'a mut serde_json::Map<String, serde_json::Value> {
    value.pointer_mut(pointer).unwrap().as_object_mut().unwrap()
  }

  #[test]
  fn server_definition_rejects_gamevoting_values_outside_java_int() {
    let definition: ServerDefinition = serde_json::from_value(serde_json::json!({
      "core": {"kind":"paper","minecraft_version":"1.21.11","java":"21"},
      "memory": {"initial_mib":1024,"maximum_mib":4096},
      "gamevoting": {
        "order": 2147483648_u32,
        "id": "bedwar",
        "name": "BedWars",
        "material": "BLUE_BED"
      }
    }))
    .unwrap();
    assert!(definition.validate().is_err());
  }
}
