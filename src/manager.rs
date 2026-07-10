use std::{
  collections::{BTreeSet, HashMap, HashSet, VecDeque},
  io,
  path::{Path, PathBuf},
  process::Stdio,
  sync::{
    Arc,
    atomic::{AtomicBool, Ordering},
  },
  time::{Duration, SystemTime, UNIX_EPOCH},
};

use chrono::{DateTime, Datelike, Local, Timelike, Utc};
use thiserror::Error;
use tokio::{
  fs::{self, OpenOptions},
  io::{AsyncBufReadExt, AsyncWriteExt, BufReader},
  net::TcpListener,
  process::{Child, ChildStdin, Command},
  sync::{Mutex, RwLock, broadcast, mpsc, oneshot, watch},
  task::JoinHandle,
  time::{Instant, interval, sleep, timeout},
};
use uuid::Uuid;

use crate::{
  config::{
    DailyRestartDefinition, GameVotingDefinition, LoadedConfig, SoloMode, SoloStartup, validate_id,
  },
  dependency_cache::{self, DependencySnapshot},
  layout::{self, PreparedInstance},
  protocol::{
    InstanceInfo, InstanceState, PendingTransfer, PlayerPresence, SoloSessionCreateRequest,
    SoloSessionInfo, TransferResultStatus,
  },
  runtime,
  solo::{self, SoloPersistence, SoloWorldMetadata},
};

const LOG_HISTORY_LIMIT: usize = 2000;
const PORT_REUSE_COOLDOWN: Duration = Duration::from_secs(30);
const SOLO_ONLINE_WAIT: Duration = Duration::from_secs(1);
const SOLO_ONLINE_RETRY: Duration = Duration::from_millis(100);

#[derive(Clone)]
pub struct InstanceManager {
  config: Arc<LoadedConfig>,
  instances: Arc<RwLock<HashMap<String, Arc<InstanceControl>>>>,
  transfers: Arc<Mutex<HashMap<Uuid, TransferRecord>>>,
  players: Arc<RwLock<HashMap<Uuid, PlayerPresence>>>,
  allocated_ports: Arc<Mutex<HashSet<u16>>>,
  port_cooldowns: Arc<Mutex<HashMap<u16, Instant>>>,
  launching: Arc<Mutex<HashSet<String>>>,
  dependency_cache_lock: Arc<Mutex<()>>,
  solo_sessions: Arc<RwLock<HashMap<String, SoloSessionRecord>>>,
  solo_operations: Arc<Mutex<()>>,
  solo_cleanup_day: Arc<Mutex<Option<i32>>>,
  probe_ports: bool,
  shutting_down: Arc<AtomicBool>,
}

struct InstanceControl {
  info: Arc<RwLock<InstanceInfo>>,
  control: mpsc::Sender<ProcessControl>,
  logs: LogBuffer,
  completion: watch::Receiver<Option<Result<(), String>>>,
  pending_players: Mutex<HashSet<String>>,
  startup_timeout_seconds: u64,
  autostart: bool,
  restart_delay_seconds: u64,
  daily_restart: Option<DailyRestartDefinition>,
  daily_restart_tracker: Mutex<DailyRestartTracker>,
  idle_timeout_seconds: Option<u64>,
  launch_spec: LaunchSpec,
  solo_mode: Option<SoloMode>,
  solo_session_id: Arc<Mutex<Option<String>>>,
  preserve_solo_session: Arc<AtomicBool>,
}

enum ProcessControl {
  Command {
    value: String,
    response: oneshot::Sender<io::Result<()>>,
  },
  Stop,
}

#[derive(Clone)]
struct LogBuffer {
  history: Arc<Mutex<VecDeque<String>>>,
  sender: broadcast::Sender<String>,
  file: Arc<Mutex<fs::File>>,
}

struct TransferRecord {
  transfer_id: Uuid,
  server_id: String,
  player: String,
  address: String,
  next_attempt: u64,
  expires_at: u64,
  attempts: u32,
}

#[derive(Debug, Clone)]
struct LaunchSpec {
  definition_id: String,
  runtime_id: String,
  persistence: Option<SoloPersistence>,
  solo_session_id: Option<String>,
}

impl LaunchSpec {
  fn static_server(server_id: &str) -> Self {
    Self {
      definition_id: server_id.to_string(),
      runtime_id: server_id.to_string(),
      persistence: None,
      solo_session_id: None,
    }
  }
}

#[derive(Debug, Clone)]
struct SoloSessionRecord {
  session_id: String,
  game_id: String,
  server_id: String,
  backend_id: String,
  owner: Uuid,
  players: Vec<Uuid>,
  mode: SoloMode,
  startup: SoloStartup,
  created: u64,
  updated: u64,
  expires: Option<u64>,
  persistence: Option<SoloPersistence>,
}

impl SoloSessionRecord {
  fn metadata(&self) -> SoloWorldMetadata {
    SoloWorldMetadata {
      game: self.game_id.clone(),
      owner: self.owner,
      players: self.players.clone(),
      created: self.created,
      updated: self.updated,
      expires: self.expires.unwrap_or_default(),
    }
  }
}

#[derive(Debug, Clone)]
struct SoloGame {
  server_id: String,
  definition: GameVotingDefinition,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SoloAccessEntry {
  pub server_id: String,
  pub instance_id: Uuid,
  pub access: SoloAccessMode,
  pub players: Vec<Uuid>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SoloAccessMode {
  Open,
  Roster,
}

impl SoloAccessMode {
  pub fn as_str(self) -> &'static str {
    match self {
      Self::Open => "open",
      Self::Roster => "roster",
    }
  }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct RestartClock {
  day: i32,
  second_of_day: u32,
}

#[derive(Debug, Default)]
struct DailyRestartTracker {
  day: Option<i32>,
  completed: bool,
  waiting_for_empty: bool,
  empty_since: Option<u64>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum DailyRestartAction {
  None,
  WaitingForEmpty,
  EmptyGraceStarted,
  Restart,
}

impl DailyRestartTracker {
  fn evaluate(
    &mut self,
    schedule: &DailyRestartDefinition,
    current: RestartClock,
    instance_started: RestartClock,
    timestamp: u64,
    online_players: usize,
  ) -> DailyRestartAction {
    if self.day.is_none() || self.day != Some(current.day) && !self.waiting_for_empty {
      self.day = Some(current.day);
      self.completed = false;
      self.waiting_for_empty = false;
      self.empty_since = None;
    }
    let scheduled_second = u32::from(schedule.hour) * 3600 + u32::from(schedule.minute) * 60;
    let scheduled_day = self.day.unwrap_or(current.day);
    let schedule_reached = current.day > scheduled_day
      || current.day == scheduled_day && current.second_of_day >= scheduled_second;
    if !schedule_reached || self.completed {
      return DailyRestartAction::None;
    }
    if scheduled_day == current.day
      && instance_started.day == current.day
      && instance_started.second_of_day >= scheduled_second
    {
      self.completed = true;
      self.waiting_for_empty = false;
      self.empty_since = None;
      return DailyRestartAction::None;
    }
    if online_players > 0 {
      let state_changed = !self.waiting_for_empty || self.empty_since.take().is_some();
      self.waiting_for_empty = true;
      return if state_changed {
        DailyRestartAction::WaitingForEmpty
      } else {
        DailyRestartAction::None
      };
    }
    if !self.waiting_for_empty {
      self.completed = true;
      self.empty_since = None;
      return DailyRestartAction::Restart;
    }
    let Some(empty_since) = self.empty_since else {
      self.empty_since = Some(timestamp);
      return DailyRestartAction::EmptyGraceStarted;
    };
    if timestamp.saturating_sub(empty_since) >= schedule.empty_grace_seconds {
      self.completed = true;
      self.waiting_for_empty = false;
      self.empty_since = None;
      return DailyRestartAction::Restart;
    }
    DailyRestartAction::None
  }
}

impl InstanceManager {
  pub fn new(config: Arc<LoadedConfig>) -> Self {
    Self {
      config,
      instances: Arc::new(RwLock::new(HashMap::new())),
      transfers: Arc::new(Mutex::new(HashMap::new())),
      players: Arc::new(RwLock::new(HashMap::new())),
      allocated_ports: Arc::new(Mutex::new(HashSet::new())),
      port_cooldowns: Arc::new(Mutex::new(HashMap::new())),
      launching: Arc::new(Mutex::new(HashSet::new())),
      dependency_cache_lock: Arc::new(Mutex::new(())),
      solo_sessions: Arc::new(RwLock::new(HashMap::new())),
      solo_operations: Arc::new(Mutex::new(())),
      solo_cleanup_day: Arc::new(Mutex::new(None)),
      probe_ports: true,
      shutting_down: Arc::new(AtomicBool::new(false)),
    }
  }

  pub async fn replace_players(&self, players: Vec<PlayerPresence>) {
    let mut registry = self.players.write().await;
    registry.clear();
    registry.extend(players.into_iter().map(|player| (player.uuid, player)));
  }

  pub async fn players(&self) -> Vec<PlayerPresence> {
    let mut players = self
      .players
      .read()
      .await
      .values()
      .cloned()
      .collect::<Vec<_>>();
    players.sort_by(|left, right| {
      left.server_id.cmp(&right.server_id).then_with(|| {
        left
          .username
          .to_lowercase()
          .cmp(&right.username.to_lowercase())
      })
    });
    players
  }

  pub async fn initialize(&self) -> Result<(), ManagerError> {
    let _operations = self.solo_operations.lock().await;
    let games = self.solo_games().await?;
    let worlds_root = self.config.resolve(&self.config.value.paths.worlds);
    let mut sessions = HashMap::<String, SoloSessionRecord>::new();
    for game in games
      .into_iter()
      .filter(|game| game.definition.solo_mode == SoloMode::PlayerWorld)
    {
      let roots = solo::game_session_roots(&worlds_root.join(&game.definition.id)).await?;
      for root in roots {
        let metadata = solo::read_metadata(&root).await?;
        if metadata.game != game.definition.id
          || root.file_name().and_then(|value| value.to_str())
            != Some(metadata.owner.to_string().as_str())
          || !metadata.players.contains(&metadata.owner)
          || metadata
            .players
            .iter()
            .copied()
            .collect::<BTreeSet<_>>()
            .len()
            != metadata.players.len()
          || metadata.created > metadata.updated
          || metadata.updated > metadata.expires
        {
          return Err(ManagerError::State(format!(
            "invalid solo world metadata at {}",
            root.display()
          )));
        }
        let players = normalize_solo_players(metadata.owner, metadata.players.clone());
        validate_solo_roster(&game.definition, metadata.owner, &players)?;
        if sessions.values().any(|session| {
          session.game_id == metadata.game
            && session
              .players
              .iter()
              .any(|player| players.contains(player))
        }) {
          return Err(ManagerError::State(format!(
            "duplicate solo world player assignment for game {}",
            metadata.game
          )));
        }
        let session_id = solo::session_id(&game.server_id, metadata.owner);
        sessions.insert(
          session_id.clone(),
          SoloSessionRecord {
            session_id: session_id.clone(),
            game_id: metadata.game,
            server_id: game.server_id.clone(),
            backend_id: session_id,
            owner: metadata.owner,
            players,
            mode: SoloMode::PlayerWorld,
            startup: game.definition.solo_startup,
            created: metadata.created,
            updated: metadata.updated,
            expires: Some(metadata.expires),
            persistence: Some(SoloPersistence {
              root,
              retention_days: game.definition.solo_retention_days,
            }),
          },
        );
      }
    }
    *self.solo_sessions.write().await = sessions;
    Ok(())
  }

  pub async fn solo_sessions(&self) -> Vec<SoloSessionInfo> {
    let records = self
      .solo_sessions
      .read()
      .await
      .values()
      .cloned()
      .collect::<Vec<_>>();
    let mut sessions = Vec::with_capacity(records.len());
    for record in records {
      sessions.push(self.solo_session_info(&record).await);
    }
    sessions.sort_by(|left, right| left.session_id.cmp(&right.session_id));
    sessions
  }

  pub async fn create_solo_session(
    &self,
    request: SoloSessionCreateRequest,
  ) -> Result<SoloSessionInfo, ManagerError> {
    let game = self.solo_game(&request.game_id).await?;
    if !request.players.contains(&request.owner) {
      return Err(ManagerError::State(
        "solo session owner must be included in players".into(),
      ));
    }
    let players = normalize_solo_players(request.owner, request.players);
    validate_solo_roster(&game.definition, request.owner, &players)?;
    self.ensure_solo_players_online(&players).await?;
    let _operations = self.solo_operations.lock().await;
    if game.definition.solo_mode == SoloMode::Shared {
      let record = self
        .upsert_shared_solo_session(&game, request.owner, players)
        .await;
      if let Some(instance) = self.instances.read().await.get(&record.backend_id).cloned() {
        *instance.solo_session_id.lock().await = Some(record.session_id.clone());
      }
      return Ok(self.solo_session_info(&record).await);
    }
    let existing = {
      let sessions = self.solo_sessions.read().await;
      sessions
        .values()
        .find(|session| {
          session.game_id == request.game_id
            && session
              .players
              .iter()
              .any(|player| players.contains(player))
        })
        .cloned()
    };
    if let Some(existing) = existing {
      if same_player_set(&existing.players, &players) {
        return Ok(self.solo_session_info(&existing).await);
      }
      return Err(ManagerError::State(format!(
        "players are already assigned to solo session {}",
        existing.session_id
      )));
    }
    let timestamp = now();
    let (persistence, metadata) = solo::create_storage(
      &self.config.resolve(&self.config.value.paths.worlds),
      &request.game_id,
      request.owner,
      players.clone(),
      game.definition.solo_retention_days,
      timestamp,
    )
    .await?;
    let backend_id = solo::session_id(&game.server_id, request.owner);
    let record = SoloSessionRecord {
      session_id: backend_id.clone(),
      game_id: request.game_id,
      server_id: game.server_id,
      backend_id,
      owner: request.owner,
      players,
      mode: SoloMode::PlayerWorld,
      startup: game.definition.solo_startup,
      created: metadata.created,
      updated: metadata.updated,
      expires: Some(metadata.expires),
      persistence: Some(persistence),
    };
    self
      .solo_sessions
      .write()
      .await
      .insert(record.session_id.clone(), record.clone());
    Ok(self.solo_session_info(&record).await)
  }

  async fn upsert_shared_solo_session(
    &self,
    game: &SoloGame,
    owner: Uuid,
    players: Vec<Uuid>,
  ) -> SoloSessionRecord {
    let session_id = solo::shared_session_id(&game.server_id);
    let timestamp = now();
    let mut sessions = self.solo_sessions.write().await;
    if let Some(record) = sessions.get_mut(&session_id) {
      record.owner = owner;
      record.players = players;
      record.updated = timestamp;
      return record.clone();
    }
    let record = SoloSessionRecord {
      session_id: session_id.clone(),
      game_id: game.definition.id.clone(),
      server_id: game.server_id.clone(),
      backend_id: game.server_id.clone(),
      owner,
      players,
      mode: SoloMode::Shared,
      startup: game.definition.solo_startup,
      created: timestamp,
      updated: timestamp,
      expires: None,
      persistence: None,
    };
    sessions.insert(session_id, record.clone());
    record
  }

  pub async fn start_solo_session(
    &self,
    session_id: &str,
  ) -> Result<SoloSessionInfo, ManagerError> {
    let initial = self
      .solo_sessions
      .read()
      .await
      .get(session_id)
      .cloned()
      .ok_or_else(|| ManagerError::NotFound(session_id.to_string()))?;
    self.ensure_solo_players_online(&initial.players).await?;
    let _operations = self.solo_operations.lock().await;
    let record = self
      .solo_sessions
      .read()
      .await
      .get(session_id)
      .cloned()
      .ok_or_else(|| ManagerError::NotFound(session_id.to_string()))?;
    let player_ids = record
      .players
      .iter()
      .map(Uuid::to_string)
      .collect::<Vec<_>>();
    if record.mode == SoloMode::Shared {
      self.wait_for_launch_registration(&record.backend_id).await;
    }
    let existing = self.instances.read().await.get(&record.backend_id).cloned();
    if let Some(instance) = existing
      && instance.info.read().await.state.active()
    {
      let mut locked = instance.solo_session_id.lock().await;
      if locked
        .as_deref()
        .is_some_and(|value| value != record.session_id)
      {
        return Err(ManagerError::State(
          "solo backend is locked by another session".into(),
        ));
      }
      *locked = Some(record.session_id.clone());
      drop(locked);
      self.queue_transfers(&record.backend_id, player_ids).await?;
    } else {
      let spec = LaunchSpec {
        definition_id: record.server_id.clone(),
        runtime_id: record.backend_id.clone(),
        persistence: record.persistence.clone(),
        solo_session_id: Some(record.session_id.clone()),
      };
      if let Err(error) = self.launch_with_spec(spec, player_ids).await {
        if record.mode == SoloMode::Shared {
          self.solo_sessions.write().await.remove(&record.session_id);
        }
        return Err(error);
      }
    }
    if let Some(persistence) = &record.persistence {
      let mut metadata = record.metadata();
      solo::touch(persistence, &mut metadata, now()).await?;
      if let Some(current) = self.solo_sessions.write().await.get_mut(&record.session_id) {
        current.updated = metadata.updated;
        current.expires = Some(metadata.expires);
      }
    }
    let current = self
      .solo_sessions
      .read()
      .await
      .get(&record.session_id)
      .cloned()
      .ok_or_else(|| ManagerError::NotFound(record.session_id.clone()))?;
    Ok(self.solo_session_info(&current).await)
  }

  pub async fn launch_solo(
    &self,
    game_id: &str,
    owner: Uuid,
    players: Vec<Uuid>,
  ) -> Result<InstanceInfo, ManagerError> {
    let game = self.solo_game(game_id).await?;
    if game.definition.solo_mode == SoloMode::Shared {
      return self.launch_shared_solo(game, owner, players).await;
    }
    let session = self
      .create_solo_session(SoloSessionCreateRequest {
        game_id: game_id.to_string(),
        owner,
        players,
      })
      .await?;
    self
      .start_solo_session(&session.session_id)
      .await?
      .instance
      .ok_or_else(|| ManagerError::State("solo instance did not start".into()))
  }

  async fn launch_shared_solo(
    &self,
    game: SoloGame,
    owner: Uuid,
    players: Vec<Uuid>,
  ) -> Result<InstanceInfo, ManagerError> {
    if !players.contains(&owner) {
      return Err(ManagerError::State(
        "solo session owner must be included in players".into(),
      ));
    }
    let players = normalize_solo_players(owner, players);
    validate_solo_roster(&game.definition, owner, &players)?;
    self.ensure_solo_players_online(&players).await?;
    let player_ids = players.iter().map(Uuid::to_string).collect::<Vec<_>>();
    let _operations = self.solo_operations.lock().await;
    let record = self.upsert_shared_solo_session(&game, owner, players).await;
    self.wait_for_launch_registration(&record.backend_id).await;
    if let Some(instance) = self.instances.read().await.get(&record.backend_id).cloned()
      && instance.info.read().await.state.active()
    {
      *instance.solo_session_id.lock().await = Some(record.session_id.clone());
      self.queue_transfers(&record.backend_id, player_ids).await?;
      return Ok(instance.info.read().await.clone());
    }
    let spec = LaunchSpec {
      definition_id: record.server_id.clone(),
      runtime_id: record.backend_id.clone(),
      persistence: None,
      solo_session_id: Some(record.session_id.clone()),
    };
    match self.launch_with_spec(spec, player_ids).await {
      Ok(instance) => Ok(instance),
      Err(error) => {
        self.solo_sessions.write().await.remove(&record.session_id);
        Err(error)
      }
    }
  }

  pub async fn destroy_solo_session(
    &self,
    session_id: &str,
  ) -> Result<Option<InstanceInfo>, ManagerError> {
    let _operations = self.solo_operations.lock().await;
    let record = self
      .solo_sessions
      .read()
      .await
      .get(session_id)
      .cloned()
      .ok_or_else(|| ManagerError::NotFound(session_id.to_string()))?;
    let instance = self.instances.read().await.get(&record.backend_id).cloned();
    let stopped = if let Some(instance) = instance {
      if instance.info.read().await.state.active() {
        Some(self.stop(&record.backend_id).await?)
      } else {
        Some(instance.info.read().await.clone())
      }
    } else {
      None
    };
    if let Some(persistence) = &record.persistence {
      solo::destroy_storage(persistence).await?;
    }
    self.solo_sessions.write().await.remove(session_id);
    Ok(stopped)
  }

  pub async fn destroy_solo_for_player(
    &self,
    game_id: &str,
    player: Uuid,
  ) -> Result<Option<InstanceInfo>, ManagerError> {
    let game = self.solo_game(game_id).await?;
    if game.definition.solo_mode == SoloMode::Shared {
      return Err(ManagerError::State(
        "shared solo games do not have a player world to destroy".into(),
      ));
    }
    let session_id = self
      .solo_sessions
      .read()
      .await
      .values()
      .find(|session| session.game_id == game_id && session.players.contains(&player))
      .map(|session| session.session_id.clone())
      .ok_or_else(|| ManagerError::NotFound(format!("{game_id}:{player}")))?;
    self.destroy_solo_session(&session_id).await
  }

  pub async fn solo_access(&self) -> Result<Vec<SoloAccessEntry>, ManagerError> {
    let instances = self
      .instances
      .read()
      .await
      .values()
      .cloned()
      .collect::<Vec<_>>();
    let sessions = self.solo_sessions.read().await;
    let mut access = Vec::new();
    for instance in instances {
      let info = instance.info.read().await;
      if !info.state.active() {
        continue;
      }
      if instance.solo_mode.is_none() {
        continue;
      }
      let access_mode = match instance.solo_mode {
        Some(SoloMode::Shared) => SoloAccessMode::Open,
        Some(SoloMode::PlayerWorld) => SoloAccessMode::Roster,
        None => continue,
      };
      let players = if access_mode == SoloAccessMode::Roster {
        let session_id = instance.solo_session_id.lock().await.clone();
        session_id
          .as_ref()
          .and_then(|session_id| sessions.get(session_id))
          .map(|session| session.players.clone())
          .unwrap_or_default()
      } else {
        Vec::new()
      };
      access.push(SoloAccessEntry {
        server_id: info.server_id.clone(),
        instance_id: info.instance_id,
        access: access_mode,
        players,
      });
    }
    access.sort_by(|left, right| left.server_id.cmp(&right.server_id));
    Ok(access)
  }

  async fn solo_game(&self, game_id: &str) -> Result<SoloGame, ManagerError> {
    validate_id(game_id).map_err(layout::LayoutError::from)?;
    let game = self
      .config
      .game_definitions()
      .await
      .map_err(layout::LayoutError::from)?
      .into_iter()
      .find(|(_, game)| game.id == game_id)
      .ok_or_else(|| ManagerError::NotFound(game_id.to_string()))?;
    if !game.1.solo {
      return Err(ManagerError::State(format!(
        "game {game_id} is not configured for solo mode"
      )));
    }
    Ok(SoloGame {
      server_id: game.0,
      definition: game.1,
    })
  }

  async fn solo_games(&self) -> Result<Vec<SoloGame>, ManagerError> {
    Ok(
      self
        .config
        .game_definitions()
        .await
        .map_err(layout::LayoutError::from)?
        .into_iter()
        .filter(|(_, game)| game.solo)
        .map(|(server_id, definition)| SoloGame {
          server_id,
          definition,
        })
        .collect(),
    )
  }

  async fn ensure_solo_players_online(&self, players: &[Uuid]) -> Result<(), ManagerError> {
    let deadline = Instant::now() + SOLO_ONLINE_WAIT;
    loop {
      let missing = {
        let online = self.players.read().await;
        players
          .iter()
          .filter(|player| !online.contains_key(player))
          .map(Uuid::to_string)
          .collect::<Vec<_>>()
      };
      if missing.is_empty() {
        return Ok(());
      }
      if Instant::now() >= deadline {
        return Err(ManagerError::State(format!(
          "solo players are not connected to Velocity: {}",
          missing.join(",")
        )));
      }
      sleep(SOLO_ONLINE_RETRY).await;
    }
  }

  async fn wait_for_launch_registration(&self, runtime_id: &str) {
    let deadline = Instant::now() + Duration::from_secs(30);
    loop {
      let registered = if let Some(instance) = self.instances.read().await.get(runtime_id).cloned()
      {
        matches!(
          instance.info.read().await.state,
          InstanceState::Starting | InstanceState::Ready
        )
      } else {
        false
      };
      if registered
        || !self.launching.lock().await.contains(runtime_id)
        || Instant::now() >= deadline
      {
        return;
      }
      sleep(Duration::from_millis(25)).await;
    }
  }

  async fn solo_session_info(&self, record: &SoloSessionRecord) -> SoloSessionInfo {
    let instance = if let Some(instance) = self.instances.read().await.get(&record.backend_id) {
      Some(instance.info.read().await.clone())
    } else {
      None
    };
    SoloSessionInfo {
      session_id: record.session_id.clone(),
      game_id: record.game_id.clone(),
      server_id: record.server_id.clone(),
      backend_id: record.backend_id.clone(),
      owner: record.owner,
      players: record.players.clone(),
      mode: record.mode,
      startup: record.startup,
      created: record.created,
      updated: record.updated,
      expires: record.expires,
      instance,
    }
  }

  pub async fn launch(
    &self,
    server_id: &str,
    players: Vec<String>,
  ) -> Result<InstanceInfo, ManagerError> {
    let definition = self
      .config
      .load_server(server_id)
      .await
      .map_err(layout::LayoutError::from)?;
    if definition.gamevoting.as_ref().is_some_and(|game| game.solo) {
      return Err(ManagerError::State(
        "solo servers must be launched through the solo session API".into(),
      ));
    }
    self
      .launch_with_spec(LaunchSpec::static_server(server_id), players)
      .await
  }

  async fn launch_static(&self, server_id: &str) -> Result<InstanceInfo, ManagerError> {
    self
      .launch_with_spec(LaunchSpec::static_server(server_id), Vec::new())
      .await
  }

  async fn launch_with_spec(
    &self,
    spec: LaunchSpec,
    players: Vec<String>,
  ) -> Result<InstanceInfo, ManagerError> {
    validate_players(&players)?;
    let runtime_id = spec.runtime_id.clone();
    {
      let mut launching = self.launching.lock().await;
      if launching.contains(&runtime_id)
        || if let Some(existing) = self.instances.read().await.get(&runtime_id) {
          existing.info.read().await.state.active()
        } else {
          false
        }
      {
        return Err(ManagerError::AlreadyRunning(runtime_id));
      }
      launching.insert(runtime_id.clone());
    }
    let result = self.launch_reserved(spec, players).await;
    self.launching.lock().await.remove(&runtime_id);
    result
  }

  async fn launch_reserved(
    &self,
    spec: LaunchSpec,
    players: Vec<String>,
  ) -> Result<InstanceInfo, ManagerError> {
    let definition = self
      .config
      .load_server(&spec.definition_id)
      .await
      .map_err(layout::LayoutError::from)?;
    let port = self.reserve_port(&definition.core.kind).await?;

    let prepared_result = if let Some(persistence) = &spec.persistence {
      layout::prepare_solo(
        &self.config,
        &spec.definition_id,
        &spec.runtime_id,
        port,
        persistence,
      )
      .await
    } else {
      layout::prepare(&self.config, &spec.definition_id, port).await
    };
    let prepared = match prepared_result {
      Ok(prepared) => prepared,
      Err(error) => {
        self.release_port(port).await;
        return Err(error.into());
      }
    };
    let log_buffer = match open_instance_log(&prepared).await {
      Ok(log_buffer) => log_buffer,
      Err(error) => {
        self.release_port(port).await;
        return Err(error);
      }
    };
    let mut command = create_process_command(&self.config, &prepared);
    let mut child = match spawn_instance_process(&mut command, &prepared).await {
      Ok(child) => child,
      Err(error) => {
        self.release_port(port).await;
        return Err(error);
      }
    };
    let Some(pid) = child.id() else {
      let error = abort_spawned_process(
        child,
        &prepared.work_dir,
        ManagerError::State("child process has no PID".into()),
      )
      .await;
      self.release_port(port).await;
      return Err(error);
    };
    let Some(stdin) = child.stdin.take() else {
      let error = abort_spawned_process(
        child,
        &prepared.work_dir,
        ManagerError::State("child process stdin is unavailable".into()),
      )
      .await;
      self.release_port(port).await;
      return Err(error);
    };
    let Some(stdout) = child.stdout.take() else {
      let error = abort_spawned_process(
        child,
        &prepared.work_dir,
        ManagerError::State("child process stdout is unavailable".into()),
      )
      .await;
      self.release_port(port).await;
      return Err(error);
    };
    let Some(stderr) = child.stderr.take() else {
      let error = abort_spawned_process(
        child,
        &prepared.work_dir,
        ManagerError::State("child process stderr is unavailable".into()),
      )
      .await;
      self.release_port(port).await;
      return Err(error);
    };
    let info = InstanceInfo {
      server_id: spec.runtime_id.clone(),
      instance_id: prepared.instance_id,
      state: InstanceState::Starting,
      pid,
      port: prepared.port,
      work_dir: prepared.work_dir.clone(),
      log_file: prepared.log_file.clone(),
      started_at: now(),
      last_heartbeat: None,
      exit_code: None,
    };
    let info = Arc::new(RwLock::new(info));
    let (control_sender, control_receiver) = mpsc::channel(64);
    let (completion_sender, completion) = watch::channel(None);
    let pending_players = players.into_iter().collect::<HashSet<_>>();
    let dynamic = spec.persistence.is_some();
    let solo_mode = prepared
      .definition
      .gamevoting
      .as_ref()
      .filter(|game| game.solo)
      .map(|game| game.solo_mode);
    let solo_session_id = Arc::new(Mutex::new(spec.solo_session_id.clone()));
    let preserve_solo_session = Arc::new(AtomicBool::new(false));
    let instance = Arc::new(InstanceControl {
      info: info.clone(),
      control: control_sender,
      logs: log_buffer.clone(),
      completion,
      pending_players: Mutex::new(pending_players),
      startup_timeout_seconds: prepared.definition.startup_timeout_seconds,
      autostart: !dynamic && required_server(&prepared.definition),
      restart_delay_seconds: prepared.definition.restart_delay_seconds,
      daily_restart: (!dynamic)
        .then(|| prepared.definition.daily_restart.clone())
        .flatten(),
      daily_restart_tracker: Mutex::new(DailyRestartTracker::default()),
      idle_timeout_seconds: prepared.definition.idle_timeout_seconds,
      launch_spec: spec.clone(),
      solo_mode,
      solo_session_id: solo_session_id.clone(),
      preserve_solo_session: preserve_solo_session.clone(),
    });
    self
      .instances
      .write()
      .await
      .insert(spec.runtime_id.clone(), instance);

    let stdout_task = tokio::spawn(drain_stream(stdout, "stdout", log_buffer.clone()));
    let stderr_task = tokio::spawn(drain_stream(stderr, "stderr", log_buffer.clone()));
    tokio::spawn(supervise(
      child,
      stdin,
      control_receiver,
      SupervisionContext {
        info: info.clone(),
        logs: log_buffer,
        stdout_task,
        stderr_task,
        allocated_ports: self.allocated_ports.clone(),
        port_cooldowns: self.port_cooldowns.clone(),
        config: self.config.clone(),
        runtime_id: prepared.server_id.clone(),
        dependency_id: spec.definition_id.clone(),
        work_dir: prepared.work_dir.clone(),
        dependency_snapshot: prepared.dependency_snapshot.clone(),
        dependency_cache_lock: self.dependency_cache_lock.clone(),
        transfers: self.transfers.clone(),
        solo_sessions: self.solo_sessions.clone(),
        solo_session_id,
        preserve_solo_session,
        solo_persistence: spec.persistence,
        completion: completion_sender,
        port,
      },
    ));
    Ok(info.read().await.clone())
  }

  async fn reserve_port(&self, core_kind: &str) -> Result<u16, ManagerError> {
    let mut allocated = self.allocated_ports.lock().await;
    if core_kind.eq_ignore_ascii_case("velocity") {
      let port = self.config.value.ports.velocity;
      if allocated.contains(&port) || self.probe_ports && !port_available(port).await {
        return Err(ManagerError::PortUnavailable(port));
      }
      allocated.insert(port);
      return Ok(port);
    }
    let timestamp = Instant::now();
    let mut cooldowns = self.port_cooldowns.lock().await;
    cooldowns.retain(|_, expires_at| *expires_at > timestamp);
    let mut unavailable = allocated.clone();
    unavailable.extend(cooldowns.keys().copied());
    while let Some(port) = first_available_port(
      self.config.value.ports.start,
      self.config.value.ports.end,
      &unavailable,
    ) {
      if !self.probe_ports || port_available(port).await {
        allocated.insert(port);
        return Ok(port);
      }
      unavailable.insert(port);
    }
    Err(ManagerError::PortRangeExhausted {
      start: self.config.value.ports.start,
      end: self.config.value.ports.end,
    })
  }

  async fn release_port(&self, port: u16) {
    self.allocated_ports.lock().await.remove(&port);
  }

  pub async fn ensure_started(
    &self,
    server_id: &str,
    players: Vec<String>,
  ) -> Result<InstanceInfo, ManagerError> {
    let definition = self
      .config
      .load_server(server_id)
      .await
      .map_err(layout::LayoutError::from)?;
    if definition.gamevoting.as_ref().is_some_and(|game| game.solo) {
      return Err(ManagerError::State(
        "solo servers must be launched through the solo session API".into(),
      ));
    }
    if let Some(instance) = self.instances.read().await.get(server_id).cloned()
      && instance.info.read().await.state.active()
    {
      self.queue_transfers(server_id, players).await?;
      return Ok(instance.info.read().await.clone());
    }
    self.launch(server_id, players).await
  }

  pub async fn queue_transfers(
    &self,
    server_id: &str,
    players: Vec<String>,
  ) -> Result<(), ManagerError> {
    validate_players(&players)?;
    let instance = self.instance(server_id).await?;
    self.validate_solo_transfers(&instance, &players).await?;
    let state = instance.info.read().await.state;
    match state {
      InstanceState::Starting => {
        instance.pending_players.lock().await.extend(players);
        Ok(())
      }
      InstanceState::Ready => {
        let port = instance.info.read().await.port;
        self.enqueue_transfers(server_id, port, players).await;
        Ok(())
      }
      _ => Err(ManagerError::NotRunning(server_id.to_string())),
    }
  }

  async fn validate_solo_transfers(
    &self,
    instance: &InstanceControl,
    players: &[String],
  ) -> Result<(), ManagerError> {
    let Some(mode) = instance.solo_mode else {
      return Ok(());
    };
    let players = players
      .iter()
      .map(|player| {
        player
          .parse::<Uuid>()
          .map_err(|_| ManagerError::State(format!("invalid solo player UUID: {player}")))
      })
      .collect::<Result<Vec<_>, _>>()?;
    if mode == SoloMode::Shared {
      return self.ensure_solo_players_online(&players).await;
    }
    let session_id = instance
      .solo_session_id
      .lock()
      .await
      .clone()
      .ok_or_else(|| ManagerError::State("solo server player list is not locked".into()))?;
    let sessions = self.solo_sessions.read().await;
    let session = sessions
      .get(&session_id)
      .ok_or_else(|| ManagerError::State("solo session does not exist".into()))?;
    for player in players {
      if !session.players.contains(&player) {
        return Err(ManagerError::State(format!(
          "player {player} is not assigned to solo session {session_id}"
        )));
      }
    }
    Ok(())
  }

  pub async fn stop(&self, server_id: &str) -> Result<InstanceInfo, ManagerError> {
    let instance = self.instance(server_id).await?;
    let should_signal = {
      let mut info = instance.info.write().await;
      if !info.state.active() {
        false
      } else {
        info.state = InstanceState::Stopping;
        true
      }
    };
    let mut completion = instance.completion.clone();
    if should_signal {
      let _ = instance.control.send(ProcessControl::Stop).await;
    }
    loop {
      if let Some(result) = completion.borrow().clone() {
        result.map_err(ManagerError::State)?;
        break;
      }
      completion.changed().await.map_err(|_| {
        ManagerError::State("process supervisor dropped the completion signal".into())
      })?;
    }
    Ok(instance.info.read().await.clone())
  }

  pub async fn restart(&self, server_id: &str) -> Result<InstanceInfo, ManagerError> {
    {
      let mut launching = self.launching.lock().await;
      if launching.contains(server_id) {
        return Err(ManagerError::AlreadyRunning(server_id.to_string()));
      }
      launching.insert(server_id.to_string());
    }
    let result = self.restart_reserved(server_id).await;
    self.launching.lock().await.remove(server_id);
    result
  }

  async fn restart_reserved(&self, server_id: &str) -> Result<InstanceInfo, ManagerError> {
    let existing = self.instances.read().await.get(server_id).cloned();
    let mut spec = if let Some(instance) = &existing {
      instance.launch_spec.clone()
    } else {
      let definition = self
        .config
        .load_server(server_id)
        .await
        .map_err(layout::LayoutError::from)?;
      if definition.gamevoting.as_ref().is_some_and(|game| game.solo) {
        return Err(ManagerError::State(
          "solo servers must be restarted through the solo session API".into(),
        ));
      }
      LaunchSpec::static_server(server_id)
    };
    if let Some(instance) = existing {
      spec.solo_session_id = instance.solo_session_id.lock().await.clone();
      let state = instance.info.read().await.state;
      if matches!(state, InstanceState::Starting | InstanceState::Ready) {
        let preserve_session = instance.solo_session_id.lock().await.is_some();
        if preserve_session {
          instance
            .preserve_solo_session
            .store(true, Ordering::Release);
        }
        if let Err(error) = self.stop(server_id).await {
          instance
            .preserve_solo_session
            .store(false, Ordering::Release);
          return Err(error);
        }
      }
    }

    let deadline = Instant::now() + Duration::from_secs(30);
    loop {
      let active = if let Some(instance) = self.instances.read().await.get(server_id).cloned() {
        instance.info.read().await.state.active()
      } else {
        false
      };
      if !active {
        break;
      }
      if Instant::now() >= deadline {
        return Err(ManagerError::State(format!(
          "server {server_id} did not stop before restart"
        )));
      }
      sleep(Duration::from_millis(100)).await;
    }

    if let Some(session_id) = &spec.solo_session_id
      && !self.solo_sessions.read().await.contains_key(session_id)
    {
      if spec.persistence.is_some() {
        return Err(ManagerError::State(
          "dynamic solo session no longer exists".into(),
        ));
      }
      spec.solo_session_id = None;
    }
    self.launch_reserved(spec, Vec::new()).await
  }

  pub async fn stop_all(&self) {
    self.shutting_down.store(true, Ordering::Release);
    let ids = self
      .instances
      .read()
      .await
      .keys()
      .cloned()
      .collect::<Vec<_>>();
    for id in ids {
      let _ = self.stop(&id).await;
    }
  }

  pub async fn command(
    &self,
    server_id: &str,
    command: String,
  ) -> Result<Vec<String>, ManagerError> {
    if command.trim().is_empty() || command.contains('\n') || command.contains('\r') {
      return Err(ManagerError::State(
        "command must contain exactly one non-empty line".into(),
      ));
    }
    let instance = self.instance(server_id).await?;
    if !instance.info.read().await.state.active() {
      return Err(ManagerError::NotRunning(server_id.to_string()));
    }
    let mut log_receiver = instance.logs.sender.subscribe();
    let (sender, receiver) = oneshot::channel();
    instance
      .control
      .send(ProcessControl::Command {
        value: command,
        response: sender,
      })
      .await
      .map_err(|_| ManagerError::State("process supervisor is unavailable".into()))?;
    receiver.await.map_err(|_| {
      ManagerError::State("process supervisor dropped the command response".into())
    })??;

    let deadline = Instant::now() + Duration::from_secs(2);
    let mut output = Vec::new();
    loop {
      let remaining = deadline.saturating_duration_since(Instant::now());
      if remaining.is_zero() {
        break;
      }
      match timeout(
        remaining.min(Duration::from_millis(500)),
        log_receiver.recv(),
      )
      .await
      {
        Ok(Ok(line)) => output.push(line),
        Ok(Err(broadcast::error::RecvError::Lagged(_))) => continue,
        _ => break,
      }
    }
    Ok(output)
  }

  pub async fn logs(&self, server_id: &str, count: usize) -> Result<Vec<String>, ManagerError> {
    validate_id(server_id).map_err(layout::LayoutError::from)?;
    if let Some(instance) = self.instances.read().await.get(server_id).cloned() {
      let history = instance.logs.history.lock().await;
      if !history.is_empty() {
        let start = history.len().saturating_sub(count.min(LOG_HISTORY_LIMIT));
        return Ok(history.iter().skip(start).cloned().collect());
      }
    }
    let path = self.latest_log(server_id).await?;
    let content = fs::read_to_string(&path)
      .await
      .map_err(|source| ManagerError::Io {
        path: path.clone(),
        source,
      })?;
    let lines = content.lines().map(str::to_string).collect::<Vec<_>>();
    let start = lines.len().saturating_sub(count.min(LOG_HISTORY_LIMIT));
    Ok(lines.into_iter().skip(start).collect())
  }

  pub async fn list(&self) -> Vec<InstanceInfo> {
    let instances = self
      .instances
      .read()
      .await
      .values()
      .cloned()
      .collect::<Vec<_>>();
    let mut result = Vec::with_capacity(instances.len());
    for instance in instances {
      result.push(instance.info.read().await.clone());
    }
    result.sort_by(|left, right| left.server_id.cmp(&right.server_id));
    result
  }

  pub async fn ready(&self, server_id: &str, instance_id: Uuid) -> Result<(), ManagerError> {
    let instance = self.instance(server_id).await?;
    let already_ready;
    {
      let mut info = instance.info.write().await;
      if info.instance_id != instance_id {
        return Err(ManagerError::State(
          "instance ID does not match the active server".into(),
        ));
      }
      if !matches!(info.state, InstanceState::Starting | InstanceState::Ready) {
        return Err(ManagerError::State(
          "server cannot become ready from its current state".into(),
        ));
      }
      already_ready = info.state == InstanceState::Ready;
      info.state = InstanceState::Ready;
      info.last_heartbeat = Some(now());
    }
    if already_ready {
      return Ok(());
    }
    let port = instance.info.read().await.port;
    let players = instance.pending_players.lock().await.drain().collect();
    self.enqueue_transfers(server_id, port, players).await;
    Ok(())
  }

  async fn enqueue_transfers(&self, server_id: &str, port: u16, players: Vec<String>) {
    let mut transfers = self.transfers.lock().await;
    for player in players {
      if transfers
        .values()
        .any(|transfer| transfer.server_id == server_id && transfer.player == player)
      {
        continue;
      }
      let transfer_id = Uuid::new_v4();
      let timestamp = now();
      transfers.insert(
        transfer_id,
        TransferRecord {
          transfer_id,
          server_id: server_id.to_string(),
          player,
          address: format!("127.0.0.1:{port}"),
          next_attempt: timestamp,
          expires_at: timestamp.saturating_add(self.config.value.bridge.transfer_timeout_seconds),
          attempts: 0,
        },
      );
    }
  }

  pub async fn heartbeat(&self, server_id: &str, instance_id: Uuid) -> Result<(), ManagerError> {
    let instance = self.instance(server_id).await?;
    let mut info = instance.info.write().await;
    if info.instance_id != instance_id {
      return Err(ManagerError::State(
        "instance ID does not match the active server".into(),
      ));
    }
    if info.state != InstanceState::Ready {
      return Err(ManagerError::State(
        "heartbeat is only accepted for ready servers".into(),
      ));
    }
    info.last_heartbeat = Some(now());
    Ok(())
  }

  pub async fn idle(&self, server_id: &str, instance_id: Uuid) -> Result<(), ManagerError> {
    let instance = self.instance(server_id).await?;
    {
      let info = instance.info.read().await;
      if info.instance_id != instance_id {
        return Err(ManagerError::State(
          "instance ID does not match the active server".into(),
        ));
      }
      if info.state != InstanceState::Ready {
        return Err(ManagerError::State(
          "idle shutdown is only accepted for ready servers".into(),
        ));
      }
      if instance.autostart || instance.idle_timeout_seconds.is_none() {
        return Err(ManagerError::State(
          "idle shutdown is disabled for this server".into(),
        ));
      }
    }
    let manager = self.clone();
    let server_id = server_id.to_string();
    tokio::spawn(async move {
      if let Err(error) = manager.stop(&server_id).await {
        tracing::error!(server_id = %server_id, %error, "Failed to stop idle server");
      }
    });
    Ok(())
  }

  pub async fn pending_transfers(&self) -> Vec<PendingTransfer> {
    let timestamp = now();
    let retry = self.config.value.bridge.transfer_retry_seconds;
    let instances = self
      .instances
      .read()
      .await
      .values()
      .cloned()
      .collect::<Vec<_>>();
    let mut ready_servers = std::collections::HashSet::new();
    for instance in instances {
      let info = instance.info.read().await;
      if info.state == InstanceState::Ready {
        ready_servers.insert(info.server_id.clone());
      }
    }
    let mut transfers = self.transfers.lock().await;
    transfers.retain(|_, transfer| {
      transfer_active(
        transfer,
        timestamp,
        self.config.value.bridge.transfer_max_attempts,
      )
    });
    let mut result = Vec::new();
    for transfer in transfers.values_mut() {
      if transfer.next_attempt <= timestamp && ready_servers.contains(&transfer.server_id) {
        transfer.attempts += 1;
        transfer.next_attempt = timestamp + retry;
        result.push(PendingTransfer {
          transfer_id: transfer.transfer_id,
          server_id: transfer.server_id.clone(),
          player: transfer.player.clone(),
          address: transfer.address.clone(),
        });
      }
    }
    result
  }

  pub async fn transfer_result(
    &self,
    transfer_id: Uuid,
    status: TransferResultStatus,
    message: String,
  ) -> Result<(), ManagerError> {
    let mut transfers = self.transfers.lock().await;
    let Some(mut transfer) = transfers.remove(&transfer_id) else {
      return Ok(());
    };
    match status {
      TransferResultStatus::Success => {}
      TransferResultStatus::Failure => {
        let timestamp = now();
        if transfer_active(
          &transfer,
          timestamp,
          self.config.value.bridge.transfer_max_attempts,
        ) && !terminal_transfer_failure(&message)
        {
          transfer.next_attempt = timestamp + self.config.value.bridge.transfer_retry_seconds;
          transfers.insert(transfer_id, transfer);
        }
      }
    }
    Ok(())
  }

  pub async fn monitor(self) {
    let mut ticker = interval(Duration::from_secs(5));
    loop {
      ticker.tick().await;
      if self.shutting_down.load(Ordering::Acquire) {
        return;
      }
      self.ensure_autostart().await;
      let instances = self
        .instances
        .read()
        .await
        .values()
        .cloned()
        .collect::<Vec<_>>();
      let timestamp = now();
      for instance in instances {
        let info = instance.info.read().await.clone();
        let startup_expired = info.state == InstanceState::Starting
          && timestamp.saturating_sub(info.started_at) > instance.startup_timeout_seconds;
        let heartbeat_expired = info.state == InstanceState::Ready
          && info.last_heartbeat.is_some_and(|heartbeat| {
            timestamp.saturating_sub(heartbeat) > self.config.value.bridge.heartbeat_timeout_seconds
          });
        if startup_expired || heartbeat_expired {
          let _ = self.stop(&info.server_id).await;
          instance.info.write().await.state = InstanceState::Failed;
        }
      }
      self.ensure_daily_restarts().await;
      self.ensure_solo_world_cleanup().await;
    }
  }

  async fn ensure_solo_world_cleanup(&self) {
    let (clock, timestamp) = current_restart_clock();
    {
      let mut completed_day = self.solo_cleanup_day.lock().await;
      if !solo_cleanup_due(clock, *completed_day) {
        return;
      }
      *completed_day = Some(clock.day);
    }
    let removed = self.cleanup_expired_solo_worlds(timestamp).await;
    tracing::info!(removed, "Expired solo world cleanup completed");
  }

  async fn cleanup_expired_solo_worlds(&self, timestamp: u64) -> usize {
    let _operations = self.solo_operations.lock().await;
    let active = {
      let instances = self.instances.read().await;
      let mut active = HashSet::new();
      for instance in instances.values() {
        let info = instance.info.read().await;
        if info.state.active() {
          active.insert(info.server_id.clone());
        }
      }
      active
    };
    let candidates = self
      .solo_sessions
      .read()
      .await
      .values()
      .filter(|session| {
        session.mode == SoloMode::PlayerWorld
          && session.expires.is_some_and(|expires| expires <= timestamp)
          && !active.contains(&session.backend_id)
      })
      .cloned()
      .collect::<Vec<_>>();
    let mut removed = 0usize;
    for session in candidates {
      let Some(persistence) = &session.persistence else {
        continue;
      };
      match solo::destroy_storage(persistence).await {
        Ok(()) => {
          self.solo_sessions.write().await.remove(&session.session_id);
          removed += 1;
        }
        Err(error) => {
          tracing::error!(
            session_id = %session.session_id,
            path = %persistence.root.display(),
            %error,
            "Failed to remove expired solo world"
          );
        }
      }
    }
    removed
  }

  async fn ensure_daily_restarts(&self) {
    let instances = self
      .instances
      .read()
      .await
      .values()
      .cloned()
      .collect::<Vec<_>>();
    let online_by_server = {
      let players = self.players.read().await;
      let mut counts = HashMap::<String, usize>::new();
      for player in players.values() {
        if let Some(server_id) = &player.server_id {
          *counts.entry(server_id.clone()).or_default() += 1;
        }
      }
      counts
    };
    let (current_clock, timestamp) = current_restart_clock();
    for instance in instances {
      let Some(schedule) = &instance.daily_restart else {
        continue;
      };
      let info = instance.info.read().await.clone();
      if info.state != InstanceState::Ready {
        continue;
      }
      let Some(instance_started) = restart_clock_at(info.started_at) else {
        tracing::error!(
          server_id = %info.server_id,
          started_at = info.started_at,
          "Failed to convert server start time for daily restart"
        );
        continue;
      };
      let online_players = online_by_server
        .get(&info.server_id)
        .copied()
        .unwrap_or_default();
      let action = instance.daily_restart_tracker.lock().await.evaluate(
        schedule,
        current_clock,
        instance_started,
        timestamp,
        online_players,
      );
      match action {
        DailyRestartAction::None => {}
        DailyRestartAction::WaitingForEmpty => {
          tracing::info!(
            server_id = %info.server_id,
            online_players,
            "Daily restart deferred until the server is empty"
          );
        }
        DailyRestartAction::EmptyGraceStarted => {
          tracing::info!(
            server_id = %info.server_id,
            empty_grace_seconds = schedule.empty_grace_seconds,
            "Daily restart empty grace period started"
          );
        }
        DailyRestartAction::Restart => {
          let stop_on_demand = self
            .config
            .load_server(&instance.launch_spec.definition_id)
            .await
            .ok()
            .is_some_and(|definition| stop_for_daily_maintenance(&definition));
          tracing::info!(server_id = %info.server_id, "Starting daily scheduled restart");
          let result = if stop_on_demand {
            self.stop(&info.server_id).await.map(|_| ())
          } else {
            self.restart(&info.server_id).await.map(|_| ())
          };
          if let Err(error) = result {
            tracing::error!(
              server_id = %info.server_id,
              %error,
              "Daily scheduled restart failed"
            );
          }
        }
      }
    }
  }

  async fn ensure_autostart(&self) {
    let Ok(ids) = self.config.server_ids().await else {
      tracing::error!("Failed to enumerate server definitions for autostart");
      return;
    };
    let mut definitions = Vec::new();
    for id in ids {
      match self.config.load_server(&id).await {
        Ok(definition) if required_server(&definition) => definitions.push((id, definition)),
        Ok(_) => {}
        Err(error) => {
          tracing::error!(server_id = %id, %error, "Failed to load autostart server definition");
        }
      }
    }
    definitions
      .sort_by_key(|(_, definition)| !definition.core.kind.eq_ignore_ascii_case("velocity"));
    for (id, _) in definitions {
      if let Some(instance) = self.instances.read().await.get(&id).cloned() {
        let info = instance.info.read().await;
        if info.state.active()
          || now().saturating_sub(info.started_at) < instance.restart_delay_seconds
        {
          continue;
        }
      }
      if let Err(error) = self.launch_static(&id).await {
        tracing::error!(server_id = %id, %error, "Failed to start required server");
      }
    }
  }

  async fn instance(&self, server_id: &str) -> Result<Arc<InstanceControl>, ManagerError> {
    self
      .instances
      .read()
      .await
      .get(server_id)
      .cloned()
      .ok_or_else(|| ManagerError::NotFound(server_id.to_string()))
  }

  async fn latest_log(&self, server_id: &str) -> Result<std::path::PathBuf, ManagerError> {
    let directory = self.config.resolve(&self.config.value.paths.logs);
    let mut entries = fs::read_dir(&directory)
      .await
      .map_err(|source| ManagerError::Io {
        path: directory.clone(),
        source,
      })?;
    let prefix = format!("{server_id}-");
    let mut latest = None;
    while let Some(entry) = entries
      .next_entry()
      .await
      .map_err(|source| ManagerError::Io {
        path: directory.clone(),
        source,
      })?
    {
      let name = entry.file_name().to_string_lossy().to_string();
      if name.starts_with(&prefix) && name.ends_with(".log") {
        let modified = entry
          .metadata()
          .await
          .and_then(|metadata| metadata.modified())
          .unwrap_or(UNIX_EPOCH);
        if latest
          .as_ref()
          .is_none_or(|(current, _)| modified > *current)
        {
          latest = Some((modified, entry.path()));
        }
      }
    }
    latest
      .map(|(_, path)| path)
      .ok_or_else(|| ManagerError::NotFound(server_id.to_string()))
  }
}

impl LogBuffer {
  async fn new(path: &std::path::Path) -> Result<Self, ManagerError> {
    let file = OpenOptions::new()
      .create(true)
      .append(true)
      .open(path)
      .await
      .map_err(|source| ManagerError::Io {
        path: path.to_path_buf(),
        source,
      })?;
    let (sender, _) = broadcast::channel(512);
    Ok(Self {
      history: Arc::new(Mutex::new(VecDeque::new())),
      sender,
      file: Arc::new(Mutex::new(file)),
    })
  }

  async fn append(&self, stream: &str, value: &str) {
    let line = format!("[{stream}] {value}");
    {
      let mut history = self.history.lock().await;
      if history.len() == LOG_HISTORY_LIMIT {
        history.pop_front();
      }
      history.push_back(line.clone());
    }
    let _ = self.sender.send(line.clone());
    let mut file = self.file.lock().await;
    let _ = file.write_all(line.as_bytes()).await;
    let _ = file.write_all(b"\n").await;
    let _ = file.flush().await;
  }
}

fn create_process_command(config: &LoadedConfig, prepared: &PreparedInstance) -> Command {
  let mut command = Command::new(&prepared.java_bin);
  command
    .current_dir(&prepared.work_dir)
    .env_clear()
    .env("HOME", &prepared.work_dir)
    .env("LANG", "C.UTF-8")
    .env("PATH", "/usr/bin:/bin")
    .env(
      "SCHEDULER_BRIDGE_URL",
      format!("http://127.0.0.1:{}", config.value.api.port),
    )
    .env("SCHEDULER_BRIDGE_TOKEN", &config.value.bridge.token)
    .env("SCHEDULER_SERVER_ID", &prepared.server_id)
    .env("SCHEDULER_INSTANCE_ID", prepared.instance_id.to_string())
    .env("SCHEDULER_SERVER_PORT", prepared.port.to_string())
    .env(
      "SCHEDULER_TRANSFER_TIMEOUT_SECONDS",
      config.value.bridge.transfer_timeout_seconds.to_string(),
    )
    .env(
      "SCHEDULER_IDLE_TIMEOUT_SECONDS",
      prepared
        .definition
        .idle_timeout_seconds
        .unwrap_or(0)
        .to_string(),
    )
    .envs(&prepared.environment)
    .arg(format!("-Xms{}M", prepared.definition.memory.initial_mib))
    .arg(format!("-Xmx{}M", prepared.definition.memory.maximum_mib))
    .args(&prepared.definition.jvm_args)
    .arg("-Dfile.encoding=UTF-8")
    .arg("-jar")
    .arg(&prepared.server_jar);
  if prepared.definition.nogui {
    command.arg("nogui");
  }
  command
    .args(&prepared.definition.server_args)
    .stdin(Stdio::piped())
    .stdout(Stdio::piped())
    .stderr(Stdio::piped())
    .kill_on_drop(true);
  command
}

struct SupervisionContext {
  info: Arc<RwLock<InstanceInfo>>,
  logs: LogBuffer,
  stdout_task: JoinHandle<io::Result<()>>,
  stderr_task: JoinHandle<io::Result<()>>,
  allocated_ports: Arc<Mutex<HashSet<u16>>>,
  port_cooldowns: Arc<Mutex<HashMap<u16, Instant>>>,
  config: Arc<LoadedConfig>,
  runtime_id: String,
  dependency_id: String,
  work_dir: std::path::PathBuf,
  dependency_snapshot: DependencySnapshot,
  dependency_cache_lock: Arc<Mutex<()>>,
  transfers: Arc<Mutex<HashMap<Uuid, TransferRecord>>>,
  solo_sessions: Arc<RwLock<HashMap<String, SoloSessionRecord>>>,
  solo_session_id: Arc<Mutex<Option<String>>>,
  preserve_solo_session: Arc<AtomicBool>,
  solo_persistence: Option<SoloPersistence>,
  completion: watch::Sender<Option<Result<(), String>>>,
  port: u16,
}

async fn supervise(
  mut child: Child,
  mut stdin: ChildStdin,
  mut controls: mpsc::Receiver<ProcessControl>,
  context: SupervisionContext,
) {
  let status = loop {
    tokio::select! {
        result = child.wait() => break result,
        control = controls.recv() => match control {
            Some(ProcessControl::Command { value, response }) => {
                let result = async {
                    stdin.write_all(value.as_bytes()).await?;
                    stdin.write_all(b"\n").await?;
                    stdin.flush().await
                }.await;
                let _ = response.send(result);
            }
            Some(ProcessControl::Stop) => {
                let write_result = async {
                    stdin.write_all(b"stop\n").await?;
                    stdin.flush().await
                }.await;
                if let Err(error) = write_result {
                    context.logs.append("scheduler", &format!("failed to send stop command: {error}; killing process")).await;
                    let _ = child.kill().await;
                }
                let result = match timeout(Duration::from_secs(60), child.wait()).await {
                    Ok(result) => result,
                    Err(_) => match child.kill().await {
                        Ok(()) => child.wait().await,
                        Err(error) => Err(error),
                    },
                };
                break result;
            }
            None => {
                let _ = child.kill().await;
                break child.wait().await;
            }
        }
    }
  };
  context
    .transfers
    .lock()
    .await
    .retain(|_, transfer| transfer.server_id != context.runtime_id);
  let runtime_root = context.config.resolve(&context.config.value.paths.runtime);
  let mut cleanup_result = Ok(());
  if let Some(persistence) = &context.solo_persistence {
    let persist_result = async {
      solo::persist_world(persistence, &context.work_dir.join("world")).await?;
      let mut metadata = solo::read_metadata(&persistence.root).await?;
      solo::touch(persistence, &mut metadata, now()).await?;
      if let Some(session_id) = context.solo_session_id.lock().await.clone()
        && let Some(session) = context.solo_sessions.write().await.get_mut(&session_id)
      {
        session.updated = metadata.updated;
        session.expires = Some(metadata.expires);
      }
      Ok::<(), solo::SoloError>(())
    }
    .await;
    if let Err(error) = persist_result {
      cleanup_result = Err(format!("failed to persist solo world: {error}"));
      context
        .logs
        .append(
          "scheduler",
          &format!("failed to persist solo world: {error}"),
        )
        .await;
    }
  } else if !context.preserve_solo_session.load(Ordering::Acquire)
    && let Some(session_id) = context.solo_session_id.lock().await.take()
  {
    context.solo_sessions.write().await.remove(&session_id);
  }
  let staged_work_dir = if cleanup_result.is_ok() {
    match runtime::stage_instance(&runtime_root, &context.work_dir).await {
      Ok(path) => path,
      Err(stage_error) => {
        context
          .logs
          .append(
            "scheduler",
            &format!("failed to stage runtime directory: {stage_error}"),
          )
          .await;
        match runtime::remove_instance(&context.work_dir).await {
          Ok(()) => None,
          Err(remove_error) => {
            cleanup_result = Err(format!(
              "failed to remove runtime directory {} after staging failed: {remove_error}",
              context.work_dir.display()
            ));
            None
          }
        }
      }
    }
  } else {
    None
  };
  let _ = timeout(Duration::from_secs(5), context.stdout_task).await;
  let _ = timeout(Duration::from_secs(5), context.stderr_task).await;
  if let Some(staged_work_dir) = &staged_work_dir {
    let _guard = context.dependency_cache_lock.lock().await;
    match dependency_cache::capture(
      &context.config,
      &context.dependency_id,
      staged_work_dir,
      &context.dependency_snapshot,
    )
    .await
    {
      Ok(count) if count > 0 => {
        context
          .logs
          .append(
            "scheduler",
            &format!("cached {count} runtime dependency files"),
          )
          .await;
      }
      Ok(_) => {}
      Err(error) => {
        context
          .logs
          .append(
            "scheduler",
            &format!("failed to cache runtime dependencies: {error}"),
          )
          .await;
      }
    }
  }
  if let Some(staged_work_dir) = &staged_work_dir {
    match runtime::remove_staged_instance(staged_work_dir).await {
      Ok(()) => {}
      Err(error) => {
        cleanup_result = Err(format!(
          "failed to remove staged runtime directory {}: {error}",
          staged_work_dir.display()
        ));
      }
    }
  }
  if cleanup_result.is_ok() {
    context
      .logs
      .append("scheduler", "removed runtime directory")
      .await;
  } else if let Err(error) = &cleanup_result {
    context.logs.append("scheduler", error).await;
  }
  release_instance_port(
    &context.allocated_ports,
    &context.port_cooldowns,
    context.port,
    context.config.value.ports.start,
    context.config.value.ports.end,
    Instant::now(),
  )
  .await;
  let mut mutable = context.info.write().await;
  match status {
    Ok(exit) => {
      mutable.state = InstanceState::Exited;
      mutable.exit_code = exit.code();
      context
        .logs
        .append("scheduler", &format!("process exited with {exit}"))
        .await;
    }
    Err(error) => {
      mutable.state = InstanceState::Failed;
      context
        .logs
        .append("scheduler", &format!("process wait failed: {error}"))
        .await;
    }
  }
  if cleanup_result.is_err() {
    mutable.state = InstanceState::Failed;
  }
  drop(mutable);
  let _ = context.completion.send(Some(cleanup_result));
}

async fn open_instance_log(prepared: &PreparedInstance) -> Result<LogBuffer, ManagerError> {
  match LogBuffer::new(&prepared.log_file).await {
    Ok(logs) => Ok(logs),
    Err(error) => Err(cleanup_failed_launch(&prepared.work_dir, error).await),
  }
}

async fn spawn_instance_process(
  command: &mut Command,
  prepared: &PreparedInstance,
) -> Result<Child, ManagerError> {
  match command.spawn() {
    Ok(child) => Ok(child),
    Err(source) => {
      let error = ManagerError::Spawn {
        server_id: prepared.server_id.clone(),
        source,
      };
      Err(cleanup_failed_launch(&prepared.work_dir, error).await)
    }
  }
}

async fn abort_spawned_process(
  mut child: Child,
  work_dir: &Path,
  error: ManagerError,
) -> ManagerError {
  let _ = child.kill().await;
  let _ = child.wait().await;
  cleanup_failed_launch(work_dir, error).await
}

async fn cleanup_failed_launch(work_dir: &Path, error: ManagerError) -> ManagerError {
  match runtime::remove_instance(work_dir).await {
    Ok(()) => error,
    Err(source) => ManagerError::LaunchCleanup {
      path: work_dir.to_path_buf(),
      failure: error.to_string(),
      source,
    },
  }
}

async fn drain_stream<R>(reader: R, stream: &'static str, logs: LogBuffer) -> io::Result<()>
where
  R: tokio::io::AsyncRead + Unpin,
{
  let mut lines = BufReader::new(reader).lines();
  while let Some(line) = lines.next_line().await? {
    logs.append(stream, &line).await;
  }
  Ok(())
}

fn validate_players(players: &[String]) -> Result<(), ManagerError> {
  for player in players {
    if player.is_empty()
      || !player
        .chars()
        .all(|character| character.is_ascii_alphanumeric() || character == '-' || character == '_')
    {
      return Err(ManagerError::State(format!(
        "invalid player identifier: {player}"
      )));
    }
  }
  Ok(())
}

fn normalize_solo_players(owner: Uuid, players: Vec<Uuid>) -> Vec<Uuid> {
  let mut unique = players.into_iter().collect::<BTreeSet<_>>();
  unique.remove(&owner);
  let mut normalized = Vec::with_capacity(unique.len() + 1);
  normalized.push(owner);
  normalized.extend(unique);
  normalized
}

fn same_player_set(left: &[Uuid], right: &[Uuid]) -> bool {
  left.iter().copied().collect::<BTreeSet<_>>() == right.iter().copied().collect::<BTreeSet<_>>()
}

fn validate_solo_roster(
  game: &GameVotingDefinition,
  owner: Uuid,
  players: &[Uuid],
) -> Result<(), ManagerError> {
  if !game.solo {
    return Err(ManagerError::State(
      "game is not configured for solo mode".into(),
    ));
  }
  if !players.contains(&owner) {
    return Err(ManagerError::State(
      "solo session owner must be included in players".into(),
    ));
  }
  let unique = players.iter().copied().collect::<BTreeSet<_>>();
  if unique.len() != players.len() {
    return Err(ManagerError::State(
      "solo session players must not contain duplicates".into(),
    ));
  }
  let count = u32::try_from(players.len()).unwrap_or(u32::MAX);
  if count < game.min_players || count > game.max_players || count > game.solo_max_players {
    return Err(ManagerError::State(format!(
      "solo session player count {count} is outside the configured range"
    )));
  }
  Ok(())
}

fn required_server(definition: &crate::config::ServerDefinition) -> bool {
  definition.autostart
    || definition.gamevoting.as_ref().is_some_and(|game| {
      game.solo && game.solo_mode == SoloMode::Shared && game.solo_startup == SoloStartup::Always
    })
}

fn stop_for_daily_maintenance(definition: &crate::config::ServerDefinition) -> bool {
  definition.gamevoting.as_ref().is_some_and(|game| {
    game.solo && game.solo_mode == SoloMode::Shared && game.solo_startup == SoloStartup::OnDemand
  })
}

fn now() -> u64 {
  SystemTime::now()
    .duration_since(UNIX_EPOCH)
    .unwrap_or_default()
    .as_secs()
}

fn terminal_transfer_failure(message: &str) -> bool {
  message
    .to_ascii_lowercase()
    .contains("player is not connected")
}

fn transfer_active(transfer: &TransferRecord, timestamp: u64, max_attempts: u32) -> bool {
  transfer.expires_at > timestamp && transfer.attempts < max_attempts
}

fn current_restart_clock() -> (RestartClock, u64) {
  let current = Local::now();
  let timestamp = u64::try_from(current.timestamp()).unwrap_or_default();
  (restart_clock(&current), timestamp)
}

fn restart_clock_at(timestamp: u64) -> Option<RestartClock> {
  let timestamp = i64::try_from(timestamp).ok()?;
  let current = DateTime::<Utc>::from_timestamp(timestamp, 0)?.with_timezone(&Local);
  Some(restart_clock(&current))
}

fn restart_clock(current: &DateTime<Local>) -> RestartClock {
  RestartClock {
    day: current.year() * 10_000 + current.month() as i32 * 100 + current.day() as i32,
    second_of_day: current.hour() * 3600 + current.minute() * 60 + current.second(),
  }
}

fn solo_cleanup_due(clock: RestartClock, completed_day: Option<i32>) -> bool {
  clock.second_of_day >= 4 * 3600 && completed_day != Some(clock.day)
}

fn first_available_port(start: u16, end: u16, allocated: &HashSet<u16>) -> Option<u16> {
  (start..=end).find(|port| !allocated.contains(port))
}

async fn port_available(port: u16) -> bool {
  TcpListener::bind(("0.0.0.0", port)).await.is_ok()
}

async fn release_instance_port(
  allocated_ports: &Mutex<HashSet<u16>>,
  port_cooldowns: &Mutex<HashMap<u16, Instant>>,
  port: u16,
  child_port_start: u16,
  child_port_end: u16,
  timestamp: Instant,
) {
  let mut allocated = allocated_ports.lock().await;
  if (child_port_start..=child_port_end).contains(&port) {
    let mut cooldowns = port_cooldowns.lock().await;
    allocated.remove(&port);
    cooldowns.insert(port, timestamp + PORT_REUSE_COOLDOWN);
  } else {
    allocated.remove(&port);
  }
}

#[derive(Debug, Error)]
pub enum ManagerError {
  #[error(transparent)]
  Layout(#[from] layout::LayoutError),
  #[error(transparent)]
  Solo(#[from] solo::SoloError),
  #[error("server {0} is already running")]
  AlreadyRunning(String),
  #[error("server {0} is not running")]
  NotRunning(String),
  #[error("server {0} does not exist in the runtime registry")]
  NotFound(String),
  #[error("server port {0} is already allocated")]
  PortUnavailable(u16),
  #[error("no free server port exists in range {start}-{end}")]
  PortRangeExhausted { start: u16, end: u16 },
  #[error("failed to spawn server {server_id}: {source}")]
  Spawn {
    server_id: String,
    source: io::Error,
  },
  #[error("failed to remove runtime directory {path} after launch failure ({failure}): {source}")]
  LaunchCleanup {
    path: PathBuf,
    failure: String,
    source: io::Error,
  },
  #[error("failed to access {path}: {source}")]
  Io {
    path: std::path::PathBuf,
    source: io::Error,
  },
  #[error("invalid runtime state: {0}")]
  State(String),
  #[error(transparent)]
  Process(#[from] io::Error),
}

#[cfg(test)]
mod tests {
  use super::*;

  static NEXT_SOLO_TEST_PORT: std::sync::atomic::AtomicU16 =
    std::sync::atomic::AtomicU16::new(54000);

  async fn solo_test_manager(
    mode: SoloMode,
    startup: SoloStartup,
  ) -> (tempfile::TempDir, InstanceManager) {
    let directory = tempfile::tempdir().unwrap();
    let root = directory.path();
    let port = NEXT_SOLO_TEST_PORT.fetch_add(1, Ordering::Relaxed);
    for path in [
      "cores/fabric/26.2",
      "java21/bin",
      "java25/bin",
      "servers",
      "worlds/puzzle/world",
    ] {
      fs::create_dir_all(root.join(path)).await.unwrap();
    }
    fs::write(root.join("cores/fabric/26.2/server.jar"), b"server")
      .await
      .unwrap();
    fs::write(root.join("worlds/puzzle/world/level.dat"), b"template")
      .await
      .unwrap();
    let java = root.join("java21/bin/java");
    fs::write(
      &java,
      b"#!/bin/sh\nprintf 'started\\n'\nwhile IFS= read -r line; do\n  if [ \"$line\" = \"stop\" ]; then\n    exit 0\n  fi\ndone\n",
    )
    .await
    .unwrap();
    #[cfg(unix)]
    {
      use std::os::unix::fs::PermissionsExt;
      let mut permissions = fs::metadata(&java).await.unwrap().permissions();
      permissions.set_mode(0o755);
      fs::set_permissions(&java, permissions).await.unwrap();
    }
    fs::write(
      root.join("config.json"),
      serde_json::to_vec_pretty(&serde_json::json!({
        "api": {
          "port": 25566,
          "token": "01234567890123456789012345678901"
        },
        "bridge": {
          "token": "11234567890123456789012345678901"
        },
        "postgresql": {
          "home": "postgres",
          "host": "127.0.0.1",
          "port": 5432,
          "username": "minigames",
          "password": "test-password"
        },
        "ports": {
          "velocity": 25565,
          "start": port,
          "end": port + 2
        },
        "java": {
          "21_home": "java21",
          "25_home": "java25"
        },
        "paths": {
          "cores": "cores",
          "plugins": "plugins",
          "mods": "mods",
          "dependencies": "dependencies",
          "files": "files",
          "network": "network",
          "servers": "servers",
          "worlds": "worlds",
          "runtime": ".runtime",
          "logs": "logs",
          "pid_file": "scheduler.pid",
          "scheduler_log": "scheduler.log"
        }
      }))
      .unwrap(),
    )
    .await
    .unwrap();
    fs::write(
      root.join("servers/game.json"),
      serde_json::to_vec_pretty(&serde_json::json!({
        "core": {
          "kind": "fabric",
          "minecraft_version": "26.2",
          "java": "21",
          "jar": "server.jar"
        },
        "memory": {
          "initial_mib": 128,
          "maximum_mib": 256
        },
        "idle_timeout_seconds": 300,
        "gamevoting": {
          "id": "puzzle",
          "name": "Puzzle",
          "material": "COMPASS",
          "min_players": 1,
          "max_players": 2,
          "solo": true,
          "solo_mode": mode,
          "solo_startup": startup,
          "solo_max_players": 2,
          "solo_retention_days": 10
        },
        "properties": {
          "level-name": "world"
        }
      }))
      .unwrap(),
    )
    .await
    .unwrap();
    let config = Arc::new(LoadedConfig::load(root.join("config.json")).await.unwrap());
    let mut manager = InstanceManager::new(config);
    manager.probe_ports = false;
    manager.initialize().await.unwrap();
    (directory, manager)
  }

  fn presence(player: Uuid) -> PlayerPresence {
    PlayerPresence {
      uuid: player,
      username: format!("P{}", &player.simple().to_string()[..8]),
      ping_ms: 1,
      server_id: Some("Lobby".into()),
    }
  }

  fn prepared_instance(
    work_dir: PathBuf,
    log_file: PathBuf,
    java_bin: PathBuf,
  ) -> PreparedInstance {
    PreparedInstance {
      server_id: "game".into(),
      instance_id: Uuid::new_v4(),
      port: 50001,
      definition: serde_json::from_value(serde_json::json!({
        "core": {
          "kind": "paper",
          "minecraft_version": "1.21.11",
          "java": "21",
          "jar": "server.jar"
        },
        "memory": {
          "initial_mib": 128,
          "maximum_mib": 256
        }
      }))
      .unwrap(),
      server_jar: work_dir.join("server.jar"),
      work_dir,
      java_bin,
      log_file,
      environment: Default::default(),
      dependency_snapshot: DependencySnapshot::new(),
    }
  }

  #[test]
  fn dynamic_ports_start_at_configured_lower_bound() {
    let allocated = HashSet::from([50001, 50002]);
    assert_eq!(first_available_port(50001, 50010, &allocated), Some(50003));
  }

  #[tokio::test]
  async fn player_world_session_reuses_original_owner_for_same_roster() {
    let (_directory, manager) =
      solo_test_manager(SoloMode::PlayerWorld, SoloStartup::OnDemand).await;
    let first = Uuid::new_v4();
    let second = Uuid::new_v4();
    manager
      .replace_players(vec![presence(first), presence(second)])
      .await;
    let created = manager
      .create_solo_session(SoloSessionCreateRequest {
        game_id: "puzzle".into(),
        owner: first,
        players: vec![first, second],
      })
      .await
      .unwrap();
    let reused = manager
      .create_solo_session(SoloSessionCreateRequest {
        game_id: "puzzle".into(),
        owner: second,
        players: vec![second, first],
      })
      .await
      .unwrap();
    assert_eq!(created.session_id, reused.session_id);
    assert_eq!(reused.owner, first);
    assert_eq!(manager.solo_sessions().await.len(), 1);

    manager
      .destroy_solo_session(&created.session_id)
      .await
      .unwrap();
    let rebuilt = manager
      .create_solo_session(SoloSessionCreateRequest {
        game_id: "puzzle".into(),
        owner: second,
        players: vec![second],
      })
      .await
      .unwrap();
    assert_eq!(rebuilt.owner, second);
    assert_ne!(rebuilt.session_id, created.session_id);
  }

  #[tokio::test]
  async fn solo_session_rejects_missing_owner_and_offline_players() {
    let (_directory, manager) =
      solo_test_manager(SoloMode::PlayerWorld, SoloStartup::OnDemand).await;
    let owner = Uuid::new_v4();
    let other = Uuid::new_v4();
    manager.replace_players(vec![presence(owner)]).await;
    let missing_owner = manager
      .create_solo_session(SoloSessionCreateRequest {
        game_id: "puzzle".into(),
        owner,
        players: vec![other],
      })
      .await
      .unwrap_err();
    assert_eq!(
      missing_owner.to_string(),
      "invalid runtime state: solo session owner must be included in players"
    );
    let offline = manager
      .create_solo_session(SoloSessionCreateRequest {
        game_id: "puzzle".into(),
        owner,
        players: vec![owner, other],
      })
      .await
      .unwrap_err();
    assert!(offline.to_string().contains("not connected to Velocity"));
  }

  #[tokio::test]
  async fn solo_online_check_waits_for_delayed_velocity_snapshot() {
    let (_directory, manager) = solo_test_manager(SoloMode::Shared, SoloStartup::OnDemand).await;
    let player = Uuid::new_v4();
    let update = manager.clone();
    tokio::spawn(async move {
      sleep(Duration::from_millis(150)).await;
      update.replace_players(vec![presence(player)]).await;
    });
    let started = Instant::now();
    manager.ensure_solo_players_online(&[player]).await.unwrap();
    assert!(started.elapsed() >= Duration::from_millis(100));
  }

  #[tokio::test]
  async fn shared_solo_player_world_destroy_is_rejected() {
    let (_directory, manager) = solo_test_manager(SoloMode::Shared, SoloStartup::OnDemand).await;
    let player = Uuid::new_v4();
    manager.replace_players(vec![presence(player)]).await;
    manager
      .create_solo_session(SoloSessionCreateRequest {
        game_id: "puzzle".into(),
        owner: player,
        players: vec![player],
      })
      .await
      .unwrap();
    let error = manager
      .destroy_solo_for_player("puzzle", player)
      .await
      .unwrap_err();
    assert_eq!(
      error.to_string(),
      "invalid runtime state: shared solo games do not have a player world to destroy"
    );
  }

  #[tokio::test]
  async fn shared_solo_create_accepts_a_different_request() {
    let (_directory, manager) = solo_test_manager(SoloMode::Shared, SoloStartup::OnDemand).await;
    let first = Uuid::new_v4();
    let second = Uuid::new_v4();
    manager
      .replace_players(vec![presence(first), presence(second)])
      .await;
    let initial = manager
      .create_solo_session(SoloSessionCreateRequest {
        game_id: "puzzle".into(),
        owner: first,
        players: vec![first],
      })
      .await
      .unwrap();
    let updated = manager
      .create_solo_session(SoloSessionCreateRequest {
        game_id: "puzzle".into(),
        owner: second,
        players: vec![second],
      })
      .await
      .unwrap();

    assert_eq!(initial.session_id, updated.session_id);
    assert_eq!(updated.owner, second);
    assert_eq!(updated.players, vec![second]);
    assert_eq!(manager.solo_sessions().await.len(), 1);
  }

  #[cfg(unix)]
  #[tokio::test]
  async fn shared_solo_different_requests_join_the_same_open_instance() {
    let (_directory, manager) = solo_test_manager(SoloMode::Shared, SoloStartup::OnDemand).await;
    let first = Uuid::new_v4();
    let second = Uuid::new_v4();
    manager
      .replace_players(vec![presence(first), presence(second)])
      .await;

    let first_instance = manager
      .launch_solo("puzzle", first, vec![first])
      .await
      .unwrap();
    let second_instance = manager
      .launch_solo("puzzle", second, vec![second])
      .await
      .unwrap();

    assert_eq!(first_instance.instance_id, second_instance.instance_id);
    let instance = manager.instance("game").await.unwrap();
    assert_eq!(
      instance.pending_players.lock().await.clone(),
      HashSet::from([first.to_string(), second.to_string()])
    );
    assert_eq!(
      manager.solo_access().await.unwrap(),
      vec![SoloAccessEntry {
        server_id: "game".into(),
        instance_id: first_instance.instance_id,
        access: SoloAccessMode::Open,
        players: Vec::new()
      }]
    );
    manager.stop("game").await.unwrap();
  }

  #[cfg(unix)]
  #[tokio::test]
  async fn concurrent_shared_solo_requests_do_not_mix_transfer_players() {
    let (_directory, manager) = solo_test_manager(SoloMode::Shared, SoloStartup::OnDemand).await;
    let first = Uuid::new_v4();
    let second = Uuid::new_v4();
    manager
      .replace_players(vec![presence(first), presence(second)])
      .await;
    let first_manager = manager.clone();
    let second_manager = manager.clone();

    let (first_result, second_result) = tokio::join!(
      first_manager.launch_solo("puzzle", first, vec![first]),
      second_manager.launch_solo("puzzle", second, vec![second])
    );
    let first_instance = first_result.unwrap();
    let second_instance = second_result.unwrap();

    assert_eq!(first_instance.instance_id, second_instance.instance_id);
    let instance = manager.instance("game").await.unwrap();
    assert_eq!(
      instance.pending_players.lock().await.clone(),
      HashSet::from([first.to_string(), second.to_string()])
    );
    manager.stop("game").await.unwrap();
  }

  #[cfg(unix)]
  #[tokio::test]
  async fn player_world_access_keeps_roster_and_rejects_additional_player() {
    let (_directory, manager) =
      solo_test_manager(SoloMode::PlayerWorld, SoloStartup::OnDemand).await;
    let owner = Uuid::new_v4();
    let outsider = Uuid::new_v4();
    manager
      .replace_players(vec![presence(owner), presence(outsider)])
      .await;
    let instance = manager
      .launch_solo("puzzle", owner, vec![owner])
      .await
      .unwrap();

    assert_eq!(
      manager.solo_access().await.unwrap(),
      vec![SoloAccessEntry {
        server_id: instance.server_id.clone(),
        instance_id: instance.instance_id,
        access: SoloAccessMode::Roster,
        players: vec![owner]
      }]
    );
    let error = manager
      .queue_transfers(&instance.server_id, vec![outsider.to_string()])
      .await
      .unwrap_err();
    assert!(
      error
        .to_string()
        .contains("is not assigned to solo session")
    );
    manager.stop(&instance.server_id).await.unwrap();
  }

  #[cfg(unix)]
  #[tokio::test]
  async fn player_world_runtime_uses_dynamic_id_and_persists_world() {
    let (directory, manager) =
      solo_test_manager(SoloMode::PlayerWorld, SoloStartup::OnDemand).await;
    let player = Uuid::new_v4();
    manager.replace_players(vec![presence(player)]).await;
    let session = manager
      .create_solo_session(SoloSessionCreateRequest {
        game_id: "puzzle".into(),
        owner: player,
        players: vec![player],
      })
      .await
      .unwrap();
    let started = manager
      .start_solo_session(&session.session_id)
      .await
      .unwrap()
      .instance
      .unwrap();
    assert_eq!(started.server_id, session.backend_id);
    manager
      .ready(&started.server_id, started.instance_id)
      .await
      .unwrap();
    fs::write(started.work_dir.join("world/level.dat"), b"updated")
      .await
      .unwrap();
    fs::write(started.work_dir.join("world/session.lock"), b"lock")
      .await
      .unwrap();
    manager.stop(&started.server_id).await.unwrap();
    let persistent = directory
      .path()
      .join("worlds/puzzle")
      .join(player.to_string())
      .join("world");
    assert_eq!(
      fs::read(persistent.join("level.dat")).await.unwrap(),
      b"updated"
    );
    assert!(!persistent.join("session.lock").exists());
    let metadata = solo::read_metadata(persistent.parent().unwrap())
      .await
      .unwrap();
    assert_eq!(
      metadata.expires,
      metadata.updated.saturating_add(10 * 86_400)
    );

    let restarted = manager
      .start_solo_session(&session.session_id)
      .await
      .unwrap()
      .instance
      .unwrap();
    assert_ne!(restarted.port, started.port);
    assert_eq!(
      fs::read(restarted.work_dir.join("world/level.dat"))
        .await
        .unwrap(),
      b"updated"
    );
    manager.stop(&restarted.server_id).await.unwrap();
  }

  #[cfg(unix)]
  #[tokio::test]
  async fn expired_solo_cleanup_skips_running_worlds() {
    let (directory, manager) =
      solo_test_manager(SoloMode::PlayerWorld, SoloStartup::OnDemand).await;
    let player = Uuid::new_v4();
    manager.replace_players(vec![presence(player)]).await;
    let session = manager
      .create_solo_session(SoloSessionCreateRequest {
        game_id: "puzzle".into(),
        owner: player,
        players: vec![player],
      })
      .await
      .unwrap();
    let started = manager
      .start_solo_session(&session.session_id)
      .await
      .unwrap()
      .instance
      .unwrap();
    manager
      .solo_sessions
      .write()
      .await
      .get_mut(&session.session_id)
      .unwrap()
      .expires = Some(0);
    assert_eq!(manager.cleanup_expired_solo_worlds(1).await, 0);
    let root = directory
      .path()
      .join("worlds/puzzle")
      .join(player.to_string());
    assert!(root.exists());

    manager.stop(&started.server_id).await.unwrap();
    manager
      .solo_sessions
      .write()
      .await
      .get_mut(&session.session_id)
      .unwrap()
      .expires = Some(0);
    assert_eq!(manager.cleanup_expired_solo_worlds(1).await, 1);
    assert!(!root.exists());
  }

  #[cfg(unix)]
  #[tokio::test]
  async fn shared_always_access_stays_open_across_restart() {
    let (_directory, manager) = solo_test_manager(SoloMode::Shared, SoloStartup::Always).await;
    let player = Uuid::new_v4();
    manager.replace_players(vec![presence(player)]).await;
    let initial = manager.launch_static("game").await.unwrap();
    assert_eq!(
      manager.solo_access().await.unwrap(),
      vec![SoloAccessEntry {
        server_id: "game".into(),
        instance_id: initial.instance_id,
        access: SoloAccessMode::Open,
        players: Vec::new()
      }]
    );
    let session = manager
      .create_solo_session(SoloSessionCreateRequest {
        game_id: "puzzle".into(),
        owner: player,
        players: vec![player],
      })
      .await
      .unwrap();
    let restarted = manager.restart("game").await.unwrap();
    assert_ne!(restarted.port, initial.port);
    assert_eq!(
      manager.solo_access().await.unwrap(),
      vec![SoloAccessEntry {
        server_id: "game".into(),
        instance_id: restarted.instance_id,
        access: SoloAccessMode::Open,
        players: Vec::new()
      }]
    );
    assert!(
      manager
        .solo_sessions
        .read()
        .await
        .contains_key(&session.session_id)
    );
    manager.stop("game").await.unwrap();
    assert!(manager.solo_sessions.read().await.is_empty());
  }

  #[test]
  fn dynamic_port_allocation_reports_exhaustion() {
    let allocated = HashSet::from([50001, 50002]);
    assert_eq!(first_available_port(50001, 50002, &allocated), None);
  }

  #[tokio::test]
  async fn released_child_port_observes_reuse_cooldown() {
    let (_directory, manager) = solo_test_manager(SoloMode::Shared, SoloStartup::OnDemand).await;
    let start = manager.config.value.ports.start;
    let end = manager.config.value.ports.end;
    let released_at = Instant::now();
    manager.allocated_ports.lock().await.insert(start);

    release_instance_port(
      &manager.allocated_ports,
      &manager.port_cooldowns,
      start,
      start,
      end,
      released_at,
    )
    .await;

    assert!(!manager.allocated_ports.lock().await.contains(&start));
    assert_eq!(
      manager.port_cooldowns.lock().await.get(&start).copied(),
      Some(released_at + PORT_REUSE_COOLDOWN)
    );
    let next = manager.reserve_port("paper").await.unwrap();
    assert_eq!(next, start + 1);
    manager.release_port(next).await;

    manager
      .port_cooldowns
      .lock()
      .await
      .insert(start, Instant::now());
    let reused = manager.reserve_port("paper").await.unwrap();
    assert_eq!(reused, start);
    manager.release_port(reused).await;
  }

  #[tokio::test]
  async fn velocity_port_does_not_enter_reuse_cooldown() {
    let (_directory, manager) = solo_test_manager(SoloMode::Shared, SoloStartup::OnDemand).await;
    let velocity = manager.config.value.ports.velocity;
    manager.allocated_ports.lock().await.insert(velocity);

    release_instance_port(
      &manager.allocated_ports,
      &manager.port_cooldowns,
      velocity,
      manager.config.value.ports.start,
      manager.config.value.ports.end,
      Instant::now(),
    )
    .await;

    assert!(!manager.allocated_ports.lock().await.contains(&velocity));
    assert!(!manager.port_cooldowns.lock().await.contains_key(&velocity));
  }

  #[test]
  fn disconnected_player_transfer_failure_is_terminal() {
    assert!(terminal_transfer_failure(
      "player is not connected to Velocity"
    ));
    assert!(!terminal_transfer_failure("CONNECTION_IN_PROGRESS"));
  }

  #[test]
  fn transfer_activity_obeys_timeout_and_attempt_limit() {
    let transfer = TransferRecord {
      transfer_id: Uuid::new_v4(),
      server_id: "game".into(),
      player: Uuid::new_v4().to_string(),
      address: "127.0.0.1:50001".into(),
      next_attempt: 100,
      expires_at: 145,
      attempts: 1,
    };
    assert!(transfer_active(&transfer, 100, 2));
    assert!(!transfer_active(&transfer, 145, 2));

    let exhausted = TransferRecord {
      attempts: 2,
      ..transfer
    };
    assert!(!transfer_active(&exhausted, 100, 2));
  }

  #[test]
  fn daily_restart_is_immediate_when_empty_at_schedule() {
    let schedule = daily_restart_schedule();
    let mut tracker = DailyRestartTracker::default();
    assert_eq!(
      tracker.evaluate(
        &schedule,
        restart_clock_value(1, 4, 0),
        restart_clock_value(0, 23, 0),
        100,
        0
      ),
      DailyRestartAction::Restart
    );
    assert_eq!(
      tracker.evaluate(
        &schedule,
        restart_clock_value(1, 4, 1),
        restart_clock_value(0, 23, 0),
        160,
        0
      ),
      DailyRestartAction::None
    );
    assert_eq!(
      tracker.evaluate(
        &schedule,
        restart_clock_value(2, 4, 0),
        restart_clock_value(0, 23, 0),
        86_500,
        0
      ),
      DailyRestartAction::Restart
    );
  }

  #[test]
  fn daily_restart_waits_for_a_continuous_empty_grace_period() {
    let schedule = daily_restart_schedule();
    let mut tracker = DailyRestartTracker::default();
    let started = restart_clock_value(0, 23, 0);
    assert_eq!(
      tracker.evaluate(&schedule, restart_clock_value(1, 4, 0), started, 100, 2),
      DailyRestartAction::WaitingForEmpty
    );
    assert_eq!(
      tracker.evaluate(&schedule, restart_clock_value(1, 4, 1), started, 200, 0),
      DailyRestartAction::EmptyGraceStarted
    );
    assert_eq!(
      tracker.evaluate(&schedule, restart_clock_value(1, 4, 2), started, 350, 1),
      DailyRestartAction::WaitingForEmpty
    );
    assert_eq!(
      tracker.evaluate(&schedule, restart_clock_value(1, 4, 3), started, 400, 0),
      DailyRestartAction::EmptyGraceStarted
    );
    assert_eq!(
      tracker.evaluate(&schedule, restart_clock_value(1, 4, 7), started, 699, 0),
      DailyRestartAction::None
    );
    assert_eq!(
      tracker.evaluate(&schedule, restart_clock_value(1, 4, 8), started, 700, 0),
      DailyRestartAction::Restart
    );
  }

  #[test]
  fn daily_restart_skips_an_instance_started_after_schedule() {
    let schedule = daily_restart_schedule();
    let mut tracker = DailyRestartTracker::default();
    assert_eq!(
      tracker.evaluate(
        &schedule,
        restart_clock_value(1, 6, 0),
        restart_clock_value(1, 5, 0),
        100,
        0
      ),
      DailyRestartAction::None
    );
    assert_eq!(
      tracker.evaluate(
        &schedule,
        restart_clock_value(1, 7, 0),
        restart_clock_value(1, 5, 0),
        200,
        0
      ),
      DailyRestartAction::None
    );
  }

  #[test]
  fn solo_cleanup_runs_once_after_local_four_am() {
    assert!(!solo_cleanup_due(restart_clock_value(1, 3, 59), None));
    assert!(solo_cleanup_due(restart_clock_value(1, 4, 0), None));
    assert!(!solo_cleanup_due(restart_clock_value(1, 20, 0), Some(1)));
    assert!(solo_cleanup_due(restart_clock_value(2, 20, 0), Some(1)));
  }

  #[test]
  fn shared_on_demand_daily_maintenance_stops_instead_of_relaunching() {
    let mut definition = prepared_instance(
      PathBuf::from("runtime"),
      PathBuf::from("game.log"),
      PathBuf::from("java"),
    )
    .definition;
    definition.gamevoting = Some(
      serde_json::from_value(serde_json::json!({
        "id": "puzzle",
        "name": "Puzzle",
        "material": "COMPASS",
        "solo": true,
        "solo_mode": "shared",
        "solo_startup": "on_demand"
      }))
      .unwrap(),
    );
    assert!(stop_for_daily_maintenance(&definition));
    definition.gamevoting.as_mut().unwrap().solo_startup = SoloStartup::Always;
    assert!(!stop_for_daily_maintenance(&definition));
  }

  #[test]
  fn daily_restart_waiting_state_survives_midnight() {
    let schedule = daily_restart_schedule();
    let mut tracker = DailyRestartTracker::default();
    let started = restart_clock_value(0, 23, 0);
    assert_eq!(
      tracker.evaluate(&schedule, restart_clock_value(1, 4, 0), started, 100, 1),
      DailyRestartAction::WaitingForEmpty
    );
    assert_eq!(
      tracker.evaluate(&schedule, restart_clock_value(2, 1, 0), started, 200, 0),
      DailyRestartAction::EmptyGraceStarted
    );
    assert_eq!(
      tracker.evaluate(&schedule, restart_clock_value(2, 1, 5), started, 500, 0),
      DailyRestartAction::Restart
    );
  }

  fn daily_restart_schedule() -> DailyRestartDefinition {
    DailyRestartDefinition {
      hour: 4,
      minute: 0,
      empty_grace_seconds: 300,
    }
  }

  fn restart_clock_value(day: i32, hour: u32, minute: u32) -> RestartClock {
    RestartClock {
      day,
      second_of_day: hour * 3600 + minute * 60,
    }
  }

  #[tokio::test]
  async fn operating_system_port_conflicts_are_detected() {
    let Ok(listener) = TcpListener::bind(("0.0.0.0", 0)).await else {
      return;
    };
    let port = listener.local_addr().unwrap().port();
    assert!(!port_available(port).await);
    drop(listener);
    assert!(port_available(port).await);
  }

  #[tokio::test]
  async fn log_open_failure_removes_prepared_runtime_directory() {
    let directory = tempfile::tempdir().unwrap();
    let work_dir = directory.path().join("runtime/game-instance");
    let invalid_log = directory.path().join("logs/game-instance.log");
    fs::create_dir_all(&work_dir).await.unwrap();
    fs::create_dir_all(&invalid_log).await.unwrap();
    let prepared = prepared_instance(
      work_dir.clone(),
      invalid_log.clone(),
      directory.path().join("java/bin/java"),
    );

    let result = open_instance_log(&prepared).await;

    assert!(result.is_err());
    assert!(!work_dir.exists());
    assert!(invalid_log.exists());
  }

  #[tokio::test]
  async fn spawn_failure_removes_prepared_runtime_directory() {
    let directory = tempfile::tempdir().unwrap();
    let work_dir = directory.path().join("runtime/game-instance");
    let missing_java = directory.path().join("java/bin/java");
    fs::create_dir_all(&work_dir).await.unwrap();
    let prepared = prepared_instance(
      work_dir.clone(),
      directory.path().join("logs/game-instance.log"),
      missing_java.clone(),
    );
    let mut command = Command::new(missing_java);

    let result = spawn_instance_process(&mut command, &prepared).await;

    assert!(matches!(result, Err(ManagerError::Spawn { .. })));
    assert!(!work_dir.exists());
  }

  #[cfg(unix)]
  #[tokio::test]
  async fn stop_waits_for_runtime_removal_and_preserves_dependencies_and_logs() {
    use std::os::unix::fs::PermissionsExt;

    let directory = tempfile::tempdir().unwrap();
    let root = directory.path();
    let port = 50001;
    fs::create_dir_all(root.join("cores/paper/1.21.11"))
      .await
      .unwrap();
    fs::create_dir_all(root.join("java21/bin")).await.unwrap();
    fs::create_dir_all(root.join("java25/bin")).await.unwrap();
    fs::create_dir_all(root.join("servers")).await.unwrap();
    fs::write(root.join("cores/paper/1.21.11/server.jar"), b"server")
      .await
      .unwrap();
    let java = root.join("java21/bin/java");
    fs::write(
      &java,
      b"#!/bin/sh\nmkdir -p libraries\nprintf runtime > libraries/generated.jar\nprintf 'started\\n'\nwhile IFS= read -r line; do\n  if [ \"$line\" = \"stop\" ]; then\n    exit 0\n  fi\ndone\n",
    )
    .await
    .unwrap();
    let mut permissions = fs::metadata(&java).await.unwrap().permissions();
    permissions.set_mode(0o755);
    fs::set_permissions(&java, permissions).await.unwrap();
    fs::write(
      root.join("config.json"),
      serde_json::to_vec(&serde_json::json!({
        "api": {
          "port": 25566,
          "token": "01234567890123456789012345678901"
        },
        "bridge": {
          "token": "11234567890123456789012345678901"
        },
        "postgresql": {
          "home": "postgres",
          "host": "127.0.0.1",
          "port": 5432,
          "username": "minigames",
          "password": "test-password"
        },
        "ports": {
          "velocity": 1,
          "start": port,
          "end": port
        },
        "java": {
          "21_home": "java21",
          "25_home": "java25"
        },
        "paths": {
          "cores": "cores",
          "plugins": "plugins",
          "mods": "mods",
          "dependencies": "dependencies",
          "servers": "servers",
          "worlds": "worlds",
          "runtime": ".runtime",
          "logs": "logs",
          "pid_file": "scheduler.pid",
          "scheduler_log": "scheduler.log"
        }
      }))
      .unwrap(),
    )
    .await
    .unwrap();
    fs::write(
      root.join("servers/game.json"),
      serde_json::to_vec(&serde_json::json!({
        "core": {
          "kind": "paper",
          "minecraft_version": "1.21.11",
          "java": "21",
          "jar": "server.jar"
        },
        "memory": {
          "initial_mib": 128,
          "maximum_mib": 256
        }
      }))
      .unwrap(),
    )
    .await
    .unwrap();
    let config = Arc::new(LoadedConfig::load(root.join("config.json")).await.unwrap());
    let manager = InstanceManager::new(config.clone());
    let prepared = layout::prepare(&config, "game", port).await.unwrap();
    let log_buffer = open_instance_log(&prepared).await.unwrap();
    let mut command = create_process_command(&config, &prepared);
    let mut child = spawn_instance_process(&mut command, &prepared)
      .await
      .unwrap();
    let pid = child.id().unwrap();
    let stdin = child.stdin.take().unwrap();
    let stdout = child.stdout.take().unwrap();
    let stderr = child.stderr.take().unwrap();
    let info = Arc::new(RwLock::new(InstanceInfo {
      server_id: "game".into(),
      instance_id: prepared.instance_id,
      state: InstanceState::Starting,
      pid,
      port,
      work_dir: prepared.work_dir.clone(),
      log_file: prepared.log_file.clone(),
      started_at: now(),
      last_heartbeat: None,
      exit_code: None,
    }));
    let (control, control_receiver) = mpsc::channel(4);
    let (completion_sender, completion) = watch::channel(None);
    let solo_session_id = Arc::new(Mutex::new(None));
    let preserve_solo_session = Arc::new(AtomicBool::new(false));
    manager.allocated_ports.lock().await.insert(port);
    manager.instances.write().await.insert(
      "game".into(),
      Arc::new(InstanceControl {
        info: info.clone(),
        control,
        logs: log_buffer.clone(),
        completion,
        pending_players: Mutex::new(HashSet::new()),
        startup_timeout_seconds: prepared.definition.startup_timeout_seconds,
        autostart: false,
        restart_delay_seconds: prepared.definition.restart_delay_seconds,
        daily_restart: prepared.definition.daily_restart.clone(),
        daily_restart_tracker: Mutex::new(DailyRestartTracker::default()),
        idle_timeout_seconds: prepared.definition.idle_timeout_seconds,
        launch_spec: LaunchSpec::static_server("game"),
        solo_mode: None,
        solo_session_id: solo_session_id.clone(),
        preserve_solo_session: preserve_solo_session.clone(),
      }),
    );
    let stdout_task = tokio::spawn(drain_stream(stdout, "stdout", log_buffer.clone()));
    let stderr_task = tokio::spawn(drain_stream(stderr, "stderr", log_buffer.clone()));
    tokio::spawn(supervise(
      child,
      stdin,
      control_receiver,
      SupervisionContext {
        info: info.clone(),
        logs: log_buffer,
        stdout_task,
        stderr_task,
        allocated_ports: manager.allocated_ports.clone(),
        port_cooldowns: manager.port_cooldowns.clone(),
        config: config.clone(),
        runtime_id: "game".into(),
        dependency_id: "game".into(),
        work_dir: prepared.work_dir.clone(),
        dependency_snapshot: prepared.dependency_snapshot,
        dependency_cache_lock: manager.dependency_cache_lock.clone(),
        transfers: manager.transfers.clone(),
        solo_sessions: manager.solo_sessions.clone(),
        solo_session_id,
        preserve_solo_session,
        solo_persistence: None,
        completion: completion_sender,
        port,
      },
    ));
    let instance = info.read().await.clone();
    let generated = instance.work_dir.join("libraries/generated.jar");
    timeout(Duration::from_secs(5), async {
      while !generated.exists() {
        sleep(Duration::from_millis(10)).await;
      }
    })
    .await
    .unwrap();

    let stopped = timeout(Duration::from_secs(5), manager.stop("game"))
      .await
      .unwrap()
      .unwrap();

    assert_eq!(stopped.state, InstanceState::Exited);
    assert!(!instance.work_dir.exists());
    assert!(instance.log_file.exists());
    assert!(
      !runtime::staging_root(&config.resolve(&config.value.paths.runtime))
        .unwrap()
        .exists()
    );
    let materialized = root.join("materialized");
    fs::create_dir_all(&materialized).await.unwrap();
    assert_eq!(
      dependency_cache::materialize(&config, "game", &materialized)
        .await
        .unwrap(),
      1
    );
    assert_eq!(
      fs::read(materialized.join("libraries/generated.jar"))
        .await
        .unwrap(),
      b"runtime"
    );
    let log = fs::read_to_string(instance.log_file).await.unwrap();
    assert!(log.contains("removed runtime directory"));
  }
}
