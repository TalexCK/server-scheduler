use std::sync::Arc;

use axum::{
  Json, Router,
  extract::{Form, Path, Query, Request, State},
  http::{HeaderMap, StatusCode, header::AUTHORIZATION},
  middleware::{self, Next},
  response::{IntoResponse, Response},
  routing::{get, post},
};
use base64::{Engine as _, engine::general_purpose::STANDARD_NO_PAD};
use serde::{Deserialize, Serialize};
use subtle::ConstantTimeEq;
use tokio::sync::broadcast;

use crate::{
  config::LoadedConfig,
  manager::{InstanceManager, ManagerError},
  protocol::{
    CommandRequest, CommandResponse, HeartbeatForm, IdleForm, LaunchRequest, LogResponse,
    PlayerListForm, PlayerPresence, PlayersForm, ReadyForm, SoloDestroyForm, SoloLaunchForm,
    SoloSessionCreateRequest, SoloSessionInfo, TransferResultForm,
  },
};

#[derive(Clone)]
pub struct ApiState {
  pub config: Arc<LoadedConfig>,
  pub manager: InstanceManager,
  pub shutdown: broadcast::Sender<()>,
}

#[derive(Clone)]
struct AuthState {
  token: Arc<str>,
}

pub fn router(state: ApiState) -> Router {
  let admin = Router::new()
    .route("/servers", get(list_servers))
    .route("/players", get(list_players))
    .route("/servers/{server_id}/launch", post(launch_server))
    .route("/servers/{server_id}/terminate", post(terminate_server))
    .route("/servers/{server_id}/restart", post(restart_server))
    .route("/servers/{server_id}/logs", get(server_logs))
    .route("/servers/{server_id}/command", post(server_command))
    .route(
      "/solo/sessions",
      get(list_solo_sessions).post(create_solo_session),
    )
    .route(
      "/solo/sessions/{session_id}/start",
      post(start_solo_session),
    )
    .route(
      "/solo/sessions/{session_id}/destroy",
      post(destroy_solo_session),
    )
    .route("/resources/update", post(resource_update))
    .route("/resources/update", get(resource_update_status))
    .route("/system/shutdown", post(shutdown))
    .route_layer(middleware::from_fn_with_state(
      AuthState {
        token: Arc::from(state.config.value.api.token.clone()),
      },
      require_token,
    ));
  let bridge = Router::new()
    .route("/ready", post(bridge_ready))
    .route("/heartbeat", post(bridge_heartbeat))
    .route("/idle", post(bridge_idle))
    .route(
      "/players",
      get(bridge_list_players).post(bridge_update_players),
    )
    .route(
      "/resources/update",
      post(resource_update_bridge).get(resource_update_status_bridge),
    )
    .route("/revision", get(catalog_revision))
    .route("/definitions", get(bridge_definitions))
    .route("/games", get(bridge_games))
    .route("/games/details", get(bridge_game_details))
    .route(
      "/external-servers/{server_id}/join",
      post(bridge_join_external),
    )
    .route("/solo/access", get(bridge_solo_access))
    .route("/solo/{game_id}/launch", post(bridge_solo_launch))
    .route("/solo/{game_id}/destroy", post(bridge_solo_destroy))
    .route(
      "/solo/sessions",
      get(list_solo_sessions).post(create_solo_session),
    )
    .route("/solo/session-index", get(bridge_list_solo_sessions))
    .route(
      "/solo/sessions/{session_id}/start",
      post(bridge_start_solo_session),
    )
    .route(
      "/solo/sessions/{session_id}/destroy",
      post(destroy_solo_session),
    )
    .route("/servers", get(bridge_servers))
    .route("/servers/{server_id}/launch", post(bridge_launch))
    .route(
      "/servers/{server_id}/transfers",
      post(bridge_queue_transfers),
    )
    .route("/servers/{server_id}/terminate", post(bridge_terminate))
    .route("/servers/{server_id}/restart", post(bridge_restart))
    .route("/servers/{server_id}/logs", get(bridge_logs))
    .route("/servers/{server_id}/command", post(bridge_command))
    .route("/transfers", get(pending_transfers))
    .route("/transfers/result", post(transfer_result))
    .route_layer(middleware::from_fn_with_state(
      AuthState {
        token: Arc::from(state.config.value.bridge.token.clone()),
      },
      require_token,
    ));

  Router::new()
    .route("/health", get(health))
    .nest("/admin/v1", admin)
    .nest("/bridge/v1", bridge)
    .with_state(state)
}

async fn resource_update(State(state): State<ApiState>) -> Json<crate::resources::UpdateStatus> {
  Json(state.manager.resources.start().await)
}
async fn resource_update_status(
  State(state): State<ApiState>,
) -> Json<Option<crate::resources::UpdateStatus>> {
  Json(state.manager.resources.status().await)
}
fn format_update(status: &crate::resources::UpdateStatus) -> String {
  format!(
    "Update {}: {} ({}/{})\n{}",
    status.id, status.phase, status.completed, status.total, status.message
  )
}
async fn resource_update_bridge(State(state): State<ApiState>) -> String {
  format_update(&state.manager.resources.start().await)
}
async fn resource_update_status_bridge(State(state): State<ApiState>) -> String {
  state
    .manager
    .resources
    .status()
    .await
    .as_ref()
    .map(format_update)
    .unwrap_or_else(|| "No resource update has been started.".into())
}
async fn catalog_revision(State(state): State<ApiState>) -> String {
  let pointer = state
    .config
    .resolve(&state.config.value.paths.resources)
    .join("current.json");
  tokio::fs::read(pointer)
    .await
    .ok()
    .and_then(|bytes| serde_json::from_slice::<crate::resources::ActiveGeneration>(&bytes).ok())
    .map(|p| p.revision)
    .unwrap_or_else(|| "legacy".into())
}

async fn require_token(
  State(auth): State<AuthState>,
  request: Request,
  next: Next,
) -> Result<Response, ApiError> {
  if !valid_bearer(request.headers(), &auth.token) {
    return Err(ApiError::new(
      StatusCode::UNAUTHORIZED,
      "missing or invalid bearer token",
    ));
  }
  Ok(next.run(request).await)
}

fn valid_bearer(headers: &HeaderMap, expected: &str) -> bool {
  headers
    .get(AUTHORIZATION)
    .and_then(|value| value.to_str().ok())
    .and_then(|value| value.strip_prefix("Bearer "))
    .is_some_and(|value| {
      value.len() == expected.len() && bool::from(value.as_bytes().ct_eq(expected.as_bytes()))
    })
}

#[derive(Serialize)]
struct HealthResponse {
  status: &'static str,
  binary_sha256: &'static str,
  deployment_layout: &'static str,
}

async fn health() -> Json<HealthResponse> {
  static BINARY: std::sync::OnceLock<String> = std::sync::OnceLock::new();
  let hash = BINARY.get_or_init(|| {
    use sha2::{Digest, Sha256};
    let path = if cfg!(target_os = "linux") {
      std::path::PathBuf::from("/proc/self/exe")
    } else {
      std::env::current_exe().unwrap_or_default()
    };
    std::fs::read(path)
      .map(|b| format!("{:x}", Sha256::digest(b)))
      .unwrap_or_default()
  });
  Json(HealthResponse {
    status: "ok",
    binary_sha256: hash,
    deployment_layout: "per-server-cache-v1",
  })
}

async fn list_servers(State(state): State<ApiState>) -> Json<Vec<crate::protocol::InstanceInfo>> {
  Json(state.manager.list().await)
}

async fn list_players(State(state): State<ApiState>) -> Json<Vec<PlayerPresence>> {
  Json(state.manager.players().await)
}

async fn launch_server(
  State(state): State<ApiState>,
  Path(server_id): Path<String>,
  Json(request): Json<LaunchRequest>,
) -> Result<(StatusCode, Json<crate::protocol::InstanceInfo>), ApiError> {
  let info = state.manager.launch(&server_id, request.players).await?;
  Ok((StatusCode::ACCEPTED, Json(info)))
}

async fn terminate_server(
  State(state): State<ApiState>,
  Path(server_id): Path<String>,
) -> Result<Json<crate::protocol::InstanceInfo>, ApiError> {
  Ok(Json(state.manager.stop(&server_id).await?))
}

async fn restart_server(
  State(state): State<ApiState>,
  Path(server_id): Path<String>,
) -> Result<(StatusCode, Json<crate::protocol::InstanceInfo>), ApiError> {
  let instance = state.manager.restart(&server_id).await?;
  Ok((StatusCode::ACCEPTED, Json(instance)))
}

#[derive(Deserialize)]
struct LogQuery {
  #[serde(default = "default_log_count")]
  lines: usize,
}

async fn server_logs(
  State(state): State<ApiState>,
  Path(server_id): Path<String>,
  Query(query): Query<LogQuery>,
) -> Result<Json<LogResponse>, ApiError> {
  Ok(Json(LogResponse {
    lines: state.manager.logs(&server_id, query.lines).await?,
  }))
}

async fn server_command(
  State(state): State<ApiState>,
  Path(server_id): Path<String>,
  Json(request): Json<CommandRequest>,
) -> Result<Json<CommandResponse>, ApiError> {
  Ok(Json(CommandResponse {
    output: state.manager.command(&server_id, request.command).await?,
  }))
}

async fn list_solo_sessions(State(state): State<ApiState>) -> Json<Vec<SoloSessionInfo>> {
  Json(state.manager.solo_sessions().await)
}

async fn create_solo_session(
  State(state): State<ApiState>,
  Json(request): Json<SoloSessionCreateRequest>,
) -> Result<(StatusCode, Json<SoloSessionInfo>), ApiError> {
  let session = state.manager.create_solo_session(request).await?;
  Ok((StatusCode::CREATED, Json(session)))
}

async fn start_solo_session(
  State(state): State<ApiState>,
  Path(session_id): Path<String>,
) -> Result<(StatusCode, Json<SoloSessionInfo>), ApiError> {
  let session = state.manager.start_solo_session(&session_id).await?;
  Ok((StatusCode::ACCEPTED, Json(session)))
}

async fn destroy_solo_session(
  State(state): State<ApiState>,
  Path(session_id): Path<String>,
) -> Result<Json<Option<crate::protocol::InstanceInfo>>, ApiError> {
  let manager = state.manager.clone();
  let result = tokio::spawn(async move { manager.destroy_solo_session(&session_id).await })
    .await
    .map_err(|error| {
      ApiError::new(
        StatusCode::INTERNAL_SERVER_ERROR,
        format!("solo destruction task failed: {error}"),
      )
    })??;
  Ok(Json(result))
}

async fn shutdown(State(state): State<ApiState>) -> StatusCode {
  let _ = state.shutdown.send(());
  StatusCode::ACCEPTED
}

async fn bridge_ready(
  State(state): State<ApiState>,
  Form(form): Form<ReadyForm>,
) -> Result<StatusCode, ApiError> {
  state
    .manager
    .ready(&form.server_id, form.instance_id)
    .await?;
  Ok(StatusCode::NO_CONTENT)
}

async fn bridge_heartbeat(
  State(state): State<ApiState>,
  Form(form): Form<HeartbeatForm>,
) -> Result<StatusCode, ApiError> {
  state
    .manager
    .heartbeat(&form.server_id, form.instance_id)
    .await?;
  Ok(StatusCode::NO_CONTENT)
}

async fn bridge_idle(
  State(state): State<ApiState>,
  Form(form): Form<IdleForm>,
) -> Result<StatusCode, ApiError> {
  state
    .manager
    .idle(&form.server_id, form.instance_id)
    .await?;
  Ok(StatusCode::ACCEPTED)
}

async fn bridge_update_players(
  State(state): State<ApiState>,
  Form(form): Form<PlayersForm>,
) -> Result<StatusCode, ApiError> {
  state
    .manager
    .replace_players(parse_players(&form.players)?)
    .await;
  Ok(StatusCode::NO_CONTENT)
}

async fn bridge_list_players(State(state): State<ApiState>) -> String {
  state
    .manager
    .players()
    .await
    .into_iter()
    .map(|player| {
      format!(
        "{}\t{}\t{}\t{}\n",
        player.uuid,
        player.username,
        player.ping_ms,
        player.server_id.unwrap_or_default()
      )
    })
    .collect()
}

async fn bridge_definitions(State(state): State<ApiState>) -> Result<String, ApiError> {
  let ids = state
    .config
    .server_ids()
    .await
    .map_err(crate::layout::LayoutError::from)
    .map_err(ManagerError::from)?;
  Ok(ids.into_iter().map(|id| format!("{id}\n")).collect())
}

#[derive(Default, Deserialize)]
struct GameCatalogQuery {
  #[serde(default)]
  include_voting: bool,
  #[serde(default)]
  include_external: bool,
}

async fn bridge_games(
  State(state): State<ApiState>,
  Query(query): Query<GameCatalogQuery>,
) -> Result<String, ApiError> {
  let games = state
    .config
    .game_definitions()
    .await
    .map_err(crate::layout::LayoutError::from)
    .map_err(ManagerError::from)?;
  let mut entries = games
    .into_iter()
    .map(|(server_id, game)| {
      let mut row = format_game_catalog_entry(
        &server_id,
        &game,
        query.include_voting || query.include_external,
      );
      if query.include_external {
        row.pop();
        row.push_str("\t\t\n");
      }
      (game.order, game.id.to_lowercase(), row)
    })
    .collect::<Vec<_>>();
  if query.include_external {
    let external = state
      .config
      .external_servers()
      .await
      .map_err(crate::layout::LayoutError::from)
      .map_err(ManagerError::from)?;
    for (server_id, destination) in external {
      let game = &destination.gamevoting;
      let mut row = format_game_catalog_entry(&server_id, game, true);
      row.pop();
      row.push_str(&format!(
        "\t{}\t{}\n",
        encode_game_field(&destination.address.to_string()),
        encode_game_field(&destination.minecraft_version)
      ));
      entries.push((game.order, game.id.to_lowercase(), row));
    }
  }
  entries.sort_by(|left, right| (&left.0, &left.1).cmp(&(&right.0, &right.1)));
  Ok(entries.into_iter().map(|(_, _, row)| row).collect())
}

async fn bridge_game_details(State(state): State<ApiState>) -> Result<String, ApiError> {
  let games = state
    .config
    .game_definitions()
    .await
    .map_err(crate::layout::LayoutError::from)
    .map_err(ManagerError::from)?;
  Ok(
    games
      .into_iter()
      .map(|(server_id, game)| format_game_details_entry(&server_id, &game))
      .collect(),
  )
}

async fn bridge_join_external(
  State(state): State<ApiState>,
  Path(server_id): Path<String>,
  Form(form): Form<PlayerListForm>,
) -> Result<StatusCode, ApiError> {
  state
    .manager
    .join_external(&server_id, parse_uuid_list(&form.players)?)
    .await?;
  Ok(StatusCode::NO_CONTENT)
}

async fn bridge_solo_access(State(state): State<ApiState>) -> Result<String, ApiError> {
  Ok(
    state
      .manager
      .solo_access()
      .await?
      .into_iter()
      .map(|entry| {
        format_solo_access(
          &entry.server_id,
          entry.instance_id,
          entry.access.as_str(),
          &entry.players,
        )
      })
      .collect(),
  )
}

async fn bridge_list_solo_sessions(State(state): State<ApiState>) -> String {
  state
    .manager
    .solo_sessions()
    .await
    .into_iter()
    .map(|session| {
      format_solo_session(
        &session.game_id,
        &session.session_id,
        session.owner,
        &session.players,
      )
    })
    .collect()
}

async fn bridge_start_solo_session(
  State(state): State<ApiState>,
  Path(session_id): Path<String>,
  Form(form): Form<PlayersForm>,
) -> Result<String, ApiError> {
  let session = state
    .manager
    .start_solo_session_for_players(&session_id, parse_uuid_list(&form.players)?)
    .await?;
  let instance = session.instance.ok_or_else(|| {
    ApiError::new(
      StatusCode::INTERNAL_SERVER_ERROR,
      "solo session started without an instance",
    )
  })?;
  Ok(format_bridge_instance(&instance))
}

fn format_solo_session(
  game_id: &str,
  session_id: &str,
  owner: uuid::Uuid,
  players: &[uuid::Uuid],
) -> String {
  format!(
    "{}\t{}\t{}\t{}\n",
    encode_game_field(game_id),
    encode_game_field(session_id),
    owner,
    players
      .iter()
      .map(uuid::Uuid::to_string)
      .collect::<Vec<_>>()
      .join(",")
  )
}

fn format_solo_access(
  server_id: &str,
  instance_id: uuid::Uuid,
  access: &str,
  players: &[uuid::Uuid],
) -> String {
  format!(
    "{}\t{}\t{}\t{}\n",
    encode_game_field(server_id),
    instance_id,
    access,
    players
      .iter()
      .map(uuid::Uuid::to_string)
      .collect::<Vec<_>>()
      .join(",")
  )
}

async fn bridge_solo_launch(
  State(state): State<ApiState>,
  Path(game_id): Path<String>,
  Form(form): Form<SoloLaunchForm>,
) -> Result<String, ApiError> {
  let instance = state
    .manager
    .launch_solo(&game_id, form.owner, parse_uuid_list(&form.players)?)
    .await?;
  Ok(format_bridge_instance(&instance))
}

async fn bridge_solo_destroy(
  State(state): State<ApiState>,
  Path(game_id): Path<String>,
  Form(form): Form<SoloDestroyForm>,
) -> Result<StatusCode, ApiError> {
  let manager = state.manager.clone();
  tokio::spawn(async move { manager.destroy_solo_for_player(&game_id, form.player).await })
    .await
    .map_err(|error| {
      ApiError::new(
        StatusCode::INTERNAL_SERVER_ERROR,
        format!("solo destruction task failed: {error}"),
      )
    })??;
  Ok(StatusCode::NO_CONTENT)
}

fn format_game_catalog_entry(
  server_id: &str,
  game: &crate::config::GameVotingDefinition,
  include_voting: bool,
) -> String {
  let mut entry = format!(
    "{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\n",
    game.order,
    encode_game_field(&game.id),
    encode_game_field(server_id),
    encode_game_field(&game.name),
    encode_game_field(&game.description.join("\u{1f}")),
    encode_game_field(&game.material),
    game.custom_model_data,
    encode_game_field(game.version.as_deref().unwrap_or_default()),
    encode_game_field(game.min_version.as_deref().unwrap_or_default()),
    encode_game_field(game.max_version.as_deref().unwrap_or_default()),
    game.min_players,
    game.max_players,
    game.solo,
    encode_game_field(game.solo_mode.as_str()),
    encode_game_field(game.solo_startup.as_str()),
    game.solo_max_players,
    game.solo_retention_days
  );
  if include_voting {
    entry.pop();
    entry.push_str(&format!("\t{}\n", game.voting_enabled()));
  }
  entry
}

fn format_game_details_entry(
  server_id: &str,
  game: &crate::config::GameVotingDefinition,
) -> String {
  format!(
    "{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\n",
    game.order,
    encode_game_field(&game.id),
    encode_game_field(server_id),
    encode_game_field(&game.name),
    encode_game_field(&game.description.join("\u{1f}")),
    encode_game_field(&game.material),
    game.custom_model_data,
    encode_game_field(game.version.as_deref().unwrap_or_default()),
    encode_game_field(game.min_version.as_deref().unwrap_or_default()),
    encode_game_field(game.max_version.as_deref().unwrap_or_default()),
    game.min_players,
    game.max_players,
    game.solo,
    encode_game_field(game.solo_mode.as_str()),
    encode_game_field(game.solo_startup.as_str()),
    game.solo_max_players,
    game.solo_retention_days,
    encode_game_field(&game.rules.join("\u{1f}")),
    encode_game_field(&game.aliases.join("\u{1f}"))
  )
}

fn encode_game_field(value: &str) -> String {
  STANDARD_NO_PAD.encode(value.as_bytes())
}

async fn bridge_servers(State(state): State<ApiState>) -> String {
  state
    .manager
    .list()
    .await
    .into_iter()
    .map(|instance| format_bridge_instance(&instance))
    .collect()
}

async fn bridge_launch(
  State(state): State<ApiState>,
  Path(server_id): Path<String>,
  Form(form): Form<PlayerListForm>,
) -> Result<String, ApiError> {
  let instance = state
    .manager
    .ensure_started(&server_id, parse_player_list(&form.players))
    .await?;
  Ok(format_bridge_instance(&instance))
}

async fn bridge_queue_transfers(
  State(state): State<ApiState>,
  Path(server_id): Path<String>,
  Form(form): Form<PlayerListForm>,
) -> Result<StatusCode, ApiError> {
  state
    .manager
    .queue_transfers(&server_id, parse_player_list(&form.players))
    .await?;
  Ok(StatusCode::NO_CONTENT)
}

async fn bridge_terminate(
  State(state): State<ApiState>,
  Path(server_id): Path<String>,
) -> Result<String, ApiError> {
  let instance = state.manager.stop(&server_id).await?;
  Ok(format_bridge_instance(&instance))
}

async fn bridge_restart(
  State(state): State<ApiState>,
  Path(server_id): Path<String>,
) -> Result<String, ApiError> {
  let instance = state.manager.restart(&server_id).await?;
  Ok(format_bridge_instance(&instance))
}

async fn bridge_logs(
  State(state): State<ApiState>,
  Path(server_id): Path<String>,
  Query(query): Query<LogQuery>,
) -> Result<String, ApiError> {
  Ok(join_lines(
    state.manager.logs(&server_id, query.lines).await?,
  ))
}

async fn bridge_command(
  State(state): State<ApiState>,
  Path(server_id): Path<String>,
  Form(request): Form<CommandRequest>,
) -> Result<String, ApiError> {
  Ok(join_lines(
    state.manager.command(&server_id, request.command).await?,
  ))
}

fn join_lines(lines: Vec<String>) -> String {
  if lines.is_empty() {
    String::new()
  } else {
    format!("{}\n", lines.join("\n"))
  }
}

fn format_bridge_instance(instance: &crate::protocol::InstanceInfo) -> String {
  format!(
    "{}\t{}\t{:?}\t{}\t{}\n",
    instance.server_id, instance.instance_id, instance.state, instance.pid, instance.port
  )
}

fn parse_player_list(value: &str) -> Vec<String> {
  value
    .split([',', '\n'])
    .map(str::trim)
    .filter(|value| !value.is_empty())
    .map(str::to_string)
    .collect()
}

fn parse_uuid_list(value: &str) -> Result<Vec<uuid::Uuid>, ApiError> {
  value
    .split([',', '\n'])
    .map(str::trim)
    .filter(|value| !value.is_empty())
    .map(|value| {
      value.parse().map_err(|_| {
        ApiError::new(
          StatusCode::BAD_REQUEST,
          format!("invalid player UUID: {value}"),
        )
      })
    })
    .collect()
}

async fn pending_transfers(State(state): State<ApiState>) -> String {
  state
    .manager
    .pending_transfers()
    .await
    .into_iter()
    .map(|transfer| {
      format!(
        "{}\t{}\t{}\t{}\n",
        transfer.transfer_id, transfer.server_id, transfer.player, transfer.address
      )
    })
    .collect()
}

async fn transfer_result(
  State(state): State<ApiState>,
  Form(form): Form<TransferResultForm>,
) -> Result<StatusCode, ApiError> {
  state
    .manager
    .transfer_result(form.transfer_id, form.status, form.message)
    .await?;
  Ok(StatusCode::NO_CONTENT)
}

fn default_log_count() -> usize {
  100
}

fn parse_players(snapshot: &str) -> Result<Vec<PlayerPresence>, ApiError> {
  if snapshot.is_empty() {
    return Ok(Vec::new());
  }
  snapshot
    .lines()
    .enumerate()
    .map(|(index, line)| {
      let fields = line.split('\t').collect::<Vec<_>>();
      if fields.len() != 4 {
        return Err(invalid_player_snapshot(index, "expected four fields"));
      }
      let uuid = fields[0]
        .parse()
        .map_err(|_| invalid_player_snapshot(index, "invalid UUID"))?;
      let username = fields[1];
      if username.is_empty()
        || username.len() > 16
        || !username
          .chars()
          .all(|character| character.is_ascii_alphanumeric() || character == '_')
      {
        return Err(invalid_player_snapshot(index, "invalid username"));
      }
      let ping_ms = fields[2]
        .parse::<i64>()
        .map_err(|_| invalid_player_snapshot(index, "invalid ping"))?;
      if !(-1..=600_000).contains(&ping_ms) {
        return Err(invalid_player_snapshot(index, "ping is out of range"));
      }
      let server_id = if fields[3].is_empty() {
        None
      } else {
        crate::config::validate_id(fields[3])
          .map_err(|_| invalid_player_snapshot(index, "invalid server ID"))?;
        Some(fields[3].to_string())
      };
      Ok(PlayerPresence {
        uuid,
        username: username.to_string(),
        ping_ms,
        server_id,
      })
    })
    .collect()
}

fn invalid_player_snapshot(index: usize, message: &str) -> ApiError {
  ApiError::new(
    StatusCode::BAD_REQUEST,
    format!("invalid player snapshot line {}: {message}", index + 1),
  )
}

#[derive(Debug)]
struct ApiError {
  status: StatusCode,
  message: String,
}

impl ApiError {
  fn new(status: StatusCode, message: impl Into<String>) -> Self {
    Self {
      status,
      message: message.into(),
    }
  }
}

impl From<ManagerError> for ApiError {
  fn from(error: ManagerError) -> Self {
    let status = match error {
      ManagerError::AlreadyRunning(_)
      | ManagerError::PersistentWorldInUse(_)
      | ManagerError::PersistentWorldRecovery { .. } => StatusCode::CONFLICT,
      ManagerError::Solo(crate::solo::SoloError::AlreadyExists(_)) => StatusCode::CONFLICT,
      ManagerError::NotFound(_) | ManagerError::NotRunning(_) => StatusCode::NOT_FOUND,
      ManagerError::Layout(_)
      | ManagerError::State(_)
      | ManagerError::Solo(crate::solo::SoloError::MissingWorld(_))
      | ManagerError::Solo(crate::solo::SoloError::WorldStore(_)) => StatusCode::BAD_REQUEST,
      _ => StatusCode::INTERNAL_SERVER_ERROR,
    };
    tracing::error!(%error, "API request failed");
    Self::new(status, error.to_string())
  }
}

#[derive(Serialize)]
struct ErrorResponse {
  error: String,
}

impl IntoResponse for ApiError {
  fn into_response(self) -> Response {
    (
      self.status,
      Json(ErrorResponse {
        error: self.message,
      }),
    )
      .into_response()
  }
}

#[cfg(test)]
mod tests {
  use super::*;

  async fn test_state() -> (tempfile::TempDir, ApiState) {
    let directory = tempfile::tempdir().unwrap();
    let root = directory.path();
    tokio::fs::create_dir(root.join("servers")).await.unwrap();
    tokio::fs::write(
      root.join("servers/external-servers.json"),
      include_str!("../tests/fixtures/external-survival.json"),
    )
    .await
    .unwrap();
    let config = Arc::new(LoadedConfig {
      generation: None,
      file: root.join("config.json"),
      root: root.to_path_buf(),
      value: serde_json::from_value(serde_json::json!({
        "api": {"port": 25566, "token": "01234567890123456789012345678901"},
        "bridge": {"token": "11234567890123456789012345678901"},
        "postgresql": {"home": "postgres", "host": "127.0.0.1", "port": 5432,
                       "username": "test", "password": "test"},
        "ports": {"velocity": 25565, "start": 50000, "end": 51000},
        "java": {"21_home": "java21", "25_home": "java25"},
        "paths": {"cores": "cores", "plugins": "plugins", "mods": "mods",
                  "servers": "servers", "worlds": "worlds", "runtime": "runtime",
                  "logs": "logs", "pid_file": "scheduler.pid", "scheduler_log": "scheduler.log"}
      }))
      .unwrap(),
    });
    let state = ApiState {
      manager: InstanceManager::new(config.clone()),
      config: config.clone(),
      shutdown: broadcast::channel(1).0,
    };
    (directory, state)
  }

  #[tokio::test]
  async fn removed_web_endpoints_return_not_found_while_player_tracking_still_works() {
    let (_directory, state) = test_state().await;
    let token = state.config.value.bridge.token.clone();
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let server = tokio::spawn(async move { axum::serve(listener, router(state)).await.unwrap() });
    let client = reqwest::Client::new();
    for path in ["/bridge/v1/sync", "/bridge/v1/leaderboards"] {
      let response = client
        .post(format!("http://{address}{path}"))
        .bearer_auth(&token)
        .json(&serde_json::json!({"game": "bingo"}))
        .send()
        .await
        .unwrap();
      assert_eq!(response.status(), StatusCode::NOT_FOUND, "{path}");
    }
    let snapshot = "00000000-0000-0000-0000-000000000001\tAlex\t42\tLobby";
    let response = client
      .post(format!("http://{address}/bridge/v1/players"))
      .bearer_auth(&token)
      .form(&[("players", snapshot)])
      .send()
      .await
      .unwrap();
    assert_eq!(response.status(), StatusCode::NO_CONTENT);
    let players = client
      .get(format!("http://{address}/bridge/v1/players"))
      .bearer_auth(&token)
      .send()
      .await
      .unwrap();
    assert_eq!(players.status(), StatusCode::OK);
    assert_eq!(players.text().await.unwrap().trim_end(), snapshot);
    server.abort();
  }

  #[tokio::test]
  async fn legacy_web_configuration_is_ignored_when_loading_config() {
    let (directory, state) = test_state().await;
    let mut value = serde_json::to_value(&state.config.value).unwrap();
    value["public-database"] = serde_json::json!({
      "enabled": true, "url": "invalid-and-unreachable", "publish_interval_seconds": 0
    });
    let file = directory.path().join("config.json");
    tokio::fs::write(&file, serde_json::to_vec(&value).unwrap())
      .await
      .unwrap();
    let config = LoadedConfig::load(&file).await.unwrap();
    assert!(
      serde_json::to_value(config.value)
        .unwrap()
        .get("public-database")
        .is_none()
    );
  }

  #[tokio::test]
  async fn external_survival_is_in_catalog_without_starting_or_probing_backend() {
    let (directory, state) = test_state().await;
    let root = directory.path();
    let config = state.config.clone();
    let legacy = bridge_games(State(state.clone()), Query(GameCatalogQuery::default()))
      .await
      .unwrap();
    assert!(legacy.is_empty());
    let catalog = bridge_games(
      State(state),
      Query(GameCatalogQuery {
        include_voting: true,
        include_external: true,
      }),
    )
    .await
    .unwrap();
    let fields = catalog
      .trim_end_matches('\n')
      .split('\t')
      .collect::<Vec<_>>();
    assert_eq!(fields.len(), 20);
    assert_eq!(fields[0], "1");
    for (index, expected) in [
      (1, "survival"),
      (2, "survival"),
      (3, "&a&l生存服务器"),
      (4, "&7点击进入生存服务器"),
      (5, "GRASS_BLOCK"),
      (7, "26.2"),
      (13, "shared"),
      (18, "10.19.72.18:25565"),
      (19, "26.2"),
    ] {
      assert_eq!(
        STANDARD_NO_PAD.decode(fields[index]).unwrap(),
        expected.as_bytes()
      );
    }
    assert_eq!(fields[12], "true");
    assert_eq!(fields[17], "false");
    assert!(!root.join("runtime").exists());

    // Nix relocates the generated config while making paths.servers absolute.
    // Neither an old root catalog nor a database-side catalog should be consulted.
    tokio::fs::create_dir(root.join("database")).await.unwrap();
    tokio::fs::write(root.join("external-servers.json"), "invalid old file")
      .await
      .unwrap();
    tokio::fs::write(
      root.join("database/external-servers.json"),
      "invalid old file",
    )
    .await
    .unwrap();
    let mut runtime_value = config.value.clone();
    runtime_value.paths.servers = root.join("servers");
    let runtime_file = root.join("database/runtime-config.json");
    tokio::fs::write(&runtime_file, serde_json::to_vec(&runtime_value).unwrap())
      .await
      .unwrap();
    let runtime_config = LoadedConfig::load(&runtime_file).await.unwrap();
    assert_eq!(
      runtime_config.external_servers_file(),
      config.external_servers_file()
    );
    assert!(runtime_config.server_ids().await.unwrap().is_empty());
    assert!(runtime_config.server_file("external-servers").is_err());
    assert!(runtime_config.server_file("EXTERNAL-SERVERS").is_err());

    let mut external: serde_json::Value =
      serde_json::from_str(include_str!("../tests/fixtures/external-survival.json")).unwrap();
    let mut second = external["survival"].clone();
    second["gamevoting"]["id"] = "second".into();
    second["address"] = "10.19.72.19:25565".into();
    external["second"] = second;
    tokio::fs::write(
      runtime_config.external_servers_file(),
      serde_json::to_vec(&external).unwrap(),
    )
    .await
    .unwrap();
    assert_eq!(runtime_config.external_servers().await.unwrap().len(), 2);

    // The directory name is configured; a singular server/ works too.
    tokio::fs::rename(root.join("servers"), root.join("server"))
      .await
      .unwrap();
    runtime_value.paths.servers = root.join("server");
    tokio::fs::write(&runtime_file, serde_json::to_vec(&runtime_value).unwrap())
      .await
      .unwrap();
    let runtime_config = LoadedConfig::load(&runtime_file).await.unwrap();
    assert_eq!(runtime_config.external_servers().await.unwrap().len(), 2);
    assert!(runtime_config.server_ids().await.unwrap().is_empty());
    tokio::fs::write(root.join("server/external-servers.json"), "invalid")
      .await
      .unwrap();
    assert!(runtime_config.external_servers().await.is_err());
    assert!(runtime_config.server_ids().await.unwrap().is_empty());
    tokio::fs::remove_file(runtime_config.external_servers_file())
      .await
      .unwrap();
    assert!(runtime_config.external_servers().await.unwrap().is_empty());
  }

  #[test]
  fn process_completion_failures_are_internal_server_errors() {
    let error = ApiError::from(ManagerError::Completion("disk write failed".into()));
    assert_eq!(error.status, StatusCode::INTERNAL_SERVER_ERROR);
  }

  #[test]
  fn bearer_validation_rejects_wrong_scheme() {
    let mut headers = HeaderMap::new();
    headers.insert(AUTHORIZATION, "Basic test".parse().unwrap());
    assert!(!valid_bearer(&headers, "01234567890123456789012345678901"));
  }

  #[test]
  fn game_catalog_includes_solo_fields() {
    let game = serde_json::from_value(serde_json::json!({
      "id": "puzzle",
      "name": "Puzzle",
      "material": "COMPASS",
      "solo": true,
      "solo_mode": "player_world",
      "solo_startup": "on_demand",
      "solo_max_players": 2,
      "solo_retention_days": 10
    }))
    .unwrap();
    let line = format_game_catalog_entry("puzzle-server", &game, false);
    let fields = line.trim_end_matches('\n').split('\t').collect::<Vec<_>>();
    assert_eq!(fields.len(), 17);
    assert_eq!(fields[12], "true");
    assert_eq!(STANDARD_NO_PAD.decode(fields[13]).unwrap(), b"player_world");
    assert_eq!(STANDARD_NO_PAD.decode(fields[14]).unwrap(), b"on_demand");
    assert_eq!(&fields[15..], ["2", "10"]);
  }

  #[test]
  fn game_catalog_negotiates_voting_without_changing_legacy_columns() {
    let game = serde_json::from_value(serde_json::json!({
      "id": "shared", "name": "Shared", "material": "MAP", "solo": true, "voting": true
    }))
    .unwrap();
    let legacy = format_game_catalog_entry("shared-server", &game, false);
    let extended = format_game_catalog_entry("shared-server", &game, true);
    assert_eq!(
      extended,
      format!("{}\ttrue\n", legacy.trim_end_matches('\n'))
    );
    let query: GameCatalogQuery = serde_json::from_value(serde_json::json!({})).unwrap();
    assert!(!query.include_voting);
  }

  #[test]
  fn detailed_game_catalog_includes_rules_and_aliases() {
    let game = serde_json::from_value(serde_json::json!({
      "id": "bedwar",
      "name": "BedWars",
      "description": ["Protect the bed"],
      "rules": ["Destroy enemy beds", "Eliminate every team"],
      "aliases": ["bedwars", "bw"],
      "material": "BLUE_BED"
    }))
    .unwrap();
    let line = format_game_details_entry("bedwar-server", &game);
    let fields = line.trim_end_matches('\n').split('\t').collect::<Vec<_>>();
    assert_eq!(fields.len(), 19);
    assert_eq!(
      STANDARD_NO_PAD.decode(fields[17]).unwrap(),
      b"Destroy enemy beds\x1fEliminate every team"
    );
    assert_eq!(
      STANDARD_NO_PAD.decode(fields[18]).unwrap(),
      b"bedwars\x1fbw"
    );
  }

  #[test]
  fn solo_access_encodes_server_id_and_players() {
    let instance = "00000000-0000-0000-0000-000000000010".parse().unwrap();
    let first = "00000000-0000-0000-0000-000000000001".parse().unwrap();
    let second = "00000000-0000-0000-0000-000000000002".parse().unwrap();
    let line = format_solo_access("puzzle-solo", instance, "roster", &[first, second]);
    let fields = line.trim_end_matches('\n').split('\t').collect::<Vec<_>>();
    assert_eq!(fields.len(), 4);
    assert_eq!(STANDARD_NO_PAD.decode(fields[0]).unwrap(), b"puzzle-solo");
    assert_eq!(
      fields[3],
      "00000000-0000-0000-0000-000000000001,00000000-0000-0000-0000-000000000002"
    );
    assert_eq!(fields[1], instance.to_string());
    assert_eq!(fields[2], "roster");
  }

  #[test]
  fn solo_session_encodes_identity_and_roster() {
    let owner = uuid::Uuid::from_u128(1);
    let teammate = uuid::Uuid::from_u128(2);
    let line = format_solo_session("puzzle", "puzzle-owner", owner, &[owner, teammate]);
    let fields = line.trim().split('\t').collect::<Vec<_>>();
    assert_eq!(fields.len(), 4);
    assert_eq!(STANDARD_NO_PAD.decode(fields[0]).unwrap(), b"puzzle");
    assert_eq!(STANDARD_NO_PAD.decode(fields[1]).unwrap(), b"puzzle-owner");
    assert_eq!(fields[2], owner.to_string());
    assert_eq!(fields[3], format!("{owner},{teammate}"));
  }

  #[test]
  fn solo_open_access_keeps_empty_player_column() {
    let instance = "00000000-0000-0000-0000-000000000010".parse().unwrap();
    let line = format_solo_access("shared", instance, "open", &[]);
    let fields = line.trim_end_matches('\n').split('\t').collect::<Vec<_>>();
    assert_eq!(fields.len(), 4);
    assert_eq!(fields[2], "open");
    assert_eq!(fields[3], "");
  }

  #[test]
  fn solo_player_list_rejects_invalid_uuid() {
    let error = parse_uuid_list("not-a-uuid").unwrap_err();
    assert_eq!(error.status, StatusCode::BAD_REQUEST);
    assert_eq!(error.message, "invalid player UUID: not-a-uuid");
  }

  #[test]
  fn parses_player_snapshot() {
    let players = parse_players(
      "00000000-0000-0000-0000-000000000001\tAlex\t42\tlobby\n\
             00000000-0000-0000-0000-000000000002\tSteve\t-1\t",
    )
    .unwrap();
    assert_eq!(players.len(), 2);
    assert_eq!(players[0].ping_ms, 42);
    assert_eq!(players[1].server_id, None);
  }

  #[test]
  fn rejects_invalid_player_snapshot() {
    let error = parse_players("invalid\tAlex\t42\tlobby").unwrap_err();
    assert_eq!(error.status, StatusCode::BAD_REQUEST);
  }
}
