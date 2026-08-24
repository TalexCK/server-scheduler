use std::{collections::BTreeSet, sync::Arc};

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
    CommandRequest, CommandResponse, HeartbeatForm, IdleForm, LaunchRequest,
    LeaderboardEntryRequest, LeaderboardPeriod, LogResponse, PlayerListForm, PlayerPresence,
    PlayersForm, ReadyForm, SoloDestroyForm, SoloLaunchForm, SoloSessionCreateRequest,
    SoloSessionInfo, TransferResultForm,
  },
};

#[derive(Clone)]
pub struct ApiState {
  pub config: Arc<LoadedConfig>,
  pub manager: InstanceManager,
  pub shutdown: broadcast::Sender<()>,
  pub public_database: Option<crate::public_database::PublicDatabase>,
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
    .route("/definitions", get(bridge_definitions))
    .route("/games", get(bridge_games))
    .route("/games/details", get(bridge_game_details))
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
    .route("/leaderboards", post(bridge_upsert_leaderboard))
    .route("/sync", post(bridge_sync))
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

async fn bridge_upsert_leaderboard(
  State(state): State<ApiState>,
  Json(mut entry): Json<LeaderboardEntryRequest>,
) -> Result<StatusCode, ApiError> {
  validate_leaderboard_entry(&mut entry)?;
  let database = state.public_database.as_ref().ok_or_else(|| {
    ApiError::new(
      StatusCode::SERVICE_UNAVAILABLE,
      "public database publication is disabled",
    )
  })?;
  database
    .upsert_leaderboard_entry(&entry)
    .await
    .map_err(|error| ApiError::new(StatusCode::SERVICE_UNAVAILABLE, error.to_string()))?;
  Ok(StatusCode::NO_CONTENT)
}

const BINGO_SYNC_COMMAND: &str = "bingo_stats sync";

async fn bridge_sync(State(state): State<ApiState>) -> Result<String, ApiError> {
  let database = state.public_database.as_ref().ok_or_else(|| {
    ApiError::new(
      StatusCode::SERVICE_UNAVAILABLE,
      "public database publication is disabled",
    )
  })?;
  database
    .publish(&state.config, &state.manager)
    .await
    .map_err(|error| ApiError::new(StatusCode::SERVICE_UNAVAILABLE, error.to_string()))?;

  let bingo_server_ids = state
    .config
    .game_definitions()
    .await
    .map_err(crate::layout::LayoutError::from)
    .map_err(ManagerError::from)?
    .into_iter()
    .filter(|(_, game)| game.id.eq_ignore_ascii_case("bingo"))
    .map(|(server_id, _)| server_id)
    .collect::<BTreeSet<_>>();
  let bingo_backend_ids = state
    .manager
    .solo_sessions()
    .await
    .into_iter()
    .filter(|session| session.game_id.eq_ignore_ascii_case("bingo"))
    .map(|session| session.backend_id)
    .collect::<BTreeSet<_>>();
  let targets = ready_bingo_instances(
    state.manager.list().await,
    &bingo_server_ids,
    &bingo_backend_ids,
  );

  let mut report = vec!["Public database snapshot published.".to_string()];
  if targets.is_empty() {
    report.push("No READY Bingo instances were available for leaderboard sync.".to_string());
    return Ok(join_lines(report));
  }

  let mut submitted = 0usize;
  let mut failed = 0usize;
  for server_id in targets {
    match state
      .manager
      .command(&server_id, BINGO_SYNC_COMMAND.to_string())
      .await
    {
      Ok(output) if output.is_empty() => {
        submitted += 1;
        report.push(format!(
          "{server_id}: sync command submitted; no output was captured."
        ));
      }
      Ok(output) => {
        submitted += 1;
        report.push(format!("{server_id}: sync command submitted."));
        report.extend(
          output
            .into_iter()
            .map(|line| format!("{server_id}: {line}")),
        );
      }
      Err(error) => {
        failed += 1;
        report.push(format!("{server_id}: sync command failed: {error}"));
      }
    }
  }
  report.push(format!(
    "Bingo sync trigger summary: {submitted} submitted, {failed} failed."
  ));
  Ok(join_lines(report))
}

fn ready_bingo_instances(
  instances: Vec<crate::protocol::InstanceInfo>,
  bingo_server_ids: &BTreeSet<String>,
  bingo_backend_ids: &BTreeSet<String>,
) -> Vec<String> {
  instances
    .into_iter()
    .filter(|instance| instance.state == crate::protocol::InstanceState::Ready)
    .filter(|instance| {
      bingo_server_ids.contains(&instance.server_id)
        || bingo_backend_ids.contains(&instance.server_id)
    })
    .map(|instance| instance.server_id)
    .collect()
}

fn validate_leaderboard_entry(entry: &mut LeaderboardEntryRequest) -> Result<(), ApiError> {
  entry.game = entry.game.trim().to_ascii_lowercase();
  entry.metric = entry.metric.trim().to_ascii_lowercase();
  entry.player_id = entry.player_id.trim().to_owned();
  entry.username = entry.username.trim().to_owned();
  if entry.game != "bingo" {
    return Err(ApiError::new(StatusCode::BAD_REQUEST, "game must be bingo"));
  }
  if !valid_identifier(&entry.metric, 64) {
    return Err(ApiError::new(
      StatusCode::BAD_REQUEST,
      "metric must be a lowercase identifier",
    ));
  }
  if entry.player_id.is_empty() || entry.player_id.len() > 64 {
    return Err(ApiError::new(StatusCode::BAD_REQUEST, "invalid player_id"));
  }
  if entry.username.is_empty() || entry.username.len() > 64 {
    return Err(ApiError::new(StatusCode::BAD_REQUEST, "invalid username"));
  }
  if entry.score <= 0 {
    return Err(ApiError::new(
      StatusCode::BAD_REQUEST,
      "score must be a positive duration in milliseconds",
    ));
  }
  match entry.period {
    LeaderboardPeriod::Month => {
      let key = entry.period_key.as_deref().unwrap_or_default();
      if !valid_month_key(key) {
        return Err(ApiError::new(
          StatusCode::BAD_REQUEST,
          "month period_key must use YYYY-MM",
        ));
      }
    }
    LeaderboardPeriod::AllTime => entry.period_key = None,
  }
  Ok(())
}

fn valid_identifier(value: &str, max_len: usize) -> bool {
  !value.is_empty()
    && value.len() <= max_len
    && value
      .bytes()
      .all(|byte| byte.is_ascii_lowercase() || byte.is_ascii_digit() || byte == b'_')
}

fn valid_month_key(value: &str) -> bool {
  let Some((year, month)) = value.split_once('-') else {
    return false;
  };
  year.len() == 4
    && year.bytes().all(|byte| byte.is_ascii_digit())
    && month.len() == 2
    && month.bytes().all(|byte| byte.is_ascii_digit())
    && matches!(month.parse::<u8>(), Ok(1..=12))
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
}

async fn health() -> Json<HealthResponse> {
  Json(HealthResponse { status: "ok" })
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

async fn bridge_games(State(state): State<ApiState>) -> Result<String, ApiError> {
  let games = state
    .config
    .game_definitions()
    .await
    .map_err(crate::layout::LayoutError::from)
    .map_err(ManagerError::from)?;
  Ok(
    games
      .into_iter()
      .map(|(server_id, game)| format_game_catalog_entry(&server_id, &game))
      .collect(),
  )
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
) -> String {
  format!(
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
  )
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
  fn leaderboard_validation_normalizes_all_time_and_rejects_non_positive_duration() {
    let mut entry = LeaderboardEntryRequest {
      game: " Bingo ".into(),
      metric: "NORMAL".into(),
      period: LeaderboardPeriod::AllTime,
      period_key: Some("2026-07".into()),
      player_id: " player-one ".into(),
      username: " Alex ".into(),
      player_uuid: uuid::Uuid::nil(),
      score: 42,
      updated_at: chrono::Utc::now(),
    };
    validate_leaderboard_entry(&mut entry).unwrap();
    assert_eq!(entry.game, "bingo");
    assert_eq!(entry.metric, "normal");
    assert_eq!(entry.period_key, None);
    assert_eq!(entry.player_id, "player-one");

    entry.score = 0;
    assert!(validate_leaderboard_entry(&mut entry).is_err());
  }

  #[test]
  fn leaderboard_month_requires_calendar_month_key() {
    assert!(valid_month_key("2026-07"));
    assert!(!valid_month_key("2026-00"));
    assert!(!valid_month_key("2026-13"));
    assert!(!valid_month_key("26-07"));
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
    let line = format_game_catalog_entry("puzzle-server", &game);
    let fields = line.trim_end_matches('\n').split('\t').collect::<Vec<_>>();
    assert_eq!(fields.len(), 17);
    assert_eq!(fields[12], "true");
    assert_eq!(STANDARD_NO_PAD.decode(fields[13]).unwrap(), b"player_world");
    assert_eq!(STANDARD_NO_PAD.decode(fields[14]).unwrap(), b"on_demand");
    assert_eq!(&fields[15..], ["2", "10"]);
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

  #[test]
  fn sync_targets_only_ready_bingo_instances() {
    let bingo_servers = BTreeSet::from(["Bingo".to_string()]);
    let bingo_backends = BTreeSet::from(["Bingo-solo-owner".to_string()]);
    let instances = vec![
      sync_instance("Bingo", crate::protocol::InstanceState::Ready),
      sync_instance("Bingo-solo-owner", crate::protocol::InstanceState::Ready),
      sync_instance("Bingo-starting", crate::protocol::InstanceState::Starting),
      sync_instance("Lobby", crate::protocol::InstanceState::Ready),
    ];

    assert_eq!(
      ready_bingo_instances(instances, &bingo_servers, &bingo_backends),
      ["Bingo", "Bingo-solo-owner"]
    );
  }

  fn sync_instance(
    server_id: &str,
    state: crate::protocol::InstanceState,
  ) -> crate::protocol::InstanceInfo {
    crate::protocol::InstanceInfo {
      server_id: server_id.to_string(),
      instance_id: uuid::Uuid::new_v4(),
      state,
      pid: 1,
      port: 25565,
      work_dir: std::path::PathBuf::new(),
      log_file: std::path::PathBuf::new(),
      started_at: 0,
      last_heartbeat: None,
      exit_code: None,
    }
  }
}
