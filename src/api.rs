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
    PlayerListForm, PlayerPresence, PlayersForm, ReadyForm, TransferResultForm,
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
      .map(|(server_id, game)| {
        format!(
          "{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\n",
          game.order,
          encode_game_field(&game.id),
          encode_game_field(&server_id),
          encode_game_field(&game.name),
          encode_game_field(&game.description.join("\u{1f}")),
          encode_game_field(&game.material),
          game.custom_model_data,
          encode_game_field(game.version.as_deref().unwrap_or_default()),
          encode_game_field(game.min_version.as_deref().unwrap_or_default()),
          encode_game_field(game.max_version.as_deref().unwrap_or_default()),
          game.min_players,
          game.max_players
        )
      })
      .collect(),
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
      ManagerError::AlreadyRunning(_) => StatusCode::CONFLICT,
      ManagerError::NotFound(_) | ManagerError::NotRunning(_) => StatusCode::NOT_FOUND,
      ManagerError::Layout(_) | ManagerError::State(_) => StatusCode::BAD_REQUEST,
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
  fn bearer_validation_rejects_wrong_scheme() {
    let mut headers = HeaderMap::new();
    headers.insert(AUTHORIZATION, "Basic test".parse().unwrap());
    assert!(!valid_bearer(&headers, "01234567890123456789012345678901"));
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
