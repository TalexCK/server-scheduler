use std::{collections::HashMap, sync::Arc, time::Duration};

use sqlx::{PgPool, postgres::PgPoolOptions};
use thiserror::Error;
use tokio::{sync::broadcast, time::MissedTickBehavior};
use tracing::{error, info};

use crate::{
  config::LoadedConfig,
  manager::InstanceManager,
  protocol::{InstanceInfo, InstanceState, PlayerPresence},
};

const SCHEMA_STATEMENTS: [&str; 15] = [
  r#"CREATE TABLE IF NOT EXISTS scheduler_server_definitions (
    server_id TEXT PRIMARY KEY,
    display_name TEXT NOT NULL,
    game_id TEXT,
    game_order INTEGER,
    autostart BOOLEAN NOT NULL,
    min_players INTEGER,
    max_players INTEGER,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
  )"#,
  r#"CREATE TABLE IF NOT EXISTS scheduler_server_instances (
    instance_id UUID PRIMARY KEY,
    server_id TEXT NOT NULL,
    state TEXT NOT NULL CHECK (state IN ('starting', 'ready', 'stopping', 'exited', 'failed')),
    pid BIGINT NOT NULL,
    port INTEGER NOT NULL,
    started_at TIMESTAMPTZ NOT NULL,
    last_heartbeat TIMESTAMPTZ,
    exit_code INTEGER,
    player_count INTEGER NOT NULL DEFAULT 0,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
  )"#,
  "CREATE INDEX IF NOT EXISTS scheduler_server_instances_server_id_idx ON scheduler_server_instances (server_id)",
  r#"CREATE TABLE IF NOT EXISTS scheduler_players (
    player_uuid UUID PRIMARY KEY,
    username TEXT NOT NULL,
    ping_ms BIGINT NOT NULL,
    server_id TEXT,
    observed_at TIMESTAMPTZ NOT NULL DEFAULT now()
  )"#,
  "CREATE INDEX IF NOT EXISTS scheduler_players_server_id_idx ON scheduler_players (server_id)",
  r#"CREATE TABLE IF NOT EXISTS scheduler_leaderboard_entries (
    board TEXT NOT NULL,
    game TEXT,
    metric TEXT,
    period TEXT,
    period_key TEXT,
    player_id TEXT,
    player_uuid UUID NOT NULL,
    username TEXT NOT NULL,
    score BIGINT NOT NULL,
    sort_order TEXT NOT NULL DEFAULT 'desc' CHECK (sort_order IN ('asc', 'desc')),
    unit TEXT NOT NULL DEFAULT 'points',
    metadata JSONB NOT NULL DEFAULT '{}'::jsonb,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (board, player_uuid)
  )"#,
  "ALTER TABLE scheduler_leaderboard_entries ADD COLUMN IF NOT EXISTS game TEXT",
  "ALTER TABLE scheduler_leaderboard_entries ADD COLUMN IF NOT EXISTS metric TEXT",
  "ALTER TABLE scheduler_leaderboard_entries ADD COLUMN IF NOT EXISTS period TEXT",
  "ALTER TABLE scheduler_leaderboard_entries ADD COLUMN IF NOT EXISTS period_key TEXT",
  "ALTER TABLE scheduler_leaderboard_entries ADD COLUMN IF NOT EXISTS player_id TEXT",
  "ALTER TABLE scheduler_leaderboard_entries ADD COLUMN IF NOT EXISTS sort_order TEXT NOT NULL DEFAULT 'desc'",
  "ALTER TABLE scheduler_leaderboard_entries ADD COLUMN IF NOT EXISTS unit TEXT NOT NULL DEFAULT 'points'",
  "CREATE INDEX IF NOT EXISTS scheduler_leaderboard_entries_ranking_v2_idx ON scheduler_leaderboard_entries (game, metric, period, period_key, score, player_uuid)",
  "CREATE INDEX IF NOT EXISTS scheduler_leaderboard_entries_catalog_idx ON scheduler_leaderboard_entries (game, metric, period, period_key) WHERE game IS NOT NULL",
];

// 所有快照发布者共享同一把 PostgreSQL 事务锁，避免定时发布、手动同步或
// 多个 Scheduler 进程同时执行 DELETE + INSERT 时产生交错快照和主键冲突。
const SNAPSHOT_ADVISORY_LOCK_ID: i64 = 0x5343_4845_4455_4c45;

#[derive(Clone)]
pub struct PublicDatabase {
  pool: PgPool,
}

#[derive(Debug, Error)]
pub enum PublicDatabaseError {
  #[error("public PostgreSQL operation failed: {0}")]
  Database(#[from] sqlx::Error),
  #[error("failed to load scheduler definition for publication: {0}")]
  Config(#[from] crate::config::ConfigError),
  #[error("public database value is outside the PostgreSQL integer range: {0}")]
  IntegerRange(&'static str),
}

impl PublicDatabase {
  pub async fn connect(config: &LoadedConfig) -> Result<Option<Self>, PublicDatabaseError> {
    if !config.value.public_database.enabled {
      return Ok(None);
    }
    let pool = PgPoolOptions::new()
      .max_connections(3)
      .acquire_timeout(Duration::from_secs(10))
      .connect(&config.value.public_database.url)
      .await?;
    let database = Self { pool };
    database.initialize_schema().await?;
    info!("public PostgreSQL publication enabled");
    Ok(Some(database))
  }

  async fn initialize_schema(&self) -> Result<(), PublicDatabaseError> {
    for statement in SCHEMA_STATEMENTS {
      sqlx::query(statement).execute(&self.pool).await?;
    }
    Ok(())
  }

  pub async fn upsert_leaderboard_entry(
    &self,
    entry: &crate::protocol::LeaderboardEntryRequest,
  ) -> Result<(), PublicDatabaseError> {
    let period_key = match entry.period {
      crate::protocol::LeaderboardPeriod::Month => entry.period_key.as_deref(),
      crate::protocol::LeaderboardPeriod::AllTime => None,
    };
    // `board` 仍作为旧版表主键的一部分保留；展示和筛选只使用结构化列。
    let board = format!(
      "{}:{}:{}:{}",
      entry.game,
      entry.metric,
      entry.period.as_str(),
      period_key.unwrap_or_default()
    );
    sqlx::query(
      r#"INSERT INTO scheduler_leaderboard_entries
        (board, game, metric, period, period_key, player_id, player_uuid, username, score,
         sort_order, unit, updated_at)
        VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12)
        ON CONFLICT (board, player_uuid) DO UPDATE SET
          game = EXCLUDED.game,
          metric = EXCLUDED.metric,
          period = EXCLUDED.period,
          period_key = EXCLUDED.period_key,
          player_id = EXCLUDED.player_id,
          username = EXCLUDED.username,
          score = EXCLUDED.score,
          sort_order = EXCLUDED.sort_order,
          unit = EXCLUDED.unit,
          updated_at = EXCLUDED.updated_at
        WHERE EXCLUDED.updated_at >= scheduler_leaderboard_entries.updated_at"#,
    )
    .bind(board)
    .bind(&entry.game)
    .bind(&entry.metric)
    .bind(entry.period.as_str())
    .bind(period_key)
    .bind(&entry.player_id)
    .bind(entry.player_uuid)
    .bind(&entry.username)
    .bind(entry.score)
    .bind("asc")
    .bind("ms")
    .bind(entry.updated_at)
    .execute(&self.pool)
    .await?;
    Ok(())
  }

  pub async fn publish(
    &self,
    config: &LoadedConfig,
    manager: &InstanceManager,
  ) -> Result<(), PublicDatabaseError> {
    let mut transaction = self.pool.begin().await?;
    lock_snapshot_publication(&mut transaction).await?;

    let definitions = load_definitions(config).await?;
    let instances = manager.list().await;
    let players = manager.players().await;
    let player_counts = count_players(&players);

    sqlx::query("DELETE FROM scheduler_server_definitions")
      .execute(&mut *transaction)
      .await?;
    for definition in definitions {
      sqlx::query(
        r#"INSERT INTO scheduler_server_definitions
          (server_id, display_name, game_id, game_order, autostart, min_players, max_players, updated_at)
          VALUES ($1, $2, $3, $4, $5, $6, $7, now())"#,
      )
      .bind(definition.server_id)
      .bind(definition.display_name)
      .bind(definition.game_id)
      .bind(definition.game_order)
      .bind(definition.autostart)
      .bind(definition.min_players)
      .bind(definition.max_players)
      .execute(&mut *transaction)
      .await?;
    }

    sqlx::query("DELETE FROM scheduler_server_instances")
      .execute(&mut *transaction)
      .await?;
    for instance in instances {
      insert_instance(&mut transaction, instance, &player_counts).await?;
    }

    sqlx::query("DELETE FROM scheduler_players")
      .execute(&mut *transaction)
      .await?;
    for player in players {
      sqlx::query(
        r#"INSERT INTO scheduler_players
          (player_uuid, username, ping_ms, server_id, observed_at)
          VALUES ($1, $2, $3, $4, now())"#,
      )
      .bind(player.uuid)
      .bind(player.username)
      .bind(player.ping_ms)
      .bind(player.server_id)
      .execute(&mut *transaction)
      .await?;
    }

    transaction.commit().await?;
    Ok(())
  }

  pub async fn run(
    self,
    config: Arc<LoadedConfig>,
    manager: InstanceManager,
    mut shutdown: broadcast::Receiver<()>,
  ) {
    let mut interval = tokio::time::interval(Duration::from_secs(
      config.value.public_database.publish_interval_seconds,
    ));
    interval.set_missed_tick_behavior(MissedTickBehavior::Skip);
    // 首次快照已在守护进程接收请求前发布，避免定时器启动时重复写入。
    interval.tick().await;
    loop {
      tokio::select! {
        _ = shutdown.recv() => break,
        _ = interval.tick() => {
          if let Err(error) = self.publish(&config, &manager).await {
            error!(%error, "failed to publish scheduler snapshot");
          }
        }
      }
    }
    if let Err(error) = self.clear_live_state().await {
      error!(%error, "failed to clear scheduler live state during shutdown");
    }
  }

  async fn clear_live_state(&self) -> Result<(), PublicDatabaseError> {
    let mut transaction = self.pool.begin().await?;
    lock_snapshot_publication(&mut transaction).await?;
    sqlx::query("DELETE FROM scheduler_players")
      .execute(&mut *transaction)
      .await?;
    sqlx::query("DELETE FROM scheduler_server_instances")
      .execute(&mut *transaction)
      .await?;
    transaction.commit().await?;
    Ok(())
  }
}

async fn lock_snapshot_publication(
  transaction: &mut sqlx::Transaction<'_, sqlx::Postgres>,
) -> Result<(), PublicDatabaseError> {
  sqlx::query("SELECT pg_advisory_xact_lock($1)")
    .bind(SNAPSHOT_ADVISORY_LOCK_ID)
    .execute(&mut **transaction)
    .await?;
  Ok(())
}

#[derive(Debug, PartialEq, Eq)]
struct PublishedDefinition {
  server_id: String,
  display_name: String,
  game_id: Option<String>,
  game_order: Option<i32>,
  autostart: bool,
  min_players: Option<i32>,
  max_players: Option<i32>,
}

async fn load_definitions(
  config: &LoadedConfig,
) -> Result<Vec<PublishedDefinition>, PublicDatabaseError> {
  let mut definitions = Vec::new();
  for server_id in config.server_ids().await? {
    let definition = config.load_server(&server_id).await?;
    let game = definition.gamevoting;
    definitions.push(PublishedDefinition {
      display_name: game
        .as_ref()
        .map(|value| clean_display_name(&value.name))
        .unwrap_or_else(|| server_id.clone()),
      game_id: game.as_ref().map(|value| value.id.clone()),
      game_order: game
        .as_ref()
        .map(|value| integer(value.order, "game order"))
        .transpose()?,
      autostart: definition.autostart,
      min_players: game
        .as_ref()
        .map(|value| integer(value.min_players, "minimum players"))
        .transpose()?,
      max_players: game
        .as_ref()
        .map(|value| integer(value.max_players, "maximum players"))
        .transpose()?,
      server_id,
    });
  }
  Ok(definitions)
}

fn clean_display_name(value: &str) -> String {
  let mut cleaned = String::with_capacity(value.len());
  let mut characters = value.chars().peekable();
  while let Some(character) = characters.next() {
    if matches!(character, '&' | '§')
      && characters
        .peek()
        .is_some_and(|code| "0123456789abcdefklmnorxABCDEFKLMNORX".contains(*code))
    {
      characters.next();
    } else {
      cleaned.push(character);
    }
  }
  let cleaned = cleaned.trim();
  if cleaned.is_empty() {
    value.to_string()
  } else {
    cleaned.to_string()
  }
}

fn integer(value: u32, field: &'static str) -> Result<i32, PublicDatabaseError> {
  i32::try_from(value).map_err(|_| PublicDatabaseError::IntegerRange(field))
}

fn count_players(players: &[PlayerPresence]) -> HashMap<&str, i32> {
  let mut counts = HashMap::new();
  for server_id in players
    .iter()
    .filter_map(|player| player.server_id.as_deref())
  {
    *counts.entry(server_id).or_default() += 1;
  }
  counts
}

async fn insert_instance(
  transaction: &mut sqlx::Transaction<'_, sqlx::Postgres>,
  instance: InstanceInfo,
  player_counts: &HashMap<&str, i32>,
) -> Result<(), PublicDatabaseError> {
  let started_at = i64::try_from(instance.started_at)
    .map_err(|_| PublicDatabaseError::IntegerRange("instance started_at"))?;
  let last_heartbeat = instance
    .last_heartbeat
    .map(|value| {
      i64::try_from(value).map_err(|_| PublicDatabaseError::IntegerRange("last heartbeat"))
    })
    .transpose()?;
  let player_count = player_counts
    .get(instance.server_id.as_str())
    .copied()
    .unwrap_or_default();
  sqlx::query(
    r#"INSERT INTO scheduler_server_instances
      (instance_id, server_id, state, pid, port, started_at, last_heartbeat, exit_code, player_count, updated_at)
      VALUES ($1, $2, $3, $4, $5, to_timestamp($6), to_timestamp($7), $8, $9, now())"#,
  )
  .bind(instance.instance_id)
  .bind(instance.server_id)
  .bind(instance_state(instance.state))
  .bind(i64::from(instance.pid))
  .bind(i32::from(instance.port))
  .bind(started_at)
  .bind(last_heartbeat)
  .bind(instance.exit_code)
  .bind(player_count)
  .execute(&mut **transaction)
  .await?;
  Ok(())
}

fn instance_state(state: InstanceState) -> &'static str {
  match state {
    InstanceState::Starting => "starting",
    InstanceState::Ready => "ready",
    InstanceState::Stopping => "stopping",
    InstanceState::Exited => "exited",
    InstanceState::Failed => "failed",
  }
}

#[cfg(test)]
mod tests {
  use super::*;
  use uuid::Uuid;

  #[test]
  fn player_counts_only_include_located_players() {
    let players = vec![
      PlayerPresence {
        uuid: Uuid::new_v4(),
        username: "one".into(),
        ping_ms: 20,
        server_id: Some("lobby".into()),
      },
      PlayerPresence {
        uuid: Uuid::new_v4(),
        username: "two".into(),
        ping_ms: 30,
        server_id: Some("lobby".into()),
      },
      PlayerPresence {
        uuid: Uuid::new_v4(),
        username: "joining".into(),
        ping_ms: 40,
        server_id: None,
      },
    ];
    assert_eq!(count_players(&players).get("lobby"), Some(&2));
    assert_eq!(count_players(&players).len(), 1);
  }

  #[test]
  fn all_instance_states_have_stable_database_values() {
    assert_eq!(instance_state(InstanceState::Starting), "starting");
    assert_eq!(instance_state(InstanceState::Ready), "ready");
    assert_eq!(instance_state(InstanceState::Stopping), "stopping");
    assert_eq!(instance_state(InstanceState::Exited), "exited");
    assert_eq!(instance_state(InstanceState::Failed), "failed");
  }

  #[test]
  fn display_names_do_not_publish_minecraft_formatting_codes() {
    assert_eq!(clean_display_name("&b&lBed Wars"), "Bed Wars");
    assert_eq!(clean_display_name("§6Lobby"), "Lobby");
    assert_eq!(clean_display_name("Cats & Dogs"), "Cats & Dogs");
  }
}
