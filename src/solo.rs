use std::{
  io,
  path::{Path, PathBuf},
};

use serde::{Deserialize, Serialize};
use thiserror::Error;
use tokio::fs;
use uuid::Uuid;

use crate::world_store::copy_tree;
use crate::world_store::{remove_path, try_exists};

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct SoloWorldMetadata {
  pub game: String,
  pub owner: Uuid,
  pub players: Vec<Uuid>,
  pub created: u64,
  pub updated: u64,
  pub expires: u64,
  #[serde(default)]
  pub template_revision: Option<String>,
}

#[derive(Debug, Clone)]
pub struct SoloPersistence {
  pub root: PathBuf,
  pub retention_days: u32,
}

pub fn session_id(server_id: &str, owner: Uuid) -> String {
  format!("{server_id}-solo-{}", owner.simple())
}

pub fn shared_session_id(server_id: &str) -> String {
  format!("{server_id}-solo-shared")
}

pub fn expires_at(timestamp: u64, retention_days: u32) -> u64 {
  timestamp.saturating_add(u64::from(retention_days).saturating_mul(86_400))
}

#[cfg(test)]
pub async fn create_storage(
  saves_root: &Path,
  template_root: &Path,
  game_id: &str,
  owner: Uuid,
  players: Vec<Uuid>,
  retention_days: u32,
  timestamp: u64,
) -> Result<(SoloPersistence, SoloWorldMetadata), SoloError> {
  let game_root = saves_root.join("players").join(owner.to_string());
  create_dir_all(&game_root).await?;
  let root = game_root.join(game_id);
  match fs::create_dir(&root).await {
    Ok(()) => {}
    Err(source) if source.kind() == io::ErrorKind::AlreadyExists => {
      return Err(SoloError::AlreadyExists(root));
    }
    Err(source) => {
      return Err(SoloError::Io { path: root, source });
    }
  }
  let result = async {
    let world = root.join("world");
    let template = template_root.join(game_id).join("world");
    if !try_exists(&template).await? {
      return Err(SoloError::MissingWorld(template));
    }
    copy_tree(&template, &world).await?;
    let metadata = SoloWorldMetadata {
      game: game_id.to_string(),
      owner,
      players,
      created: timestamp,
      updated: timestamp,
      expires: expires_at(timestamp, retention_days),
      template_revision: None,
    };
    write_metadata(&root, &metadata).await?;
    Ok((
      SoloPersistence {
        root: root.clone(),
        retention_days,
      },
      metadata,
    ))
  }
  .await;
  if result.is_err() {
    let _ = fs::remove_dir_all(&root).await;
  }
  result
}

pub async fn create_from_config(
  config: &crate::config::LoadedConfig,
  game_id: &str,
  template_id: &str,
  owner: Uuid,
  players: Vec<Uuid>,
  retention_days: u32,
  timestamp: u64,
) -> Result<(SoloPersistence, SoloWorldMetadata), SoloError> {
  let parent = config
    .resolve(&config.value.paths.saves)
    .join("players")
    .join(owner.to_string());
  create_dir_all(&parent).await?;
  let root = parent.join(game_id);
  match fs::create_dir(&root).await {
    Ok(()) => {}
    Err(source) if source.kind() == io::ErrorKind::AlreadyExists => {
      return Err(SoloError::AlreadyExists(root));
    }
    Err(source) => return Err(SoloError::Io { path: root, source }),
  }
  let result = async {
    crate::resources::materialize_world(config, template_id, &root)
      .await
      .map_err(|e| SoloError::Template(e.to_string()))?;
    if !try_exists(&root.join("world")).await? {
      return Err(SoloError::MissingWorld(root.join("world")));
    }
    let persistence = SoloPersistence {
      root: root.clone(),
      retention_days,
    };
    let metadata = SoloWorldMetadata {
      game: game_id.to_owned(),
      owner,
      players,
      created: timestamp,
      updated: timestamp,
      expires: expires_at(timestamp, retention_days),
      template_revision: crate::resources::active_root(config)
        .and_then(|p| p.file_name().map(|n| n.to_string_lossy().to_string())),
    };
    write_metadata(&persistence.root, &metadata).await?;
    Ok((persistence, metadata))
  }
  .await;
  if result.is_err() {
    let _ = fs::remove_dir_all(&root).await;
  }
  result
}

pub async fn player_session_roots(saves: &Path, game_id: &str) -> Result<Vec<PathBuf>, SoloError> {
  let players = saves.join("players");
  if !try_exists(&players).await? {
    return Ok(Vec::new());
  }
  let mut entries = fs::read_dir(&players)
    .await
    .map_err(|source| SoloError::Io {
      path: players.clone(),
      source,
    })?;
  let mut roots = Vec::new();
  while let Some(entry) = entries.next_entry().await.map_err(|source| SoloError::Io {
    path: players.clone(),
    source,
  })? {
    if entry
      .file_type()
      .await
      .map_err(|source| SoloError::Io {
        path: entry.path(),
        source,
      })?
      .is_dir()
      && entry
        .file_name()
        .to_str()
        .is_some_and(|s| s.parse::<Uuid>().is_ok())
    {
      let root = entry.path().join(game_id);
      if root.join("config.json").is_file() {
        roots.push(root);
      }
    }
  }
  roots.sort();
  Ok(roots)
}

pub async fn read_metadata(root: &Path) -> Result<SoloWorldMetadata, SoloError> {
  let path = root.join("config.json");
  let bytes = fs::read(&path).await.map_err(|source| SoloError::Io {
    path: path.clone(),
    source,
  })?;
  serde_json::from_slice(&bytes).map_err(|source| SoloError::Json { path, source })
}

pub async fn write_metadata(root: &Path, metadata: &SoloWorldMetadata) -> Result<(), SoloError> {
  let path = root.join("config.json");
  let staging = root.join(format!(".config-{}.json", Uuid::new_v4()));
  let mut bytes = serde_json::to_vec_pretty(metadata).map_err(|source| SoloError::Json {
    path: path.clone(),
    source,
  })?;
  bytes.push(b'\n');
  fs::write(&staging, bytes)
    .await
    .map_err(|source| SoloError::Io {
      path: staging.clone(),
      source,
    })?;
  if let Err(source) = fs::rename(&staging, &path).await {
    let _ = fs::remove_file(&staging).await;
    return Err(SoloError::Io { path, source });
  }
  Ok(())
}

pub async fn touch(
  persistence: &SoloPersistence,
  metadata: &mut SoloWorldMetadata,
  timestamp: u64,
) -> Result<(), SoloError> {
  metadata.updated = timestamp;
  metadata.expires = expires_at(timestamp, persistence.retention_days);
  write_metadata(&persistence.root, metadata).await
}

pub async fn materialize_world(
  persistence: &SoloPersistence,
  runtime_world: &Path,
) -> Result<(), SoloError> {
  let source = persistence.root.join("world");
  if !try_exists(&source).await? {
    return Err(SoloError::MissingWorld(source));
  }
  remove_path(runtime_world).await?;
  copy_tree(&source, runtime_world).await.map_err(Into::into)
}

pub async fn persist_world(
  persistence: &SoloPersistence,
  runtime_world: &Path,
) -> Result<(), SoloError> {
  if !try_exists(runtime_world).await? {
    return Err(SoloError::MissingWorld(runtime_world.to_path_buf()));
  }
  let target = persistence.root.join("world");
  let staging = persistence
    .root
    .join(format!(".world-staging-{}", Uuid::new_v4()));
  let backup = persistence
    .root
    .join(format!(".world-backup-{}", Uuid::new_v4()));
  if let Err(error) = copy_tree(runtime_world, &staging).await {
    let _ = fs::remove_dir_all(&staging).await;
    return Err(error.into());
  }
  let had_target = try_exists(&target).await?;
  if had_target && let Err(source) = fs::rename(&target, &backup).await {
    let _ = fs::remove_dir_all(&staging).await;
    return Err(SoloError::Io {
      path: target,
      source,
    });
  }
  if let Err(source) = fs::rename(&staging, &target).await {
    if had_target {
      let _ = fs::rename(&backup, &target).await;
    }
    let _ = fs::remove_dir_all(&staging).await;
    return Err(SoloError::Io {
      path: target,
      source,
    });
  }
  if had_target {
    fs::remove_dir_all(&backup)
      .await
      .map_err(|source| SoloError::Io {
        path: backup,
        source,
      })?;
  }
  Ok(())
}

pub async fn destroy_storage(persistence: &SoloPersistence) -> Result<(), SoloError> {
  match fs::remove_dir_all(&persistence.root).await {
    Ok(()) => Ok(()),
    Err(source) if source.kind() == io::ErrorKind::NotFound => Ok(()),
    Err(source) => Err(SoloError::Io {
      path: persistence.root.clone(),
      source,
    }),
  }
}

async fn create_dir_all(path: &Path) -> Result<(), SoloError> {
  fs::create_dir_all(path)
    .await
    .map_err(|source| SoloError::Io {
      path: path.to_path_buf(),
      source,
    })
}

#[derive(Debug, Error)]
pub enum SoloError {
  #[error("world template failed: {0}")]
  Template(String),
  #[error(transparent)]
  WorldStore(#[from] crate::world_store::WorldStoreError),
  #[error("failed to access {path}: {source}")]
  Io { path: PathBuf, source: io::Error },
  #[error("failed to parse {path}: {source}")]
  Json {
    path: PathBuf,
    source: serde_json::Error,
  },
  #[error("solo world already exists at {0}")]
  AlreadyExists(PathBuf),
  #[error("solo world is missing at {0}")]
  MissingWorld(PathBuf),
}

#[cfg(test)]
mod tests {
  use super::*;

  #[tokio::test]
  async fn world_round_trip_is_atomic_and_excludes_session_lock() {
    let directory = tempfile::tempdir().unwrap();
    let worlds = directory.path().join("worlds");
    let template = worlds.join("puzzle/world");
    fs::create_dir_all(template.join("region")).await.unwrap();
    fs::write(template.join("region/r.0.0.mca"), b"template")
      .await
      .unwrap();
    fs::write(template.join("session.lock"), b"lock")
      .await
      .unwrap();
    let owner = Uuid::new_v4();
    let (persistence, mut metadata) =
      create_storage(&worlds, &worlds, "puzzle", owner, vec![owner], 10, 100)
        .await
        .unwrap();
    assert!(persistence.root.join("world/region/r.0.0.mca").exists());
    assert!(!persistence.root.join("world/session.lock").exists());

    let runtime = directory.path().join("runtime/world");
    materialize_world(&persistence, &runtime).await.unwrap();
    fs::write(runtime.join("region/r.0.0.mca"), b"updated")
      .await
      .unwrap();
    fs::write(runtime.join("session.lock"), b"runtime-lock")
      .await
      .unwrap();
    persist_world(&persistence, &runtime).await.unwrap();
    touch(&persistence, &mut metadata, 200).await.unwrap();

    assert_eq!(
      fs::read(persistence.root.join("world/region/r.0.0.mca"))
        .await
        .unwrap(),
      b"updated"
    );
    assert!(!persistence.root.join("world/session.lock").exists());
    let stored = read_metadata(&persistence.root).await.unwrap();
    assert_eq!(stored.updated, 200);
    assert_eq!(stored.expires, 200 + 10 * 86_400);
    assert!(
      fs::read_dir(&persistence.root)
        .await
        .unwrap()
        .next_entry()
        .await
        .unwrap()
        .is_some()
    );
  }
}
