use std::{
  io,
  path::{Path, PathBuf},
};

use serde::{Deserialize, Serialize};
use thiserror::Error;
use tokio::fs;
use uuid::Uuid;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct SoloWorldMetadata {
  pub game: String,
  pub owner: Uuid,
  pub players: Vec<Uuid>,
  pub created: u64,
  pub updated: u64,
  pub expires: u64,
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

pub async fn create_storage(
  worlds_root: &Path,
  game_id: &str,
  owner: Uuid,
  players: Vec<Uuid>,
  retention_days: u32,
  timestamp: u64,
) -> Result<(SoloPersistence, SoloWorldMetadata), SoloError> {
  let game_root = worlds_root.join(game_id);
  create_dir_all(&game_root).await?;
  let root = game_root.join(owner.to_string());
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
    let template = game_root.join("world");
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
  copy_tree(&source, runtime_world).await
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
    return Err(error);
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

pub async fn game_session_roots(game_root: &Path) -> Result<Vec<PathBuf>, SoloError> {
  if !try_exists(game_root).await? {
    return Ok(Vec::new());
  }
  let mut entries = fs::read_dir(game_root)
    .await
    .map_err(|source| SoloError::Io {
      path: game_root.to_path_buf(),
      source,
    })?;
  let mut roots = Vec::new();
  while let Some(entry) = entries.next_entry().await.map_err(|source| SoloError::Io {
    path: game_root.to_path_buf(),
    source,
  })? {
    let path = entry.path();
    if entry
      .file_type()
      .await
      .map_err(|source| SoloError::Io {
        path: path.clone(),
        source,
      })?
      .is_dir()
      && path.join("config.json").is_file()
    {
      roots.push(path);
    }
  }
  roots.sort();
  Ok(roots)
}

pub async fn destroy_storage(persistence: &SoloPersistence) -> Result<(), SoloError> {
  fs::remove_dir_all(&persistence.root)
    .await
    .map_err(|source| SoloError::Io {
      path: persistence.root.clone(),
      source,
    })
}

async fn copy_tree(source: &Path, target: &Path) -> Result<(), SoloError> {
  let metadata = fs::symlink_metadata(source)
    .await
    .map_err(|source_error| SoloError::Io {
      path: source.to_path_buf(),
      source: source_error,
    })?;
  if !metadata.is_dir() || metadata.file_type().is_symlink() {
    return Err(SoloError::InvalidWorld(source.to_path_buf()));
  }
  create_dir_all(target).await?;
  let mut pending = vec![(source.to_path_buf(), target.to_path_buf())];
  while let Some((source_dir, target_dir)) = pending.pop() {
    let mut entries = fs::read_dir(&source_dir)
      .await
      .map_err(|source| SoloError::Io {
        path: source_dir.clone(),
        source,
      })?;
    while let Some(entry) = entries.next_entry().await.map_err(|source| SoloError::Io {
      path: source_dir.clone(),
      source,
    })? {
      if entry.file_name() == "session.lock" {
        continue;
      }
      let source_path = entry.path();
      let target_path = target_dir.join(entry.file_name());
      let file_type = entry.file_type().await.map_err(|source| SoloError::Io {
        path: source_path.clone(),
        source,
      })?;
      if file_type.is_symlink() {
        return Err(SoloError::InvalidWorld(source_path));
      }
      if file_type.is_dir() {
        create_dir_all(&target_path).await?;
        pending.push((source_path, target_path));
      } else if file_type.is_file() {
        fs::copy(&source_path, &target_path)
          .await
          .map_err(|source| SoloError::Io {
            path: target_path,
            source,
          })?;
      }
    }
  }
  Ok(())
}

async fn remove_path(path: &Path) -> Result<(), SoloError> {
  let Ok(metadata) = fs::symlink_metadata(path).await else {
    return Ok(());
  };
  if metadata.file_type().is_symlink() || metadata.is_file() {
    fs::remove_file(path).await.map_err(|source| SoloError::Io {
      path: path.to_path_buf(),
      source,
    })
  } else {
    fs::remove_dir_all(path)
      .await
      .map_err(|source| SoloError::Io {
        path: path.to_path_buf(),
        source,
      })
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

async fn try_exists(path: &Path) -> Result<bool, SoloError> {
  fs::try_exists(path).await.map_err(|source| SoloError::Io {
    path: path.to_path_buf(),
    source,
  })
}

#[derive(Debug, Error)]
pub enum SoloError {
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
  #[error("solo world contains an invalid entry at {0}")]
  InvalidWorld(PathBuf),
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
      create_storage(&worlds, "puzzle", owner, vec![owner], 10, 100)
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
