use std::{
  io,
  path::{Path, PathBuf},
};

use serde::{Deserialize, Serialize};
use thiserror::Error;
use tokio::fs;
use uuid::Uuid;

use crate::config::{LoadedConfig, ServerDefinition, WorldDefinition};

pub const RECOVERY_MARKER: &str = ".scheduler-persistent-world.json";

#[derive(Debug, Serialize, Deserialize)]
struct RecoveryMarker {
  id: String,
  paths: Vec<PathBuf>,
}

#[derive(Debug, Clone)]
pub struct PersistentWorld {
  pub id: String,
  pub root: PathBuf,
  pub paths: Vec<PathBuf>,
}

impl PersistentWorld {
  pub fn from_definition(config: &LoadedConfig, definition: &ServerDefinition) -> Option<Self> {
    let WorldDefinition::Persistent { id, paths } = &definition.world else {
      return None;
    };
    Some(Self {
      id: id.clone(),
      root: config
        .resolve(&config.value.paths.saves)
        .join("shared")
        .join(id),
      paths: paths.clone(),
    })
  }
}

pub async fn materialize(
  persistent: &PersistentWorld,
  runtime_root: &Path,
) -> Result<(), WorldStoreError> {
  recover(persistent).await?;
  ensure_directory(&persistent.root).await?;
  for relative in &persistent.paths {
    let source = persistent.root.join(relative);
    if !try_exists(&source).await? {
      continue;
    }
    let target = runtime_root.join(relative);
    remove_path(&target).await?;
    copy_tree(&source, &target).await?;
  }
  Ok(())
}

pub async fn write_recovery_marker(
  persistent: &PersistentWorld,
  runtime_root: &Path,
) -> Result<(), WorldStoreError> {
  let path = runtime_root.join(RECOVERY_MARKER);
  let mut bytes = serde_json::to_vec_pretty(&RecoveryMarker {
    id: persistent.id.clone(),
    paths: persistent.paths.clone(),
  })
  .map_err(|source| WorldStoreError::Json {
    path: path.clone(),
    source,
  })?;
  bytes.push(b'\n');
  fs::write(&path, bytes)
    .await
    .map_err(|source| WorldStoreError::Io { path, source })
}

pub async fn remove_recovery_marker(runtime_root: &Path) -> Result<(), WorldStoreError> {
  let path = runtime_root.join(RECOVERY_MARKER);
  match fs::remove_file(&path).await {
    Ok(()) => Ok(()),
    Err(source) if source.kind() == io::ErrorKind::NotFound => Ok(()),
    Err(source) => Err(WorldStoreError::Io { path, source }),
  }
}

pub async fn has_recovery_marker(runtime_root: &Path) -> io::Result<bool> {
  fs::try_exists(runtime_root.join(RECOVERY_MARKER)).await
}

pub async fn pending_recovery(
  config: &LoadedConfig,
  world_id: &str,
) -> Result<Option<PathBuf>, WorldStoreError> {
  let runtime_root = config.resolve(&config.value.paths.runtime);
  if !try_exists(&runtime_root).await? {
    return Ok(None);
  }
  let mut entries = fs::read_dir(&runtime_root)
    .await
    .map_err(|source| WorldStoreError::Io {
      path: runtime_root.clone(),
      source,
    })?;
  while let Some(entry) = entries
    .next_entry()
    .await
    .map_err(|source| WorldStoreError::Io {
      path: runtime_root.clone(),
      source,
    })?
  {
    if !entry
      .file_type()
      .await
      .map_err(|source| WorldStoreError::Io {
        path: entry.path(),
        source,
      })?
      .is_dir()
    {
      continue;
    }
    let marker_path = entry.path().join(RECOVERY_MARKER);
    if !try_exists(&marker_path).await? {
      continue;
    }
    let marker_metadata =
      fs::symlink_metadata(&marker_path)
        .await
        .map_err(|source| WorldStoreError::Io {
          path: marker_path.clone(),
          source,
        })?;
    if !marker_metadata.is_file() || marker_metadata.file_type().is_symlink() {
      return Err(WorldStoreError::InvalidWorld(marker_path));
    }
    let bytes = fs::read(&marker_path)
      .await
      .map_err(|source| WorldStoreError::Io {
        path: marker_path.clone(),
        source,
      })?;
    let marker: RecoveryMarker =
      serde_json::from_slice(&bytes).map_err(|source| WorldStoreError::Json {
        path: marker_path,
        source,
      })?;
    if marker.id.eq_ignore_ascii_case(world_id) {
      return Ok(Some(entry.path()));
    }
  }
  Ok(None)
}

pub async fn persist(
  persistent: &PersistentWorld,
  runtime_root: &Path,
) -> Result<(), WorldStoreError> {
  recover(persistent).await?;
  let parent = persistent
    .root
    .parent()
    .ok_or_else(|| WorldStoreError::InvalidWorld(persistent.root.clone()))?;
  fs::create_dir_all(parent)
    .await
    .map_err(|source| WorldStoreError::Io {
      path: parent.to_path_buf(),
      source,
    })?;
  let staging = parent.join(format!(
    ".{}-world-staging-{}",
    persistent.id,
    Uuid::new_v4()
  ));
  let backup = parent.join(format!(
    ".{}-world-backup-{}",
    persistent.id,
    Uuid::new_v4()
  ));

  fs::create_dir(&staging)
    .await
    .map_err(|source| WorldStoreError::Io {
      path: staging.clone(),
      source,
    })?;
  let had_target = try_exists(&persistent.root).await?;
  let copy_result = async {
    if had_target {
      copy_tree(&persistent.root, &staging).await?;
    }
    let mut copied = 0usize;
    for relative in &persistent.paths {
      let source = runtime_root.join(relative);
      if !try_exists(&source).await? {
        continue;
      }
      remove_path(&staging.join(relative)).await?;
      copy_tree(&source, &staging.join(relative)).await?;
      copied += 1;
    }
    if copied == 0 {
      return Err(WorldStoreError::MissingRuntimeWorlds(
        runtime_root.to_path_buf(),
      ));
    }
    Ok(())
  }
  .await;
  if let Err(error) = copy_result {
    let _ = fs::remove_dir_all(&staging).await;
    return Err(error);
  }

  if had_target && let Err(source) = fs::rename(&persistent.root, &backup).await {
    let _ = fs::remove_dir_all(&staging).await;
    return Err(WorldStoreError::Io {
      path: persistent.root.clone(),
      source,
    });
  }
  if let Err(source) = fs::rename(&staging, &persistent.root).await {
    if had_target {
      let _ = fs::rename(&backup, &persistent.root).await;
    }
    let _ = fs::remove_dir_all(&staging).await;
    return Err(WorldStoreError::Io {
      path: persistent.root.clone(),
      source,
    });
  }
  if had_target {
    fs::remove_dir_all(&backup)
      .await
      .map_err(|source| WorldStoreError::Io {
        path: backup,
        source,
      })?;
  }
  Ok(())
}

pub async fn recover(persistent: &PersistentWorld) -> Result<(), WorldStoreError> {
  let parent = persistent
    .root
    .parent()
    .ok_or_else(|| WorldStoreError::InvalidWorld(persistent.root.clone()))?;
  if !try_exists(parent).await? {
    return Ok(());
  }
  let backup_prefix = format!(".{}-world-backup-", persistent.id);
  let staging_prefix = format!(".{}-world-staging-", persistent.id);
  let mut backups = Vec::new();
  let mut staging = Vec::new();
  let mut entries = fs::read_dir(parent)
    .await
    .map_err(|source| WorldStoreError::Io {
      path: parent.to_path_buf(),
      source,
    })?;
  while let Some(entry) = entries
    .next_entry()
    .await
    .map_err(|source| WorldStoreError::Io {
      path: parent.to_path_buf(),
      source,
    })?
  {
    let name = entry.file_name().to_string_lossy().to_string();
    if transaction_candidate(&name, &backup_prefix) || transaction_candidate(&name, &staging_prefix)
    {
      let path = entry.path();
      let metadata = fs::symlink_metadata(&path)
        .await
        .map_err(|source| WorldStoreError::Io {
          path: path.clone(),
          source,
        })?;
      if !metadata.is_dir() || metadata.file_type().is_symlink() {
        return Err(WorldStoreError::InvalidWorld(path));
      }
      if has_prefix_ignore_ascii_case(&name, &backup_prefix) {
        backups.push(path);
      } else {
        staging.push(path);
      }
    }
  }
  backups.sort();
  staging.sort();

  if try_exists(&persistent.root).await? {
    ensure_directory(&persistent.root).await?;
    if backups.len() + staging.len() > 1 {
      return Err(WorldStoreError::AmbiguousRecovery(persistent.root.clone()));
    }
    for path in backups.into_iter().chain(staging) {
      remove_path(&path).await?;
    }
    return Ok(());
  }
  match backups.as_slice() {
    [backup] if staging.len() <= 1 => {
      fs::rename(backup, &persistent.root)
        .await
        .map_err(|source| WorldStoreError::Io {
          path: persistent.root.clone(),
          source,
        })?;
      for path in staging {
        remove_path(&path).await?;
      }
      Ok(())
    }
    [] if staging.is_empty() => Ok(()),
    _ => Err(WorldStoreError::AmbiguousRecovery(persistent.root.clone())),
  }
}

fn transaction_candidate(name: &str, prefix: &str) -> bool {
  has_prefix_ignore_ascii_case(name, prefix)
    && name
      .get(prefix.len()..)
      .is_some_and(|suffix| Uuid::parse_str(suffix).is_ok())
}

fn has_prefix_ignore_ascii_case(value: &str, prefix: &str) -> bool {
  value
    .get(..prefix.len())
    .is_some_and(|candidate| candidate.eq_ignore_ascii_case(prefix))
}

async fn ensure_directory(path: &Path) -> Result<(), WorldStoreError> {
  let metadata = fs::symlink_metadata(path)
    .await
    .map_err(|source| WorldStoreError::Io {
      path: path.to_path_buf(),
      source,
    })?;
  if metadata.is_dir() && !metadata.file_type().is_symlink() {
    Ok(())
  } else {
    Err(WorldStoreError::InvalidWorld(path.to_path_buf()))
  }
}

pub(crate) async fn copy_tree(source: &Path, target: &Path) -> Result<(), WorldStoreError> {
  ensure_directory(source).await?;
  fs::create_dir_all(target)
    .await
    .map_err(|source| WorldStoreError::Io {
      path: target.to_path_buf(),
      source,
    })?;
  let mut pending = vec![(source.to_path_buf(), target.to_path_buf())];
  while let Some((source_dir, target_dir)) = pending.pop() {
    let mut entries = fs::read_dir(&source_dir)
      .await
      .map_err(|source| WorldStoreError::Io {
        path: source_dir.clone(),
        source,
      })?;
    while let Some(entry) = entries
      .next_entry()
      .await
      .map_err(|source| WorldStoreError::Io {
        path: source_dir.clone(),
        source,
      })?
    {
      if entry.file_name() == "session.lock" {
        continue;
      }
      let source_path = entry.path();
      let target_path = target_dir.join(entry.file_name());
      let file_type = entry
        .file_type()
        .await
        .map_err(|source| WorldStoreError::Io {
          path: source_path.clone(),
          source,
        })?;
      if file_type.is_symlink() {
        return Err(WorldStoreError::InvalidWorld(source_path));
      }
      if file_type.is_dir() {
        fs::create_dir_all(&target_path)
          .await
          .map_err(|source| WorldStoreError::Io {
            path: target_path.clone(),
            source,
          })?;
        pending.push((source_path, target_path));
      } else if file_type.is_file() {
        fs::copy(&source_path, &target_path)
          .await
          .map_err(|source| WorldStoreError::Io {
            path: target_path,
            source,
          })?;
      } else {
        return Err(WorldStoreError::InvalidWorld(source_path));
      }
    }
  }
  Ok(())
}

pub(crate) async fn remove_path(path: &Path) -> Result<(), WorldStoreError> {
  let metadata = match fs::symlink_metadata(path).await {
    Ok(metadata) => metadata,
    Err(source) if source.kind() == io::ErrorKind::NotFound => return Ok(()),
    Err(source) => {
      return Err(WorldStoreError::Io {
        path: path.to_path_buf(),
        source,
      });
    }
  };
  if metadata.file_type().is_symlink() || metadata.is_file() {
    fs::remove_file(path)
      .await
      .map_err(|source| WorldStoreError::Io {
        path: path.to_path_buf(),
        source,
      })
  } else {
    fs::remove_dir_all(path)
      .await
      .map_err(|source| WorldStoreError::Io {
        path: path.to_path_buf(),
        source,
      })
  }
}

pub(crate) async fn try_exists(path: &Path) -> Result<bool, WorldStoreError> {
  fs::try_exists(path)
    .await
    .map_err(|source| WorldStoreError::Io {
      path: path.to_path_buf(),
      source,
    })
}

#[derive(Debug, Error)]
pub enum WorldStoreError {
  #[error("failed to access {path}: {source}")]
  Io { path: PathBuf, source: io::Error },
  #[error("failed to parse {path}: {source}")]
  Json {
    path: PathBuf,
    source: serde_json::Error,
  },
  #[error("persistent world contains an invalid entry at {0}")]
  InvalidWorld(PathBuf),
  #[error("no configured persistent world directories exist under {0}")]
  MissingRuntimeWorlds(PathBuf),
  #[error("persistent world has ambiguous interrupted-save data near {0}")]
  AmbiguousRecovery(PathBuf),
}

#[cfg(test)]
mod tests {
  use super::*;

  async fn test_config(root: &Path) -> LoadedConfig {
    let path = root.join("config.json");
    fs::write(
      &path,
      serde_json::to_vec(&serde_json::json!({
        "api":{"port":25566,"token":"01234567890123456789012345678901"},
        "bridge":{"token":"11234567890123456789012345678901"},
        "postgresql":{
          "home":"postgres",
          "host":"127.0.0.1",
          "port":5432,
          "username":"minigames",
          "password":"test-password"
        },
        "ports":{"velocity":25565,"start":50001,"end":59999},
        "java":{"21_home":"java21","25_home":"java25"},
        "paths":{
          "cores":"cores",
          "plugins":"plugins",
          "mods":"mods",
          "servers":"servers",
          "worlds":"worlds",
          "runtime":"runtime",
          "logs":"logs",
          "pid_file":"scheduler.pid",
          "scheduler_log":"scheduler.log"
        }
      }))
      .unwrap(),
    )
    .await
    .unwrap();
    LoadedConfig::load(path).await.unwrap()
  }

  #[tokio::test]
  async fn pending_recovery_matches_world_id_case_insensitively() {
    let directory = tempfile::tempdir().unwrap();
    let config = test_config(directory.path()).await;
    let recovery = directory.path().join("runtime/survival-instance");
    fs::create_dir_all(&recovery).await.unwrap();
    fs::write(
      recovery.join(RECOVERY_MARKER),
      serde_json::to_vec(&RecoveryMarker {
        id: "Survival".into(),
        paths: vec![PathBuf::from("world")],
      })
      .unwrap(),
    )
    .await
    .unwrap();

    assert_eq!(
      pending_recovery(&config, "survival").await.unwrap(),
      Some(recovery)
    );
  }

  #[tokio::test]
  async fn persistent_world_round_trip_replaces_all_configured_worlds() {
    let directory = tempfile::tempdir().unwrap();
    let root = directory.path().join("saves/shared/survival");
    for name in ["world", "world_nether", "world_the_end"] {
      fs::create_dir_all(root.join(name).join("region"))
        .await
        .unwrap();
      fs::write(root.join(name).join("region/data.mca"), b"old")
        .await
        .unwrap();
    }
    let persistent = PersistentWorld {
      id: "survival".into(),
      root: root.clone(),
      paths: ["world", "world_nether", "world_the_end"]
        .into_iter()
        .map(PathBuf::from)
        .collect(),
    };
    let runtime = directory.path().join("runtime");

    materialize(&persistent, &runtime).await.unwrap();
    for name in ["world", "world_nether", "world_the_end"] {
      fs::write(runtime.join(name).join("region/data.mca"), b"new")
        .await
        .unwrap();
      fs::write(runtime.join(name).join("session.lock"), b"lock")
        .await
        .unwrap();
    }
    persist(&persistent, &runtime).await.unwrap();

    for name in ["world", "world_nether", "world_the_end"] {
      assert_eq!(
        fs::read(root.join(name).join("region/data.mca"))
          .await
          .unwrap(),
        b"new"
      );
      assert!(!root.join(name).join("session.lock").exists());
    }
    let mut entries = fs::read_dir(root.parent().unwrap()).await.unwrap();
    let mut names = Vec::new();
    while let Some(entry) = entries.next_entry().await.unwrap() {
      names.push(entry.file_name());
    }
    names.sort();
    assert_eq!(names, ["survival"]);
  }

  #[tokio::test]
  async fn missing_runtime_world_keeps_the_previous_saved_copy() {
    let directory = tempfile::tempdir().unwrap();
    let root = directory.path().join("saves/shared/survival");
    fs::create_dir_all(root.join("world")).await.unwrap();
    fs::create_dir_all(root.join("world_nether")).await.unwrap();
    fs::write(root.join("world/level.dat"), b"old-main")
      .await
      .unwrap();
    fs::write(root.join("world_nether/level.dat"), b"old-nether")
      .await
      .unwrap();
    fs::write(root.join("notes.txt"), b"keep").await.unwrap();
    let runtime = directory.path().join("runtime");
    fs::create_dir_all(runtime.join("world")).await.unwrap();
    fs::write(runtime.join("world/level.dat"), b"new-main")
      .await
      .unwrap();
    let persistent = PersistentWorld {
      id: "survival".into(),
      root: root.clone(),
      paths: vec![PathBuf::from("world"), PathBuf::from("world_nether")],
    };

    persist(&persistent, &runtime).await.unwrap();

    assert_eq!(
      fs::read(root.join("world/level.dat")).await.unwrap(),
      b"new-main"
    );
    assert_eq!(
      fs::read(root.join("world_nether/level.dat")).await.unwrap(),
      b"old-nether"
    );
    assert_eq!(fs::read(root.join("notes.txt")).await.unwrap(), b"keep");
  }

  #[tokio::test]
  async fn materialize_recovers_an_interrupted_package_swap() {
    let directory = tempfile::tempdir().unwrap();
    let worlds = directory.path().join("worlds");
    let root = worlds.join("survival");
    let backup = worlds.join(format!(".survival-world-backup-{}", Uuid::new_v4()));
    let staging = worlds.join(format!(".survival-world-staging-{}", Uuid::new_v4()));
    fs::create_dir_all(backup.join("world")).await.unwrap();
    fs::write(backup.join("world/level.dat"), b"safe")
      .await
      .unwrap();
    fs::create_dir_all(staging.join("world")).await.unwrap();
    fs::write(staging.join("world/level.dat"), b"uncommitted")
      .await
      .unwrap();
    let persistent = PersistentWorld {
      id: "survival".into(),
      root: root.clone(),
      paths: vec![PathBuf::from("world")],
    };
    let runtime = directory.path().join("runtime");

    materialize(&persistent, &runtime).await.unwrap();

    assert_eq!(
      fs::read(runtime.join("world/level.dat")).await.unwrap(),
      b"safe"
    );
    assert!(root.exists());
    assert!(!backup.exists());
    assert!(!staging.exists());
  }

  #[tokio::test]
  async fn recovery_ignores_manually_named_directories() {
    let directory = tempfile::tempdir().unwrap();
    let worlds = directory.path().join("worlds");
    let root = worlds.join("survival");
    let manual = worlds.join(".survival-world-backup-manual");
    fs::create_dir_all(root.join("world")).await.unwrap();
    fs::create_dir_all(&manual).await.unwrap();
    let persistent = PersistentWorld {
      id: "survival".into(),
      root,
      paths: vec![PathBuf::from("world")],
    };

    recover(&persistent).await.unwrap();

    assert!(manual.exists());
  }

  #[test]
  fn transaction_candidates_match_world_ids_case_insensitively() {
    let suffix = Uuid::new_v4();
    assert!(transaction_candidate(
      &format!(".Survival-world-backup-{suffix}"),
      ".survival-world-backup-"
    ));
    assert!(!transaction_candidate(
      ".survival-world-backup-manual",
      ".survival-world-backup-"
    ));
  }

  #[cfg(unix)]
  #[tokio::test]
  async fn persist_failure_keeps_the_previous_world_package() {
    let directory = tempfile::tempdir().unwrap();
    let root = directory.path().join("saves/shared/survival");
    fs::create_dir_all(root.join("world")).await.unwrap();
    fs::write(root.join("world/level.dat"), b"old")
      .await
      .unwrap();
    let runtime = directory.path().join("runtime");
    fs::create_dir_all(runtime.join("world")).await.unwrap();
    std::os::unix::fs::symlink(
      runtime.join("world/level.dat"),
      runtime.join("world/invalid-link"),
    )
    .unwrap();
    let persistent = PersistentWorld {
      id: "survival".into(),
      root: root.clone(),
      paths: vec![PathBuf::from("world")],
    };

    let result = persist(&persistent, &runtime).await;

    assert!(matches!(result, Err(WorldStoreError::InvalidWorld(_))));
    assert_eq!(
      fs::read(root.join("world/level.dat")).await.unwrap(),
      b"old"
    );
  }
}
