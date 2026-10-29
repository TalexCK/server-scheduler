use std::{
  collections::BTreeMap,
  io,
  path::{Component, Path, PathBuf},
  time::SystemTime,
};

use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use thiserror::Error;
use tokio::{fs, io::AsyncReadExt};
use uuid::Uuid;

use crate::config::{LoadedConfig, validate_id};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DependencyStamp {
  length: u64,
  modified: Option<SystemTime>,
}

pub type DependencySnapshot = BTreeMap<String, DependencyStamp>;

#[derive(Debug, Default, Serialize, Deserialize)]
struct DependencyManifest {
  files: BTreeMap<String, String>,
}

pub async fn snapshot(root: &Path) -> Result<DependencySnapshot, DependencyError> {
  Ok(
    scan(root)
      .await?
      .into_iter()
      .map(|(relative, file)| (relative, file.stamp))
      .collect(),
  )
}

pub async fn materialize(
  config: &LoadedConfig,
  server_id: &str,
  work_dir: &Path,
) -> Result<usize, DependencyError> {
  validate_server_id(server_id)?;
  materialize_at(
    &config.resolve(&config.value.paths.dependencies),
    server_id,
    work_dir,
  )
  .await
}

pub async fn capture(
  config: &LoadedConfig,
  server_id: &str,
  work_dir: &Path,
  baseline: &DependencySnapshot,
) -> Result<usize, DependencyError> {
  validate_server_id(server_id)?;
  capture_at(
    &config.resolve(&config.value.paths.dependencies),
    server_id,
    work_dir,
    baseline,
  )
  .await
}

async fn materialize_at(
  cache_root: &Path,
  server_id: &str,
  work_dir: &Path,
) -> Result<usize, DependencyError> {
  let manifest = read_manifest(cache_root, server_id).await?;
  for (relative, hash) in &manifest.files {
    validate_hash(hash)?;
    let relative_path = parse_relative_path(relative)?;
    let target = work_dir.join(relative_path);
    if try_exists(&target).await? {
      let metadata = symlink_metadata(&target).await?;
      if metadata.file_type().is_symlink() {
        return Err(DependencyError::Symlink(target));
      }
      if !metadata.is_file() {
        return Err(DependencyError::InvalidPath(target));
      }
      // Declared core/plugin resources win over an older runtime cache.
      continue;
    }
    let object = object_path(cache_root, hash);
    if !try_exists(&object).await? {
      return Err(DependencyError::MissingObject(object));
    }
    if let Some(parent) = target.parent() {
      create_dir_all(parent).await?;
    }
    link_or_copy(&object, &target).await?;
  }
  Ok(manifest.files.len())
}

async fn capture_at(
  cache_root: &Path,
  server_id: &str,
  work_dir: &Path,
  baseline: &DependencySnapshot,
) -> Result<usize, DependencyError> {
  let existing = read_manifest(cache_root, server_id).await?;
  let current = scan(work_dir).await?;
  let mut files = BTreeMap::new();
  for (relative, file) in current {
    if baseline.get(&relative) == Some(&file.stamp) {
      if let Some(hash) = existing.files.get(&relative) {
        files.insert(relative, hash.clone());
      }
      continue;
    }
    let hash = hash_file(&file.path).await?;
    store_object(cache_root, &hash, &file.path).await?;
    files.insert(relative, hash);
  }
  write_manifest(cache_root, server_id, DependencyManifest { files }).await
}

#[derive(Debug)]
struct ScannedFile {
  path: PathBuf,
  stamp: DependencyStamp,
}

async fn scan(root: &Path) -> Result<BTreeMap<String, ScannedFile>, DependencyError> {
  let mut files = BTreeMap::new();
  if !try_exists(root).await? {
    return Ok(files);
  }
  let mut pending = vec![root.to_path_buf()];
  while let Some(directory) = pending.pop() {
    let mut entries = fs::read_dir(&directory)
      .await
      .map_err(|source| DependencyError::Io {
        path: directory.clone(),
        source,
      })?;
    while let Some(entry) = entries
      .next_entry()
      .await
      .map_err(|source| DependencyError::Io {
        path: directory.clone(),
        source,
      })?
    {
      let path = entry.path();
      let file_type = entry
        .file_type()
        .await
        .map_err(|source| DependencyError::Io {
          path: path.clone(),
          source,
        })?;
      if file_type.is_symlink() {
        return Err(DependencyError::Symlink(path));
      }
      if file_type.is_dir() {
        pending.push(path);
        continue;
      }
      if !file_type.is_file() {
        continue;
      }
      let relative = path
        .strip_prefix(root)
        .map_err(|_| DependencyError::InvalidPath(path.clone()))?;
      if !is_dependency_file(relative) {
        continue;
      }
      let key = relative_key(relative)?;
      let metadata = entry
        .metadata()
        .await
        .map_err(|source| DependencyError::Io {
          path: path.clone(),
          source,
        })?;
      files.insert(
        key,
        ScannedFile {
          path,
          stamp: DependencyStamp {
            length: metadata.len(),
            modified: metadata.modified().ok(),
          },
        },
      );
    }
  }
  Ok(files)
}

fn is_dependency_file(relative: &Path) -> bool {
  let components = relative
    .components()
    .filter_map(|component| match component {
      Component::Normal(value) => value.to_str(),
      _ => None,
    })
    .collect::<Vec<_>>();
  components.len() > 1
    && components[..components.len() - 1].iter().any(|component| {
      matches!(
        component.to_ascii_lowercase().as_str(),
        ".cache"
          | ".fabric"
          | ".inject"
          | ".paper-remapped"
          | "cache"
          | "caches"
          | "lib"
          | "libs"
          | "libraries"
          | "versions"
      )
    })
}

fn relative_key(relative: &Path) -> Result<String, DependencyError> {
  let mut parts = Vec::new();
  for component in relative.components() {
    let Component::Normal(value) = component else {
      return Err(DependencyError::InvalidPath(relative.to_path_buf()));
    };
    let value = value
      .to_str()
      .ok_or_else(|| DependencyError::InvalidPath(relative.to_path_buf()))?;
    parts.push(value);
  }
  if parts.is_empty() {
    return Err(DependencyError::InvalidPath(relative.to_path_buf()));
  }
  Ok(parts.join("/"))
}

fn parse_relative_path(value: &str) -> Result<PathBuf, DependencyError> {
  if value.is_empty() || value.contains('\\') {
    return Err(DependencyError::InvalidPath(PathBuf::from(value)));
  }
  let path = PathBuf::from(value);
  if path.is_absolute()
    || path
      .components()
      .any(|component| !matches!(component, Component::Normal(_)))
  {
    return Err(DependencyError::InvalidPath(path));
  }
  Ok(path)
}

async fn read_manifest(
  cache_root: &Path,
  server_id: &str,
) -> Result<DependencyManifest, DependencyError> {
  let path = manifest_path(cache_root, server_id);
  if !try_exists(&path).await? {
    return Ok(DependencyManifest::default());
  }
  let data = fs::read(&path)
    .await
    .map_err(|source| DependencyError::Io {
      path: path.clone(),
      source,
    })?;
  serde_json::from_slice(&data).map_err(|source| DependencyError::Parse { path, source })
}

async fn write_manifest(
  cache_root: &Path,
  server_id: &str,
  manifest: DependencyManifest,
) -> Result<usize, DependencyError> {
  let path = manifest_path(cache_root, server_id);
  if manifest.files.is_empty() {
    if try_exists(&path).await? {
      remove_file(&path).await?;
    }
    return Ok(0);
  }
  if let Some(parent) = path.parent() {
    create_dir_all(parent).await?;
  }
  let data = serde_json::to_vec_pretty(&manifest)
    .map_err(|source| DependencyError::Serialize(source.to_string()))?;
  let temporary = path.with_extension(format!("json.tmp-{}", Uuid::new_v4()));
  fs::write(&temporary, data)
    .await
    .map_err(|source| DependencyError::Io {
      path: temporary.clone(),
      source,
    })?;
  fs::rename(&temporary, &path)
    .await
    .map_err(|source| DependencyError::Io {
      path: path.clone(),
      source,
    })?;
  Ok(manifest.files.len())
}

async fn hash_file(path: &Path) -> Result<String, DependencyError> {
  let mut file = fs::File::open(path)
    .await
    .map_err(|source| DependencyError::Io {
      path: path.to_path_buf(),
      source,
    })?;
  let mut digest = Sha256::new();
  let mut buffer = vec![0_u8; 1024 * 1024];
  loop {
    let count = file
      .read(&mut buffer)
      .await
      .map_err(|source| DependencyError::Io {
        path: path.to_path_buf(),
        source,
      })?;
    if count == 0 {
      break;
    }
    digest.update(&buffer[..count]);
  }
  Ok(format!("{:x}", digest.finalize()))
}

async fn store_object(cache_root: &Path, hash: &str, source: &Path) -> Result<(), DependencyError> {
  let object = object_path(cache_root, hash);
  if try_exists(&object).await? {
    return Ok(());
  }
  let parent = object
    .parent()
    .ok_or_else(|| DependencyError::InvalidPath(object.clone()))?;
  create_dir_all(parent).await?;
  match fs::hard_link(source, &object).await {
    Ok(()) => return Ok(()),
    Err(error) if error.kind() == io::ErrorKind::AlreadyExists => return Ok(()),
    Err(_) => {}
  }
  let temporary = parent.join(format!(".{}.tmp-{}", hash, Uuid::new_v4()));
  fs::copy(source, &temporary)
    .await
    .map_err(|source| DependencyError::Io {
      path: temporary.clone(),
      source,
    })?;
  match fs::rename(&temporary, &object).await {
    Ok(()) => Ok(()),
    Err(error) if error.kind() == io::ErrorKind::AlreadyExists => {
      remove_file(&temporary).await?;
      Ok(())
    }
    Err(source) => Err(DependencyError::Io {
      path: object,
      source,
    }),
  }
}

async fn link_or_copy(source: &Path, target: &Path) -> Result<(), DependencyError> {
  if fs::hard_link(source, target).await.is_ok() {
    return Ok(());
  }
  fs::copy(source, target)
    .await
    .map(|_| ())
    .map_err(|source| DependencyError::Io {
      path: target.to_path_buf(),
      source,
    })
}

fn manifest_path(cache_root: &Path, server_id: &str) -> PathBuf {
  cache_root
    .join("manifests")
    .join("servers")
    .join(format!("{server_id}.json"))
}

fn object_path(cache_root: &Path, hash: &str) -> PathBuf {
  cache_root.join("objects").join(&hash[..2]).join(hash)
}

fn validate_hash(hash: &str) -> Result<(), DependencyError> {
  if hash.len() != 64 || !hash.bytes().all(|value| value.is_ascii_hexdigit()) {
    return Err(DependencyError::InvalidHash(hash.to_string()));
  }
  Ok(())
}

fn validate_server_id(server_id: &str) -> Result<(), DependencyError> {
  validate_id(server_id).map_err(|_| DependencyError::InvalidServerId(server_id.to_string()))
}

async fn try_exists(path: &Path) -> Result<bool, DependencyError> {
  fs::try_exists(path)
    .await
    .map_err(|source| DependencyError::Io {
      path: path.to_path_buf(),
      source,
    })
}

async fn create_dir_all(path: &Path) -> Result<(), DependencyError> {
  fs::create_dir_all(path)
    .await
    .map_err(|source| DependencyError::Io {
      path: path.to_path_buf(),
      source,
    })
}

async fn remove_file(path: &Path) -> Result<(), DependencyError> {
  fs::remove_file(path)
    .await
    .map_err(|source| DependencyError::Io {
      path: path.to_path_buf(),
      source,
    })
}

async fn symlink_metadata(path: &Path) -> Result<std::fs::Metadata, DependencyError> {
  fs::symlink_metadata(path)
    .await
    .map_err(|source| DependencyError::Io {
      path: path.to_path_buf(),
      source,
    })
}

#[derive(Debug, Error)]
pub enum DependencyError {
  #[error("failed to access dependency path {path}: {source}")]
  Io { path: PathBuf, source: io::Error },
  #[error("failed to parse dependency manifest {path}: {source}")]
  Parse {
    path: PathBuf,
    source: serde_json::Error,
  },
  #[error("failed to serialize dependency manifest: {0}")]
  Serialize(String),
  #[error("invalid dependency path: {0}")]
  InvalidPath(PathBuf),
  #[error("invalid dependency object hash: {0}")]
  InvalidHash(String),
  #[error("dependency object does not exist: {0}")]
  MissingObject(PathBuf),
  #[error("dependency path is a symbolic link: {0}")]
  Symlink(PathBuf),
  #[error("invalid dependency server ID: {0}")]
  InvalidServerId(String),
}

#[cfg(test)]
mod tests {
  use super::*;

  #[tokio::test]
  async fn declared_dependencies_are_not_replaced_by_stale_runtime_cache() {
    let directory = tempfile::tempdir().unwrap();
    let cache = directory.path().join("cache");
    let original = directory.path().join("original");
    fs::create_dir_all(original.join("libraries"))
      .await
      .unwrap();
    fs::write(original.join("libraries/core.jar"), b"old")
      .await
      .unwrap();
    capture_at(&cache, "game", &original, &BTreeMap::new())
      .await
      .unwrap();
    let runtime = directory.path().join("runtime");
    fs::create_dir_all(runtime.join("libraries")).await.unwrap();
    fs::write(runtime.join("libraries/core.jar"), b"new declared core")
      .await
      .unwrap();
    materialize_at(&cache, "game", &runtime).await.unwrap();
    assert_eq!(
      fs::read(runtime.join("libraries/core.jar")).await.unwrap(),
      b"new declared core"
    );
  }
  #[tokio::test]
  async fn snapshot_only_contains_dependency_directories() {
    let directory = tempfile::tempdir().unwrap();
    let root = directory.path();
    fs::create_dir_all(root.join("libraries/example"))
      .await
      .unwrap();
    fs::create_dir_all(root.join("plugins/example/libs"))
      .await
      .unwrap();
    fs::create_dir_all(root.join(".inject")).await.unwrap();
    fs::create_dir_all(root.join("config/example"))
      .await
      .unwrap();
    fs::write(root.join("libraries/example/library.jar"), b"one")
      .await
      .unwrap();
    fs::write(root.join("plugins/example/libs/library.jar"), b"two")
      .await
      .unwrap();
    fs::write(root.join(".inject/inject-support.jar"), b"three")
      .await
      .unwrap();
    fs::write(root.join("config/example/config.yml"), b"three")
      .await
      .unwrap();

    let files = snapshot(root).await.unwrap();

    assert_eq!(files.len(), 3);
    assert!(files.contains_key(".inject/inject-support.jar"));
    assert!(files.contains_key("libraries/example/library.jar"));
    assert!(files.contains_key("plugins/example/libs/library.jar"));
  }

  #[tokio::test]
  async fn identical_dependencies_share_one_object() {
    let directory = tempfile::tempdir().unwrap();
    let cache = directory.path().join("dependencies");
    let first = directory.path().join("first");
    let second = directory.path().join("second");
    fs::create_dir_all(first.join("libraries/example"))
      .await
      .unwrap();
    fs::create_dir_all(second.join("plugins/example/libs"))
      .await
      .unwrap();
    fs::write(first.join("libraries/example/library.jar"), b"same")
      .await
      .unwrap();
    fs::write(second.join("plugins/example/libs/library.jar"), b"same")
      .await
      .unwrap();

    capture_at(&cache, "first", &first, &DependencySnapshot::new())
      .await
      .unwrap();
    capture_at(&cache, "second", &second, &DependencySnapshot::new())
      .await
      .unwrap();

    let mut object_directories = fs::read_dir(cache.join("objects")).await.unwrap();
    let object_directory = object_directories.next_entry().await.unwrap().unwrap();
    assert!(object_directories.next_entry().await.unwrap().is_none());
    let mut objects = fs::read_dir(object_directory.path()).await.unwrap();
    assert!(objects.next_entry().await.unwrap().is_some());
    assert!(objects.next_entry().await.unwrap().is_none());

    let materialized = directory.path().join("materialized");
    fs::create_dir_all(&materialized).await.unwrap();
    assert_eq!(
      materialize_at(&cache, "first", &materialized)
        .await
        .unwrap(),
      1
    );
    assert_eq!(
      fs::read(materialized.join("libraries/example/library.jar"))
        .await
        .unwrap(),
      b"same"
    );
  }
}
