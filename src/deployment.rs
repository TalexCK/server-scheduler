//! One current resource set per server. No per-update history is retained.
use crate::{
  config::{LoadedConfig, ServerDefinition},
  resources::{ActiveGeneration, Manifest, Updater},
};
use serde::Serialize;
use std::{
  collections::{BTreeMap, BTreeSet},
  io,
  path::{Path, PathBuf},
  sync::Arc,
  time::Duration,
};
use uuid::Uuid;
type Result<T> = std::result::Result<T, Box<dyn std::error::Error + Send + Sync>>;

pub fn root(config: &LoadedConfig) -> PathBuf {
  config.resolve(&config.value.paths.resources)
}
pub fn server_root(config: &LoadedConfig, id: &str) -> PathBuf {
  root(config).join("servers").join(id)
}
pub fn catalog(config: &LoadedConfig) -> PathBuf {
  root(config).join("catalog")
}

pub(crate) async fn select_manifests(
  config: &LoadedConfig,
  staging: &Path,
  id: &str,
  manifests: &mut BTreeMap<&str, Manifest>,
) -> Result<()> {
  let mut source = config.clone();
  source.generation = Some(config.resolve(&config.value.paths.manifests));
  let definition = source.load_server(id).await?;
  for (kind, manifest) in manifests.iter() {
    for key in manifest.artifacts.keys() {
      tokio::fs::create_dir_all(staging.join(kind).join(key)).await?;
    }
  }
  let mut selected: BTreeMap<&str, BTreeSet<String>> = BTreeMap::new();
  let core_root = staging.join("cores");
  let core = crate::layout::resolve_core_version(
    &core_root.join(&definition.core.kind),
    None,
    &definition.core.minecraft_version,
    definition.core.loader_version.as_deref(),
  )
  .await?;
  selected
    .entry("cores")
    .or_default()
    .insert(core.strip_prefix(&core_root)?.to_string_lossy().to_string());
  for (kind, name, version) in definition
    .plugins
    .iter()
    .map(|p| ("plugins", &p.id, p.version.as_deref()))
    .chain(
      definition
        .mods
        .iter()
        .map(|m| ("mods", &m.id, m.version.as_deref())),
    )
  {
    let base = staging.join(kind);
    let path = crate::layout::resolve_artifact_version(
      &base.join(name),
      version,
      &definition.core.minecraft_version,
    )
    .await?;
    selected
      .entry(kind)
      .or_default()
      .insert(path.strip_prefix(base)?.to_string_lossy().to_string());
  }
  if let crate::config::WorldDefinition::Template { id } = definition.world {
    selected.entry("worlds").or_default().insert(id);
  }
  for (kind, manifest) in manifests {
    manifest
      .artifacts
      .retain(|id, _| selected.get(kind).is_some_and(|s| s.contains(id)));
    // Selection scaffolding must not make missing bundles appear installed.
    if staging.join(kind).exists() {
      tokio::fs::remove_dir_all(staging.join(kind)).await?;
    }
  }
  Ok(())
}

#[derive(Serialize)]
pub struct Prepared {
  pub server_id: String,
  pub revision: String,
  pub changed: bool,
}

fn atomic_json(path: &Path, value: &impl Serialize) -> Result<()> {
  std::fs::create_dir_all(path.parent().unwrap())?;
  let temp = path.with_extension(format!("tmp-{}", Uuid::new_v4()));
  let bytes = serde_json::to_vec_pretty(value)?;
  std::fs::write(&temp, bytes)?;
  std::fs::File::open(&temp)?.sync_all()?;
  std::fs::rename(temp, path)?;
  Ok(())
}

/// Shared while an instance links its files; exclusive only for publication/GC.
pub struct PublicationLock(std::fs::File);
impl PublicationLock {
  pub fn acquire(config: &LoadedConfig, exclusive: bool) -> io::Result<Self> {
    let root = root(config);
    std::fs::create_dir_all(&root)?;
    let file = std::fs::OpenOptions::new()
      .create(true)
      .truncate(false)
      .write(true)
      .open(root.join("publish.lock"))?;
    #[cfg(unix)]
    {
      use std::os::fd::AsRawFd;
      if unsafe {
        libc::flock(
          file.as_raw_fd(),
          if exclusive {
            libc::LOCK_EX
          } else {
            libc::LOCK_SH
          },
        )
      } != 0
      {
        return Err(io::Error::last_os_error());
      }
    }
    Ok(Self(file))
  }
}
impl Drop for PublicationLock {
  fn drop(&mut self) {
    #[cfg(unix)]
    {
      use std::os::fd::AsRawFd;
      unsafe {
        libc::flock(self.0.as_raw_fd(), libc::LOCK_UN);
      }
    }
  }
}

pub async fn prepare(config: Arc<LoadedConfig>, id: &str, local_only: bool) -> Result<Prepared> {
  crate::config::validate_id(id)?;
  let cache = root(&config);
  tokio::fs::create_dir_all(cache.join("temporary")).await?;
  let _job = crate::resources::UpdateLock::acquire(&cache.join("update.lock"))?;
  let stage = cache
    .join("temporary")
    .join(format!("{id}-{}", Uuid::new_v4()));
  tokio::fs::create_dir_all(&stage).await?;
  let result = prepare_at(config.clone(), id, local_only, &stage).await;
  let _ = tokio::fs::remove_dir_all(&stage).await;
  result
}
async fn prepare_at(
  config: Arc<LoadedConfig>,
  id: &str,
  local_only: bool,
  stage: &Path,
) -> Result<Prepared> {
  // For a Git-only change reuse the resolved remote files; do not require unrelated
  // upstream APIs just to deploy a local plugin. Full refresh remains the default.
  let updater = Updater::new(config.clone());
  let current = server_root(&config, id);
  if local_only && current.is_dir() {
    clone_tree(&current, stage)?;
    refresh_local(&config, stage)?;
    let mut source = (*config).clone();
    source.generation = Some(config.resolve(&config.value.paths.manifests));
    let definition = source.load_server(id).await?;
    atomic_json(
      &stage.join("servers").join(format!("{id}.json")),
      &definition,
    )?;
  } else {
    updater.prepare(stage, Some(id)).await?;
  }
  let definition: ServerDefinition = serde_json::from_slice(&std::fs::read(
    stage.join("servers").join(format!("{id}.json")),
  )?)?;
  let file_root = stage.join("files");
  if file_root.exists() {
    std::fs::remove_dir_all(&file_root)?;
  }
  std::fs::create_dir_all(&file_root)?;
  for layer in &definition.files {
    copy_configuration(
      &config
        .resolve(&config.value.paths.files)
        .join(&layer.source),
      &file_root.join(&layer.source),
    )?;
  }
  if !local_only {
    warm_paper(&config, id, stage, &definition).await?;
  }
  let revision = fingerprint(stage)?;
  let old_revision = std::fs::read_to_string(current.join("revision")).ok();
  let changed = old_revision.as_deref() != Some(&revision);
  if !changed {
    let _publish = PublicationLock::acquire(&config, true)?;
    if sync_external(&config)? {
      atomic_json(
        &root(&config).join("current.json"),
        &ActiveGeneration {
          revision: Uuid::new_v4().to_string(),
        },
      )?;
    }
    eprintln!("{id}: no resource changes");
    return Ok(Prepared {
      server_id: id.into(),
      revision,
      changed,
    });
  }
  let rev_temp = stage.join("revision.tmp");
  std::fs::write(&rev_temp, &revision)?;
  std::fs::rename(rev_temp, stage.join("revision"))?;
  let _publish = PublicationLock::acquire(&config, true)?;
  std::fs::create_dir_all(current.parent().unwrap())?;
  let previous = root(&config)
    .join("temporary")
    .join(format!("previous-{id}"));
  if previous.exists() {
    std::fs::remove_dir_all(&previous)?;
  }
  let rollback = root(&config).join("rollback").join(id);
  std::fs::create_dir_all(rollback.parent().unwrap())?;
  if current.exists() {
    std::fs::rename(&current, &previous)?;
  }
  if let Err(error) = std::fs::rename(stage, &current) {
    if previous.exists() {
      let _ = std::fs::rename(&previous, &current);
    }
    return Err(error.into());
  }
  let cat = catalog(&config);
  std::fs::create_dir_all(cat.join("servers"))?;
  let source = current.join("servers").join(format!("{id}.json"));
  atomic_json(
    &cat.join("servers").join(format!("{id}.json")),
    &serde_json::from_slice::<serde_json::Value>(&std::fs::read(source)?)?,
  )?;
  sync_external(&config)?;
  atomic_json(
    &root(&config).join("current.json"),
    &ActiveGeneration {
      revision: Uuid::new_v4().to_string(),
    },
  )?;
  if previous.exists() {
    if rollback.exists() {
      std::fs::remove_dir_all(previous)?;
    } else {
      std::fs::rename(previous, rollback)?;
    }
  }
  Ok(Prepared {
    server_id: id.into(),
    revision,
    changed,
  })
}
fn sync_external(config: &LoadedConfig) -> Result<bool> {
  let source = config
    .resolve(&config.value.paths.servers)
    .join("external-servers.json");
  let target = catalog(config).join("servers/external-servers.json");
  let new = if source.is_file() {
    Some(serde_json::from_slice::<serde_json::Value>(
      &std::fs::read(source)?,
    )?)
  } else {
    None
  };
  let old = std::fs::read(&target)
    .ok()
    .and_then(|b| serde_json::from_slice::<serde_json::Value>(&b).ok());
  if new == old {
    return Ok(false);
  }
  if let Some(value) = new {
    atomic_json(&target, &value)?;
  } else {
    std::fs::remove_file(target)?;
  }
  Ok(true)
}
fn copy_configuration(source: &Path, target: &Path) -> Result<()> {
  if source.is_symlink() {
    return Err(io::Error::other("configuration contains a symlink").into());
  }
  if source.is_dir() {
    std::fs::create_dir_all(target)?;
    for entry in std::fs::read_dir(source)? {
      let entry = entry?;
      copy_configuration(&entry.path(), &target.join(entry.file_name()))?;
    }
  } else {
    std::fs::create_dir_all(target.parent().unwrap())?;
    std::fs::copy(source, target)?;
  }
  Ok(())
}
fn clone_tree(source: &Path, target: &Path) -> Result<()> {
  if source.is_symlink() {
    return Err(io::Error::other("resource cache contains a symlink").into());
  }
  if source.is_file() {
    std::fs::create_dir_all(target.parent().unwrap())?;
    std::fs::hard_link(source, target)?;
    return Ok(());
  }
  std::fs::create_dir_all(target)?;
  for entry in std::fs::read_dir(source)? {
    let entry = entry?;
    let path = entry.path();
    let dst = target.join(entry.file_name());
    if entry.file_type()?.is_dir() {
      clone_tree(&path, &dst)?;
    } else if entry.file_type()?.is_file() {
      std::fs::hard_link(path, dst)?;
    } else {
      return Err(io::Error::other("resource cache contains a symlink").into());
    }
  }
  Ok(())
}
fn refresh_local(config: &LoadedConfig, stage: &Path) -> Result<()> {
  let base = config.resolve(&config.value.paths.manifests);
  for kind in ["cores", "mods", "plugins", "worlds"] {
    let path = stage.join(format!("{kind}.json"));
    let mut manifest: Manifest = serde_json::from_slice(&std::fs::read(&path)?)?;
    // Only reuse this fast path when the worker has established manifests and
    // definitions are unchanged. Local files are operator-managed, never hashed.
    for (id, bundle) in &mut manifest.artifacts {
      for file in &mut bundle.files {
        if let crate::resources::Source::File { path } = &file.source {
          let source = base.join(path).canonicalize()?;
          if !source.starts_with(base.join("depend").canonicalize()?) {
            return Err(io::Error::other("local source outside depend").into());
          }
          let mut reader = std::fs::File::open(&source)?;
          let mut header = [0u8; 128];
          use std::io::Read;
          let n = reader.read(&mut header)?;
          if header[..n].starts_with(b"version https://git-lfs.github.com/spec/v1") {
            return Err(io::Error::other("Git LFS content missing").into());
          }
          let target = stage.join(kind).join(id).join(&file.target);
          std::fs::remove_file(&target)?;
          std::fs::hard_link(&source, &target)?;
          file.size = source.metadata()?.len();
          file.sha256.clear();
          file.sha512 = None;
        }
      }
    }
    for (id, bundle) in &manifest.artifacts {
      if let Some(archive) = &bundle.archive {
        crate::resources::validate_archive(&stage.join(kind).join(id).join(archive))?;
      }
    }
    // clone_tree hard-links metadata too: replace, never overwrite in place.
    atomic_json(&path, &manifest)?;
  }
  Ok(())
}
/// Include metadata and local inode identity (not local file hash validation).
fn fingerprint(path: &Path) -> Result<String> {
  use sha2::{Digest, Sha256};
  fn visit(base: &Path, path: &Path, out: &mut Vec<(PathBuf, Vec<u8>)>) -> Result<()> {
    for entry in std::fs::read_dir(path)? {
      let entry = entry?;
      let p = entry.path();
      let rel = p.strip_prefix(base)?.to_path_buf();
      if entry.file_type()?.is_dir() {
        visit(base, &p, out)?;
      } else if entry.file_type()?.is_file() && rel != Path::new("revision") {
        let data = if rel.starts_with("files") || p.extension().is_some_and(|e| e == "json") {
          std::fs::read(&p)?
        } else {
          let m = p.metadata()?;
          #[cfg(unix)]
          {
            use std::os::unix::fs::MetadataExt;
            format!(
              "{}:{}:{}:{}:{}",
              m.dev(),
              m.ino(),
              m.len(),
              m.mtime(),
              m.mtime_nsec()
            )
            .into_bytes()
          }
          #[cfg(not(unix))]
          {
            format!("{}:{:?}", m.len(), m.modified()?).into_bytes()
          }
        };
        out.push((rel, data));
      }
    }
    Ok(())
  }
  let mut entries = Vec::new();
  visit(path, path, &mut entries)?;
  entries.sort();
  let mut hash = Sha256::new();
  for (path, bytes) in entries {
    hash.update(path.to_string_lossy().as_bytes());
    hash.update([0]);
    hash.update(bytes);
    hash.update([0]);
  }
  Ok(format!("{:x}", hash.finalize()))
}
async fn warm_paper(
  config: &LoadedConfig,
  id: &str,
  stage: &Path,
  definition: &ServerDefinition,
) -> Result<()> {
  if definition.core.kind != "paper" && definition.core.kind != "folia" {
    return Ok(());
  }
  let resource_core = crate::layout::resolve_core_version(
    &stage.join("cores").join(&definition.core.kind),
    None,
    &definition.core.minecraft_version,
    definition.core.loader_version.as_deref(),
  )
  .await?;
  let jar = definition
    .core
    .jar
    .as_ref()
    .ok_or_else(|| io::Error::other("Paper entry point must be configured"))?;
  let core = stage.join(".paper-preparation");
  clone_tree(&resource_core, &core)?;
  eprintln!("{id}: preparing Paper/Mojang runtime dependencies");
  crate::dependency_cache::materialize(config, id, &core).await?;
  let baseline = crate::dependency_cache::snapshot(&core).await?;
  let output = crate::resources::logged_command(
    crate::resources::java_command(config, definition.core.java)
      .arg("-jar")
      .arg(core.join(jar))
      .arg("--help")
      .current_dir(&core)
      .stdin(std::process::Stdio::null()),
    Duration::from_secs(600),
  )
  .await?;
  if !output.status.success() {
    return Err(
      io::Error::other(format!(
        "{id}: Paper dependency preparation failed: {}",
        String::from_utf8_lossy(&output.stderr)
      ))
      .into(),
    );
  }
  let versions = core
    .join("versions")
    .join(&definition.core.minecraft_version);
  if !versions.is_dir()
    || std::fs::read_dir(&versions)?
      .filter_map(|e| e.ok())
      .all(|e| e.path().extension().is_none_or(|e| e != "jar"))
  {
    return Err(
      io::Error::other(format!(
        "{id}: Paper did not prepare a runnable Minecraft JAR"
      ))
      .into(),
    );
  }
  crate::dependency_cache::capture(config, id, &core, &baseline).await?;
  tokio::fs::remove_dir_all(core).await?;
  Ok(())
}

pub fn rollback(config: &LoadedConfig, id: &str) -> Result<()> {
  crate::config::validate_id(id)?;
  let _publish = PublicationLock::acquire(config, true)?;
  let cache = root(config);
  let backup = cache.join("rollback").join(id);
  if !backup.is_dir() {
    return Err(io::Error::other(format!("{id}: no prior published resources available")).into());
  }
  let current = server_root(config, id);
  let failed = cache.join("temporary").join(format!("failed-{id}"));
  std::fs::create_dir_all(failed.parent().unwrap())?;
  if failed.exists() {
    std::fs::remove_dir_all(&failed)?;
  }
  if current.exists() {
    std::fs::rename(&current, &failed)?;
  }
  std::fs::rename(backup, &current)?;
  let source = current.join("servers").join(format!("{id}.json"));
  atomic_json(
    &catalog(config).join("servers").join(format!("{id}.json")),
    &serde_json::from_slice::<serde_json::Value>(&std::fs::read(source)?)?,
  )?;
  atomic_json(
    &cache.join("current.json"),
    &ActiveGeneration {
      revision: Uuid::new_v4().to_string(),
    },
  )?;
  if failed.exists() {
    std::fs::remove_dir_all(failed)?;
  }
  Ok(())
}

pub fn gc(config: &LoadedConfig) -> Result<usize> {
  let cache = root(config);
  std::fs::create_dir_all(&cache)?;
  let _job = crate::resources::UpdateLock::acquire(&cache.join("update.lock"))?;
  let _publish = PublicationLock::acquire(config, true)?;
  let mut hashes = BTreeSet::new();
  for entry in std::fs::read_dir(cache.join("servers"))? {
    let path = entry?.path();
    for kind in ["cores", "mods", "plugins", "worlds"] {
      let manifest: Manifest =
        serde_json::from_slice(&std::fs::read(path.join(format!("{kind}.json")))?)?;
      for bundle in manifest.artifacts.values() {
        for file in &bundle.files {
          if file.sha256.len() == 64 {
            hashes.insert(file.sha256.clone());
          }
        }
      }
    }
  }
  let mut removed = 0;
  if let Ok(dirs) = std::fs::read_dir(cache.join("objects")) {
    for dir in dirs {
      let dir = dir?;
      let name = dir.file_name().to_string_lossy().to_string();
      if name.len() != 2 || !dir.file_type()?.is_dir() {
        continue;
      }
      for entry in std::fs::read_dir(dir.path())? {
        let entry = entry?;
        let name = entry.file_name().to_string_lossy().to_string();
        if name.len() == 64 && !hashes.contains(&name) {
          std::fs::remove_file(entry.path())?;
          removed += 1;
        }
      }
    }
  }
  // Published runtime JARs retain their hard links; deleting an unreferenced
  // cache name does not alter the inode used by a running JVM.
  if cache.join("temporary").exists() {
    std::fs::remove_dir_all(cache.join("temporary"))?;
  }
  for name in ["metadata", "objects/indices", "objects/fabric"] {
    let path = cache.join(name);
    if let Ok(entries) = std::fs::read_dir(&path) {
      for entry in entries {
        let entry = entry?;
        if !entry.file_type()?.is_file() {
          continue;
        }
        let keep = if name.ends_with("indices") {
          std::fs::read(entry.path())
            .ok()
            .and_then(|b| serde_json::from_slice::<crate::resources::ResourceFile>(&b).ok())
            .is_some_and(|f| hashes.contains(&f.sha256))
        } else if name.ends_with("fabric") {
          std::fs::read(entry.path())
            .ok()
            .and_then(|b| serde_json::from_slice::<Vec<crate::resources::ResourceFile>>(&b).ok())
            .is_some_and(|files| files.iter().all(|f| hashes.contains(&f.sha256)))
        } else {
          false
        };
        if !keep {
          std::fs::remove_file(entry.path())?;
        }
      }
    }
  }
  Ok(removed)
}

#[cfg(test)]
mod tests {
  use super::*;
  async fn fixture() -> (tempfile::TempDir, Arc<LoadedConfig>) {
    let (temp, config) = crate::resources::tests::fixture().await;
    let root = temp.path();
    let definition = root.join("servers/game.json");
    let mut value: serde_json::Value =
      serde_json::from_slice(&std::fs::read(&definition).unwrap()).unwrap();
    value["core"]["kind"] = "fabric".into();
    std::fs::write(definition, serde_json::to_vec(&value).unwrap()).unwrap();
    let path = root.join("cores.json");
    let mut manifest: Manifest = serde_json::from_slice(&std::fs::read(&path).unwrap()).unwrap();
    let bundle = manifest.artifacts.remove("paper/1.21.11").unwrap();
    manifest.artifacts.insert("fabric/1.21.11".into(), bundle);
    // An unused network source must never be contacted.
    manifest.artifacts.insert(
      "paper/unused".into(),
      crate::resources::Bundle {
        files: vec![crate::resources::ResourceFile {
          target: "bad.jar".into(),
          sha256: "a".repeat(64),
          sha512: None,
          size: 0,
          source: crate::resources::Source::Url {
            url: "https://unused.invalid/bad.jar".into(),
          },
        }],
        archive: None,
      },
    );
    std::fs::write(path, serde_json::to_vec(&manifest).unwrap()).unwrap();
    (temp, config)
  }
  #[tokio::test]
  async fn rollback_restores_previous_resources_and_file_layers() {
    let (temp, config) = fixture().await;
    let root = temp.path();
    std::fs::create_dir_all(root.join("files/game")).unwrap();
    std::fs::write(root.join("files/game/config.yml"), b"old settings").unwrap();
    let definition = root.join("servers/game.json");
    let mut value: serde_json::Value =
      serde_json::from_slice(&std::fs::read(&definition).unwrap()).unwrap();
    value["files"] = serde_json::json!([{"source":"game","target":"."}]);
    std::fs::write(&definition, serde_json::to_vec(&value).unwrap()).unwrap();
    let first = prepare(config.clone(), "game", false).await.unwrap();
    std::fs::write(root.join("files/game/config.yml"), b"new settings").unwrap();
    prepare(config.clone(), "game", true).await.unwrap();
    assert_eq!(
      std::fs::read(
        config
          .pinned_for("game")
          .resolve(&config.value.paths.files)
          .join("game/config.yml")
      )
      .unwrap(),
      b"new settings"
    );
    rollback(&config, "game").unwrap();
    assert_eq!(
      std::fs::read_to_string(server_root(&config, "game").join("revision")).unwrap(),
      first.revision
    );
    assert_eq!(
      std::fs::read(
        config
          .pinned_for("game")
          .resolve(&config.value.paths.files)
          .join("game/config.yml")
      )
      .unwrap(),
      b"old settings"
    );
  }
  #[cfg(unix)]
  #[tokio::test]
  async fn paper_dependencies_are_prepared_before_publication_and_reused() {
    use std::os::unix::fs::PermissionsExt;
    let (temp, config) = crate::resources::tests::fixture().await;
    let java = temp.path().join("java21/bin/java");
    std::fs::create_dir_all(java.parent().unwrap()).unwrap();
    std::fs::write(&java,b"#!/bin/sh\nmkdir -p versions/1.21.11 libraries/example\nprintf runtime > versions/1.21.11/paper.jar\nprintf library > libraries/example/lib.jar\necho 'prepared Minecraft'\n").unwrap();
    std::fs::set_permissions(java, std::fs::Permissions::from_mode(0o755)).unwrap();
    prepare(config.clone(), "game", false).await.unwrap();
    assert!(
      config
        .resolve(&config.value.paths.dependencies)
        .join("manifests/servers/game.json")
        .is_file()
    );
    assert!(
      server_root(&config, "game")
        .join("cores/paper/1.21.11/server.jar")
        .is_file()
    );
    assert!(
      !server_root(&config, "game")
        .join("cores/paper/1.21.11/versions")
        .exists()
    );
  }
  #[tokio::test]
  async fn per_server_publication_is_noop_when_unchanged_and_keeps_no_history() {
    let (_temp, config) = fixture().await;
    let first = prepare(config.clone(), "game", false).await.unwrap();
    assert!(first.changed);
    assert!(
      !prepare(config.clone(), "game", false)
        .await
        .unwrap()
        .changed
    );
    assert!(!root(&config).join("generations").exists());
    assert_eq!(config.server_ids().await.unwrap(), vec!["game"]);
    assert!(
      config
        .pinned_for("game")
        .resource_root("cores")
        .join("fabric/1.21.11/server.jar")
        .is_file()
    );
  }
  #[tokio::test]
  async fn local_refresh_replaces_files_without_changing_running_inodes() {
    let (temp, config) = fixture().await;
    prepare(config.clone(), "game", false).await.unwrap();
    let installed = server_root(&config, "game").join("cores/fabric/1.21.11/server.jar");
    let runtime = temp.path().join("running.jar");
    std::fs::hard_link(&installed, &runtime).unwrap();
    std::fs::write(temp.path().join("depend/new.jar"), b"core-v2").unwrap();
    std::fs::rename(
      temp.path().join("depend/new.jar"),
      temp.path().join("depend/server.jar"),
    )
    .unwrap();
    assert!(prepare(config.clone(), "game", true).await.unwrap().changed);
    assert_eq!(std::fs::read(installed).unwrap(), b"core-v2");
    assert_eq!(std::fs::read(runtime).unwrap(), b"core-v1");
  }
  #[tokio::test]
  async fn failed_prepare_keeps_the_previous_current_resources_and_cleans_temporary() {
    let (temp, config) = fixture().await;
    let first = prepare(config.clone(), "game", false).await.unwrap();
    std::fs::remove_file(temp.path().join("depend/server.jar")).unwrap();
    assert!(prepare(config.clone(), "game", false).await.is_err());
    assert_eq!(
      std::fs::read_to_string(server_root(&config, "game").join("revision")).unwrap(),
      first.revision
    );
    assert_eq!(
      std::fs::read(server_root(&config, "game").join("cores/fabric/1.21.11/server.jar")).unwrap(),
      b"core-v1"
    );
    assert_eq!(
      std::fs::read_dir(root(&config).join("temporary"))
        .unwrap()
        .count(),
      0
    );
  }
  #[tokio::test]
  async fn cache_gc_removes_unreferenced_objects_but_keeps_running_hard_links() {
    let (temp, config) = fixture().await;
    prepare(config.clone(), "game", false).await.unwrap();
    let object = root(&config).join("objects/aa").join("a".repeat(64));
    std::fs::create_dir_all(object.parent().unwrap()).unwrap();
    std::fs::write(&object, b"old").unwrap();
    let running = temp.path().join("running.jar");
    std::fs::hard_link(&object, &running).unwrap();
    assert_eq!(gc(&config).unwrap(), 1);
    assert!(!object.exists());
    assert_eq!(std::fs::read(running).unwrap(), b"old");
    assert!(server_root(&config, "game").exists());
  }
}
