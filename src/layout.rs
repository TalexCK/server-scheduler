use std::{
  collections::BTreeMap,
  path::{Path, PathBuf},
};

use thiserror::Error;
use tokio::fs;
use uuid::Uuid;

use crate::config::{
  ArtifactDefinition, FileDefinition, LoadedConfig, ModConfigTarget, ModDefinition,
  ServerDefinition, WorldDefinition,
};
use crate::dependency_cache::{self, DependencySnapshot};
use crate::solo::{self, SoloPersistence};

#[derive(Debug, Clone)]
pub struct PreparedInstance {
  pub server_id: String,
  pub instance_id: Uuid,
  pub port: u16,
  pub definition: ServerDefinition,
  pub work_dir: PathBuf,
  pub java_bin: PathBuf,
  pub server_jar: PathBuf,
  pub log_file: PathBuf,
  pub environment: BTreeMap<String, String>,
  pub dependency_snapshot: DependencySnapshot,
}

struct InstanceIdentity<'a> {
  definition_id: &'a str,
  runtime_id: &'a str,
  instance_id: Uuid,
  port: u16,
}

pub async fn prepare(
  config: &LoadedConfig,
  server_id: &str,
  port: u16,
) -> Result<PreparedInstance, LayoutError> {
  prepare_instance(config, server_id, server_id, port, None).await
}

pub async fn prepare_solo(
  config: &LoadedConfig,
  definition_id: &str,
  runtime_id: &str,
  port: u16,
  persistence: &SoloPersistence,
) -> Result<PreparedInstance, LayoutError> {
  prepare_instance(config, definition_id, runtime_id, port, Some(persistence)).await
}

async fn prepare_instance(
  config: &LoadedConfig,
  definition_id: &str,
  runtime_id: &str,
  port: u16,
  persistence: Option<&SoloPersistence>,
) -> Result<PreparedInstance, LayoutError> {
  let definition = config.load_server(definition_id).await?;
  let instance_id = Uuid::new_v4();
  let runtime_root = config.resolve(&config.value.paths.runtime);
  let logs_root = config.resolve(&config.value.paths.logs);
  fs::create_dir_all(&runtime_root)
    .await
    .map_err(|source| LayoutError::Io {
      path: runtime_root.clone(),
      source,
    })?;
  fs::create_dir_all(&logs_root)
    .await
    .map_err(|source| LayoutError::Io {
      path: logs_root.clone(),
      source,
    })?;
  let work_dir = runtime_root.join(format!("{runtime_id}-{instance_id}"));
  fs::create_dir(&work_dir)
    .await
    .map_err(|source| LayoutError::Io {
      path: work_dir.clone(),
      source,
    })?;

  let result = prepare_inner(
    config,
    InstanceIdentity {
      definition_id,
      runtime_id,
      instance_id,
      port,
    },
    definition,
    work_dir.clone(),
    logs_root,
    persistence,
  )
  .await;
  if result.is_err() {
    let _ = fs::remove_dir_all(&work_dir).await;
  }
  result
}

async fn prepare_inner(
  config: &LoadedConfig,
  identity: InstanceIdentity<'_>,
  definition: ServerDefinition,
  work_dir: PathBuf,
  logs_root: PathBuf,
  persistence: Option<&SoloPersistence>,
) -> Result<PreparedInstance, LayoutError> {
  let core_root = config
    .resolve(&config.value.paths.cores)
    .join(&definition.core.kind);
  let core = resolve_core_version(
    &core_root,
    None,
    &definition.core.minecraft_version,
    definition.core.loader_version.as_deref(),
  )
  .await?;
  link_contents(&core, &work_dir).await?;

  let plugins_dir = work_dir.join("plugins");
  let mods_dir = work_dir.join("mods");
  fs::create_dir_all(&plugins_dir)
    .await
    .map_err(|source| LayoutError::Io {
      path: plugins_dir.clone(),
      source,
    })?;
  fs::create_dir_all(&mods_dir)
    .await
    .map_err(|source| LayoutError::Io {
      path: mods_dir.clone(),
      source,
    })?;

  for plugin in &definition.plugins {
    install_plugin(config, &definition, plugin, &work_dir, &plugins_dir).await?;
  }
  for module in &definition.mods {
    install_mod(config, &definition, module, &work_dir, &mods_dir).await?;
  }

  if persistence.is_none()
    && let WorldDefinition::Template { id } = &definition.world
  {
    let source = config.resolve(&config.value.paths.worlds).join(id);
    ensure_exists(&source).await?;
    copy_contents(&source, &work_dir).await?;
  }

  for layer in &definition.files {
    install_file_layer(config, layer, &work_dir).await?;
  }

  dependency_cache::materialize(config, identity.definition_id, &work_dir).await?;

  if let Some(persistence) = persistence {
    solo::materialize_world(persistence, &work_dir.join("world")).await?;
  }

  write_properties(
    &work_dir.join("server.properties"),
    identity.port,
    !definition.core.kind.eq_ignore_ascii_case("velocity"),
    &definition.properties,
  )
  .await?;
  fs::write(work_dir.join("eula.txt"), b"eula=true\n")
    .await
    .map_err(|source| LayoutError::Io {
      path: work_dir.join("eula.txt"),
      source,
    })?;

  for path in &definition.render_files {
    render_file(
      &work_dir.join(path),
      config,
      &definition,
      identity.runtime_id,
      identity.instance_id,
      identity.port,
    )
    .await?;
  }

  let server_jar = match &definition.core.jar {
    Some(jar) => work_dir.join(jar),
    None => find_server_jar(&work_dir).await?,
  };
  ensure_exists(&server_jar).await?;
  let java_bin = config.java_bin(definition.core.java);
  ensure_exists(&java_bin).await?;
  let environment = definition
    .environment
    .iter()
    .map(|(name, value)| {
      render_value(
        value,
        config,
        &definition,
        identity.runtime_id,
        identity.instance_id,
        identity.port,
      )
      .map(|value| (name.clone(), value))
    })
    .collect::<Result<BTreeMap<_, _>, _>>()?;
  let dependency_snapshot = dependency_cache::snapshot(&work_dir).await?;

  Ok(PreparedInstance {
    server_id: identity.runtime_id.to_string(),
    instance_id: identity.instance_id,
    port: identity.port,
    definition,
    work_dir,
    java_bin,
    server_jar,
    log_file: logs_root.join(format!(
      "{}-{}.log",
      identity.runtime_id, identity.instance_id
    )),
    environment,
    dependency_snapshot,
  })
}

async fn install_plugin(
  config: &LoadedConfig,
  server: &ServerDefinition,
  plugin: &ArtifactDefinition,
  work_dir: &Path,
  target: &Path,
) -> Result<(), LayoutError> {
  let root = config.resolve(&config.value.paths.plugins).join(&plugin.id);
  let artifact = resolve_artifact_version(
    &root,
    plugin.version.as_deref(),
    &server.core.minecraft_version,
  )
  .await?;
  link_contents(&artifact, target).await?;
  if let Some(config) = &plugin.config {
    let source = root.join(config);
    ensure_exists(&source).await?;
    let destination = plugin
      .config_target
      .as_ref()
      .map(|path| work_dir.join(path))
      .unwrap_or_else(|| target.join(&plugin.id));
    copy_contents(&source, &destination).await?;
  }
  Ok(())
}

async fn install_mod(
  config: &LoadedConfig,
  server: &ServerDefinition,
  module: &ModDefinition,
  work_dir: &Path,
  mods_dir: &Path,
) -> Result<(), LayoutError> {
  let root = config.resolve(&config.value.paths.mods).join(&module.id);
  let artifact = resolve_artifact_version(
    &root,
    module.version.as_deref(),
    &server.core.minecraft_version,
  )
  .await?;
  link_contents(&artifact, mods_dir).await?;
  if let Some(config) = &module.config {
    let source = root.join(config);
    ensure_exists(&source).await?;
    let target = module.config_path.as_ref().map_or_else(
      || match module.config_target {
        ModConfigTarget::Config => work_dir.join("config"),
        ModConfigTarget::Mods => mods_dir.join(&module.id),
      },
      |path| work_dir.join(path),
    );
    copy_contents(&source, &target).await?;
  }
  Ok(())
}

async fn install_file_layer(
  config: &LoadedConfig,
  layer: &FileDefinition,
  work_dir: &Path,
) -> Result<(), LayoutError> {
  let source = config
    .resolve(&config.value.paths.files)
    .join(&layer.source);
  ensure_exists(&source).await?;
  let target = if layer.target.as_os_str().is_empty() || layer.target == Path::new(".") {
    work_dir.to_path_buf()
  } else {
    work_dir.join(&layer.target)
  };
  copy_contents(&source, &target).await
}

async fn render_file(
  path: &Path,
  config: &LoadedConfig,
  definition: &ServerDefinition,
  server_id: &str,
  instance_id: Uuid,
  port: u16,
) -> Result<(), LayoutError> {
  let value = fs::read_to_string(path)
    .await
    .map_err(|source| LayoutError::Io {
      path: path.to_path_buf(),
      source,
    })?;
  let rendered = render_value(&value, config, definition, server_id, instance_id, port)?;
  fs::write(path, rendered)
    .await
    .map_err(|source| LayoutError::Io {
      path: path.to_path_buf(),
      source,
    })
}

fn render_value(
  value: &str,
  config: &LoadedConfig,
  definition: &ServerDefinition,
  server_id: &str,
  instance_id: Uuid,
  port: u16,
) -> Result<String, LayoutError> {
  let mut rendered = value
    .replace("${SERVER_ID}", server_id)
    .replace("${INSTANCE_ID}", &instance_id.to_string())
    .replace("${SERVER_PORT}", &port.to_string())
    .replace("${POSTGRES_HOST}", &config.value.postgresql.host)
    .replace(
      "${POSTGRES_PORT}",
      &config.value.postgresql.port.to_string(),
    )
    .replace("${POSTGRES_USER}", &config.value.postgresql.username)
    .replace("${POSTGRES_PASSWORD}", &config.value.postgresql.password);
  if rendered.contains("${RESOURCE_PACK_URL}") {
    let value = definition
      .properties
      .get("resource-pack")
      .filter(|value| !value.is_empty())
      .ok_or_else(|| LayoutError::MissingTemplateValue("resource-pack".into()))?;
    rendered = rendered.replace("${RESOURCE_PACK_URL}", value);
  }
  if rendered.contains("${RESOURCE_PACK_SHA1}") {
    let value = definition
      .properties
      .get("resource-pack-sha1")
      .filter(|value| !value.is_empty())
      .ok_or_else(|| LayoutError::MissingTemplateValue("resource-pack-sha1".into()))?;
    rendered = rendered.replace("${RESOURCE_PACK_SHA1}", value);
  }
  if rendered.contains("${") {
    return Err(LayoutError::InvalidTemplate(rendered));
  }
  Ok(rendered)
}

async fn resolve_core_version(
  root: &Path,
  explicit: Option<&str>,
  minecraft_version: &str,
  loader_version: Option<&str>,
) -> Result<PathBuf, LayoutError> {
  let mut candidates = Vec::new();
  if let Some(version) = explicit {
    candidates.push(version.to_string());
  } else {
    if let Some(loader) = loader_version {
      candidates.push(format!("{minecraft_version}-{loader}"));
    }
    candidates.push(minecraft_version.to_string());
    if let Some(loader) = loader_version {
      candidates.push(loader.to_string());
    }
    candidates.push("common".into());
  }

  for candidate in &candidates {
    let path = root.join(candidate);
    if fs::try_exists(&path)
      .await
      .map_err(|source| LayoutError::Io {
        path: path.clone(),
        source,
      })?
    {
      return Ok(path);
    }
  }

  Err(LayoutError::VersionNotFound {
    root: root.to_path_buf(),
    candidates,
  })
}

async fn resolve_artifact_version(
  root: &Path,
  explicit: Option<&str>,
  minecraft_version: &str,
) -> Result<PathBuf, LayoutError> {
  if let Some(version) = explicit {
    let path = root.join(version);
    if fs::try_exists(&path)
      .await
      .map_err(|source| LayoutError::Io {
        path: path.clone(),
        source,
      })?
    {
      return Ok(path);
    }
    return Err(LayoutError::VersionNotFound {
      root: root.to_path_buf(),
      candidates: vec![version.to_string()],
    });
  }

  let exact = root.join(minecraft_version);
  if fs::try_exists(&exact)
    .await
    .map_err(|source| LayoutError::Io {
      path: exact.clone(),
      source,
    })?
  {
    return Ok(exact);
  }

  let current = parse_minecraft_version(minecraft_version);
  let mut ranges = Vec::new();
  let mut discovered = vec![minecraft_version.to_string()];
  if fs::try_exists(root)
    .await
    .map_err(|source| LayoutError::Io {
      path: root.to_path_buf(),
      source,
    })?
  {
    let mut entries = fs::read_dir(root).await.map_err(|source| LayoutError::Io {
      path: root.to_path_buf(),
      source,
    })?;
    while let Some(entry) = entries
      .next_entry()
      .await
      .map_err(|source| LayoutError::Io {
        path: root.to_path_buf(),
        source,
      })?
    {
      if !entry
        .file_type()
        .await
        .map_err(|source| LayoutError::Io {
          path: entry.path(),
          source,
        })?
        .is_dir()
      {
        continue;
      }
      let name = entry.file_name().to_string_lossy().to_string();
      let Some((lower, upper)) = parse_minecraft_range(&name) else {
        continue;
      };
      discovered.push(name.clone());
      if current
        .as_ref()
        .is_some_and(|version| lower <= *version && *version <= upper)
      {
        ranges.push((lower, upper, name, entry.path()));
      }
    }
  }
  ranges.sort_by(|left, right| {
    right
      .0
      .cmp(&left.0)
      .then_with(|| left.1.cmp(&right.1))
      .then_with(|| left.2.cmp(&right.2))
  });
  if let Some((_, _, _, path)) = ranges.into_iter().next() {
    return Ok(path);
  }

  let common = root.join("common");
  discovered.push("common".to_string());
  if fs::try_exists(&common)
    .await
    .map_err(|source| LayoutError::Io {
      path: common.clone(),
      source,
    })?
  {
    return Ok(common);
  }
  Err(LayoutError::VersionNotFound {
    root: root.to_path_buf(),
    candidates: discovered,
  })
}

fn parse_minecraft_range(value: &str) -> Option<(Vec<u32>, Vec<u32>)> {
  let (lower, upper) = value.split_once('-')?;
  let lower = parse_minecraft_version(lower)?;
  let upper = parse_minecraft_version(upper)?;
  (lower <= upper).then_some((lower, upper))
}

fn parse_minecraft_version(value: &str) -> Option<Vec<u32>> {
  let parts = value
    .split('.')
    .map(str::parse::<u32>)
    .collect::<Result<Vec<_>, _>>()
    .ok()?;
  (parts.len() >= 2 && parts.len() <= 3).then_some(parts)
}

async fn copy_contents(source: &Path, target: &Path) -> Result<(), LayoutError> {
  transfer_contents(source, target, false).await
}

async fn link_contents(source: &Path, target: &Path) -> Result<(), LayoutError> {
  transfer_contents(source, target, true).await
}

async fn transfer_contents(
  source: &Path,
  target: &Path,
  prefer_link: bool,
) -> Result<(), LayoutError> {
  let metadata = fs::symlink_metadata(source)
    .await
    .map_err(|error| LayoutError::Io {
      path: source.to_path_buf(),
      source: error,
    })?;
  if metadata.file_type().is_symlink() {
    return Err(LayoutError::Symlink(source.to_path_buf()));
  }
  if metadata.is_file() {
    fs::create_dir_all(target)
      .await
      .map_err(|error| LayoutError::Io {
        path: target.to_path_buf(),
        source: error,
      })?;
    let name = source
      .file_name()
      .ok_or_else(|| LayoutError::InvalidPath(source.to_path_buf()))?;
    let destination = target.join(name);
    transfer_file(source, &destination, prefer_link).await?;
    return Ok(());
  }

  fs::create_dir_all(target)
    .await
    .map_err(|error| LayoutError::Io {
      path: target.to_path_buf(),
      source: error,
    })?;
  let mut pending = vec![(source.to_path_buf(), target.to_path_buf())];
  while let Some((current_source, current_target)) = pending.pop() {
    let mut entries = fs::read_dir(&current_source)
      .await
      .map_err(|error| LayoutError::Io {
        path: current_source.clone(),
        source: error,
      })?;
    while let Some(entry) = entries
      .next_entry()
      .await
      .map_err(|error| LayoutError::Io {
        path: current_source.clone(),
        source: error,
      })?
    {
      let source_path = entry.path();
      let target_path = current_target.join(entry.file_name());
      let entry_type = entry.file_type().await.map_err(|error| LayoutError::Io {
        path: source_path.clone(),
        source: error,
      })?;
      if entry_type.is_symlink() {
        return Err(LayoutError::Symlink(source_path));
      }
      if entry_type.is_dir() {
        fs::create_dir_all(&target_path)
          .await
          .map_err(|error| LayoutError::Io {
            path: target_path.clone(),
            source: error,
          })?;
        pending.push((source_path, target_path));
      } else if entry_type.is_file() {
        transfer_file(&source_path, &target_path, prefer_link).await?;
      }
    }
  }
  Ok(())
}

async fn transfer_file(source: &Path, target: &Path, prefer_link: bool) -> Result<(), LayoutError> {
  if prefer_link {
    if fs::try_exists(target)
      .await
      .map_err(|error| LayoutError::Io {
        path: target.to_path_buf(),
        source: error,
      })?
    {
      fs::remove_file(target)
        .await
        .map_err(|error| LayoutError::Io {
          path: target.to_path_buf(),
          source: error,
        })?;
    }
    if fs::hard_link(source, target).await.is_ok() {
      return Ok(());
    }
  }
  fs::copy(source, target)
    .await
    .map(|_| ())
    .map_err(|error| LayoutError::Io {
      path: target.to_path_buf(),
      source: error,
    })
}

async fn write_properties(
  path: &Path,
  port: u16,
  bind_to_loopback: bool,
  overrides: &BTreeMap<String, String>,
) -> Result<(), LayoutError> {
  let mut properties = BTreeMap::new();
  if fs::try_exists(path)
    .await
    .map_err(|source| LayoutError::Io {
      path: path.to_path_buf(),
      source,
    })?
  {
    let existing = fs::read_to_string(path)
      .await
      .map_err(|source| LayoutError::Io {
        path: path.to_path_buf(),
        source,
      })?;
    for line in existing.lines() {
      if let Some((key, value)) = line.split_once('=') {
        properties.insert(key.trim().to_string(), value.trim().to_string());
      }
    }
  }
  for (key, value) in overrides {
    properties.insert(key.clone(), value.clone());
  }
  properties.insert("server-port".into(), port.to_string());
  if bind_to_loopback {
    properties.insert("server-ip".into(), "127.0.0.1".into());
  } else {
    properties.remove("server-ip");
  }
  let data = properties
    .into_iter()
    .map(|(key, value)| format!("{key}={value}\n"))
    .collect::<String>();
  fs::write(path, data)
    .await
    .map_err(|source| LayoutError::Io {
      path: path.to_path_buf(),
      source,
    })
}

async fn find_server_jar(work_dir: &Path) -> Result<PathBuf, LayoutError> {
  let mut entries = fs::read_dir(work_dir)
    .await
    .map_err(|source| LayoutError::Io {
      path: work_dir.to_path_buf(),
      source,
    })?;
  let mut jars = Vec::new();
  while let Some(entry) = entries
    .next_entry()
    .await
    .map_err(|source| LayoutError::Io {
      path: work_dir.to_path_buf(),
      source,
    })?
  {
    let path = entry.path();
    if entry
      .file_type()
      .await
      .map_err(|source| LayoutError::Io {
        path: path.clone(),
        source,
      })?
      .is_file()
      && path
        .extension()
        .and_then(|value| value.to_str())
        .is_some_and(|value| value.eq_ignore_ascii_case("jar"))
    {
      jars.push(path);
    }
  }
  match jars.len() {
    1 => Ok(jars.remove(0)),
    0 => Err(LayoutError::ServerJarNotFound(work_dir.to_path_buf())),
    _ => Err(LayoutError::MultipleServerJars(jars)),
  }
}

async fn ensure_exists(path: &Path) -> Result<(), LayoutError> {
  if fs::try_exists(path)
    .await
    .map_err(|source| LayoutError::Io {
      path: path.to_path_buf(),
      source,
    })?
  {
    Ok(())
  } else {
    Err(LayoutError::Missing(path.to_path_buf()))
  }
}

#[derive(Debug, Error)]
pub enum LayoutError {
  #[error(transparent)]
  Dependency(#[from] dependency_cache::DependencyError),
  #[error(transparent)]
  Config(#[from] crate::config::ConfigError),
  #[error(transparent)]
  Solo(#[from] crate::solo::SoloError),
  #[error("failed to access {path}: {source}")]
  Io {
    path: PathBuf,
    source: std::io::Error,
  },
  #[error("no compatible version exists under {root}; tried {candidates:?}")]
  VersionNotFound {
    root: PathBuf,
    candidates: Vec<String>,
  },
  #[error("symlinks are not allowed in server assets: {0}")]
  Symlink(PathBuf),
  #[error("invalid path: {0}")]
  InvalidPath(PathBuf),
  #[error("required path does not exist: {0}")]
  Missing(PathBuf),
  #[error("no server JAR exists in {0}")]
  ServerJarNotFound(PathBuf),
  #[error("multiple server JAR files exist: {0:?}")]
  MultipleServerJars(Vec<PathBuf>),
  #[error("required template value is not configured: {0}")]
  MissingTemplateValue(String),
  #[error("invalid template expression in value: {0}")]
  InvalidTemplate(String),
}

#[cfg(test)]
mod tests {
  use super::*;
  use crate::config::LoadedConfig;

  #[tokio::test]
  async fn version_resolution_prefers_combined_version() {
    let directory = tempfile::tempdir().unwrap();
    fs::create_dir_all(directory.path().join("1.21.11-26.2"))
      .await
      .unwrap();
    fs::create_dir_all(directory.path().join("1.21.11"))
      .await
      .unwrap();
    let resolved = resolve_core_version(directory.path(), None, "1.21.11", Some("26.2"))
      .await
      .unwrap();
    assert!(resolved.ends_with("1.21.11-26.2"));
  }

  #[tokio::test]
  async fn artifact_resolution_prefers_exact_minecraft_version() {
    let directory = tempfile::tempdir().unwrap();
    fs::create_dir_all(directory.path().join("1.21.11"))
      .await
      .unwrap();
    fs::create_dir_all(directory.path().join("1.21.11-26.2"))
      .await
      .unwrap();
    let resolved = resolve_artifact_version(directory.path(), None, "1.21.11")
      .await
      .unwrap();
    assert!(resolved.ends_with("1.21.11"));
  }

  #[tokio::test]
  async fn artifact_resolution_supports_minecraft_version_ranges() {
    let directory = tempfile::tempdir().unwrap();
    fs::create_dir_all(directory.path().join("1.21.11-26.2"))
      .await
      .unwrap();
    fs::create_dir_all(directory.path().join("common"))
      .await
      .unwrap();
    let resolved = resolve_artifact_version(directory.path(), None, "26.1.2")
      .await
      .unwrap();
    assert!(resolved.ends_with("1.21.11-26.2"));
  }

  #[tokio::test]
  async fn prepare_builds_complete_server_layout() {
    let directory = tempfile::tempdir().unwrap();
    let root = directory.path();
    fs::create_dir_all(root.join("cores/paper/1.21.11"))
      .await
      .unwrap();
    fs::create_dir_all(root.join("plugins/example/1.21.11"))
      .await
      .unwrap();
    fs::create_dir_all(root.join("plugins/example/profile"))
      .await
      .unwrap();
    fs::create_dir_all(root.join("mods/example/1.21.11-26.2"))
      .await
      .unwrap();
    fs::create_dir_all(root.join("mods/example/profile"))
      .await
      .unwrap();
    fs::create_dir_all(root.join("worlds/arena/world"))
      .await
      .unwrap();
    fs::create_dir_all(root.join("java21/bin")).await.unwrap();
    fs::create_dir_all(root.join("java25/bin")).await.unwrap();
    fs::create_dir_all(root.join("servers")).await.unwrap();
    fs::write(root.join("cores/paper/1.21.11/paper.jar"), b"core")
      .await
      .unwrap();
    fs::write(root.join("plugins/example/1.21.11/example.jar"), b"plugin")
      .await
      .unwrap();
    fs::write(
      root.join("plugins/example/profile/config.yml"),
      b"plugin-config",
    )
    .await
    .unwrap();
    fs::write(root.join("mods/example/1.21.11-26.2/example.jar"), b"mod")
      .await
      .unwrap();
    fs::write(
      root.join("mods/example/profile/settings.toml"),
      b"mod-config",
    )
    .await
    .unwrap();
    fs::write(root.join("worlds/arena/world/level.dat"), b"world")
      .await
      .unwrap();
    fs::write(root.join("java21/bin/java"), b"java")
      .await
      .unwrap();
    fs::write(root.join("java25/bin/java"), b"java")
      .await
      .unwrap();
    fs::write(
            root.join("config.json"),
            serde_json::to_vec(&serde_json::json!({
                "api":{"port":25566,"token":"01234567890123456789012345678901"},
                "bridge":{"token":"11234567890123456789012345678901","heartbeat_timeout_seconds":45,"transfer_retry_seconds":30},
                "postgresql":{"home":"postgres","host":"127.0.0.1","port":5432,"username":"minigames","password":"test-password"},
                "ports":{"velocity":25565,"start":50001,"end":59999},
                "java":{"21_home":"java21","25_home":"java25"},
                "paths":{"cores":"cores","plugins":"plugins","mods":"mods","servers":"servers","worlds":"worlds","runtime":"runtime","logs":"logs","pid_file":"scheduler.pid","scheduler_log":"scheduler.log"}
            }))
            .unwrap(),
        )
        .await
        .unwrap();
    fs::write(
            root.join("servers/game.json"),
            serde_json::to_vec(&serde_json::json!({
                "core":{"kind":"paper","minecraft_version":"1.21.11","loader_version":"26.2","java":"21","jar":"paper.jar"},
                "memory":{"initial_mib":1024,"maximum_mib":4096},
                "plugins":[{"id":"example","config":"profile"}],
                "mods":[{"id":"example","config":"profile","config_target":"config"}],
                "properties":{"server-ip":"0.0.0.0"},
                "world":{"mode":"template","id":"arena"}
            }))
            .unwrap(),
        )
        .await
        .unwrap();

    let config = LoadedConfig::load(root.join("config.json")).await.unwrap();
    assert!(!config.value.enable_frpc);
    assert_eq!(config.value.frpc_core, PathBuf::from("frpc"));
    assert_eq!(config.value.paths.network, PathBuf::from("network"));
    assert_eq!(
      config.value.paths.dependencies,
      PathBuf::from("dependencies")
    );
    let prepared = prepare(&config, "game", 25567).await.unwrap();

    assert!(prepared.work_dir.join("paper.jar").exists());
    assert!(prepared.work_dir.join("plugins/example.jar").exists());
    assert!(
      prepared
        .work_dir
        .join("plugins/example/config.yml")
        .exists()
    );
    assert!(prepared.work_dir.join("mods/example.jar").exists());
    assert!(prepared.work_dir.join("config/settings.toml").exists());
    assert!(prepared.work_dir.join("world/level.dat").exists());
    let properties = fs::read_to_string(prepared.work_dir.join("server.properties"))
      .await
      .unwrap();
    assert!(properties.contains("server-ip=127.0.0.1\n"));
    assert_eq!(prepared.port, 25567);
  }

  #[tokio::test]
  async fn velocity_properties_remove_server_ip() {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("server.properties");
    fs::write(&path, b"server-ip=0.0.0.0\n").await.unwrap();
    let mut overrides = BTreeMap::new();
    overrides.insert("server-ip".into(), "127.0.0.1".into());

    write_properties(&path, 25565, false, &overrides)
      .await
      .unwrap();

    let properties = fs::read_to_string(&path).await.unwrap();
    assert!(!properties.contains("server-ip="));
    assert!(properties.contains("server-port=25565\n"));
  }
}
