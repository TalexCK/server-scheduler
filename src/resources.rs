//! Verified resource generations. Publishing current.json is the only activation step.
use crate::config::{LoadedConfig, ServerDefinition, validate_id, validate_version};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::{
  collections::BTreeMap,
  io::{self, Read, Write},
  path::{Component, Path, PathBuf},
  sync::Arc,
  time::Duration,
};
use tokio::sync::Mutex;
use uuid::Uuid;

const KINDS: [&str; 4] = ["cores", "mods", "plugins", "worlds"];
const USER_AGENT: &str =
  "SHTechCraft-Scheduler/0.2 (https://github.com/TalexCK/shtechcraft-server)";
type Result<T> = std::result::Result<T, Box<dyn std::error::Error + Send + Sync>>;

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Manifest {
  pub schema_version: u32,
  pub artifacts: BTreeMap<String, Bundle>,
}
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Bundle {
  pub files: Vec<ResourceFile>,
  #[serde(default, skip_serializing_if = "Option::is_none")]
  pub archive: Option<PathBuf>,
}
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ResourceFile {
  pub target: PathBuf,
  #[serde(default, skip_serializing_if = "String::is_empty")]
  pub sha256: String,
  #[serde(default, skip_serializing_if = "Option::is_none")]
  pub sha512: Option<String>,
  #[serde(default)]
  pub size: u64,
  pub source: Source,
}
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "lowercase", deny_unknown_fields)]
pub enum Source {
  #[serde(rename = "spigot_latest")]
  SpigotLatest {
    resource_id: u64,
    game_version: String,
  },
  #[serde(rename = "curseforge_latest")]
  CurseforgeLatest {
    project_id: u64,
    game_version: String,
    loader_type: u32,
  },
  #[serde(rename = "modrinth_latest")]
  ModrinthLatest {
    project: String,
    game_version: String,
    loader: String,
  },
  #[serde(rename = "paper")]
  PaperLatest {
    project: String,
    game_version: String,
  },
  #[serde(rename = "fabric")]
  FabricLatest {
    game_version: String,
  },
  File {
    path: PathBuf,
  },
  Url {
    url: String,
  },
  Modrinth {
    project: String,
    version: String,
    filename: String,
  },
  Curseforge {
    project_id: u64,
    file_id: u64,
  },
  Spigot {
    resource_id: u64,
    version_id: u64,
  },
}
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ActiveGeneration {
  pub revision: String,
}

pub fn active_root(config: &LoadedConfig) -> Option<PathBuf> {
  if let Some(root) = &config.generation {
    return Some(root.clone());
  }
  let root = config.resolve(&config.value.paths.resources);
  if root.join("catalog").is_dir() && root.join("current.json").is_file() {
    return Some(root.join("catalog"));
  }
  match std::fs::read(root.join("current.json")) {
    Ok(bytes) => {
      let revision = serde_json::from_slice::<ActiveGeneration>(&bytes)
        .ok()
        .and_then(|v| Uuid::parse_str(&v.revision).ok());
      // A damaged pointer must fail closed, never fall back to a different deployment.
      Some(
        root.join("generations").join(
          revision
            .map(|v| v.to_string())
            .unwrap_or_else(|| "invalid-active-generation".into()),
        ),
      )
    }
    Err(e) if e.kind() == io::ErrorKind::NotFound => None,
    Err(_) => Some(root.join("unreadable-active-generation")),
  }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct UpdateStatus {
  pub id: String,
  pub phase: String,
  pub completed: usize,
  pub total: usize,
  pub message: String,
  pub revision: Option<String>,
}
#[derive(Clone)]
pub struct Updater {
  config: Arc<LoadedConfig>,
  status: Arc<Mutex<Option<UpdateStatus>>>,
}
impl Updater {
  pub fn new(config: Arc<LoadedConfig>) -> Self {
    Self {
      config,
      status: Arc::new(Mutex::new(None)),
    }
  }
  pub async fn status(&self) -> Option<UpdateStatus> {
    let path = self
      .config
      .resolve(&self.config.value.paths.manifests)
      .join("state/update.json");
    if let Ok(bytes) = tokio::fs::read(&path).await {
      if let Ok(mut status) = serde_json::from_slice::<UpdateStatus>(&bytes) {
        if status.phase == "queued" {
          let age = std::fs::metadata(&path)
            .and_then(|m| m.modified())
            .ok()
            .and_then(|t| t.elapsed().ok());
          if age.is_some_and(|a| a.as_secs() > 30) {
            status.phase = "failed".into();
            status.message =
              "Update launcher did not start its worker; inspect logs/update-launch.log".into();
          }
        }
        #[cfg(unix)]
        if matches!(status.phase.as_str(), "running" | "waiting") {
          let value: serde_json::Value = serde_json::from_slice(&bytes).ok()?;
          if let Some(pid) = value["pid"].as_i64() {
            if unsafe { libc::kill(pid as i32, 0) } != 0
              && io::Error::last_os_error().raw_os_error() == Some(libc::ESRCH)
            {
              status.phase = "failed".into();
              status.message = "Update worker exited unexpectedly; inspect logs/update.log".into();
            }
          }
        }
        return Some(status);
      }
    }
    self.status.lock().await.clone()
  }
  pub async fn start(&self) -> UpdateStatus {
    if let Some(current) = self
      .status()
      .await
      .filter(|s| matches!(s.phase.as_str(), "queued" | "running" | "waiting"))
    {
      return current;
    }
    let mut state = self.status.lock().await;
    if let Some(current) = state.as_ref().filter(|s| s.phase == "running") {
      return current.clone();
    }
    let root = self.config.resolve(&self.config.value.paths.manifests);
    let mut next = UpdateStatus {
      id: Uuid::new_v4().to_string(),
      phase: "queued".into(),
      completed: 0,
      total: 1,
      message: "Starting independent deployment worker; log: logs/update.log".into(),
      revision: None,
    };
    let task_state = self.status.clone();
    let task_id = next.id.clone();
    let result = (|| -> io::Result<()> {
      let log_path = self
        .config
        .resolve(&self.config.value.paths.logs)
        .join("update-launch.log");
      std::fs::create_dir_all(log_path.parent().unwrap())?;
      let log = std::fs::OpenOptions::new()
        .create(true)
        .append(true)
        .open(log_path)?;
      let mut command = std::process::Command::new(root.join("server.sh"));
      command
        .arg("update")
        .arg("--background")
        .current_dir(&root)
        .stdin(std::process::Stdio::null())
        .stdout(log.try_clone()?)
        .stderr(log);
      #[cfg(unix)]
      {
        use std::os::unix::process::CommandExt;
        command.process_group(0);
      }
      let mut child = command.spawn()?;
      // Reap the short-lived launcher; the worker has its own session and lock.
      std::thread::spawn(move || {
        let result = child.wait();
        if !result.as_ref().is_ok_and(|status| status.success()) {
          let mut state = task_state.blocking_lock();
          if let Some(status) = state.as_mut().filter(|s| s.id == task_id) {
            status.phase = "failed".into();
            status.message =
              format!("Update launcher failed: {result:?}; inspect logs/update-launch.log");
          }
        }
      });
      Ok(())
    })();
    if let Err(error) = result {
      next.phase = "failed".into();
      next.message = format!("Unable to launch deployment update: {error}");
    }
    *state = Some(next.clone());
    next
  }
  #[cfg(test)]
  pub async fn start_legacy(&self) -> UpdateStatus {
    let mut status = self.status.lock().await;
    if let Some(current) = status.as_ref().filter(|s| s.phase == "running") {
      return current.clone();
    }
    let next = UpdateStatus {
      id: Uuid::new_v4().to_string(),
      phase: "running".into(),
      completed: 0,
      total: 1,
      message: format!(
        "Running ./server.sh update; output: {}",
        self
          .config
          .resolve(&self.config.value.paths.logs)
          .join("resource-update.log")
          .display()
      ),
      revision: None,
    };
    *status = Some(next.clone());
    let updater = self.clone();
    tokio::spawn(async move {
      let result = updater.run_server_update().await;
      let mut status = updater.status.lock().await;
      let s = status.as_mut().unwrap();
      match result {
        Ok(revision) => {
          s.phase = "complete".into();
          s.completed = 1;
          s.message =
            "Resources and server catalog activated; running instances keep their generation"
              .into();
          s.revision = Some(revision);
        }
        Err(error) => {
          tracing::error!(%error,"resource update failed");
          s.phase = "failed".into();
          s.message = error.to_string();
        }
      }
    });
    next
  }

  #[cfg(test)]
  async fn run_server_update(&self) -> Result<String> {
    // Runtime configuration can live under database/. The manifests path names
    // the deployment root containing server.sh, independently of the daemon cwd.
    let root = self.config.resolve(&self.config.value.paths.manifests);
    let script = root.join("server.sh");
    let log_path = self
      .config
      .resolve(&self.config.value.paths.logs)
      .join("resource-update.log");
    tokio::fs::create_dir_all(log_path.parent().unwrap()).await?;
    let log = std::fs::OpenOptions::new()
      .create(true)
      .truncate(true)
      .write(true)
      .open(&log_path)?;
    let status = tokio::process::Command::new(&script)
      .arg("update")
      .current_dir(&root)
      .stdin(std::process::Stdio::null())
      .stdout(log.try_clone()?)
      .stderr(log)
      .kill_on_drop(true)
      .status()
      .await
      .map_err(|error| {
        io::Error::other(format!(
          "failed to execute {} update: {error}",
          script.display()
        ))
      })?;
    if !status.success() {
      let mut output = std::fs::File::open(&log_path)?;
      use std::io::{Seek, SeekFrom};
      let length = output.metadata()?.len();
      output.seek(SeekFrom::Start(length.saturating_sub(4096)))?;
      let mut tail = Vec::new();
      output.read_to_end(&mut tail)?;
      return Err(
        io::Error::other(format!(
          "./server.sh update failed ({status}): {}\nOutput: {}",
          String::from_utf8_lossy(&tail).trim(),
          log_path.display()
        ))
        .into(),
      );
    }
    let pointer = self
      .config
      .resolve(&self.config.value.paths.resources)
      .join("current.json");
    let active: ActiveGeneration = serde_json::from_slice(&tokio::fs::read(pointer).await?)?;
    Uuid::parse_str(&active.revision)?;
    Ok(active.revision)
  }
  async fn progress(&self, message: String) {
    eprintln!("{message}");
    report_worker_message(&message);
    if let Some(s) = self.status.lock().await.as_mut() {
      s.completed += 1;
      s.message = message;
    }
  }
  #[cfg(test)]
  pub async fn run(&self) -> Result<String> {
    let config = &self.config;
    let resource_root = config.resolve(&config.value.paths.resources);
    tokio::fs::create_dir_all(resource_root.join("generations")).await?;
    let _lock = UpdateLock::acquire(&resource_root.join("update.lock"))?;
    let revision = Uuid::new_v4().to_string();
    let staging = resource_root
      .join("generations")
      .join(format!(".staging-{revision}"));
    tokio::fs::create_dir(&staging).await?;
    let result = self.prepare(&staging, None).await;
    if let Err(error) = result {
      let _ = tokio::fs::remove_dir_all(&staging).await;
      return Err(error);
    }
    let generation = resource_root.join("generations").join(&revision);
    tokio::fs::rename(&staging, &generation).await?;
    // All content is immutable after activation. Existing instances retain hard links.
    atomic_json(
      &resource_root.join("current.json"),
      &ActiveGeneration {
        revision: revision.clone(),
      },
    )?;
    Ok(revision)
  }
  pub(crate) async fn prepare(&self, staging: &Path, server: Option<&str>) -> Result<()> {
    let config = &self.config;
    let mut manifests = BTreeMap::new();
    for kind in KINDS {
      let bytes = tokio::fs::read(
        config
          .resolve(&config.value.paths.manifests)
          .join(format!("{kind}.json")),
      )
      .await?;
      let manifest: Manifest = serde_json::from_slice(&bytes)?;
      validate_manifest(kind, &manifest)?;
      tokio::fs::write(staging.join(format!("{kind}.json")), bytes).await?;
      manifests.insert(kind, manifest);
    }
    if let Some(id) = server {
      crate::deployment::select_manifests(config, staging, id, &mut manifests).await?;
    }
    let total = manifests
      .values()
      .flat_map(|m| m.artifacts.values())
      .map(|b| b.files.len())
      .sum();
    if let Some(s) = self.status.lock().await.as_mut() {
      s.total = total;
    }
    let client = reqwest::Client::builder()
      .user_agent(USER_AGENT)
      .timeout(Duration::from_secs(300))
      .build()?;
    for (kind, manifest) in &mut manifests {
      tokio::fs::create_dir_all(staging.join(kind)).await?;
      for (id, bundle) in &mut manifest.artifacts {
        let mut prepared = Vec::new();
        for declared in &bundle.files {
          let message = format!("Preparing {kind}/{id}/{}", declared.target.display());
          eprintln!("{message}");
          report_worker_message(&message);
          let files = if let Source::FabricLatest { game_version } = &declared.source {
            if *kind != "cores" || declared.target.components().count() != 1 {
              return Err(
                io::Error::other("Fabric must declare a core launcher at the bundle root").into(),
              );
            }
            if let Some(status) = self.status.lock().await.as_mut() {
              status.message = format!("Preparing Fabric {game_version} server core and libraries");
            }
            self
              .fabric_files(&client, game_version, &declared.target)
              .await?
          } else {
            vec![resolve_file(&client, declared).await.map_err(|e| {
              io::Error::other(format!("{kind}/{id}/{}: {e}", declared.target.display()))
            })?]
          };
          if let Some(status) = self.status.lock().await.as_mut() {
            status.total += files.len().saturating_sub(1);
          }
          for mut file in files {
            let objects = config
              .resolve(&config.value.paths.resources)
              .join("objects");
            let mut attempt = 0;
            let object = loop {
              attempt += 1;
              match self.obtain(&client, &file, &objects).await {
                Ok(path) => break path,
                Err(error) => {
                  eprintln!(
                    "{kind}/{id}/{} attempt {attempt}/3: {error}",
                    file.target.display()
                  );
                  if attempt >= 3 || matches!(file.source, Source::File { .. }) {
                    return Err(
                      io::Error::other(format!("{kind}/{id}/{}: {error}", file.target.display()))
                        .into(),
                    );
                  }
                  tokio::time::sleep(Duration::from_secs(attempt)).await;
                }
              }
            };
            if matches!(file.source, Source::File { .. }) {
              file.sha256.clear();
              file.sha512 = None;
              file.size = std::fs::metadata(&object)?.len();
            } else {
              let (hash, size) = digest_file(&object)?;
              file.sha256 = hash;
              file.size = size;
            }
            let target = staging.join(kind).join(id).join(&file.target);
            if target.exists() {
              return Err(
                io::Error::other(format!("duplicate resource target: {}", target.display())).into(),
              );
            }
            tokio::fs::create_dir_all(target.parent().unwrap()).await?;
            if tokio::fs::hard_link(&object, &target).await.is_err() {
              tokio::fs::copy(&object, &target).await?;
            }
            self
              .progress(format!("Verified {kind}/{id}/{}", file.target.display()))
              .await;
            prepared.push(file);
          }
        }
        bundle.files = prepared;
        if let Some(archive) = &bundle.archive {
          let path = staging.join(kind).join(id).join(archive);
          tokio::task::spawn_blocking(move || validate_archive(&path)).await??;
        }
      }
      validate_manifest(kind, manifest)?;
    }
    for (kind, manifest) in &manifests {
      atomic_json(&staging.join(format!("{kind}.json")), manifest)?;
    }
    // Snapshot definitions before publishing. No updater operation reads or writes saves/.
    let servers = config.resolve(&config.value.paths.servers);
    tokio::fs::create_dir(staging.join("servers")).await?;
    let mut entries = tokio::fs::read_dir(&servers).await?;
    while let Some(entry) = entries.next_entry().await? {
      if entry.file_type().await?.is_file()
        && entry.path().extension().is_some_and(|s| s == "json")
        && server.is_none_or(|id| {
          entry.file_name() == format!("{id}.json").as_str()
            || entry.file_name() == "external-servers.json"
        })
      {
        tokio::fs::copy(
          entry.path(),
          staging.join("servers").join(entry.file_name()),
        )
        .await?;
      }
    }
    let mut candidate = (**config).clone();
    candidate.generation = Some(staging.to_path_buf());
    for id in candidate.server_ids().await? {
      let definition = candidate.load_server(&id).await?;
      validate_references(&candidate, &definition).await?;
    }
    candidate.game_definitions().await?;
    candidate.external_servers().await?;
    Ok(())
  }

  async fn fabric_files(
    &self,
    client: &reqwest::Client,
    game_version: &str,
    target: &Path,
  ) -> Result<Vec<ResourceFile>> {
    validate_version(game_version)?;
    let loaders = json_get(
      client,
      &format!("https://meta.fabricmc.net/v2/versions/loader/{game_version}"),
    )
    .await?;
    let loader = loaders
      .as_array()
      .and_then(|a| a.first())
      .and_then(|v| v["loader"]["version"].as_str())
      .ok_or_else(|| io::Error::other("Fabric has no loader for this game version"))?;
    validate_version(loader)?;
    let installers = json_get(client, "https://meta.fabricmc.net/v2/versions/installer").await?;
    let installer = installers
      .as_array()
      .and_then(|a| a.first())
      .ok_or_else(|| io::Error::other("Fabric installer metadata missing"))?;
    let version = installer["version"]
      .as_str()
      .ok_or_else(|| io::Error::other("Fabric installer version missing"))?;
    validate_version(version)?;
    let url = installer["url"]
      .as_str()
      .ok_or_else(|| io::Error::other("Fabric installer download URL missing"))?;
    validate_url(url)?;
    let objects = self
      .config
      .resolve(&self.config.value.paths.resources)
      .join("objects");
    let key = format!(
      "{:x}",
      Sha256::digest(format!(
        "installed-fabric-v1/{game_version}/{loader}/{version}"
      ))
    );
    let index = objects.join("fabric").join(format!("{key}.json"));
    let mut files = if let Ok(bytes) = tokio::fs::read(&index).await
      && let Ok(files) = serde_json::from_slice::<Vec<ResourceFile>>(&bytes)
      && fabric_cache_valid(&objects, &files).await?
    {
      files
    } else {
      let installer_file = ResourceFile {
        target: "fabric-installer.jar".into(),
        sha256: String::new(),
        sha512: None,
        size: 0,
        source: Source::Url {
          url: url.to_string(),
        },
      };
      let jar = self.obtain(client, &installer_file, &objects).await?;
      let files = self
        .install_fabric(&objects, game_version, loader, &jar)
        .await?;
      tokio::fs::create_dir_all(index.parent().unwrap()).await?;
      atomic_json(&index, &files)?;
      files
    };
    for file in &mut files {
      if file.target == Path::new("fabric-server-launch.jar") {
        file.target = target.to_path_buf();
      }
    }
    Ok(files)
  }

  async fn install_fabric(
    &self,
    objects: &Path,
    game_version: &str,
    loader: &str,
    installer: &Path,
  ) -> Result<Vec<ResourceFile>> {
    let stage = objects.join(format!(".fabric-{}", Uuid::new_v4()));
    tokio::fs::create_dir_all(&stage).await?;
    let result = async {
      let output = logged_command(
        java_command(&self.config, crate::config::JavaVersion::Java25)
          .arg("-Xmx512M")
          .arg("-jar")
          .arg(installer)
          .arg("server")
          .arg("-mcversion")
          .arg(game_version)
          .arg("-loader")
          .arg(loader)
          .arg("-dir")
          .arg(&stage)
          .arg("-downloadMinecraft")
          .stdin(std::process::Stdio::null()),
        Duration::from_secs(600),
      )
      .await?;
      if !output.status.success() {
        return Err(
          io::Error::other(format!(
            "Fabric server preparation failed ({}): {} {}",
            output.status,
            String::from_utf8_lossy(&output.stdout),
            String::from_utf8_lossy(&output.stderr)
          ))
          .into(),
        );
      }
      let mut files = Vec::new();
      let mut pending = vec![stage.clone()];
      while let Some(directory) = pending.pop() {
        let mut entries = tokio::fs::read_dir(directory).await?;
        while let Some(entry) = entries.next_entry().await? {
          let path = entry.path();
          let kind = entry.file_type().await?;
          if kind.is_dir() {
            pending.push(path);
            continue;
          }
          if !kind.is_file() {
            return Err(io::Error::other("Fabric preparation produced a non-regular file").into());
          }
          let target = path.strip_prefix(&stage)?.to_path_buf();
          relative(&target)?;
          let (sha256, size) = digest_file(&path)?;
          let file = ResourceFile {
            target,
            sha256,
            sha512: None,
            size,
            source: Source::FabricLatest {
              game_version: game_version.to_string(),
            },
          };
          let object = objects.join(&file.sha256[..2]).join(&file.sha256);
          tokio::fs::create_dir_all(object.parent().unwrap()).await?;
          if !verified(&object, &file).await? {
            tokio::fs::rename(&path, &object).await?;
          }
          files.push(file);
        }
      }
      if !fabric_cache_valid(objects, &files).await? {
        return Err(
          io::Error::other(
            "Fabric preparation did not produce a complete server core and libraries",
          )
          .into(),
        );
      }
      files.sort_by(|a, b| a.target.cmp(&b.target));
      Ok(files)
    }
    .await;
    let _ = tokio::fs::remove_dir_all(&stage).await;
    result
  }
  async fn obtain(
    &self,
    client: &reqwest::Client,
    file: &ResourceFile,
    objects: &Path,
  ) -> Result<PathBuf> {
    if let Source::File { path } = &file.source {
      let input = self
        .config
        .resolve(&self.config.value.paths.manifests)
        .join(path)
        .canonicalize()?;
      let depend = self
        .config
        .resolve(&self.config.value.paths.manifests)
        .join("depend")
        .canonicalize()?;
      if !input.starts_with(depend) || !input.is_file() {
        return Err(io::Error::other("local resource must be a file inside depend/").into());
      }
      let mut reader = std::fs::File::open(&input)?;
      let mut header = [0u8; 128];
      let count = reader.read(&mut header)?;
      if header[..count].starts_with(b"version https://git-lfs.github.com/spec/v1") {
        return Err(
          io::Error::other(format!("Git LFS content is missing: {}", path.display())).into(),
        );
      }
      // Operator-managed local files are trusted and linked directly, without hash checks.
      return Ok(input);
    }
    if file.sha256.len() == 64 {
      let object = objects.join(&file.sha256[..2]).join(&file.sha256);
      if verified(&object, file).await? {
        eprintln!("Cache hit: {}", file.target.display());
        return Ok(object);
      }
    }
    // Latest metadata resolves to an immutable URL or exact upstream version ID.
    // Remember its local SHA-256 so SHA-512-only and Fabric downloads are reused too.
    let cache_key = format!(
      "{:x}",
      Sha256::digest(serde_json::to_vec(&(
        &file.source,
        &file.sha512,
        file.size
      ))?)
    );
    let index = objects.join("indices").join(format!("{cache_key}.json"));
    if let Ok(bytes) = tokio::fs::read(&index).await {
      if let Ok(cached) = serde_json::from_slice::<ResourceFile>(&bytes) {
        if cached.sha256.len() == 64 && cached.sha256.bytes().all(|b| b.is_ascii_hexdigit()) {
          let object = objects.join(&cached.sha256[..2]).join(&cached.sha256);
          let mut expected = file.clone();
          expected.sha256 = cached.sha256;
          if expected.size == 0 {
            expected.size = cached.size;
          }
          if verified(&object, &expected).await? {
            eprintln!("Cache hit: {}", file.target.display());
            return Ok(object);
          }
        }
      }
    }
    tokio::fs::create_dir_all(objects).await?;
    let temporary = objects.join(format!(".part-{}", Uuid::new_v4()));
    let result: Result<PathBuf> = async {
      match &file.source {
        Source::File { .. } => unreachable!(),
        source => {
          let url = resolve_url(client, source).await?;
          validate_url(&url)?;
          let message = format!("Downloading {} from {url}", file.target.display());
          eprintln!("{message}");
          report_worker_message(&message);
          let mut response = client.get(&url).send().await?.error_for_status()?;
          let mut output = tokio::fs::File::create(&temporary).await?;
          use tokio::io::AsyncWriteExt;
          let mut size = 0u64;
          let mut reported = std::time::Instant::now();
          while let Some(chunk) = response.chunk().await? {
            size += chunk.len() as u64;
            if reported.elapsed() >= Duration::from_secs(5) {
              eprintln!("Downloading {}: {size} bytes", file.target.display());
              report_worker_message(&format!(
                "Downloading {}: {size} bytes",
                file.target.display()
              ));
              reported = std::time::Instant::now();
            }
            if size
              > if file.size > 0 {
                file.size
              } else {
                512 * 1024 * 1024
              }
            {
              return Err(io::Error::other("download exceeds declared size").into());
            }
            output.write_all(&chunk).await?;
          }
          output.sync_all().await?;
        }
      }
      if !verified(&temporary, file).await? {
        return Err(
          io::Error::other(format!(
            "resource size or SHA-256 mismatch: {}",
            file.target.display()
          ))
          .into(),
        );
      }
      let (hash, size) = digest_file(&temporary)?;
      let object = objects.join(&hash[..2]).join(&hash);
      tokio::fs::create_dir_all(object.parent().unwrap()).await?;
      tokio::fs::rename(&temporary, &object).await?;
      let mut cached = file.clone();
      cached.sha256 = hash;
      cached.size = size;
      tokio::fs::create_dir_all(index.parent().unwrap()).await?;
      atomic_json(&index, &cached)?;
      Ok(object)
    }
    .await;
    if result.is_err() {
      let _ = tokio::fs::remove_file(&temporary).await;
    }
    result
  }
}
fn report_worker_message(message: &str) {
  let (Ok(root), Ok(task)) = (
    std::env::var("SHTECHCRAFT_ROOT"),
    std::env::var("SHTECHCRAFT_UPDATE_ID"),
  ) else {
    return;
  };
  let path = PathBuf::from(root).join("state/update.json");
  let Ok(bytes) = std::fs::read(&path) else {
    return;
  };
  let Ok(mut value) = serde_json::from_slice::<serde_json::Value>(&bytes) else {
    return;
  };
  if value["id"].as_str() != Some(task.as_str()) {
    return;
  }
  value["message"] = message.into();
  let _ = atomic_json(&path, &value);
}

pub(crate) fn java_command(
  config: &LoadedConfig,
  version: crate::config::JavaVersion,
) -> tokio::process::Command {
  let mut command = tokio::process::Command::new(config.java_bin(version));
  if let Ok(proxy) = std::env::var("HTTPS_PROXY").or_else(|_| std::env::var("https_proxy")) {
    if let Ok(url) = reqwest::Url::parse(&proxy) {
      if matches!(url.scheme(), "http" | "https") {
        if let Some(host) = url.host_str() {
          let port = url.port_or_known_default().unwrap_or(80);
          command
            .arg(format!("-Dhttps.proxyHost={host}"))
            .arg(format!("-Dhttps.proxyPort={port}"))
            .arg(format!("-Dhttp.proxyHost={host}"))
            .arg(format!("-Dhttp.proxyPort={port}"));
        }
      }
    }
  }
  command
}

pub(crate) async fn logged_command(
  command: &mut tokio::process::Command,
  timeout: Duration,
) -> Result<std::process::Output> {
  use tokio::io::AsyncReadExt;
  async fn stream(mut reader: impl tokio::io::AsyncRead + Unpin) -> io::Result<Vec<u8>> {
    let mut tail = Vec::new();
    let mut chunk = [0u8; 4096];
    loop {
      let n = reader.read(&mut chunk).await?;
      if n == 0 {
        break;
      }
      eprint!("{}", String::from_utf8_lossy(&chunk[..n]));
      tail.extend_from_slice(&chunk[..n]);
      if tail.len() > 8192 {
        tail.drain(..tail.len() - 8192);
      }
    }
    Ok(tail)
  }
  let mut child = command
    .stdout(std::process::Stdio::piped())
    .stderr(std::process::Stdio::piped())
    .kill_on_drop(true)
    .spawn()?;
  let stdout = child.stdout.take().unwrap();
  let stderr = child.stderr.take().unwrap();
  let result = tokio::time::timeout(timeout, async {
    let (status, stdout, stderr) = tokio::join!(child.wait(), stream(stdout), stream(stderr));
    Ok::<_, io::Error>(std::process::Output {
      status: status?,
      stdout: stdout?,
      stderr: stderr?,
    })
  })
  .await
  .map_err(|_| io::Error::other("core preparation timed out"))??;
  Ok(result)
}

fn validate_url(url: &str) -> Result<()> {
  let url = reqwest::Url::parse(url)?;
  if url.scheme() != "https" || !url.username().is_empty() || url.password().is_some() {
    return Err(
      io::Error::other("resource URLs require HTTPS without embedded credentials").into(),
    );
  }
  Ok(())
}
async fn resolve_url(client: &reqwest::Client, source: &Source) -> Result<String> {
  match source {
    Source::Url { url } => Ok(url.clone()),
    Source::Modrinth {
      project,
      version,
      filename,
    } => {
      validate_id(project)?;
      validate_id(version)?;
      let value: serde_json::Value = client
        .get(format!("https://api.modrinth.com/v2/version/{version}"))
        .send()
        .await?
        .error_for_status()?
        .json()
        .await?;
      if value["project_id"].as_str() != Some(project) {
        return Err(io::Error::other("Modrinth version belongs to a different project").into());
      }
      value["files"]
        .as_array()
        .and_then(|a| a.iter().find(|f| f["filename"].as_str() == Some(filename)))
        .and_then(|f| f["url"].as_str())
        .map(str::to_owned)
        .ok_or_else(|| io::Error::other("Modrinth file not found").into())
    }
    Source::Curseforge {
      project_id,
      file_id,
    } => {
      let key = std::env::var("CURSEFORGE_API_KEY")
        .map_err(|_| io::Error::other("CURSEFORGE_API_KEY is required for this source"))?;
      let value: serde_json::Value = client
        .get(format!(
          "https://api.curseforge.com/v1/mods/{project_id}/files/{file_id}/download-url"
        ))
        .header("x-api-key", key)
        .send()
        .await?
        .error_for_status()?
        .json()
        .await?;
      value["data"]
        .as_str()
        .map(str::to_owned)
        .ok_or_else(|| io::Error::other("CurseForge did not provide a download URL").into())
    }
    Source::Spigot {
      resource_id,
      version_id,
    } => Ok(format!(
      "https://api.spiget.org/v2/resources/{resource_id}/versions/{version_id}/download"
    )),
    Source::File { .. }
    | Source::ModrinthLatest { .. }
    | Source::PaperLatest { .. }
    | Source::SpigotLatest { .. }
    | Source::CurseforgeLatest { .. } => unreachable!(),
    Source::FabricLatest { .. } => {
      Err(io::Error::other("prepared Fabric core object is missing").into())
    }
  }
}
async fn fabric_cache_valid(objects: &Path, files: &[ResourceFile]) -> Result<bool> {
  if !files
    .iter()
    .any(|f| f.target == Path::new("fabric-server-launch.jar"))
    || !files.iter().any(|f| f.target == Path::new("server.jar"))
    || !files.iter().any(|f| f.target.starts_with("libraries"))
  {
    return Ok(false);
  }
  let mut targets = std::collections::BTreeSet::new();
  for file in files {
    if relative(&file.target).is_err()
      || file.size == 0
      || file.sha256.len() != 64
      || !file.sha256.bytes().all(|b| b.is_ascii_hexdigit())
      || !targets.insert(file.target.clone())
    {
      return Ok(false);
    }
    if !verified(&objects.join(&file.sha256[..2]).join(&file.sha256), file).await? {
      return Ok(false);
    }
  }
  Ok(true)
}
async fn verified(path: &Path, file: &ResourceFile) -> Result<bool> {
  let path = path.to_path_buf();
  let expected = file.clone();
  tokio::task::spawn_blocking(move || -> Result<bool> {
    let metadata = match std::fs::symlink_metadata(&path) {
      Ok(m) => m,
      Err(e) if e.kind() == io::ErrorKind::NotFound => return Ok(false),
      Err(e) => return Err(e.into()),
    };
    if !metadata.is_file()
      || metadata.file_type().is_symlink()
      || (expected.size > 0 && metadata.len() != expected.size)
    {
      return Ok(false);
    }
    let mut input = std::fs::File::open(path)?;
    let mut hash = Sha256::new();
    let mut hash512 = sha2::Sha512::new();
    let mut buffer = [0u8; 65536];
    loop {
      let n = input.read(&mut buffer)?;
      if n == 0 {
        break;
      }
      hash.update(&buffer[..n]);
      hash512.update(&buffer[..n]);
    }
    Ok(
      (expected.sha256.is_empty() || format!("{:x}", hash.finalize()) == expected.sha256)
        && expected
          .sha512
          .as_ref()
          .is_none_or(|value| format!("{:x}", hash512.finalize()) == *value),
    )
  })
  .await?
}
fn relative(path: &Path) -> Result<()> {
  if path.as_os_str().is_empty()
    || path
      .components()
      .any(|c| !matches!(c, Component::Normal(_)))
    || path.to_string_lossy().contains('\\')
  {
    return Err(io::Error::other(format!("invalid resource path: {}", path.display())).into());
  }
  Ok(())
}
fn validate_manifest(kind: &str, manifest: &Manifest) -> Result<()> {
  if manifest.schema_version != 1 {
    return Err(io::Error::other("unsupported resource manifest schema").into());
  }
  for (id, bundle) in &manifest.artifacts {
    relative(Path::new(id))?;
    let mut targets = std::collections::BTreeSet::new();
    if bundle.files.is_empty() {
      return Err(io::Error::other("resource bundle has no files").into());
    }
    for file in &bundle.files {
      relative(&file.target)?;
      let latest = matches!(
        file.source,
        Source::ModrinthLatest { .. }
          | Source::PaperLatest { .. }
          | Source::FabricLatest { .. }
          | Source::SpigotLatest { .. }
          | Source::CurseforgeLatest { .. }
      );
      if !latest
        && !matches!(file.source, Source::File { .. })
        && (file.sha256.len() != 64
          || !file
            .sha256
            .bytes()
            .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b)))
      {
        return Err(io::Error::other("invalid SHA-256").into());
      }
      if !targets.insert(file.target.to_string_lossy().to_lowercase()) {
        return Err(io::Error::other("duplicate resource target").into());
      }
      match &file.source {
        Source::File { path } => {
          relative(path)?;
          if path.components().next().unwrap().as_os_str() != "depend" {
            return Err(io::Error::other("file source must be in depend/").into());
          }
        }
        Source::Url { url } => validate_url(url)?,
        _ => {}
      }
    }
    if kind == "worlds" && bundle.archive.is_none() {
      return Err(io::Error::other("world bundle requires an archive").into());
    }
    if let Some(archive) = &bundle.archive {
      relative(archive)?;
      if !bundle.files.iter().any(|f| &f.target == archive) {
        return Err(io::Error::other("world archive is not declared in files").into());
      }
    }
  }
  Ok(())
}
async fn validate_references(config: &LoadedConfig, d: &ServerDefinition) -> Result<()> {
  let core = crate::layout::resolve_core_version(
    &config.resource_root("cores").join(&d.core.kind),
    None,
    &d.core.minecraft_version,
    d.core.loader_version.as_deref(),
  )
  .await?;
  if let Some(jar) = &d.core.jar {
    if !core.join(jar).is_file() {
      return Err(
        io::Error::other(format!(
          "core entry point missing: {}",
          core.join(jar).display()
        ))
        .into(),
      );
    }
  }
  for (kind, id, version) in d
    .plugins
    .iter()
    .map(|a| ("plugins", &a.id, a.version.as_deref()))
    .chain(d.mods.iter().map(|a| ("mods", &a.id, a.version.as_deref())))
  {
    crate::layout::resolve_artifact_version(
      &config.resource_root(kind).join(id),
      version,
      &d.core.minecraft_version,
    )
    .await?;
  }
  if let crate::config::WorldDefinition::Template { id } = &d.world {
    world_archive(config, id)?;
  }
  for layer in &d.files {
    if !config
      .resolve(&config.value.paths.files)
      .join(&layer.source)
      .exists()
    {
      return Err(
        io::Error::other(format!("file layer missing: {}", layer.source.display())).into(),
      );
    }
  }
  Ok(())
}
fn atomic_json(path: &Path, value: &impl Serialize) -> Result<()> {
  let temporary = path.with_extension(format!("tmp-{}", Uuid::new_v4()));
  let mut output = std::fs::File::create(&temporary)?;
  serde_json::to_writer_pretty(&mut output, value)?;
  output.write_all(b"\n")?;
  output.sync_all()?;
  std::fs::rename(temporary, path)?;
  Ok(())
}
fn world_archive(config: &LoadedConfig, id: &str) -> Result<PathBuf> {
  validate_id(id)?;
  let root = active_root(config)
    .ok_or_else(|| io::Error::other("run scheduler update before using archive worlds"))?;
  let manifest: Manifest = serde_json::from_slice(&std::fs::read(root.join("worlds.json"))?)?;
  let bundle = manifest
    .artifacts
    .get(id)
    .ok_or_else(|| io::Error::other(format!("world template is not declared: {id}")))?;
  Ok(
    root.join("worlds").join(id).join(
      bundle
        .archive
        .as_ref()
        .ok_or_else(|| io::Error::other("world archive is missing"))?,
    ),
  )
}
pub async fn materialize_world(config: &LoadedConfig, id: &str, target: &Path) -> Result<()> {
  if active_root(config).is_none() {
    crate::world_store::copy_tree(&config.resolve(&config.value.paths.worlds).join(id), target)
      .await?;
    return Ok(());
  }
  let archive = world_archive(config, id)?;
  let target = target.to_path_buf();
  tokio::task::spawn_blocking(move || extract_archive(&archive, &target)).await?
}
pub(crate) fn validate_archive(path: &Path) -> Result<()> {
  visit_archive(path, None)
}
fn extract_archive(path: &Path, target: &Path) -> Result<()> {
  std::fs::create_dir_all(target)?;
  visit_archive(path, Some(target))
}
fn visit_archive(path: &Path, target: Option<&Path>) -> Result<()> {
  let input = std::fs::File::open(path)?;
  let decoder = flate2::read::GzDecoder::new(input);
  let mut archive = tar::Archive::new(decoder);
  let mut total = 0u64;
  let mut seen = std::collections::BTreeSet::new();
  for entry in archive.entries()? {
    let mut entry = entry?;
    let path = entry.path()?.into_owned();
    relative(&path)?;
    let kind = entry.header().entry_type();
    if !kind.is_dir() && !kind.is_file() {
      return Err(
        io::Error::other("world archives may only contain regular files and directories").into(),
      );
    }
    if !seen.insert(path.clone()) {
      return Err(io::Error::other("duplicate world archive entry").into());
    }
    total = total
      .checked_add(entry.size())
      .ok_or_else(|| io::Error::other("archive size overflow"))?;
    if total > 64 * 1024 * 1024 * 1024 {
      return Err(io::Error::other("world archive exceeds 64 GiB unpacked limit").into());
    }
    if let Some(target) = target {
      if !entry.unpack_in(target)? {
        return Err(io::Error::other("archive entry escaped destination").into());
      }
    } else {
      io::copy(&mut entry, &mut io::sink())?;
    }
  }
  Ok(())
}

fn digest_file(path: &Path) -> Result<(String, u64)> {
  let mut input = std::fs::File::open(path)?;
  let mut hash = Sha256::new();
  let mut size = 0;
  let mut buffer = [0u8; 65536];
  loop {
    let n = input.read(&mut buffer)?;
    if n == 0 {
      break;
    }
    size += n as u64;
    hash.update(&buffer[..n]);
  }
  Ok((format!("{:x}", hash.finalize()), size))
}
pub(crate) struct UpdateLock(std::fs::File);
impl UpdateLock {
  pub(crate) fn acquire(path: &Path) -> Result<Self> {
    let file = std::fs::OpenOptions::new()
      .create(true)
      .truncate(false)
      .write(true)
      .open(path)?;
    #[cfg(unix)]
    {
      use std::os::fd::AsRawFd;
      if unsafe { libc::flock(file.as_raw_fd(), libc::LOCK_EX | libc::LOCK_NB) } != 0 {
        return Err(io::Error::other("another resource update is in progress").into());
      }
    }
    Ok(Self(file))
  }
}
impl Drop for UpdateLock {
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
async fn json_get(client: &reqwest::Client, url: &str) -> Result<serde_json::Value> {
  request_json(client.get(url)).await
}
async fn request_json(request: reqwest::RequestBuilder) -> Result<serde_json::Value> {
  let url = request.try_clone().unwrap().build()?.url().to_string();
  let memo = std::env::var("SHTECHCRAFT_ROOT")
    .ok()
    .zip(std::env::var("SHTECHCRAFT_UPDATE_ID").ok())
    .map(|(root, task)| {
      PathBuf::from(root).join("cache/metadata").join(format!(
        "{:x}.json",
        Sha256::digest(format!("{task}/{url}"))
      ))
    });
  if let Some(path) = &memo {
    if let Ok(bytes) = tokio::fs::read(path).await {
      if let Ok(value) = serde_json::from_slice(&bytes) {
        return Ok(value);
      }
    }
  }
  let mut last = String::new();
  for attempt in 1..=3 {
    match async {
      request
        .try_clone()
        .unwrap()
        .timeout(Duration::from_secs(45))
        .send()
        .await?
        .error_for_status()?
        .json::<serde_json::Value>()
        .await
    }
    .await
    {
      Ok(value) => {
        if let Some(path) = &memo {
          std::fs::create_dir_all(path.parent().unwrap())?;
          atomic_json(path, &value)?;
        }
        return Ok(value);
      }
      Err(error) => {
        last = format!("GET {url}: {error}");
        eprintln!("Attempt {attempt}/3 failed: {last}");
        if attempt < 3 {
          tokio::time::sleep(Duration::from_secs(attempt)).await;
        }
      }
    }
  }
  Err(io::Error::other(last).into())
}
async fn resolve_file(client: &reqwest::Client, file: &ResourceFile) -> Result<ResourceFile> {
  let mut resolved = file.clone();
  match &file.source {
    Source::SpigotLatest {
      resource_id,
      game_version,
    } => {
      let resource = json_get(
        client,
        &format!("https://api.spiget.org/v2/resources/{resource_id}"),
      )
      .await?;
      let family = game_version
        .split('.')
        .take(2)
        .collect::<Vec<_>>()
        .join(".");
      let compatible = resource["testedVersions"].as_array().is_some_and(|a| {
        a.iter()
          .any(|v| v.as_str().is_some_and(|v| v == game_version || v == family))
      });
      if !compatible {
        return Err(
          io::Error::other(format!(
            "Spigot resource {resource_id} does not declare compatibility with {game_version}"
          ))
          .into(),
        );
      }
      let version = json_get(
        client,
        &format!("https://api.spiget.org/v2/resources/{resource_id}/versions/latest"),
      )
      .await?;
      let id = version["id"]
        .as_u64()
        .ok_or_else(|| io::Error::other("Spigot version is missing"))?;
      resolved.source = Source::Spigot {
        resource_id: *resource_id,
        version_id: id,
      };
      resolved.sha256 = String::new();
      resolved.sha512 = None;
      resolved.size = 0;
    }
    Source::CurseforgeLatest {
      project_id,
      game_version,
      loader_type,
    } => {
      let key = std::env::var("CURSEFORGE_API_KEY")
        .map_err(|_| io::Error::other("CURSEFORGE_API_KEY is required"))?;
      let value: serde_json::Value = client
        .get(format!(
          "https://api.curseforge.com/v1/mods/{project_id}/files"
        ))
        .header("x-api-key", key)
        .query(&[
          ("gameVersion", game_version.clone()),
          ("modLoaderType", loader_type.to_string()),
          ("pageSize", "50".into()),
        ])
        .send()
        .await?
        .error_for_status()?
        .json()
        .await?;
      let latest = value["data"]
        .as_array()
        .and_then(|a| {
          a.iter()
            .filter(|v| {
              v["gameVersions"]
                .as_array()
                .is_some_and(|a| a.iter().any(|s| s.as_str() == Some(game_version)))
            })
            .max_by_key(|v| v["fileDate"].as_str().unwrap_or_default())
        })
        .ok_or_else(|| io::Error::other("no compatible CurseForge file"))?;
      let id = latest["id"]
        .as_u64()
        .ok_or_else(|| io::Error::other("CurseForge file ID missing"))?;
      resolved.source = Source::Curseforge {
        project_id: *project_id,
        file_id: id,
      };
      resolved.sha256 = String::new();
      resolved.sha512 = None;
      resolved.size = latest["fileLength"].as_u64().unwrap_or(0);
    }
    Source::ModrinthLatest {
      project,
      game_version,
      loader,
    } => {
      validate_id(project)?;
      let loaders = if loader == "paper" {
        vec!["paper", "spigot", "bukkit"]
      } else {
        vec![loader.as_str()]
      };
      let mut query = vec![("loaders", serde_json::to_string(&loaders)?)];
      // Proxy releases use Velocity's own version scheme, not Minecraft's versions.
      if loader != "velocity" {
        query.push(("game_versions", serde_json::to_string(&[game_version])?));
      }
      let versions: Vec<serde_json::Value> = serde_json::from_value(
        request_json(
          client
            .get(format!(
              "https://api.modrinth.com/v2/project/{project}/version"
            ))
            .query(&query),
        )
        .await?,
      )?;
      let v = versions
        .iter()
        .max_by_key(|v| v["date_published"].as_str().unwrap_or_default())
        .ok_or_else(|| {
          io::Error::other(format!(
            "no compatible Modrinth version for {project} on {loader} {game_version}"
          ))
        })?;
      let files = v["files"]
        .as_array()
        .ok_or_else(|| io::Error::other("Modrinth returned no files"))?;
      let f = files
        .iter()
        .filter(|f| f["filename"].as_str().is_some_and(|s| s.ends_with(".jar")))
        .max_by_key(|f| f["primary"].as_bool().unwrap_or(false))
        .ok_or_else(|| io::Error::other("Modrinth returned no runtime JAR"))?;
      resolved.sha256 = String::new();
      resolved.sha512 = Some(
        f["hashes"]["sha512"]
          .as_str()
          .ok_or_else(|| io::Error::other("Modrinth file has no SHA-512"))?
          .into(),
      );
      resolved.size = f["size"]
        .as_u64()
        .ok_or_else(|| io::Error::other("Modrinth file has no size"))?;
      resolved.source = Source::Url {
        url: f["url"]
          .as_str()
          .ok_or_else(|| io::Error::other("Modrinth file has no URL"))?
          .into(),
      };
    }
    Source::PaperLatest {
      project,
      game_version,
    } => {
      if !["paper", "velocity", "folia"].contains(&project.as_str()) {
        return Err(io::Error::other("unknown PaperMC project").into());
      }
      validate_version(game_version)?;
      let versions = json_get(
        client,
        &format!("https://fill.papermc.io/v3/projects/{project}/versions/{game_version}/builds"),
      )
      .await?;
      let build = versions
        .as_array()
        .and_then(|a| {
          a.iter()
            .max_by_key(|v| v["id"].as_u64().unwrap_or_default())
        })
        .ok_or_else(|| io::Error::other("PaperMC has no builds for this version"))?;
      let f = &build["downloads"]["server:default"];
      resolved.sha256 = f["checksums"]["sha256"]
        .as_str()
        .ok_or_else(|| io::Error::other("PaperMC download has no SHA-256"))?
        .into();
      resolved.sha512 = None;
      resolved.size = f["size"].as_u64().unwrap_or(0);
      resolved.source = Source::Url {
        url: f["url"]
          .as_str()
          .ok_or_else(|| io::Error::other("PaperMC download missing"))?
          .into(),
      };
    }
    Source::FabricLatest { game_version } => {
      return Err(
        io::Error::other(format!(
          "Fabric {game_version} requires server core preparation"
        ))
        .into(),
      );
    }
    _ => {}
  }
  Ok(resolved)
}

#[cfg(test)]
pub(crate) mod tests {
  use super::*;

  #[cfg(unix)]
  async fn command_fixture(script: &str) -> (tempfile::TempDir, Updater, PathBuf) {
    use std::os::unix::fs::PermissionsExt;
    let (temp, config) = fixture().await;
    let root = temp.path().join("deployment with spaces");
    std::fs::create_dir_all(root.join("database")).unwrap();
    let root = root.canonicalize().unwrap();
    let mut config = (*config).clone();
    config.root = root.join("database");
    config.file = config.root.join("runtime-config.json");
    config.value.paths.manifests = root.clone();
    config.value.paths.resources = root.join(".resources");
    config.value.paths.logs = root.join("logs");
    let path = root.join("server.sh");
    std::fs::write(&path, script).unwrap();
    std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o755)).unwrap();
    (temp, Updater::new(Arc::new(config)), root)
  }

  #[cfg(unix)]
  async fn finished_update(updater: &Updater) -> UpdateStatus {
    tokio::time::timeout(Duration::from_secs(5), async {
      loop {
        if let Some(status) = updater.status().await
          && status.phase != "running"
        {
          return status;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
      }
    })
    .await
    .unwrap()
  }

  #[cfg(unix)]
  #[tokio::test]
  async fn network_update_executes_deployment_script_once_and_captures_output() {
    let (_temp, updater, root) = command_fixture(
      r##"#!/bin/sh
set -eu
[ "$#" = 1 ] && [ "$1" = update ]
printf '%s\n' "$PWD" >> invoked-roots
printf 'update stdout\n'
printf 'update stderr\n' >&2
while [ ! -f allow-update ]; do sleep 0.01; done
mkdir -p .resources
printf '{"revision":"10000000-0000-0000-0000-000000000001"}\n' > .resources/current.json
"##,
    )
    .await;
    let first = updater.start_legacy().await;
    let concurrent = updater.start_legacy().await;
    assert_eq!(first.id, concurrent.id);
    std::fs::write(root.join("allow-update"), b"").unwrap();
    let status = finished_update(&updater).await;
    assert_eq!(status.phase, "complete");
    assert_eq!(status.completed, 1);
    assert_eq!(
      status.revision.as_deref(),
      Some("10000000-0000-0000-0000-000000000001")
    );
    assert_eq!(
      std::fs::read_to_string(root.join("invoked-roots")).unwrap(),
      format!("{}\n", root.display())
    );
    let output = std::fs::read_to_string(root.join("logs/resource-update.log")).unwrap();
    assert!(output.contains("update stdout") && output.contains("update stderr"));
  }

  #[cfg(unix)]
  #[tokio::test]
  async fn network_update_reports_script_failure_and_preserves_active_generation() {
    let (_temp, updater, root) =
      command_fixture("#!/bin/sh\nprintf 'download failed\\n' >&2\nexit 7\n").await;
    std::fs::create_dir(root.join(".resources")).unwrap();
    let pointer = root.join(".resources/current.json");
    std::fs::write(
      &pointer,
      br#"{"revision":"10000000-0000-0000-0000-000000000002"}"#,
    )
    .unwrap();
    let before = std::fs::read(&pointer).unwrap();
    updater.start_legacy().await;
    let status = finished_update(&updater).await;
    assert_eq!(status.phase, "failed");
    assert!(status.message.contains("download failed"));
    assert!(status.message.contains("7"));
    assert_eq!(std::fs::read(pointer).unwrap(), before);
  }

  #[cfg(unix)]
  #[tokio::test]
  async fn network_update_requires_server_sh_and_does_not_fall_back_to_native_update() {
    let (_temp, updater, root) = command_fixture("#!/bin/sh\nexit 0\n").await;
    std::fs::remove_file(root.join("server.sh")).unwrap();
    updater.start_legacy().await;
    let status = finished_update(&updater).await;
    assert_eq!(status.phase, "failed");
    assert!(status.message.contains("server.sh update"));
    assert!(!root.join(".resources/current.json").exists());
  }

  #[cfg(unix)]
  async fn fabric_fixture(script: &str) -> (tempfile::TempDir, Updater, PathBuf, PathBuf) {
    use std::os::unix::fs::PermissionsExt;
    let (temp, config) = fixture().await;
    let java = temp.path().join("java25/bin/java");
    std::fs::create_dir_all(java.parent().unwrap()).unwrap();
    std::fs::write(&java, script).unwrap();
    std::fs::set_permissions(&java, std::fs::Permissions::from_mode(0o755)).unwrap();
    let installer = temp.path().join("installer.jar");
    std::fs::write(&installer, b"installer fixture").unwrap();
    let objects = temp.path().join("objects");
    (temp, Updater::new(config), objects, installer)
  }

  #[cfg(unix)]
  const INSTALL_FABRIC: &str = r##"#!/bin/sh
set -eu
while [ "$#" -gt 0 ]; do
  case "$1" in
    -mcversion) game="$2"; shift ;;
    -loader) loader="$2"; shift ;;
    -dir) output="$2"; shift ;;
    -downloadMinecraft) minecraft=yes ;;
  esac
  shift
done
[ "$game" = '1.20.4' ] && [ "$loader" = '0.19.5' ] && [ "$minecraft" = yes ]
mkdir -p "$output/libraries/net/fabricmc/fabric-loader"
printf 'installed server launcher' > "$output/fabric-server-launch.jar"
printf 'minecraft server core' > "$output/server.jar"
printf 'loader library' > "$output/libraries/net/fabricmc/fabric-loader/loader.jar"
"##;

  #[cfg(unix)]
  #[tokio::test]
  async fn fabric_prepares_a_complete_cached_server_before_launch() {
    let (_temp, updater, objects, installer) = fabric_fixture(INSTALL_FABRIC).await;
    let files = updater
      .install_fabric(&objects, "1.20.4", "0.19.5", &installer)
      .await
      .unwrap();
    assert_eq!(files.len(), 3);
    assert!(fabric_cache_valid(&objects, &files).await.unwrap());
    let launcher = files
      .iter()
      .find(|f| f.target == Path::new("fabric-server-launch.jar"))
      .unwrap();
    assert_eq!(
      std::fs::read(objects.join(&launcher.sha256[..2]).join(&launcher.sha256)).unwrap(),
      b"installed server launcher"
    );
    assert!(std::fs::read_dir(&objects).unwrap().all(|e| {
      !e.unwrap()
        .file_name()
        .to_string_lossy()
        .starts_with(".fabric-")
    }));
    std::fs::remove_file(objects.join(&launcher.sha256[..2]).join(&launcher.sha256)).unwrap();
    assert!(!fabric_cache_valid(&objects, &files).await.unwrap());
  }

  #[cfg(unix)]
  #[tokio::test]
  async fn failed_or_incomplete_fabric_installations_are_not_usable() {
    for script in ["#!/bin/sh\nexit 17\n", "#!/bin/sh\nexit 0\n"] {
      let (_temp, updater, objects, installer) = fabric_fixture(script).await;
      assert!(
        updater
          .install_fabric(&objects, "1.20.4", "0.19.5", &installer)
          .await
          .is_err()
      );
      assert!(std::fs::read_dir(&objects).unwrap().next().is_none());
    }
  }

  #[tokio::test]
  async fn fabric_cannot_fall_back_to_a_runtime_installer_download() {
    let file = ResourceFile {
      target: "fabric.jar".into(),
      sha256: String::new(),
      sha512: None,
      size: 0,
      source: Source::FabricLatest {
        game_version: "1.20.4".into(),
      },
    };
    assert!(resolve_file(&reqwest::Client::new(), &file).await.is_err());
    assert!(
      resolve_url(&reqwest::Client::new(), &file.source)
        .await
        .is_err()
    );
  }
  pub(crate) async fn fixture() -> (tempfile::TempDir, Arc<LoadedConfig>) {
    let temp = tempfile::tempdir().unwrap();
    let root = temp.path();
    std::fs::create_dir_all(root.join("depend")).unwrap();
    std::fs::create_dir_all(root.join("servers")).unwrap();
    std::fs::write(root.join("depend/server.jar"), b"core-v1").unwrap();
    let config = serde_json::json!({"api":{"port":25566,"token":"01234567890123456789012345678901"},"bridge":{"token":"11234567890123456789012345678901"},"postgresql":{"home":"pg","host":"127.0.0.1","port":5432,"username":"test","password":"test"},"ports":{"velocity":25565,"start":50001,"end":59999},"java":{"21_home":"java21","25_home":"java25"},"paths":{"cores":"cores","mods":"mods","plugins":"plugins","servers":"servers","worlds":"worlds","runtime":"runtime","logs":"logs","pid_file":"scheduler.pid","scheduler_log":"scheduler.log"}});
    std::fs::write(
      root.join("config.json"),
      serde_json::to_vec(&config).unwrap(),
    )
    .unwrap();
    std::fs::write(root.join("servers/game.json"),serde_json::to_vec(&serde_json::json!({"core":{"kind":"paper","minecraft_version":"1.21.11","java":"21","jar":"server.jar"},"memory":{"initial_mib":128,"maximum_mib":256},"properties":{"server-ip":"127.0.0.1"}})).unwrap()).unwrap();
    for kind in KINDS {
      atomic_json(
        &root.join(format!("{kind}.json")),
        &Manifest {
          schema_version: 1,
          artifacts: BTreeMap::new(),
        },
      )
      .unwrap();
    }
    write_core(root);
    let config = Arc::new(LoadedConfig::load(root.join("config.json")).await.unwrap());
    (temp, config)
  }
  fn write_core(root: &Path) {
    let (hash, size) = digest_file(&root.join("depend/server.jar")).unwrap();
    let file = ResourceFile {
      target: "server.jar".into(),
      sha256: hash,
      sha512: None,
      size,
      source: Source::File {
        path: "depend/server.jar".into(),
      },
    };
    atomic_json(
      &root.join("cores.json"),
      &Manifest {
        schema_version: 1,
        artifacts: BTreeMap::from([(
          "paper/1.21.11".into(),
          Bundle {
            files: vec![file],
            archive: None,
          },
        )]),
      },
    )
    .unwrap();
  }
  #[tokio::test]
  async fn local_resources_are_trusted_without_hash_or_size_checks() {
    let (temp, config) = fixture().await;
    let file = ResourceFile {
      target: "launcher.jar".into(),
      sha256: "wrong".into(),
      sha512: Some("wrong".into()),
      size: 99999,
      source: Source::File {
        path: "depend/server.jar".into(),
      },
    };
    let path = Updater::new(config)
      .obtain(&reqwest::Client::new(), &file, &temp.path().join("objects"))
      .await
      .unwrap();
    assert_eq!(std::fs::read(path).unwrap(), b"core-v1");
  }
  #[tokio::test]
  async fn resolved_downloads_reuse_and_verify_cached_objects() {
    let (temp, config) = fixture().await;
    let file = ResourceFile {
      target: "launcher.jar".into(),
      sha256: String::new(),
      sha512: None,
      size: 0,
      source: Source::Url {
        url: "https://example.invalid/launcher/1.0.jar".into(),
      },
    };
    let objects = temp.path().join("objects");
    let (hash, size) = digest_file(&temp.path().join("depend/server.jar")).unwrap();
    let object = objects.join(&hash[..2]).join(&hash);
    std::fs::create_dir_all(object.parent().unwrap()).unwrap();
    std::fs::copy(temp.path().join("depend/server.jar"), &object).unwrap();
    let key = format!(
      "{:x}",
      Sha256::digest(serde_json::to_vec(&(&file.source, &file.sha512, file.size)).unwrap())
    );
    let index = objects.join("indices").join(format!("{key}.json"));
    std::fs::create_dir_all(index.parent().unwrap()).unwrap();
    let mut cached = file.clone();
    cached.sha256 = hash;
    cached.size = size;
    atomic_json(&index, &cached).unwrap();
    assert_eq!(
      Updater::new(config)
        .obtain(&reqwest::Client::new(), &file, &objects)
        .await
        .unwrap(),
      object
    );
    std::fs::write(&object, b"corrupted").unwrap();
    assert!(!verified(&object, &cached).await.unwrap());
  }
  #[tokio::test]
  async fn activation_is_atomic_and_does_not_touch_saves() {
    let (temp, config) = fixture().await;
    let root = temp.path();
    std::fs::create_dir_all(root.join("saves/shared/keep")).unwrap();
    std::fs::write(root.join("saves/shared/keep/progress"), b"progress").unwrap();
    let updater = Updater::new(config.clone());
    let revision = updater.run().await.unwrap();
    let pinned = config.pinned();
    std::fs::write(root.join("depend/new.jar"), b"core-v2").unwrap();
    std::fs::rename(root.join("depend/new.jar"), root.join("depend/server.jar")).unwrap();
    let manifest_path = root.join("cores.json");
    let mut invalid: serde_json::Value =
      serde_json::from_slice(&std::fs::read(&manifest_path).unwrap()).unwrap();
    invalid["artifacts"]["paper/1.21.11"]["files"][0]["source"]["path"] =
      "depend/missing.jar".into();
    std::fs::write(&manifest_path, serde_json::to_vec(&invalid).unwrap()).unwrap();
    assert!(updater.run().await.is_err());
    assert_eq!(
      active_root(&config).unwrap().file_name().unwrap(),
      revision.as_str()
    );
    assert_eq!(
      std::fs::read(
        config
          .resource_root("cores")
          .join("paper/1.21.11/server.jar")
      )
      .unwrap(),
      b"core-v1"
    );
    write_core(root);
    let second = updater.run().await.unwrap();
    assert_ne!(revision, second);
    assert_eq!(
      std::fs::read(
        pinned
          .resource_root("cores")
          .join("paper/1.21.11/server.jar")
      )
      .unwrap(),
      b"core-v1"
    );
    assert_eq!(
      std::fs::read(
        config
          .resource_root("cores")
          .join("paper/1.21.11/server.jar")
      )
      .unwrap(),
      b"core-v2"
    );
    assert_eq!(
      std::fs::read(root.join("saves/shared/keep/progress")).unwrap(),
      b"progress"
    );
  }
  #[tokio::test]
  async fn missing_lfs_content_and_invalid_definitions_never_activate() {
    let (temp, config) = fixture().await;
    let root = temp.path();
    std::fs::write(
      root.join("depend/server.jar"),
      b"version https://git-lfs.github.com/spec/v1\noid sha256:abc\n",
    )
    .unwrap();
    write_core(root);
    assert!(
      Updater::new(config.clone())
        .run()
        .await
        .unwrap_err()
        .to_string()
        .contains("Git LFS")
    );
    assert!(active_root(&config).is_none());
    std::fs::write(root.join("depend/server.jar"), b"real").unwrap();
    write_core(root);
    std::fs::write(root.join("servers/game.json"), b"invalid").unwrap();
    assert!(Updater::new(config.clone()).run().await.is_err());
    assert!(active_root(&config).is_none());
  }
  #[test]
  fn archives_reject_links_and_manifest_paths_cannot_escape() {
    let temp = tempfile::tempdir().unwrap();
    let path = temp.path().join("world.tar.gz");
    let gzip = flate2::write::GzEncoder::new(
      std::fs::File::create(&path).unwrap(),
      flate2::Compression::default(),
    );
    let mut archive = tar::Builder::new(gzip);
    let mut header = tar::Header::new_gnu();
    header.set_entry_type(tar::EntryType::Symlink);
    header.set_size(0);
    header.set_link_name("../../outside").unwrap();
    header.set_cksum();
    archive
      .append_data(&mut header, "world/link", io::empty())
      .unwrap();
    archive.into_inner().unwrap().finish().unwrap();
    assert!(validate_archive(&path).is_err());
    for value in ["../outside", "/tmp/outside", "a/../../outside", "a\\b"] {
      assert!(relative(Path::new(value)).is_err());
    }
  }
  #[test]
  fn valid_archive_extracts_to_private_runtime_files() {
    let temp = tempfile::tempdir().unwrap();
    let path = temp.path().join("world.tar.gz");
    let gzip = flate2::write::GzEncoder::new(
      std::fs::File::create(&path).unwrap(),
      flate2::Compression::default(),
    );
    let mut archive = tar::Builder::new(gzip);
    let mut header = tar::Header::new_gnu();
    header.set_size(5);
    header.set_mode(0o644);
    header.set_cksum();
    archive
      .append_data(&mut header, "world/level.dat", &b"world"[..])
      .unwrap();
    archive.into_inner().unwrap().finish().unwrap();
    validate_archive(&path).unwrap();
    let runtime = temp.path().join("runtime");
    extract_archive(&path, &runtime).unwrap();
    assert_eq!(
      std::fs::read(runtime.join("world/level.dat")).unwrap(),
      b"world"
    );
  }
}
