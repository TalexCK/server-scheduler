use std::{
  io,
  path::{Path, PathBuf},
  process::Stdio,
  sync::Arc,
  time::Duration,
};

use thiserror::Error;
use tokio::{
  fs,
  process::{Child, Command},
  sync::oneshot,
  task::JoinHandle,
  time::sleep,
};
use tracing::{error, info, warn};

use crate::config::LoadedConfig;

pub struct FrpcService {
  stop: Option<oneshot::Sender<()>>,
  task: JoinHandle<()>,
}

impl FrpcService {
  pub async fn start(config: Arc<LoadedConfig>) -> Result<Option<Self>, NetworkError> {
    if !config.value.enable_frpc {
      return Ok(None);
    }
    let network_dir = config.resolve(&config.value.paths.network);
    let core = network_dir.join(&config.value.frpc_core);
    ensure_file(&core, "FRPC core").await?;
    let child = spawn(&core, &network_dir)?;
    let pid = child.id();
    let (stop, receiver) = oneshot::channel();
    let task = tokio::spawn(supervise(child, core, network_dir, receiver));
    info!(?pid, "FRPC started");
    Ok(Some(Self {
      stop: Some(stop),
      task,
    }))
  }

  pub async fn stop(mut self) {
    if let Some(stop) = self.stop.take() {
      let _ = stop.send(());
    }
    if let Err(join_error) = self.task.await {
      error!(%join_error, "FRPC supervisor failed");
    }
  }
}

async fn ensure_file(path: &Path, label: &'static str) -> Result<(), NetworkError> {
  let metadata = fs::metadata(path)
    .await
    .map_err(|source| NetworkError::Access {
      label,
      path: path.to_path_buf(),
      source,
    })?;
  if !metadata.is_file() {
    return Err(NetworkError::NotFile {
      label,
      path: path.to_path_buf(),
    });
  }
  Ok(())
}

fn spawn(core: &Path, network_dir: &Path) -> Result<Child, NetworkError> {
  Command::new(core)
    .current_dir(network_dir)
    .stdin(Stdio::null())
    .stdout(Stdio::inherit())
    .stderr(Stdio::inherit())
    .kill_on_drop(true)
    .spawn()
    .map_err(|source| NetworkError::Spawn {
      path: core.to_path_buf(),
      source,
    })
}

async fn supervise(
  mut child: Child,
  core: PathBuf,
  network_dir: PathBuf,
  mut stop: oneshot::Receiver<()>,
) {
  loop {
    let result = tokio::select! {
      _ = &mut stop => {
        if let Err(kill_error) = child.kill().await {
          warn!(%kill_error, "Failed to stop FRPC");
        }
        let _ = child.wait().await;
        info!("FRPC stopped");
        return;
      }
      result = child.wait() => result,
    };
    match result {
      Ok(status) => warn!(%status, "FRPC exited unexpectedly"),
      Err(wait_error) => error!(%wait_error, "Failed to wait for FRPC"),
    }
    loop {
      tokio::select! {
        _ = &mut stop => return,
        _ = sleep(Duration::from_secs(5)) => {}
      }
      match spawn(&core, &network_dir) {
        Ok(next) => {
          child = next;
          info!(pid = ?child.id(), "FRPC restarted");
          break;
        }
        Err(spawn_error) => error!(%spawn_error, "Failed to restart FRPC"),
      }
    }
  }
}

#[derive(Debug, Error)]
pub enum NetworkError {
  #[error("failed to access {label} at {path}: {source}")]
  Access {
    label: &'static str,
    path: PathBuf,
    source: io::Error,
  },
  #[error("{label} is not a file: {path}")]
  NotFile { label: &'static str, path: PathBuf },
  #[error("failed to start FRPC core {path}: {source}")]
  Spawn { path: PathBuf, source: io::Error },
}

#[cfg(all(test, unix))]
mod tests {
  use std::os::unix::fs::PermissionsExt;

  use tempfile::tempdir;
  use tokio::time::timeout;

  use super::*;

  #[tokio::test]
  async fn frpc_starts_from_the_network_directory() {
    let directory = tempdir().unwrap();
    let root = directory.path();
    fs::create_dir_all(root.join("network")).await.unwrap();
    fs::write(
      root.join("network/frpc"),
      b"#!/bin/sh\nprintf '%s\\n' \"$#\" > frpc.args\npwd > frpc.cwd\nwhile :; do sleep 1; done\n",
    )
    .await
    .unwrap();
    let mut permissions = std::fs::metadata(root.join("network/frpc"))
      .unwrap()
      .permissions();
    permissions.set_mode(0o755);
    std::fs::set_permissions(root.join("network/frpc"), permissions).unwrap();
    fs::write(
      root.join("config.json"),
      serde_json::to_vec(&serde_json::json!({
        "api":{"port":25566,"token":"01234567890123456789012345678901"},
        "bridge":{"token":"11234567890123456789012345678901"},
        "postgresql":{"home":"postgres","host":"127.0.0.1","port":5432,"username":"minigames","password":"test-password"},
        "ports":{"velocity":25565,"start":50001,"end":59999},
        "enable-frpc":true,
        "frpc-core":"frpc",
        "java":{"21_home":"java21","25_home":"java25"},
        "paths":{"cores":"cores","plugins":"plugins","mods":"mods","network":"network","servers":"servers","worlds":"worlds","runtime":"runtime","logs":"logs","pid_file":"scheduler.pid","scheduler_log":"scheduler.log"}
      }))
      .unwrap(),
    )
    .await
    .unwrap();
    let config = Arc::new(LoadedConfig::load(root.join("config.json")).await.unwrap());
    let service = FrpcService::start(config).await.unwrap().unwrap();
    let arguments = timeout(Duration::from_secs(5), async {
      loop {
        if let Ok(arguments) = fs::read_to_string(root.join("network/frpc.args")).await {
          break arguments;
        }
        sleep(Duration::from_millis(10)).await;
      }
    })
    .await
    .unwrap();
    let working_directory = fs::read_to_string(root.join("network/frpc.cwd"))
      .await
      .unwrap();
    let expected_working_directory = root.join("network").canonicalize().unwrap();
    assert_eq!(arguments.trim(), "0");
    assert_eq!(
      working_directory.trim(),
      expected_working_directory.to_str().unwrap()
    );
    assert!(!root.join("network/frpc.toml").exists());
    timeout(Duration::from_secs(5), service.stop())
      .await
      .unwrap();
  }
}
