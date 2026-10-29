mod api;
mod catalog_check;
mod client;
mod config;
mod daily_log;
mod dependency_cache;
mod deployment;
mod layout;
mod manager;
mod network;
mod protocol;
mod resources;
mod runtime;
mod solo;
mod world_store;

use std::{
  fs::{self, OpenOptions},
  io::{self, Write},
  path::{Path, PathBuf},
  process::Stdio,
  sync::Arc,
  time::Duration,
};

use clap::{Parser, Subcommand};
use tokio::{net::TcpListener, sync::broadcast, time::sleep};
use tracing::info;
use tracing_subscriber::EnvFilter;

use crate::{
  api::ApiState, client::SchedulerClient, config::LoadedConfig, manager::InstanceManager,
};

#[derive(Debug, Parser)]
#[command(
  name = "server-scheduler",
  version,
  about = "Minecraft server scheduler"
)]
struct Arguments {
  #[arg(long, default_value = "config.json", global = true)]
  config: PathBuf,
  #[command(subcommand)]
  command: SchedulerCommand,
}

#[derive(Debug, Subcommand)]
enum SchedulerCommand {
  Start {
    server_id: Option<String>,
    #[arg(long = "player", alias = "players", value_delimiter = ',', num_args = 1..)]
    players: Vec<String>,
  },
  Daemon,
  Stop,
  Restart,
  Terminate {
    server_id: String,
  },
  Log {
    server_id: String,
    #[arg(short, long, default_value_t = 100)]
    lines: usize,
  },
  List,
  /// Prepare and atomically activate the resource manifests.
  Update,
  /// Prepare just one server and publish its current resource set.
  Prepare {
    server_id: String,
    #[arg(long)]
    local_only: bool,
  },
  /// Remove resource cache objects not referenced by a published server.
  CacheGc,
  /// Restore the previous per-server set after failed readiness.
  RollbackResources {
    server_id: String,
  },
  /// Compare local external definitions with the running daemon's game catalog (read-only).
  Catalog {
    game_id: Option<String>,
  },
  Command {
    server_id: String,
    #[arg(required = true, trailing_var_arg = true)]
    command: Vec<String>,
  },
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
  let arguments = Arguments::parse();
  let config = Arc::new(LoadedConfig::load(&arguments.config).await?);
  match arguments.command {
    SchedulerCommand::Start { server_id, players } => match server_id {
      Some(server_id) => {
        let value = SchedulerClient::new(&config)?
          .launch(&server_id, players)
          .await?;
        println!(
          "{} {} {:?} {}",
          value.server_id, value.instance_id, value.state, value.pid
        );
      }
      None => start_background(&config).await?,
    },
    SchedulerCommand::Daemon => run_daemon(config).await?,
    SchedulerCommand::Stop => stop_scheduler(&config).await?,
    SchedulerCommand::Restart => restart_scheduler(&config).await?,
    SchedulerCommand::Terminate { server_id } => {
      let value = SchedulerClient::new(&config)?.terminate(&server_id).await?;
      println!("{} {:?}", value.server_id, value.state);
    }
    SchedulerCommand::Log { server_id, lines } => {
      for line in SchedulerClient::new(&config)?
        .logs(&server_id, lines)
        .await?
        .lines
      {
        println!("{line}");
      }
    }
    SchedulerCommand::List => {
      for instance in SchedulerClient::new(&config)?.list().await? {
        println!(
          "{}\t{}\t{:?}\t{}\t{}",
          instance.server_id, instance.instance_id, instance.state, instance.pid, instance.port
        );
      }
    }
    SchedulerCommand::Prepare {
      server_id,
      local_only,
    } => {
      let result = deployment::prepare(config, &server_id, local_only)
        .await
        .map_err(|e| io::Error::other(e.to_string()))?;
      println!("{}", serde_json::to_string(&result)?);
    }
    SchedulerCommand::RollbackResources { server_id } => {
      deployment::rollback(&config, &server_id).map_err(|e| io::Error::other(e.to_string()))?;
    }
    SchedulerCommand::CacheGc => {
      println!(
        "Removed {} unused objects",
        deployment::gc(&config).map_err(|e| io::Error::other(e.to_string()))?
      );
    }
    SchedulerCommand::Update => {
      let mut source = (*config).clone();
      source.generation = Some(config.resolve(&config.value.paths.manifests));
      let mut failures = Vec::new();
      for id in source.server_ids().await? {
        match deployment::prepare(config.clone(), &id, false).await {
          Ok(result) => println!("{}", serde_json::to_string(&result)?),
          Err(error) => failures.push(format!("{id}: {error}")),
        }
      }
      if !failures.is_empty() {
        return Err(io::Error::other(failures.join("\n")).into());
      }
      deployment::gc(&config).map_err(|e| io::Error::other(e.to_string()))?;
    }
    SchedulerCommand::Catalog { game_id } => {
      catalog_check::run(&config, game_id.as_deref()).await?;
    }
    SchedulerCommand::Command { server_id, command } => {
      for line in SchedulerClient::new(&config)?
        .command(&server_id, command.join(" "))
        .await?
        .output
      {
        println!("{line}");
      }
    }
  }
  Ok(())
}

async fn run_daemon(config: Arc<LoadedConfig>) -> Result<(), Box<dyn std::error::Error>> {
  let _pid_guard = ensure_daemon_pid_claim(&config).await?;
  let daily_log = daily_log::DailyLog::new(&config.resolve(&config.value.paths.scheduler_log))?;
  tracing_subscriber::fmt()
    .with_ansi(false)
    .with_writer(daily_log.clone())
    .with_env_filter(EnvFilter::try_from_default_env().unwrap_or_else(|_| "info".into()))
    .init();
  tokio::spawn(async move {
    let mut interval = tokio::time::interval(Duration::from_secs(60));
    loop {
      interval.tick().await;
      let log = daily_log.clone();
      if let Ok(Err(error)) = tokio::task::spawn_blocking(move || log.maintain()).await {
        eprintln!("Scheduler log maintenance failed: {error}");
      }
    }
  });
  create_runtime_directories(&config).await?;
  let runtime_root = config.resolve(&config.value.paths.runtime);
  clear_runtime_directory(&runtime_root).await?;
  runtime::clear_staging(&runtime_root).await?;
  let manager = InstanceManager::new(config.clone());
  manager.initialize().await?;
  let frpc = network::FrpcService::start(config.clone()).await?;
  let monitor = manager.clone();
  tokio::spawn(async move { monitor.monitor().await });
  let (shutdown, mut shutdown_receiver) = broadcast::channel(4);
  let state = ApiState {
    config: config.clone(),
    manager: manager.clone(),
    shutdown: shutdown.clone(),
  };
  let listener = TcpListener::bind(("127.0.0.1", config.value.api.port)).await?;
  info!(address = %listener.local_addr()?, "scheduler daemon started");
  let result = axum::serve(listener, api::router(state))
    .with_graceful_shutdown(async move {
      tokio::select! {
          _ = shutdown_receiver.recv() => {}
          _ = tokio::signal::ctrl_c() => {}
      }
    })
    .await;
  let _ = shutdown.send(());
  for (server_id, error) in manager.stop_all().await {
    tracing::error!(%server_id, %error, "server failed to stop cleanly during shutdown");
  }
  if let Some(frpc) = frpc {
    frpc.stop().await;
  }
  result?;
  Ok(())
}

async fn start_background(config: &LoadedConfig) -> Result<(), Box<dyn std::error::Error>> {
  let client = SchedulerClient::new(config)?;
  if client.healthy().await {
    return Err(
      io::Error::new(io::ErrorKind::AlreadyExists, "scheduler is already running").into(),
    );
  }
  fs::create_dir_all(config.resolve(&config.value.paths.logs))?;
  fs::create_dir_all(config.resolve(&config.value.paths.runtime))?;
  let log_path = config
    .resolve(&config.value.paths.logs)
    .join("scheduler-bootstrap.log");
  let stdout = OpenOptions::new()
    .create(true)
    .append(true)
    .open(&log_path)?;
  let stderr = stdout.try_clone()?;
  let executable = std::env::current_exe()?;
  let mut command = std::process::Command::new(executable);
  command
    .arg("--config")
    .arg(&config.file)
    .arg("daemon")
    .env("SCHEDULER_PID_CLAIMED", "1")
    .current_dir(&config.root)
    .stdin(Stdio::null())
    .stdout(Stdio::from(stdout))
    .stderr(Stdio::from(stderr));
  #[cfg(unix)]
  {
    use std::os::unix::process::CommandExt;
    command.process_group(0);
  }
  let pid_path = config.resolve(&config.value.paths.pid_file);
  let mut pid_file = claim_pid_file(&pid_path)?;
  let mut child = match command.spawn() {
    Ok(child) => child,
    Err(error) => {
      let _ = fs::remove_file(&pid_path);
      return Err(error.into());
    }
  };
  if let Err(error) = writeln!(pid_file, "{}", child.id()).and_then(|()| pid_file.sync_all()) {
    let _ = child.kill();
    let _ = fs::remove_file(&pid_path);
    return Err(error.into());
  }
  for _ in 0..100 {
    if client.healthy().await {
      println!("scheduler started with PID {}", child.id());
      return Ok(());
    }
    sleep(Duration::from_millis(100)).await;
  }
  Err(
    io::Error::new(
      io::ErrorKind::TimedOut,
      format!("scheduler failed to start; inspect {}", log_path.display()),
    )
    .into(),
  )
}

fn claim_pid_file(path: &Path) -> io::Result<fs::File> {
  match OpenOptions::new().write(true).create_new(true).open(path) {
    Ok(file) => Ok(file),
    Err(error) if error.kind() == io::ErrorKind::AlreadyExists => {
      let detail = if scheduler_pid_is_live(path)? {
        "scheduler is already running or starting"
      } else {
        "stale PID file exists; confirm the old process has exited, then remove the file manually"
      };
      Err(io::Error::new(
        io::ErrorKind::AlreadyExists,
        format!("{detail}: {}", path.display()),
      ))
    }
    Err(error) => Err(error),
  }
}

fn scheduler_pid_is_live(path: &Path) -> io::Result<bool> {
  let content = fs::read_to_string(path)?;
  let Ok(pid) = content.trim().parse::<u32>() else {
    return Ok(true);
  };
  Ok(process_is_running(pid))
}

struct PidFileGuard {
  path: PathBuf,
  pid: u32,
}

impl Drop for PidFileGuard {
  fn drop(&mut self) {
    if fs::read_to_string(&self.path)
      .ok()
      .and_then(|value| value.trim().parse::<u32>().ok())
      == Some(self.pid)
    {
      let _ = fs::remove_file(&self.path);
    }
  }
}

async fn ensure_daemon_pid_claim(config: &LoadedConfig) -> io::Result<PidFileGuard> {
  let path = config.resolve(&config.value.paths.pid_file);
  let current_pid = std::process::id();
  if std::env::var_os("SCHEDULER_PID_CLAIMED").is_some() {
    for _ in 0..500 {
      if fs::read_to_string(&path)
        .ok()
        .and_then(|value| value.trim().parse::<u32>().ok())
        == Some(current_pid)
      {
        return Ok(PidFileGuard {
          path,
          pid: current_pid,
        });
      }
      sleep(Duration::from_millis(10)).await;
    }
    return Err(io::Error::new(
      io::ErrorKind::TimedOut,
      format!(
        "parent process did not publish daemon PID at {}",
        path.display()
      ),
    ));
  }

  let mut file = claim_pid_file(&path)?;
  writeln!(file, "{current_pid}")?;
  file.sync_all()?;
  Ok(PidFileGuard {
    path,
    pid: current_pid,
  })
}

#[cfg(unix)]
fn process_is_running(pid: u32) -> bool {
  let Ok(pid) = i32::try_from(pid) else {
    return false;
  };
  // SAFETY：kill(pid, 0) 不发送信号，只检查目标进程是否存在。
  if unsafe { libc::kill(pid, 0) } == 0 {
    return true;
  }
  io::Error::last_os_error().raw_os_error() == Some(libc::EPERM)
}

#[cfg(not(unix))]
fn process_is_running(_pid: u32) -> bool {
  // 缺少可移植的进程查询时，保留锁比启动第二个守护进程更安全。
  true
}

async fn stop_scheduler(config: &LoadedConfig) -> Result<(), Box<dyn std::error::Error>> {
  let client = SchedulerClient::new(config)?;
  if !client.healthy().await {
    return Err(io::Error::new(io::ErrorKind::NotConnected, "scheduler is not running").into());
  }
  client.shutdown().await?;
  let pid_file = config.resolve(&config.value.paths.pid_file);
  for _ in 0..700 {
    if !client.healthy().await && !pid_file.exists() {
      println!("scheduler stopped");
      return Ok(());
    }
    sleep(Duration::from_millis(100)).await;
  }
  Err(
    io::Error::new(
      io::ErrorKind::TimedOut,
      "scheduler did not stop within 70 seconds",
    )
    .into(),
  )
}

async fn restart_scheduler(config: &LoadedConfig) -> Result<(), Box<dyn std::error::Error>> {
  let client = SchedulerClient::new(config)?;
  if client.healthy().await {
    stop_scheduler(config).await?;
  }
  start_background(config).await
}

async fn create_runtime_directories(config: &LoadedConfig) -> io::Result<()> {
  for path in [&config.value.paths.runtime, &config.value.paths.logs] {
    tokio::fs::create_dir_all(config.resolve(path)).await?;
  }
  Ok(())
}

async fn clear_runtime_directory(runtime: &Path) -> io::Result<()> {
  let mut entries = tokio::fs::read_dir(runtime).await?;
  while let Some(entry) = entries.next_entry().await? {
    let path = entry.path();
    if entry.file_type().await?.is_dir() {
      if world_store::has_recovery_marker(&path).await? {
        tracing::warn!(
          path = %path.display(),
          "preserving persistent world recovery directory"
        );
        continue;
      }
      tokio::fs::remove_dir_all(path).await?;
    } else {
      tokio::fs::remove_file(path).await?;
    }
  }
  Ok(())
}

#[cfg(test)]
mod tests {
  use super::*;

  #[test]
  fn player_option_accepts_a_list() {
    let arguments = Arguments::try_parse_from([
      "server-scheduler",
      "start",
      "skywars",
      "--player",
      "first,second",
      "third",
    ])
    .unwrap();
    match arguments.command {
      SchedulerCommand::Start { server_id, players } => {
        assert_eq!(server_id.as_deref(), Some("skywars"));
        assert_eq!(players, ["first", "second", "third"]);
      }
      _ => panic!("unexpected scheduler command"),
    }
  }

  #[test]
  fn pid_claim_rejects_a_live_scheduler_process() {
    let root = tempfile::tempdir().unwrap();
    let path = root.path().join("scheduler.pid");
    fs::write(&path, format!("{}\n", std::process::id())).unwrap();

    let error = claim_pid_file(&path).unwrap_err();

    assert_eq!(error.kind(), io::ErrorKind::AlreadyExists);
  }

  #[test]
  fn pid_claim_never_removes_an_existing_stale_file() {
    let root = tempfile::tempdir().unwrap();
    let path = root.path().join("scheduler.pid");
    fs::write(&path, "stale\n").unwrap();

    let error = claim_pid_file(&path).unwrap_err();

    assert_eq!(error.kind(), io::ErrorKind::AlreadyExists);
    assert_eq!(fs::read_to_string(path).unwrap(), "stale\n");
  }

  #[tokio::test]
  async fn runtime_cleanup_removes_all_entries() {
    let root = tempfile::tempdir().unwrap();
    let runtime = root.path().join("runtime");
    tokio::fs::create_dir_all(runtime.join("server/files"))
      .await
      .unwrap();
    tokio::fs::write(runtime.join("server/files/value.txt"), "value")
      .await
      .unwrap();
    tokio::fs::write(runtime.join("orphan.txt"), "value")
      .await
      .unwrap();

    clear_runtime_directory(&runtime).await.unwrap();

    assert!(
      tokio::fs::read_dir(&runtime)
        .await
        .unwrap()
        .next_entry()
        .await
        .unwrap()
        .is_none()
    );
  }

  #[tokio::test]
  async fn runtime_cleanup_preserves_persistent_world_recovery() {
    let root = tempfile::tempdir().unwrap();
    let runtime = root.path().join("runtime");
    let recovery = runtime.join("survival-instance");
    tokio::fs::create_dir_all(recovery.join("world"))
      .await
      .unwrap();
    tokio::fs::write(
      recovery.join(world_store::RECOVERY_MARKER),
      r#"{"id":"survival","paths":["world"]}"#,
    )
    .await
    .unwrap();
    tokio::fs::create_dir_all(runtime.join("temporary"))
      .await
      .unwrap();

    clear_runtime_directory(&runtime).await.unwrap();

    assert!(recovery.join("world").exists());
    assert!(!runtime.join("temporary").exists());
  }
}
