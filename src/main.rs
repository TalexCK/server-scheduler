mod api;
mod client;
mod config;
mod dependency_cache;
mod layout;
mod manager;
mod network;
mod protocol;
mod public_database;
mod runtime;
mod solo;

use std::{
  fs::{self, OpenOptions},
  io,
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
  tracing_subscriber::fmt()
    .with_env_filter(EnvFilter::try_from_default_env().unwrap_or_else(|_| "info".into()))
    .init();
  create_runtime_directories(&config).await?;
  let runtime_root = config.resolve(&config.value.paths.runtime);
  clear_runtime_directory(&runtime_root).await?;
  runtime::clear_staging(&runtime_root).await?;
  let manager = InstanceManager::new(config.clone());
  manager.initialize().await?;
  let public_database = public_database::PublicDatabase::connect(&config).await?;
  if let Some(database) = &public_database {
    database.publish(&config, &manager).await?;
  }
  let frpc = network::FrpcService::start(config.clone()).await?;
  let monitor = manager.clone();
  tokio::spawn(async move { monitor.monitor().await });
  let (shutdown, mut shutdown_receiver) = broadcast::channel(4);
  let publisher = public_database.clone().map(|database| {
    let config = config.clone();
    let manager = manager.clone();
    let receiver = shutdown.subscribe();
    tokio::spawn(async move { database.run(config, manager, receiver).await })
  });
  let state = ApiState {
    config: config.clone(),
    manager: manager.clone(),
    shutdown: shutdown.clone(),
    public_database,
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
  manager.stop_all().await;
  if let Some(publisher) = publisher {
    let _ = publisher.await;
  }
  if let Some(frpc) = frpc {
    frpc.stop().await;
  }
  let _ = tokio::fs::remove_file(config.resolve(&config.value.paths.pid_file)).await;
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
  let log_path = config.resolve(&config.value.paths.scheduler_log);
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
    .current_dir(&config.root)
    .stdin(Stdio::null())
    .stdout(Stdio::from(stdout))
    .stderr(Stdio::from(stderr));
  #[cfg(unix)]
  {
    use std::os::unix::process::CommandExt;
    command.process_group(0);
  }
  let child = command.spawn()?;
  fs::write(
    config.resolve(&config.value.paths.pid_file),
    format!("{}\n", child.id()),
  )?;
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
}
