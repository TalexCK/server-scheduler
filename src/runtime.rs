use std::{
  io,
  path::{Path, PathBuf},
};

use tokio::fs;

pub fn staging_root(runtime_root: &Path) -> io::Result<PathBuf> {
  let parent = runtime_root.parent().ok_or_else(|| {
    io::Error::new(
      io::ErrorKind::InvalidInput,
      format!(
        "runtime root has no parent directory: {}",
        runtime_root.display()
      ),
    )
  })?;
  let name = runtime_root
    .file_name()
    .and_then(|value| value.to_str())
    .ok_or_else(|| {
      io::Error::new(
        io::ErrorKind::InvalidInput,
        format!(
          "runtime root has no valid directory name: {}",
          runtime_root.display()
        ),
      )
    })?;
  Ok(parent.join(format!(
    ".server-scheduler-staging-{}",
    name.trim_start_matches('.')
  )))
}

pub async fn stage_instance(runtime_root: &Path, work_dir: &Path) -> io::Result<Option<PathBuf>> {
  if work_dir.parent() != Some(runtime_root) {
    return Err(io::Error::new(
      io::ErrorKind::InvalidInput,
      format!(
        "instance directory is outside the runtime root: {}",
        work_dir.display()
      ),
    ));
  }
  if !fs::try_exists(work_dir).await? {
    return Ok(None);
  }
  let root = staging_root(runtime_root)?;
  fs::create_dir_all(&root).await?;
  let name = work_dir.file_name().ok_or_else(|| {
    io::Error::new(
      io::ErrorKind::InvalidInput,
      format!("instance directory has no name: {}", work_dir.display()),
    )
  })?;
  let staged = root.join(name);
  fs::rename(work_dir, &staged).await?;
  Ok(Some(staged))
}

pub async fn remove_instance(path: &Path) -> io::Result<()> {
  match fs::remove_dir_all(path).await {
    Ok(()) => Ok(()),
    Err(error) if error.kind() == io::ErrorKind::NotFound => Ok(()),
    Err(error) => Err(error),
  }
}

pub async fn remove_staged_instance(path: &Path) -> io::Result<()> {
  remove_instance(path).await?;
  if let Some(root) = path.parent() {
    match fs::remove_dir(root).await {
      Ok(()) => {}
      Err(error)
        if matches!(
          error.kind(),
          io::ErrorKind::NotFound | io::ErrorKind::DirectoryNotEmpty
        ) => {}
      Err(error) => return Err(error),
    }
  }
  Ok(())
}

pub async fn clear_staging(runtime_root: &Path) -> io::Result<()> {
  remove_instance(&staging_root(runtime_root)?).await
}

#[cfg(test)]
mod tests {
  use super::*;

  #[tokio::test]
  async fn staging_moves_instance_outside_runtime_root() {
    let directory = tempfile::tempdir().unwrap();
    let runtime_root = directory.path().join("runtime");
    let work_dir = runtime_root.join("game-instance");
    let log_file = directory.path().join("logs/game-instance.log");
    fs::create_dir_all(work_dir.join("libraries"))
      .await
      .unwrap();
    fs::create_dir_all(log_file.parent().unwrap())
      .await
      .unwrap();
    fs::write(work_dir.join("libraries/library.jar"), b"library")
      .await
      .unwrap();
    fs::write(&log_file, b"log").await.unwrap();

    let staged = stage_instance(&runtime_root, &work_dir)
      .await
      .unwrap()
      .unwrap();

    assert!(!work_dir.exists());
    assert!(staged.exists());
    assert!(!staged.starts_with(&runtime_root));
    assert!(log_file.exists());

    remove_staged_instance(&staged).await.unwrap();

    assert!(!staged.exists());
    assert!(log_file.exists());
  }

  #[tokio::test]
  async fn daemon_cleanup_removes_staging_left_by_a_crash() {
    let directory = tempfile::tempdir().unwrap();
    let runtime_root = directory.path().join(".runtime");
    fs::create_dir_all(&runtime_root).await.unwrap();
    let staging = staging_root(&runtime_root).unwrap();
    fs::create_dir_all(staging.join("game-instance/libraries"))
      .await
      .unwrap();
    fs::write(
      staging.join("game-instance/libraries/library.jar"),
      b"library",
    )
    .await
    .unwrap();

    clear_staging(&runtime_root).await.unwrap();

    assert!(!staging.exists());
    assert!(runtime_root.exists());
  }
}
