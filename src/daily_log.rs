use chrono::{DateTime, Local, NaiveDate};
use std::{
  fs::{self, File, OpenOptions},
  io::{self, Write},
  path::{Path, PathBuf},
  sync::{Arc, Mutex, MutexGuard},
};

#[derive(Clone)]
pub struct DailyLog(Arc<Mutex<State>>);
struct State {
  file: File,
  path: PathBuf,
  day: NaiveDate,
  bytes: u64,
  limit: u64,
  retention_days: i64,
}
pub struct Guard<'a>(MutexGuard<'a, State>);
impl DailyLog {
  pub fn maintain(&self) -> io::Result<()> {
    self
      .0
      .lock()
      .unwrap_or_else(|e| e.into_inner())
      .rotate(Local::now().date_naive())
  }
  pub fn new(path: &Path) -> io::Result<Self> {
    if let Some(parent) = path.parent() {
      fs::create_dir_all(parent)?;
    }
    let file = OpenOptions::new().create(true).append(true).open(path)?;
    let metadata = file.metadata()?;
    let day = metadata
      .modified()
      .map(DateTime::<Local>::from)
      .map(|d| d.date_naive())
      .unwrap_or_else(|_| Local::now().date_naive());
    let mut state = State {
      file,
      path: path.to_path_buf(),
      day,
      bytes: metadata.len(),
      limit: 64 * 1024 * 1024,
      retention_days: 14,
    };
    state.rotate(Local::now().date_naive())?;
    Ok(Self(Arc::new(Mutex::new(state))))
  }
}
impl State {
  fn rotate(&mut self, today: NaiveDate) -> io::Result<()> {
    if self.day == today && self.bytes < self.limit {
      return Ok(());
    }
    self.file.flush()?;
    let parent = self.path.parent().unwrap_or(Path::new("."));
    let name = self.path.file_name().unwrap().to_string_lossy();
    let archives = parent.join("scheduler-archives");
    fs::create_dir_all(&archives)?;
    if self.bytes > 0 {
      let archive = archives.join(format!("{name}.{}.{}.gz", self.day, uuid::Uuid::new_v4()));
      let temporary = archive.with_extension("part");
      let mut input = File::open(&self.path)?;
      let mut gzip =
        flate2::write::GzEncoder::new(File::create(&temporary)?, flate2::Compression::default());
      io::copy(&mut input, &mut gzip)?;
      gzip.finish()?.sync_all()?;
      fs::rename(temporary, archive)?;
    }
    self.file = OpenOptions::new()
      .create(true)
      .write(true)
      .truncate(true)
      .open(&self.path)?;
    self.day = today;
    self.bytes = 0;
    for entry in fs::read_dir(archives)? {
      let entry = entry?;
      let filename = entry.file_name().to_string_lossy().to_string();
      let Some(rest) = filename.strip_prefix(&format!("{name}.")) else {
        continue;
      };
      if !filename.ends_with(".gz") {
        continue;
      }
      if let Some(date) = rest
        .get(..10)
        .and_then(|s| NaiveDate::parse_from_str(s, "%Y-%m-%d").ok())
        && today.signed_duration_since(date).num_days() > self.retention_days
      {
        fs::remove_file(entry.path())?;
      }
    }
    Ok(())
  }
}
impl<'a> tracing_subscriber::fmt::MakeWriter<'a> for DailyLog {
  type Writer = Guard<'a>;
  fn make_writer(&'a self) -> Self::Writer {
    let mut state = self.0.lock().unwrap_or_else(|e| e.into_inner());
    if let Err(error) = state.rotate(Local::now().date_naive()) {
      eprintln!("Scheduler log rotation failed: {error}");
    }
    Guard(state)
  }
}
impl Write for Guard<'_> {
  fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
    let n = self.0.file.write(bytes)?;
    self.0.bytes += n as u64;
    Ok(n)
  }
  fn flush(&mut self) -> io::Result<()> {
    self.0.file.flush()
  }
}
#[cfg(test)]
mod tests {
  use super::*;
  #[test]
  fn rotates_to_compressed_archive_and_keeps_new_log_writable() {
    let temp = tempfile::tempdir().unwrap();
    let path = temp.path().join("scheduler.log");
    let log = DailyLog::new(&path).unwrap();
    {
      let mut state = log.0.lock().unwrap();
      state.file.write_all(b"yesterday\n").unwrap();
      state.bytes = 10;
      state.day -= chrono::Duration::days(1);
      state.rotate(Local::now().date_naive()).unwrap();
      state.file.write_all(b"today\n").unwrap();
    }
    assert_eq!(fs::read(&path).unwrap(), b"today\n");
    let archive = fs::read_dir(temp.path().join("scheduler-archives"))
      .unwrap()
      .next()
      .unwrap()
      .unwrap()
      .path();
    let mut decoded = String::new();
    use std::io::Read;
    flate2::read::GzDecoder::new(File::open(archive).unwrap())
      .read_to_string(&mut decoded)
      .unwrap();
    assert_eq!(decoded, "yesterday\n");
  }
}
