use std::{collections::BTreeMap, io, time::Duration};

use base64::{Engine as _, engine::general_purpose::STANDARD_NO_PAD};

use crate::config::LoadedConfig;

pub async fn run(
  config: &LoadedConfig,
  game_id: Option<&str>,
) -> Result<(), Box<dyn std::error::Error>> {
  println!("Config: {}", config.file.display());
  let external_path = config.external_servers_file();
  println!("External file: {}", external_path.display());
  println!("External file exists: {}", external_path.is_file());
  match config.external_servers().await {
    Ok(external) => {
      println!("Configured external entries: {}", external.len());
      for (server, entry) in external {
        if selected(game_id, &entry.gamevoting.id) {
          println!(
            "Configured: game={:?} server={server:?} address={:?} version={:?}",
            entry.gamevoting.id, entry.address, entry.minecraft_version
          );
        }
      }
    }
    Err(error) => println!("Local catalog validation failed: {error}"),
  }

  println!("API: http://127.0.0.1:{}", config.value.api.port);
  let entries = fetch(config.value.api.port, &config.value.bridge.token).await?;
  println!("API total games: {}", entries.len());
  let mut counts = BTreeMap::new();
  for entry in &entries {
    *counts.entry(entry.columns).or_insert(0) += 1;
  }
  for (columns, count) in counts {
    println!("API records with {columns} columns: {count}");
  }
  let mut matched = 0;
  for entry in entries.iter().filter(|entry| selected(game_id, &entry.id)) {
    matched += 1;
    println!(
      "API game: id={:?} server={:?} solo={:?} voting={:?} external_address={:?} external_version={:?}",
      entry.id, entry.server, entry.solo, entry.voting, entry.address, entry.version
    );
  }
  println!("API matching games: {matched}");
  if !entries.is_empty() && entries.iter().all(|entry| entry.columns < 20) {
    println!("The running API returned legacy records despite include_external=true.");
  }
  println!("This command only read files and sent a GET request; no servers were changed.");
  Ok(())
}

fn selected(filter: Option<&str>, id: &str) -> bool {
  filter.is_none_or(|filter| filter.eq_ignore_ascii_case(id))
}

async fn fetch(port: u16, token: &str) -> Result<Vec<Entry>, Box<dyn std::error::Error>> {
  let response = reqwest::Client::builder()
    .no_proxy()
    .redirect(reqwest::redirect::Policy::none())
    .timeout(Duration::from_secs(10))
    .build()?
    .get(format!(
      "http://127.0.0.1:{port}/bridge/v1/games?include_voting=true&include_external=true"
    ))
    .bearer_auth(token)
    .send()
    .await?;
  if !response.status().is_success() {
    return Err(
      io::Error::other(format!(
        "Catalog HTTP {}; check Scheduler logs at this time for details.",
        response.status()
      ))
      .into(),
    );
  }
  parse(&response.text().await?).map_err(Into::into)
}

#[derive(Debug)]
struct Entry {
  columns: usize,
  id: String,
  server: String,
  solo: Option<bool>,
  voting: Option<bool>,
  address: Option<String>,
  version: Option<String>,
}

fn parse(body: &str) -> io::Result<Vec<Entry>> {
  body
    .lines()
    .filter(|line| !line.trim().is_empty())
    .map(|line| {
      let fields = line.split('\t').collect::<Vec<_>>();
      let columns = fields.len();
      if ![12, 17, 18, 20].contains(&columns) {
        return Err(io::Error::other(format!(
          "Invalid catalog record: {columns} columns"
        )));
      }
      Ok(Entry {
        columns,
        id: decode(fields[1])?,
        server: decode(fields[2])?,
        solo: if columns >= 17 {
          Some(boolean(fields[12])?)
        } else {
          None
        },
        voting: if columns >= 18 {
          Some(boolean(fields[17])?)
        } else {
          None
        },
        address: if columns == 20 && !fields[18].is_empty() {
          Some(decode(fields[18])?)
        } else {
          None
        },
        version: if columns == 20 && !fields[19].is_empty() {
          Some(decode(fields[19])?)
        } else {
          None
        },
      })
    })
    .collect()
}

fn decode(field: &str) -> io::Result<String> {
  let bytes = STANDARD_NO_PAD
    .decode(field.trim_end_matches('='))
    .map_err(|_| io::Error::other("Invalid base64 in catalog"))?;
  String::from_utf8(bytes).map_err(|_| io::Error::other("Invalid UTF-8 in catalog"))
}

fn boolean(field: &str) -> io::Result<bool> {
  field
    .parse()
    .map_err(|_| io::Error::other("Invalid boolean in catalog"))
}

#[cfg(test)]
mod tests {
  use super::*;

  fn row() -> String {
    let enc = |value: &str| STANDARD_NO_PAD.encode(value);
    [
      "1".into(),
      enc("survival"),
      enc("survival"),
      enc("&a&l生存服务器"),
      enc("&7点击进入生存服务器"),
      enc("GRASS_BLOCK"),
      "0".into(),
      enc("26.2"),
      "".into(),
      "".into(),
      "1".into(),
      "50".into(),
      "true".into(),
      enc("shared"),
      enc("on_demand"),
      "1".into(),
      "10".into(),
      "false".into(),
      enc("10.19.72.18:25565"),
      enc("26.2"),
    ]
    .join("\t")
  }

  #[test]
  fn distinguishes_external_legacy_and_managed_records_with_empty_trailing_fields() {
    let row = row();
    let legacy = row.split('\t').take(18).collect::<Vec<_>>().join("\t");
    let entries = parse(&format!("{row}\n{legacy}\n{legacy}\t\t\n")).unwrap();
    assert_eq!(entries.len(), 3);
    assert_eq!(entries[0].id, "survival");
    assert_eq!(entries[0].address.as_deref(), Some("10.19.72.18:25565"));
    assert_eq!(entries[0].solo, Some(true));
    assert_eq!(entries[0].voting, Some(false));
    assert_eq!(entries[1].columns, 18);
    assert_eq!(entries[2].columns, 20);
    assert_eq!(entries[2].address, None);
    assert!(parse("").unwrap().is_empty());
    assert!(parse("invalid").is_err());
  }

  #[tokio::test]
  async fn requests_external_catalog_using_bridge_auth_without_other_requests() {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let port = listener.local_addr().unwrap().port();
    let router = axum::Router::new().route(
      "/bridge/v1/games",
      axum::routing::get(|request: axum::extract::Request| async move {
        assert_eq!(
          request.uri().query(),
          Some("include_voting=true&include_external=true")
        );
        assert_eq!(
          request.headers()["authorization"],
          "Bearer test-bridge-token"
        );
        row()
      }),
    );
    let server = tokio::spawn(async move { axum::serve(listener, router).await.unwrap() });
    let entries = fetch(port, "test-bridge-token").await.unwrap();
    server.abort();
    assert_eq!(entries.len(), 1);
    assert_eq!(entries[0].id, "survival");
    assert_eq!(entries[0].version.as_deref(), Some("26.2"));
  }
}
