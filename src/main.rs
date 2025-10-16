use std::path::Path;
use std::time::{Instant};
use seahash::SeaHasher;
use std::hash::{Hasher};
use clap::Parser;
//use sha2::{Digest, Sha256};
//use tokio::time::error::Elapsed;
use tokio_postgres::{NoTls, Error};
use walkdir::WalkDir;

#[derive(Parser, Debug)]
#[command(author, version, about, long_about = None)]
struct Args {
    #[arg(short, long)]
    path: String,

    #[arg(short, long)]
    db_conn: String
}

#[tokio::main]
async fn main() -> Result<(), Error> {
  let start_time = Instant::now();
  let args = Args::parse();
  let path = Path::new(&args.path);

  let (client, connection) =
      tokio_postgres::connect(&args.db_conn, NoTls).await?;

  tokio::spawn(async move {
      if let Err(e) = connection.await {
          eprintln!("connection error: {}", e);
      }
  });

  client
      .batch_execute(
          "
          CREATE TABLE IF NOT EXISTS files (
              id          SERIAL PRIMARY KEY,
              file_name   TEXT NOT NULL,
              full_path   TEXT NOT NULL UNIQUE,
              checksum    TEXT,
              last_access TIMESTAMPTZ,
              last_write  TIMESTAMPTZ,
              created     TIMESTAMPTZ,
              file_size   BIGINT
          )
          ",
      ).await?;

  println!("Scanning directory: {}", path.display());

  for entry in WalkDir::new(path).into_iter().filter_map(|e| e.ok()) {
      if entry.file_type().is_file() {
          let path = entry.path();
          let file_name = path.file_name().unwrap_or_default().to_string_lossy().to_string();
          let full_path = path.to_string_lossy().to_string();

          eprintln!("Processing: {}", &full_path);

          let metadata = match entry.metadata() {
              Ok(meta) => Some(meta),
              Err(e) => {
                  eprintln!("Could not get metadata for {}: {}", full_path, e);
                  None
              }
          };

          let (last_access, last_write, created, file_size) = if let Some(meta) = metadata {
              (
                  meta.accessed().ok().map(chrono::DateTime::<chrono::Utc>::from),
                  meta.modified().ok().map(chrono::DateTime::<chrono::Utc>::from),
                  meta.created().ok().map(chrono::DateTime::<chrono::Utc>::from),
                  Some(meta.len() as i64),
              )
          } else {
              (None, None, None, None)
          };
          
          let checksum = match std::fs::read(path) {
              Ok(data) => {
                  let mut hasher = SeaHasher::new();
                  hasher.write(&data);
                  let result = hasher.finish();
                  Some(format!("{:x}", result))
              }
              Err(e) => {
                  eprintln!("Could not read file {}: {}", full_path, e);
                  None
              }
          };

          let statement = client
              .prepare(
                  "INSERT INTO files (file_name, full_path, checksum, last_access, last_write, created, file_size)
                   VALUES ($1, $2, $3, $4, $5, $6, $7)
                   ON CONFLICT (full_path) DO UPDATE
                   SET file_name = EXCLUDED.file_name,
                       full_path = EXCLUDED.full_path,
                       checksum = EXCLUDED.checksum,
                       last_access = EXCLUDED.last_access,
                       last_write = EXCLUDED.last_write,
                       created = EXCLUDED.created,
                       file_size = EXCLUDED.file_size",
              ).await?;

          client
              .execute(
                  &statement,
                  &[&file_name, &full_path, &checksum, &last_access, &last_write, &created, &file_size],
              ).await?;
      }
    }

  let elapsed = start_time.elapsed();
  println!("Scanning complete, execution time: {:.2?}", elapsed);

  Ok(())
}
