use super::*;
use duckdb::{Config, Connection, Result};
use serde::{Deserialize, Serialize};
/// Start and return a new in-memory SurrealDB connection and apply base schema.
pub async fn start_duck_db(
    max_mem: &str,
    thread_count: i64,
) -> Result<Connection, crate::error::AppError> {
    let config = Config::default()
        .enable_object_cache(true)?
        .max_memory(max_mem)?
        .threads(thread_count)?;
    let db = Connection::open_in_memory_with_flags(config)?;
    Ok(db)
}

/// Open a DuckDB database from a `.db` file on disk.
/// Returns a connection configured with the same tuning knobs as `start_duck_db`.
pub async fn open_duck_db_from_file(
    path: &str,
    max_mem: &str,
    thread_count: i64,
) -> Result<Connection, crate::error::AppError> {
    // Ensure the file exists to avoid implicitly creating a new DB
    if !std::path::Path::new(path).exists() {
        return Err(std::io::Error::new(
            std::io::ErrorKind::NotFound,
            format!("DuckDB file not found: {}", path),
        )
        .into());
    }
    let config = Config::default()
        .enable_object_cache(true)?
        .max_memory(max_mem)?
        .threads(thread_count)?;

    let conn = Connection::open_with_flags(path, config)?;
    Ok(conn)
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
pub enum DbType {
    GlobalDailyIndex,
    GlobalRets,
    UsMarket,
}

impl DbType {
    pub async fn ingest(
        self,
        conn: Arc<Connection>,
        parquet_path: &str,
    ) -> Result<usize, crate::error::AppError> {
        match self {
            DbType::GlobalDailyIndex => {
                crsp::GlobalDailyIndex::duck_from_parquet(conn, parquet_path.to_string()).await
            }
            DbType::GlobalRets => {
                world_indices::GlobalRets::duck_from_parquet(conn, parquet_path.to_string()).await
            }
            DbType::UsMarket => {
                usindexes::UsMarketIndex::duck_from_parquet(conn, parquet_path.to_string()).await
            }
        }
    }
}

/// Persist the current in-memory DB to a DuckDB file.
/// If `path` exists, this removes it first (DuckDB expects a new file).
pub fn persist_in_memory_to_file(
    conn: &Connection,
    path: &str,
) -> Result<(), crate::error::AppError> {
    // DuckDB's ATTACH wants the target file to not exist
    if std::path::Path::new(path).exists() {
        std::fs::remove_file(path)?;
    }
    // Simple quote-escape for the SQL literal
    let path_sql = path.replace('\'', "''");
    let sql = format!(
        r#"
        ATTACH '{path}' AS diskdb;
        COPY FROM DATABASE memory TO diskdb;
        DETACH diskdb;
    "#,
        path = path_sql
    );

    conn.execute_batch(&sql)?;
    Ok(())
}
