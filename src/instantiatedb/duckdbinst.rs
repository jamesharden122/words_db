use super::*;
use duckdb::{Config, Connection, Result};
use serde::{Deserialize, Serialize};
use std::sync::{Arc, Mutex};
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

/// Attach an existing DuckDB database file to an already-open connection under `alias`.
///
/// This lets you work with multiple `.duckdb` files on a single connection:
/// `SELECT ... FROM a.table JOIN b.table ...` after attaching `a` and `b`.
pub async fn attach_duck_db_from_file(
    conn: &Connection,
    path: &str,
    alias: &str,
    read_only: bool,
) -> Result<(), crate::error::AppError> {
    if !std::path::Path::new(path).exists() {
        return Err(std::io::Error::new(
            std::io::ErrorKind::NotFound,
            format!("DuckDB file not found: {}", path),
        )
        .into());
    }

    let alias = alias.trim();
    if alias.is_empty() {
        return Err(std::io::Error::new(std::io::ErrorKind::InvalidInput, "alias is empty").into());
    }

    let path_sql = path.replace('\'', "''");
    let alias_sql = format!("\"{}\"", alias.replace('\"', "\"\""));
    let ro_sql = if read_only { " (READ_ONLY)" } else { "" };
    conn.execute_batch(&format!(
        "ATTACH '{}' AS {}{};",
        path_sql, alias_sql, ro_sql
    ))?;
    Ok(())
}

/// Detach a previously attached DuckDB database by `alias`.
pub fn detach_duck_db(conn: &Connection, alias: &str) -> Result<(), crate::error::AppError> {
    let alias = alias.trim();
    if alias.is_empty() {
        return Err(std::io::Error::new(std::io::ErrorKind::InvalidInput, "alias is empty").into());
    }
    let alias_sql = format!("\"{}\"", alias.replace('\"', "\"\""));
    conn.execute_batch(&format!("DETACH {};", alias_sql))?;
    Ok(())
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
pub enum DbType {
    GlobalDailyIndex,
    GlobalRets,
    UsMarket,
    UsCrspDly,
    UsCrspMthly,
    GlobalEquities,
    GlobalEquitiesMonthly,
    EquityFactorsMonthly,
    FamaFrenDly,
    FamaFrenMthly,
    GlobalFundQtrly,
    WdiWide,
    AnnComp,
    DefComp,
    LtAwdTab,
    OutstAwrd,
    Pension,
    PlanBaseAwrd,
    StGrtTab,
    BhckCrspLink,
    BhckLegacy1,
    BhckOther1,
    BhckSeries1,
    BhckSeries2,
}

impl DbType {
    pub async fn ingest(
        self,
        conn: std::sync::Arc<std::sync::Mutex<Connection>>,
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
            DbType::UsCrspDly => {
                crsp::UsCrspDly::duck_from_parquet(conn, parquet_path.to_string()).await
            }
            DbType::UsCrspMthly => {
                crsp::UsCrspMthly::duck_from_parquet(conn, parquet_path.to_string()).await
            }
            DbType::GlobalEquities => {
                global_equities::GlobalEquities::duck_from_parquet(conn, parquet_path.to_string())
                    .await
            }
            DbType::GlobalEquitiesMonthly => {
                global_equities::GlobalEquitiesMonthly::duck_from_parquet(
                    conn,
                    parquet_path.to_string(),
                )
                .await
            }
            DbType::WdiWide => {
                crate::finance_data_structs::wdi::WdiWide::duck_from_parquet(
                    conn,
                    parquet_path.to_string(),
                    None,
                )
                .await
            }
            DbType::EquityFactorsMonthly => {
                crate::finance_data_structs::equity_factors::EquityFactorsMonthly::duck_from_parquet(
                    conn,
                    parquet_path.to_string(),
                )
                .await
            }
            DbType::FamaFrenDly => {
                crate::finance_data_structs::cross_factors::FamaFrenDly::duck_from_parquet(
                    conn,
                    parquet_path.to_string(),
                )
                .await
            }
            DbType::FamaFrenMthly => {
                crate::finance_data_structs::cross_factors::FamaFrenMthly::duck_from_parquet(
                    conn,
                    parquet_path.to_string(),
                )
                .await
            }
            DbType::GlobalFundQtrly => {
                crate::finance_data_structs::global_fundamentals_compustat::GlobalFundQtrly::duck_from_parquet(
                    conn,
                    parquet_path.to_string(),
                )
                .await
            }
            DbType::AnnComp => {
                crate::finance_data_structs::execucomp::AnnComp::duck_from_parquet(
                    conn,
                    parquet_path.to_string(),
                )
                .await
            }
            DbType::DefComp => {
                crate::finance_data_structs::execucomp::DefComp::duck_from_parquet(
                    conn,
                    parquet_path.to_string(),
                )
                .await
            }
            DbType::LtAwdTab => {
                crate::finance_data_structs::execucomp::LtAwdTab::duck_from_parquet(
                    conn,
                    parquet_path.to_string(),
                )
                .await
            }
            DbType::OutstAwrd => {
                crate::finance_data_structs::execucomp::OutstAwrd::duck_from_parquet(
                    conn,
                    parquet_path.to_string(),
                )
                .await
            }
            DbType::Pension => {
                crate::finance_data_structs::execucomp::Pension::duck_from_parquet(
                    conn,
                    parquet_path.to_string(),
                )
                .await
            }
            DbType::PlanBaseAwrd => {
                crate::finance_data_structs::execucomp::PlanBaseAwrd::duck_from_parquet(
                    conn,
                    parquet_path.to_string(),
                )
                .await
            }
            DbType::StGrtTab => {
                crate::finance_data_structs::execucomp::StGrtTab::duck_from_parquet(
                    conn,
                    parquet_path.to_string(),
                )
                .await
            }
            DbType::BhckCrspLink => {
                crate::finance_data_structs::bank_regulatory::BhckCrspLink::duck_from_parquet(
                    conn,
                    parquet_path.to_string(),
                )
                .await
            }
            DbType::BhckLegacy1 => {
                crate::finance_data_structs::bank_regulatory::BhckLegacy1::duck_from_parquet(
                    conn,
                    parquet_path.to_string(),
                )
                .await
            }
            DbType::BhckOther1 => {
                crate::finance_data_structs::bank_regulatory::BhckOther1::duck_from_parquet(
                    conn,
                    parquet_path.to_string(),
                )
                .await
            }
            DbType::BhckSeries1 => {
                crate::finance_data_structs::bank_regulatory::BhckSeries1::duck_from_parquet(
                    conn,
                    parquet_path.to_string(),
                )
                .await
            }
            DbType::BhckSeries2 => {
                crate::finance_data_structs::bank_regulatory::BhckSeries2::duck_from_parquet(
                    conn,
                    parquet_path.to_string(),
                )
                .await
            }
        }
    }
}

/// Persist the current in-memory DB to a DuckDB file.
/// If `path` exists, this removes it first (DuckDB expects a new file).
pub fn persist_in_memory_to_file(
    conn: Arc<Mutex<Connection>>,
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

    let guard = conn.lock()?;
    guard.execute_batch(&sql)?;
    Ok(())
}

/// Persist only a selected set of tables from the current database to a DuckDB file.
///
/// Notes:
/// - If `path` exists, this removes it first (same semantics as `persist_in_memory_to_file`).
/// - Table names may be `"table"` or `"schema.table"`. Other formats are rejected.
/// - Tables are copied with `CREATE OR REPLACE TABLE ... AS SELECT * FROM ...`.
pub fn persist_selected_tables_to_file(
    conn: Arc<Mutex<Connection>>,
    path: &str,
    tables: Vec<String>,
) -> Result<(), crate::error::AppError> {
    if tables.is_empty() {
        return Ok(());
    }
    // DuckDB's ATTACH wants the target file to not exist for a clean export.
    if std::path::Path::new(path).exists() {
        std::fs::remove_file(path)?;
    }
    let path_sql = path.replace('\'', "''");
    let guard = conn.lock()?;
    let res: Result<(), crate::error::AppError> = (|| {
        guard.execute_batch(&format!("ATTACH '{}' AS diskdb;", path_sql))?;
        guard.execute_batch("BEGIN TRANSACTION;")?;
        for table in tables {
            guard.execute_batch(&format!(
                "CREATE OR REPLACE TABLE {dst} AS SELECT * FROM {src};",
                dst = format!("diskdb.{}", table),
                src = table
            ))?;
        }
        guard.execute_batch("COMMIT;")?;
        Ok(())
    })();

    match res {
        Ok(()) => {
            guard.execute_batch("DETACH diskdb;")?;
            Ok(())
        }
        Err(e) => {
            let _ = guard.execute_batch("ROLLBACK;");
            let _ = guard.execute_batch("DETACH diskdb;");
            Err(e)
        }
    }
}
