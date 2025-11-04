pub mod error;
pub mod finance_data_structs;
pub mod schema;

use duckdb::{Config, Connection};
use finance_data_structs::*;
use std::sync::Arc;
use surrealdb::engine::local::{Db, Mem};
use surrealdb::Surreal;

/// Start and return a new in-memory SurrealDB connection and apply base schema.
pub async fn start_mem_db() -> surrealdb::Result<Surreal<Db>> {
    let config = surrealdb::opt::Config::default().strict();
    let db = Surreal::new::<Mem>(config).await?;
    schema::apply_base_schema(&db).await?;
    Ok(db)
}

/// Start and return a new in-memory SurrealDB connection and apply base schema.
pub async fn start_mem_db_no_table() -> surrealdb::Result<Surreal<Db>> {
    let config = surrealdb::opt::Config::default().strict();
    let db = Surreal::new::<Mem>(config).await?;
    schema::apply_base_schema_no_table(&db).await?;
    Ok(db)
}

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

pub async fn start_mem_db_global_indexes(
    global_index_pth: &str,
    batch_size: usize,
    cores: usize,
) -> surrealdb::Result<Surreal<Db>> {
    let db = start_mem_db().await.expect("db should start");
    db.use_ns("indexes")
        .use_db("daily")
        .await
        .expect("indexes/daily exists");
    let df_vec = crsp::GlobalDailyIndex::from_parquet(global_index_pth).unwrap();
    println!("{:?}", "checkpoint 4");
    let time = std::time::Instant::now();
    println!(
        "The data was uploaded: {:?}",
        crsp::GlobalDailyIndex::create_gdi_result(
            df_vec, &db, "indexes", "daily", batch_size, cores
        )
        .await
        .unwrap()
    );
    let duration = time.elapsed();
    println!("duration: {:?}", duration);
    db.export("../global_daily_backup.sql").await.unwrap();
    Ok(db)
}

async fn import_mem_db_global_indexes(backup_file: &str) -> surrealdb::Result<Surreal<Db>> {
    let db = start_mem_db_no_table().await.expect("db should start");
    let time = std::time::Instant::now();
    db.import(backup_file).await.unwrap();
    let duration = time.elapsed();
    println!("duration: {:?}", duration);
    Ok(db)
}

// Convenience: start DuckDB and ingest global indexes Parquet into JSON-doc table.
pub async fn start_duck_db_global_indexes(
    parquet_path: &str,
    size: &str,
    thread_count: i64,
) -> Result<Arc<Connection>, crate::error::AppError> {
    let conn = start_duck_db(size, thread_count).await?;
    let conn = Arc::new(conn);
    // Pass owned String to satisfy any 'static bounds in the callee.
    crsp::GlobalDailyIndex::duck_from_parquet(conn.clone(), parquet_path.to_string()).await?;
    Ok(conn)
}

// World indices (GlobalRets) helpers — names differ to avoid collision with price-index helpers.
pub async fn start_duck_db_global_rets(
    parquet_path: &str,
    size: &str,
    thread_count: i64,
) -> Result<Arc<Connection>, crate::error::AppError> {
    let conn = start_duck_db(size, thread_count).await?;
    let conn = Arc::new(conn);
    world_indices::GlobalRets::duck_from_parquet(conn.clone(), parquet_path.to_string()).await?;
    Ok(conn)
}

pub async fn start_mem_db_world_indices() -> surrealdb::Result<Surreal<Db>> {
    // Starts a fresh in-memory SurrealDB and applies base schema; does not ingest.
    start_mem_db().await
}

pub async fn import_mem_db_world_indices(backup_file: &str) -> surrealdb::Result<Surreal<Db>> {
    let db = start_mem_db_no_table().await.expect("db should start");
    let time = std::time::Instant::now();
    db.import(backup_file).await.unwrap();
    let duration = time.elapsed();
    println!("duration: {:?}", duration);
    Ok(db)
}
