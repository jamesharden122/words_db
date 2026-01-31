use crate::error::AppError;
use crate::finance_data_structs::DuckCrudModel;
use crate::instantiatedb::duckdbinst::{persist_in_memory_to_file, start_duck_db};
use std::path::Path;
use std::sync::{Arc, Mutex};
pub mod compustat;
pub mod usbanks;
//take in a list of finance_data_structs so impl DuckCrud
pub async fn parquet_to_duckdb<D: DuckCrudModel>(
    parquet_pth: impl AsRef<Path>,
    max_mem: &str,
    thread_count: i64,
    out_dir: &Path,
) -> Result<(), AppError> {
    let parquet_pth = parquet_pth.as_ref();
    let duckdb_pth = out_dir.join(format!("{}.duckdb", D::table()));
    //instantiate the duckdb database for connection
    let conn = start_duck_db(max_mem, thread_count).await?;
    let conn = Arc::new(Mutex::new(conn));

    //read in the parquet file
    <D as DuckCrudModel>::upsert_from_parquet_one_file(
        conn.clone(),
        parquet_pth,
        None,
        Some(D::table().to_string()),
    )
    .await?;

    //dump the parquet file data to a duckdb
    let duckdb_pth_str = duckdb_pth.to_str().ok_or_else(|| {
        std::io::Error::new(
            std::io::ErrorKind::InvalidInput,
            format!("output path is not valid UTF-8: {}", duckdb_pth.display()),
        )
    })?;
    persist_in_memory_to_file(conn, duckdb_pth_str)?;
    Ok(())
}
//

pub async fn batch_parquet_to_duckdb<D: DuckCrudModel>(
    parquet_pth_vec: Vec<String>,
    max_mem: &str,
    thread_count: i64,
    out_dir: &Path,
) -> Result<(), AppError> {
    let Some(first) = parquet_pth_vec.first() else {
        return Ok(());
    };

    let duckdb_pth = out_dir.join(format!("{}.duckdb", D::table()));
    let conn = start_duck_db(max_mem, thread_count).await?;
    let conn = Arc::new(Mutex::new(conn));

    for parquet_pth in parquet_pth_vec.iter() {
        <D as DuckCrudModel>::append_from_parquet_one_file(
            conn.clone(),
            parquet_pth,
            Some(D::table().to_string()),
        )
        .await?;
    }
    let duckdb_pth_str = duckdb_pth.to_str().ok_or_else(|| {
        std::io::Error::new(
            std::io::ErrorKind::InvalidInput,
            format!("output path is not valid UTF-8: {}", duckdb_pth.display()),
        )
    })?;
    persist_in_memory_to_file(conn, duckdb_pth_str)?;
    Ok(())
}
//
