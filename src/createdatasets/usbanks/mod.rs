use crate::error::AppError;
use crate::finance_data_structs::{
    bank_regulatory::{BhckCrspLink, BhckLegacy1, BhckOther1, BhckSeries1, BhckSeries2,FullBankData},
    cross_factors::{FamaFrenDly, FamaFrenMthly},
    crsp::{UsCrspDly, UsCrspMthly},
    DuckCrudModel,
    get_polars_df_from_sql,
};
use crate::instantiatedb::duckdbinst::{
    attach_duck_db_from_file, detach_duck_db, persist_selected_tables_to_file, start_duck_db, open_duck_db_from_file,
};
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};

#[cfg(feature = "server")]
use crate::instantiatedb::polars_utils::{load_cache, save_cache};

#[cfg(feature = "server")]
use polars::prelude::DataFrame;

pub struct BankFundPaths {
    pub bhck_other1: Option<Vec<String>>,
    pub bhck_series1: Option<Vec<String>>,
    pub bhck_series2: Option<Vec<String>>,
    pub bhck_legacy: Option<Vec<String>>,
}

pub struct BankCrspPaths {
    pub bhck_crsp: Option<Vec<String>>,
    pub crsp_mthly: Option<Vec<String>>,
    pub ff_mthly: Option<Vec<String>>,
    pub crsp_dly: Option<Vec<String>>,
    pub ff_dly: Option<Vec<String>>,
}

pub struct BankCrspDly {
    pub bhck_crsp: String,
    pub crsp_dly: String,
    pub ff_dly: String,
}

pub struct BankCrspMthly {
    pub bhck_crsp: String,
    pub crsp_mthly: String,
    pub ff_mthly: String,
}

pub struct BankFund {
    pub bhck_legacy: String,
    pub bhck_other1: String,
    pub bhck_series1: String,
    pub bhck_series2: String,
}

#[cfg(feature = "server")]
pub async fn full_bank_ds(
    parquet_path: Option<&str>,
    db_dir: Option<&str>,
    cache_file: impl AsRef<std::path::Path>,
    max_mem: Option<&str>,
    thread_count: Option<i64>,
) -> Result<DataFrame, AppError> {
    let cache_file = cache_file.as_ref();
    if cache_file.exists() {
        return Ok(load_cache(cache_file)?);
    }

    let db_dir = db_dir.ok_or_else(|| {
        std::io::Error::new(std::io::ErrorKind::InvalidInput, "db_dir is required when cache miss")
    })?;
    let max_mem = max_mem.unwrap_or("30GB");
    let thread_count = thread_count.unwrap_or(8);

    let table = <FullBankData as DuckCrudModel>::table();
    let db_dir_path = std::path::Path::new(db_dir);
    let db_path = db_dir_path.join(format!("{table}.duckdb"));

    if !db_path.exists() {
        let parquet_path = parquet_path.ok_or_else(|| {
            std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                "duckdb file missing and parquet_path not provided",
            )
        })?;
        std::fs::create_dir_all(db_dir_path)?;
        crate::createdatasets::parquet_to_duckdb::<FullBankData>(
            parquet_path,
            max_mem,
            thread_count,
            db_dir_path,
        )
        .await?;
    }

    let db_path_str = db_path.to_str().ok_or_else(|| {
        std::io::Error::new(
            std::io::ErrorKind::InvalidInput,
            format!("db_path is not valid utf-8: {}", db_path.display()),
        )
    })?;
    let conn = open_duck_db_from_file(db_path_str, max_mem, thread_count).await?;

    let sql = format!("SELECT * FROM {table}");
    let mut chunks = get_polars_df_from_sql(&conn, &sql).await?;
    if chunks.is_empty() {
        return Err(std::io::Error::new(std::io::ErrorKind::Other, "empty query result from duckdb").into());
    }

    let mut df = chunks.remove(0);
    for c in chunks {
        df.vstack_mut(&c)?;
    }

    if let Some(parent) = cache_file.parent() {
        std::fs::create_dir_all(parent)?;
    }
    save_cache(&df, cache_file)?;

    Ok(df)
}

pub async fn create_fundamental_duckdb_files(
    bhck_other1: Option<Vec<String>>,
    bhck_series1: Option<Vec<String>>,
    bhck_series2: Option<Vec<String>>,
    bhck_legacy: Option<Vec<String>>,
    out_dir: &Path,
) -> Result<(), AppError> {
    if let Some(p) = bhck_other1 {
        match p.len() {
            0 => {}
            1 => super::parquet_to_duckdb::<BhckOther1>(p[0].as_str(), "30GB", 8, out_dir).await?,
            _ => super::batch_parquet_to_duckdb::<BhckOther1>(p, "20GB", 8, out_dir).await?,
        }
    }
    if let Some(p) = bhck_series1 {
        match p.len() {
            0 => {}
            1 => super::parquet_to_duckdb::<BhckSeries1>(p[0].as_str(), "30GB", 8, out_dir).await?,
            _ => super::batch_parquet_to_duckdb::<BhckSeries1>(p, "20GB", 8, out_dir).await?,
        }
    }
    if let Some(p) = bhck_series2 {
        match p.len() {
            0 => {}
            1 => super::parquet_to_duckdb::<BhckSeries2>(p[0].as_str(), "30GB", 8, out_dir).await?,
            _ => super::batch_parquet_to_duckdb::<BhckSeries2>(p, "20GB", 8, out_dir).await?,
        }
    }
    if let Some(p) = bhck_legacy {
        match p.len() {
            0 => {}
            1 => super::parquet_to_duckdb::<BhckLegacy1>(p[0].as_str(), "40GB", 8, out_dir).await?,
            _ => super::batch_parquet_to_duckdb::<BhckLegacy1>(p, "40GB", 8, out_dir).await?,
        }
    }
    Ok(())
}

pub async fn create_securities_duckdb_files(
    bhck_crsp: Option<Vec<String>>,
    crsp_mthly: Option<Vec<String>>,
    ff_mthly: Option<Vec<String>>,
    crsp_dly: Option<Vec<String>>,
    ff_dly: Option<Vec<String>>,
    out_dir: &Path,
) -> Result<(), AppError> {
    if let Some(p) = bhck_crsp {
        match p.len() {
            0 => {}
            1 => {
                super::parquet_to_duckdb::<BhckCrspLink>(p[0].as_str(), "20GB", 8, out_dir).await?
            }
            _ => super::batch_parquet_to_duckdb::<BhckCrspLink>(p, "20GB", 8, out_dir).await?,
        }
    }
    if let Some(p) = crsp_mthly {
        match p.len() {
            0 => {}
            1 => super::parquet_to_duckdb::<UsCrspMthly>(p[0].as_str(), "20GB", 8, out_dir).await?,
            _ => super::batch_parquet_to_duckdb::<UsCrspMthly>(p, "20GB", 8, out_dir).await?,
        }
    }
    if let Some(p) = ff_mthly {
        match p.len() {
            0 => {}
            1 => {
                super::parquet_to_duckdb::<FamaFrenMthly>(p[0].as_str(), "20GB", 8, out_dir).await?
            }
            _ => super::batch_parquet_to_duckdb::<FamaFrenMthly>(p, "20GB", 8, out_dir).await?,
        }
    }
    if let Some(p) = crsp_dly {
        match p.len() {
            0 => {}
            1 => super::parquet_to_duckdb::<UsCrspDly>(p[0].as_str(), "20GB", 8, out_dir).await?,
            _ => super::batch_parquet_to_duckdb::<UsCrspDly>(p, "20GB", 8, out_dir).await?,
        }
    }
    if let Some(p) = ff_dly {
        match p.len() {
            0 => {}
            1 => super::parquet_to_duckdb::<FamaFrenDly>(p[0].as_str(), "20GB", 8, out_dir).await?,
            _ => super::batch_parquet_to_duckdb::<FamaFrenDly>(p, "20GB", 8, out_dir).await?,
        }
    }
    Ok(())
}

pub async fn fundamental_ds_from_db_files(
    max_mem: &str,
    thread_count: i64,
    _bhck_legacy: &str,
    bhck_other1: &str,
    bhck_series1: &str,
    bhck_series2: &str,
    out_path: PathBuf,
) -> std::result::Result<PathBuf, AppError> {
    let conn = start_duck_db(max_mem, thread_count).await?;
    std::fs::create_dir_all(&out_path)?;

    // Allow large joins to spill to disk instead of hard-failing under the memory limit.
    // Also disable insertion-order preservation, which can increase memory usage.
    let duckdb_tmp_dir = out_path.join("duckdb_tmp");
    std::fs::create_dir_all(&duckdb_tmp_dir)?;
    let duckdb_tmp_dir_str = duckdb_tmp_dir.to_str().ok_or_else(|| {
        std::io::Error::new(
            std::io::ErrorKind::InvalidInput,
            format!("temp path is not valid UTF-8: {}", duckdb_tmp_dir.display()),
        )
    })?;
    let duckdb_tmp_dir_sql = duckdb_tmp_dir_str.replace('\'', "''");
    conn.execute_batch(&format!(
        "PRAGMA temp_directory='{}';\nSET preserve_insertion_order=false;\n",
        duckdb_tmp_dir_sql
    ))?;

    // Ensure all ATTACH statements complete before building the join table.
    //attach_duck_db_from_file(&conn, bhck_legacy, BhckLegacy1::table(), true).await?;
    attach_duck_db_from_file(&conn, bhck_other1, BhckOther1::table(), true).await?;
    attach_duck_db_from_file(&conn, bhck_series1, BhckSeries1::table(), true).await?;
    attach_duck_db_from_file(&conn, bhck_series2, BhckSeries2::table(), true).await?;
    println!("Attach the bhck data.");
    // Materialize the join in stages so DuckDB can release join working memory between steps.
    conn.execute_batch(
        r#"
    CREATE TEMP TABLE bank_fundamentals_stage AS
    SELECT *
    FROM bhck_series1.bhck_series1
    FULL OUTER JOIN bhck_series2.bhck_series2 USING (rssd9999, rssd9001);
    "#,
    )?;
    println!("Executed the bhck series merge.");
    detach_duck_db(&conn, BhckSeries1::table())?;
    detach_duck_db(&conn, BhckSeries2::table())?;
    println!("Detach the bhck series.");
    conn.execute_batch(
        r#"
    CREATE OR REPLACE TABLE bank_fundamentals AS
    SELECT *
    FROM bank_fundamentals_stage
    FULL OUTER JOIN bhck_other.bhck_other USING (rssd9999, rssd9001);
    DROP TABLE bank_fundamentals_stage;
    "#,
    )?;
    println!("Executed the bhck other merge.");
    detach_duck_db(&conn, BhckOther1::table())?;
    println!("Detach the bhck other.");
    let out_file = out_path.join("bank_fundamentals.duckdb");
    let out_file_str = out_file.to_str().ok_or_else(|| {
        std::io::Error::new(
            std::io::ErrorKind::InvalidInput,
            format!("output path is not valid UTF-8: {}", out_file.display()),
        )
    })?;
    let conn = Arc::new(Mutex::new(conn));
    persist_selected_tables_to_file(conn, out_file_str, vec!["bank_fundamentals".to_string()])?;
    println!("The Database Finished Merging");
    Ok(out_file)
}

pub async fn mthly_securities_ds_from_db_files(
    max_mem: &str,
    thread_count: i64,
    bhck_crsp: &str,
    crsp_mthly: &str,
    ff_mthly: &str,
    out_path: PathBuf,
) -> Result<PathBuf, AppError> {
    let conn = start_duck_db(max_mem, thread_count).await?;
    attach_duck_db_from_file(&conn, bhck_crsp, BhckCrspLink::table(), true).await?;
    attach_duck_db_from_file(&conn, crsp_mthly, UsCrspMthly::table(), true).await?;
    attach_duck_db_from_file(&conn, ff_mthly, FamaFrenMthly::table(), true).await?;
    conn.execute_batch(
        r#"
    BEGIN;
    CREATE OR REPLACE TABLE bank_securities_mthly AS
    SELECT *
    FROM bhck_crsp_link.bhck_crsp_link
    LEFT JOIN us_crsp_mthly.us_crsp_mthly USING (permco);
    SELECT *
    FROM bank_securities_mthly
    LEFT JOIN fama_french_monthly.fama_french_monthly  ON 
    CAST(bank_securities_mthly.bank_securities_mthly.date AS DATE) = CAST(fama_french_monthly.fama_french_monthly.dateff AS DATE);
    COMMIT;
    "#,
    )?;
    std::fs::create_dir_all(&out_path)?;
    let out_file = out_path.join("bank_monthly_sec.duckdb");
    let out_file_str = out_file.to_str().ok_or_else(|| {
        std::io::Error::new(
            std::io::ErrorKind::InvalidInput,
            format!("output path is not valid UTF-8: {}", out_file.display()),
        )
    })?;
    let conn = Arc::new(Mutex::new(conn));
    persist_selected_tables_to_file(
        conn,
        out_file_str,
        vec!["bank_securities_mthly".to_string()],
    )?;
    Ok(out_file)
}

pub async fn dly_securities_ds_from_db_files(
    max_mem: &str,
    thread_count: i64,
    bhck_crsp: &str,
    crsp_dly: &str,
    ff_dly: &str,
    out_path: PathBuf,
) -> Result<PathBuf, AppError> {
    let conn = start_duck_db(max_mem, thread_count).await?;
    attach_duck_db_from_file(&conn, bhck_crsp, BhckCrspLink::table(), true).await?;
    attach_duck_db_from_file(&conn, crsp_dly, UsCrspDly::table(), true).await?;
    attach_duck_db_from_file(&conn, ff_dly, FamaFrenDly::table(), true).await?;

    conn.execute_batch(
        r#"
    BEGIN;
    CREATE OR REPLACE TABLE bank_securities_dly AS
    SELECT *
    FROM bhck_crsp_link.bhck_crsp_link
    LEFT JOIN us_crsp_dly.us_crsp_dly USING (permco)
    LEFT JOIN fama_french_daily.fama_french_daily USING (date);
    COMMIT;
    "#,
    )?;
    std::fs::create_dir_all(&out_path)?;
    let out_file = out_path.join("bank_securities_dly.duckdb");
    let out_file_str = out_file.to_str().ok_or_else(|| {
        std::io::Error::new(
            std::io::ErrorKind::InvalidInput,
            format!("output path is not valid UTF-8: {}", out_file.display()),
        )
    })?;
    let conn = Arc::new(Mutex::new(conn));
    persist_selected_tables_to_file(conn, out_file_str, vec!["bank_securities_dly".to_string()])?;
    Ok(out_file)
}
