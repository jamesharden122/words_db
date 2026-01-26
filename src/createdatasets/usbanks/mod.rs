use crate::error::AppError;
use crate::instantiatedb::duckdbinst::{
    attach_duck_db_from_file, persist_selected_tables_to_file, start_duck_db,
};
use std::path::PathBuf;

pub async fn fundamental_ds_from_db_files(
    max_mem: &str,
    thread_count: i64,
    bhck_other1: &str,
    bhck_series1: &str,
    bhck_series2: &str,
    out_path: PathBuf,
) -> std::result::Result<PathBuf, AppError> {
    let conn = start_duck_db(max_mem, thread_count).await?;

    // Ensure all ATTACH statements complete before building the join table.
    attach_duck_db_from_file(&conn, bhck_other1, "bhck_other", true).await?;
    attach_duck_db_from_file(&conn, bhck_series1, "bhck_series1", true).await?;
    attach_duck_db_from_file(&conn, bhck_series2, "bhck_series2", true).await?;
    conn.execute_batch(
        r#"
    BEGIN;
    CREATE OR REPLACE TABLE bank_fundamentals AS
    SELECT *
    FROM bhck_series1.bhck_series1
    FULL OUTER JOIN bhck_series2.bhck_series2 USING (rssd9999, rssd9001)
    FULL OUTER JOIN bhck_other.bhck_other USING (rssd9999, rssd9001);
    COMMIT;
    "#,
    )?;
    std::fs::create_dir_all(&out_path)?;
    let out_file = out_path.join("bank_fundamentals.duckdb");
    let out_file_str = out_file.to_str().ok_or_else(|| {
        std::io::Error::new(
            std::io::ErrorKind::InvalidInput,
            format!("output path is not valid UTF-8: {}", out_file.display()),
        )
    })?;
    persist_selected_tables_to_file(&conn, out_file_str, vec!["bank_fundamentals".to_string()])?;

    Ok(out_file)
}

pub async fn mthly_securities_ds_from_db_files(
    max_mem: &str,
    thread_count: i64,
    bhck_crsp: &str,
    crsp_mthly: &str,
    out_path: PathBuf,
) -> Result<PathBuf, AppError> {
    let conn = start_duck_db(max_mem, thread_count).await?;
    attach_duck_db_from_file(&conn, bhck_crsp, "bhck_crsp_link", true).await?;
    attach_duck_db_from_file(&conn, crsp_mthly, "us_crsp_mthly", true).await?;
    conn.execute_batch(
        r#"
    BEGIN;
    CREATE OR REPLACE TABLE bank_securities_mthly AS
    SELECT *
    FROM bhck_crsp_link.bhck_crsp_link
    FULL OUTER JOIN us_crsp_mthly.us_crsp_mthly USING (permco);
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
    persist_selected_tables_to_file(
        &conn,
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
    out_path: PathBuf,
) -> Result<PathBuf, AppError> {
    let conn = start_duck_db(max_mem, thread_count).await?;
    attach_duck_db_from_file(&conn, bhck_crsp, "bhck_crsp_link", true).await?;
    attach_duck_db_from_file(&conn, crsp_dly, "us_crsp_dly", true).await?;
    conn.execute_batch(
        r#"
    BEGIN;
    CREATE OR REPLACE TABLE bank_securities_dly AS
    SELECT *
    FROM bhck_crsp_link.bhck_crsp_link
    FULL OUTER JOIN us_crsp_dly.us_crsp_dly USING (permco);
    COMMIT;
    "#,
    )?;
    std::fs::create_dir_all(&out_path)?;
    let out_file = out_path.join("bank_daily_sec.duckdb");
    let out_file_str = out_file.to_str().ok_or_else(|| {
        std::io::Error::new(
            std::io::ErrorKind::InvalidInput,
            format!("output path is not valid UTF-8: {}", out_file.display()),
        )
    })?;
    persist_selected_tables_to_file(&conn, out_file_str, vec!["bank_securities_dly".to_string()])?;
    Ok(out_file)
}
