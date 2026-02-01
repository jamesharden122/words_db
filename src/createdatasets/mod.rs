use crate::error::AppError;
use crate::finance_data_structs::DuckCrudModel;
use crate::instantiatedb::duckdbinst::{persist_in_memory_to_file, start_duck_db};
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};
pub mod compustat;
pub mod usbanks;

use usbanks::{BankCrspDly, BankCrspMthly, BankCrspPaths, BankFund, BankFundPaths};

pub enum CreateDuckFls {
    UsBankCrsp(BankCrspPaths),
    UsBankFund(BankFundPaths),
}

pub enum MergeDuckFls {
    UsBankCrspDly(BankCrspDly),
    UsBankCrspMthly(BankCrspMthly),
    UsBankFund(BankFund),
}

impl CreateDuckFls {
    pub async fn create_db_files(self, out_dir: &Path) -> Result<(), AppError> {
        std::fs::create_dir_all(out_dir)?;
        match self {
            Self::UsBankCrsp(pths) => {
                usbanks::create_securities_duckdb_files(
                    pths.bhck_crsp,
                    pths.crsp_mthly,
                    pths.ff_mthly,
                    pths.crsp_dly,
                    pths.ff_dly,
                    out_dir,
                )
                .await
            }
            Self::UsBankFund(pths) => {
                usbanks::create_fundamental_duckdb_files(
                    pths.bhck_other1,
                    pths.bhck_series1,
                    pths.bhck_series2,
                    pths.bhck_legacy,
                    out_dir,
                )
                .await
            }
        }
    }
}
impl MergeDuckFls {
    pub async fn merge_db_files(
        self,
        out_dir: impl AsRef<Path>,
        max_mem: &str,
        thread_count: i64,
    ) -> Result<PathBuf, AppError> {
        let out_dir = out_dir.as_ref().to_path_buf();
        match self {
            Self::UsBankCrspDly(pths) => {
                usbanks::dly_securities_ds_from_db_files(
                    max_mem,
                    thread_count,
                    pths.bhck_crsp.as_str(),
                    pths.crsp_dly.as_str(),
                    pths.ff_dly.as_str(),
                    out_dir,
                )
                .await
            }
            Self::UsBankCrspMthly(pths) => {
                usbanks::mthly_securities_ds_from_db_files(
                    max_mem,
                    thread_count,
                    pths.bhck_crsp.as_str(),
                    pths.crsp_mthly.as_str(),
                    pths.ff_mthly.as_str(),
                    out_dir,
                )
                .await
            }
            Self::UsBankFund(pths) => {
                usbanks::fundamental_ds_from_db_files(
                    max_mem,
                    thread_count,
                    pths.bhck_legacy.as_str(),
                    pths.bhck_other1.as_str(),
                    pths.bhck_series1.as_str(),
                    pths.bhck_series2.as_str(),
                    out_dir,
                )
                .await
            }
        }
    }
}
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
    if parquet_pth_vec.is_empty() {
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
