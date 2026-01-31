use crate::error::AppError;
use crate::finance_data_structs::{DuckCrudModel, ToPolars};
use chrono::NaiveDate;
use duckdb::Connection;
use polars::frame::row::Row;
use polars::prelude::*;
use serde::{Deserialize, Serialize};
use std::{
    path::{Path, PathBuf},
    sync::{Arc, Mutex},
};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AnnComp {
    pub address: Option<String>,
    pub age: Option<f64>,
    pub allothpd: Option<f64>,
    pub allothtot: Option<f64>,
    pub becameceo: Option<NaiveDate>,
    pub bonus: Option<f64>,
    pub ceoann: Option<String>,
    pub cfoann: Option<String>,
    pub chg_ctrl_pymt: Option<f64>,
    pub city: Option<String>,
    pub co_per_rol: Option<f64>,
    pub comment: Option<String>,
    pub coname: Option<String>,
    pub cusip: Option<String>,
    pub defer_balance_tot: Option<f64>,
    pub defer_contrib_co_tot: Option<f64>,
    pub defer_contrib_exec_tot: Option<f64>,
    pub defer_earnings_tot: Option<f64>,
    pub defer_rpt_as_comp_tot: Option<f64>,
    pub defer_withdr_tot: Option<f64>,
    pub eip_unearn_num: Option<f64>,
    pub eip_unearn_val: Option<f64>,
    pub exchange: Option<String>,
    pub exec_fname: Option<String>,
    pub exec_fullname: Option<String>,
    pub exec_lname: Option<String>,
    pub exec_mname: Option<String>,
    pub execdir: Option<f64>,
    pub execid: Option<String>,
    pub execrank: Option<f64>,
    pub execrankann: Option<f64>,
    pub gender: Option<String>,
    pub gvkey: Option<String>,
    pub inddesc: Option<String>,
    pub interlock: Option<f64>,
    pub joined_co: Option<NaiveDate>,
    pub leftco: Option<NaiveDate>,
    pub leftofc: Option<NaiveDate>,
    pub ltip: Option<f64>,
    pub naics: Option<f64>,
    pub naicsdesc: Option<String>,
    pub nameprefix: Option<String>,
    pub noneq_incent: Option<f64>,
    pub old_datafmt_flag: Option<f64>,
    pub opt_exer_num: Option<f64>,
    pub opt_exer_val: Option<f64>,
    pub opt_unex_exer_est_val: Option<f64>,
    pub opt_unex_exer_num: Option<f64>,
    pub opt_unex_unexer_est_val: Option<f64>,
    pub opt_unex_unexer_num: Option<f64>,
    pub option_awards: Option<f64>,
    pub option_awards_blk_value: Option<f64>,
    pub option_awards_fv: Option<f64>,
    pub option_awards_num: Option<f64>,
    pub option_awards_rpt_value: Option<f64>,
    pub othann: Option<f64>,
    pub othcomp: Option<f64>,
    pub page: Option<f64>,
    pub pceo: Option<String>,
    pub pcfo: Option<String>,
    pub pension_chg: Option<f64>,
    pub pension_pymts_tot: Option<f64>,
    pub pension_value_tot: Option<f64>,
    pub reason: Option<String>,
    pub rejoin: Option<NaiveDate>,
    pub releft: Option<NaiveDate>,
    pub reprice: Option<f64>,
    pub ret_yrs: Option<f64>,
    pub rstkgrnt: Option<f64>,
    pub rstkvyrs: Option<f64>,
    pub sal_pct: Option<f64>,
    pub salary: Option<f64>,
    pub shrown_excl_opts: Option<f64>,
    pub shrown_excl_opts_pct: Option<f64>,
    pub shrown_tot: Option<f64>,
    pub shrown_tot_pct: Option<f64>,
    pub shrs_vest_num: Option<f64>,
    pub shrs_vest_val: Option<f64>,
    pub sic: Option<f64>,
    pub sicdesc: Option<String>,
    pub spcode: Option<String>,
    pub spindex: Option<f64>,
    pub state: Option<String>,
    pub stock_awards: Option<f64>,
    pub stock_awards_fv: Option<f64>,
    pub stock_unvest_num: Option<f64>,
    pub stock_unvest_val: Option<f64>,
    pub sub_tele: Option<f64>,
    pub tdc1: Option<f64>,
    pub tdc1_pct: Option<f64>,
    pub tdc2: Option<f64>,
    pub tdc2_pct: Option<f64>,
    pub tele: Option<String>,
    pub term_pymt: Option<f64>,
    pub ticker: Option<String>,
    pub title: Option<String>,
    pub titleann: Option<String>,
    pub total_alt1: Option<f64>,
    pub total_alt1_pct: Option<f64>,
    pub total_alt2: Option<f64>,
    pub total_alt2_pct: Option<f64>,
    pub total_curr: Option<f64>,
    pub total_curr_pct: Option<f64>,
    pub total_sec: Option<f64>,
    pub total_sec_pct: Option<f64>,
    pub year: Option<f64>,
    pub zip: Option<String>,
}

impl ToPolars for AnnComp {
    fn schema() -> Schema {
        AnnComp::polars_schema()
    }
}

impl DuckCrudModel for AnnComp {
    fn table() -> &'static str {
        "anncomp"
    }
}

impl AnnComp {
    pub fn polars_schema() -> Schema {
        Schema::from_iter(vec![
            Field::new("address".into(), DataType::String),
            Field::new("age".into(), DataType::Float64),
            Field::new("allothpd".into(), DataType::Float64),
            Field::new("allothtot".into(), DataType::Float64),
            Field::new("becameceo".into(), DataType::Date),
            Field::new("bonus".into(), DataType::Float64),
            Field::new("ceoann".into(), DataType::String),
            Field::new("cfoann".into(), DataType::String),
            Field::new("chg_ctrl_pymt".into(), DataType::Float64),
            Field::new("city".into(), DataType::String),
            Field::new("co_per_rol".into(), DataType::Float64),
            Field::new("comment".into(), DataType::String),
            Field::new("coname".into(), DataType::String),
            Field::new("cusip".into(), DataType::String),
            Field::new("defer_balance_tot".into(), DataType::Float64),
            Field::new("defer_contrib_co_tot".into(), DataType::Float64),
            Field::new("defer_contrib_exec_tot".into(), DataType::Float64),
            Field::new("defer_earnings_tot".into(), DataType::Float64),
            Field::new("defer_rpt_as_comp_tot".into(), DataType::Float64),
            Field::new("defer_withdr_tot".into(), DataType::Float64),
            Field::new("eip_unearn_num".into(), DataType::Float64),
            Field::new("eip_unearn_val".into(), DataType::Float64),
            Field::new("exchange".into(), DataType::String),
            Field::new("exec_fname".into(), DataType::String),
            Field::new("exec_fullname".into(), DataType::String),
            Field::new("exec_lname".into(), DataType::String),
            Field::new("exec_mname".into(), DataType::String),
            Field::new("execdir".into(), DataType::Float64),
            Field::new("execid".into(), DataType::String),
            Field::new("execrank".into(), DataType::Float64),
            Field::new("execrankann".into(), DataType::Float64),
            Field::new("gender".into(), DataType::String),
            Field::new("gvkey".into(), DataType::String),
            Field::new("inddesc".into(), DataType::String),
            Field::new("interlock".into(), DataType::Float64),
            Field::new("joined_co".into(), DataType::Date),
            Field::new("leftco".into(), DataType::Date),
            Field::new("leftofc".into(), DataType::Date),
            Field::new("ltip".into(), DataType::Float64),
            Field::new("naics".into(), DataType::Float64),
            Field::new("naicsdesc".into(), DataType::String),
            Field::new("nameprefix".into(), DataType::String),
            Field::new("noneq_incent".into(), DataType::Float64),
            Field::new("old_datafmt_flag".into(), DataType::Float64),
            Field::new("opt_exer_num".into(), DataType::Float64),
            Field::new("opt_exer_val".into(), DataType::Float64),
            Field::new("opt_unex_exer_est_val".into(), DataType::Float64),
            Field::new("opt_unex_exer_num".into(), DataType::Float64),
            Field::new("opt_unex_unexer_est_val".into(), DataType::Float64),
            Field::new("opt_unex_unexer_num".into(), DataType::Float64),
            Field::new("option_awards".into(), DataType::Float64),
            Field::new("option_awards_blk_value".into(), DataType::Float64),
            Field::new("option_awards_fv".into(), DataType::Float64),
            Field::new("option_awards_num".into(), DataType::Float64),
            Field::new("option_awards_rpt_value".into(), DataType::Float64),
            Field::new("othann".into(), DataType::Float64),
            Field::new("othcomp".into(), DataType::Float64),
            Field::new("page".into(), DataType::Float64),
            Field::new("pceo".into(), DataType::String),
            Field::new("pcfo".into(), DataType::String),
            Field::new("pension_chg".into(), DataType::Float64),
            Field::new("pension_pymts_tot".into(), DataType::Float64),
            Field::new("pension_value_tot".into(), DataType::Float64),
            Field::new("reason".into(), DataType::String),
            Field::new("rejoin".into(), DataType::Date),
            Field::new("releft".into(), DataType::Date),
            Field::new("reprice".into(), DataType::Float64),
            Field::new("ret_yrs".into(), DataType::Float64),
            Field::new("rstkgrnt".into(), DataType::Float64),
            Field::new("rstkvyrs".into(), DataType::Float64),
            Field::new("sal_pct".into(), DataType::Float64),
            Field::new("salary".into(), DataType::Float64),
            Field::new("shrown_excl_opts".into(), DataType::Float64),
            Field::new("shrown_excl_opts_pct".into(), DataType::Float64),
            Field::new("shrown_tot".into(), DataType::Float64),
            Field::new("shrown_tot_pct".into(), DataType::Float64),
            Field::new("shrs_vest_num".into(), DataType::Float64),
            Field::new("shrs_vest_val".into(), DataType::Float64),
            Field::new("sic".into(), DataType::Float64),
            Field::new("sicdesc".into(), DataType::String),
            Field::new("spcode".into(), DataType::String),
            Field::new("spindex".into(), DataType::Float64),
            Field::new("state".into(), DataType::String),
            Field::new("stock_awards".into(), DataType::Float64),
            Field::new("stock_awards_fv".into(), DataType::Float64),
            Field::new("stock_unvest_num".into(), DataType::Float64),
            Field::new("stock_unvest_val".into(), DataType::Float64),
            Field::new("sub_tele".into(), DataType::Float64),
            Field::new("tdc1".into(), DataType::Float64),
            Field::new("tdc1_pct".into(), DataType::Float64),
            Field::new("tdc2".into(), DataType::Float64),
            Field::new("tdc2_pct".into(), DataType::Float64),
            Field::new("tele".into(), DataType::String),
            Field::new("term_pymt".into(), DataType::Float64),
            Field::new("ticker".into(), DataType::String),
            Field::new("title".into(), DataType::String),
            Field::new("titleann".into(), DataType::String),
            Field::new("total_alt1".into(), DataType::Float64),
            Field::new("total_alt1_pct".into(), DataType::Float64),
            Field::new("total_alt2".into(), DataType::Float64),
            Field::new("total_alt2_pct".into(), DataType::Float64),
            Field::new("total_curr".into(), DataType::Float64),
            Field::new("total_curr_pct".into(), DataType::Float64),
            Field::new("total_sec".into(), DataType::Float64),
            Field::new("total_sec_pct".into(), DataType::Float64),
            Field::new("year".into(), DataType::Float64),
            Field::new("zip".into(), DataType::String),
        ])
    }

    pub async fn duck_from_parquet(
        conn: Arc<Mutex<Connection>>,
        parquet_path: impl AsRef<Path>,
    ) -> Result<usize, AppError> {
        <Self as DuckCrudModel>::upsert_from_parquet_one_file(
            conn,
            parquet_path,
            None,
            Some(<Self as DuckCrudModel>::table().into()),
        )
        .await
    }

    pub async fn read_range_to_parquet(
        conn: Arc<Mutex<Connection>>,
        year_range: (i32, i32),
        out_path: impl AsRef<Path>,
    ) -> Result<PathBuf, AppError> {
        let out_path = out_path.as_ref().to_path_buf();
        tokio::task::spawn_blocking(move || {
            if let Some(parent) = out_path.parent() {
                std::fs::create_dir_all(parent)?;
            }
            if out_path.exists() {
                std::fs::remove_file(&out_path)?;
            }

            let table = <Self as DuckCrudModel>::table();
            let out_sql = out_path.to_string_lossy().replace('\'', "''");
            let sql = format!(
                r#"COPY (
    SELECT *
    FROM {table}
    WHERE CAST(year AS BIGINT) BETWEEN {start_year} AND {end_year}
) TO '{out}' (FORMAT PARQUET);"#,
                table = table,
                start_year = year_range.0,
                end_year = year_range.1,
                out = out_sql
            );

            let conn_guard = conn.lock().expect("duckdb connection mutex poisoned");
            conn_guard.execute_batch(&sql)?;
            Ok::<PathBuf, AppError>(out_path)
        })
        .await?
    }

    pub async fn read_range<'a>(
        _conn: Arc<Mutex<Connection>>,
        _year_range: (i32, i32),
    ) -> Result<Vec<Row<'a>>, AppError> {
        Err(std::io::Error::new(
            std::io::ErrorKind::Other,
            "AnnComp::read_range not implemented; prefer AnnComp::read_range_to_parquet + Polars scan",
        )
        .into())
    }
}
