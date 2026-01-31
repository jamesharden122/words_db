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
pub struct StGrtTab {
    pub address: Option<String>,
    pub becameceo: Option<NaiveDate>,
    pub blkshval: Option<f64>,
    pub city: Option<String>,
    pub co_per_rol: Option<f64>,
    pub coname: Option<String>,
    pub cusip: Option<String>,
    pub exchange: Option<String>,
    pub exdate: Option<NaiveDate>,
    pub exec_fname: Option<String>,
    pub exec_fullname: Option<String>,
    pub exec_lname: Option<String>,
    pub exec_mname: Option<String>,
    pub execid: Option<String>,
    pub execrank: Option<f64>,
    pub expric: Option<f64>,
    pub gender: Option<String>,
    pub grntnum: Option<f64>,
    pub gvkey: Option<String>,
    pub inddesc: Option<String>,
    pub joined_co: Option<NaiveDate>,
    pub leftco: Option<NaiveDate>,
    pub leftofc: Option<NaiveDate>,
    pub mktpric: Option<f64>,
    pub naics: Option<f64>,
    pub naicsdesc: Option<String>,
    pub nameprefix: Option<String>,
    pub numsecur: Option<f64>,
    pub page: Option<f64>,
    pub pcdvalue: Option<String>,
    pub pceo: Option<String>,
    pub pcfo: Option<String>,
    pub pcttotopt: Option<f64>,
    pub reason: Option<String>,
    pub rejoin: Option<NaiveDate>,
    pub releft: Option<NaiveDate>,
    pub reload: Option<f64>,
    pub sic: Option<f64>,
    pub sicdesc: Option<String>,
    pub spcode: Option<String>,
    pub spindex: Option<f64>,
    pub state: Option<String>,
    pub sub_tele: Option<f64>,
    pub tele: Option<String>,
    pub ticker: Option<String>,
    pub title: Option<String>,
    pub value: Option<f64>,
    pub year: Option<f64>,
    pub zip: Option<String>,
}

impl ToPolars for StGrtTab {
    fn schema() -> Schema {
        StGrtTab::polars_schema()
    }
}

impl DuckCrudModel for StGrtTab {
    fn table() -> &'static str {
        "stgrttab"
    }
}

impl StGrtTab {
    pub fn polars_schema() -> Schema {
        Schema::from_iter(vec![
            Field::new("address".into(), DataType::String),
            Field::new("becameceo".into(), DataType::Date),
            Field::new("blkshval".into(), DataType::Float64),
            Field::new("city".into(), DataType::String),
            Field::new("co_per_rol".into(), DataType::Float64),
            Field::new("coname".into(), DataType::String),
            Field::new("cusip".into(), DataType::String),
            Field::new("exchange".into(), DataType::String),
            Field::new("exdate".into(), DataType::Date),
            Field::new("exec_fname".into(), DataType::String),
            Field::new("exec_fullname".into(), DataType::String),
            Field::new("exec_lname".into(), DataType::String),
            Field::new("exec_mname".into(), DataType::String),
            Field::new("execid".into(), DataType::String),
            Field::new("execrank".into(), DataType::Float64),
            Field::new("expric".into(), DataType::Float64),
            Field::new("gender".into(), DataType::String),
            Field::new("grntnum".into(), DataType::Float64),
            Field::new("gvkey".into(), DataType::String),
            Field::new("inddesc".into(), DataType::String),
            Field::new("joined_co".into(), DataType::Date),
            Field::new("leftco".into(), DataType::Date),
            Field::new("leftofc".into(), DataType::Date),
            Field::new("mktpric".into(), DataType::Float64),
            Field::new("naics".into(), DataType::Float64),
            Field::new("naicsdesc".into(), DataType::String),
            Field::new("nameprefix".into(), DataType::String),
            Field::new("numsecur".into(), DataType::Float64),
            Field::new("page".into(), DataType::Float64),
            Field::new("pcdvalue".into(), DataType::String),
            Field::new("pceo".into(), DataType::String),
            Field::new("pcfo".into(), DataType::String),
            Field::new("pcttotopt".into(), DataType::Float64),
            Field::new("reason".into(), DataType::String),
            Field::new("rejoin".into(), DataType::Date),
            Field::new("releft".into(), DataType::Date),
            Field::new("reload".into(), DataType::Float64),
            Field::new("sic".into(), DataType::Float64),
            Field::new("sicdesc".into(), DataType::String),
            Field::new("spcode".into(), DataType::String),
            Field::new("spindex".into(), DataType::Float64),
            Field::new("state".into(), DataType::String),
            Field::new("sub_tele".into(), DataType::Float64),
            Field::new("tele".into(), DataType::String),
            Field::new("ticker".into(), DataType::String),
            Field::new("title".into(), DataType::String),
            Field::new("value".into(), DataType::Float64),
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
            "StGrtTab::read_range not implemented; prefer StGrtTab::read_range_to_parquet + Polars scan",
        )
        .into())
    }
}
