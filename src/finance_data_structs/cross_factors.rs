use super::{AppError, DuckCrudModel, SurrealCrudModel, ToPolars};
use arrow_array::{Array, BooleanArray, Date32Array, Float64Array, Int32Array};
use chrono::{Datelike, NaiveDate};
use duckdb::Connection;
use polars::frame::row::Row;
use polars::prelude::*;
use rayon::prelude::*;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::path::Path;
use std::sync::{Arc, Mutex};
use surrealdb::engine::local::Db;
use surrealdb::Surreal;

/// Daily Fama-French factors (plus UMD) and the risk-free rate.
///
/// Column order matches the upstream dataset:
/// `cma, date, hml, mktrf, month, rf, rmw, smb, umd`.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FamaFrenDly {
    pub cma: Option<f64>,
    pub date: NaiveDate,
    pub hml: Option<f64>,
    pub mktrf: Option<f64>,
    pub month: Option<bool>,
    pub rf: Option<f64>,
    pub rmw: Option<f64>,
    pub smb: Option<f64>,
    pub umd: Option<f64>,
}

impl SurrealCrudModel for FamaFrenDly {
    fn table() -> &'static str {
        "fama_french_daily"
    }
    fn id_key(&self) -> Option<String> {
        Some(self.date.to_string())
    }
}

impl DuckCrudModel for FamaFrenDly {
    fn table() -> &'static str {
        <Self as SurrealCrudModel>::table()
    }
    fn id_key(&self) -> Option<String> {
        <Self as SurrealCrudModel>::id_key(self)
    }
}

impl ToPolars for FamaFrenDly {
    fn schema() -> Schema {
        FamaFrenDly::polars_schema()
    }
}

impl FamaFrenDly {
    pub fn polars_schema() -> Schema {
        Schema::from_iter(vec![
            Field::new("cma".into(), DataType::Float64),
            Field::new("date".into(), DataType::Date),
            Field::new("hml".into(), DataType::Float64),
            Field::new("mktrf".into(), DataType::Float64),
            Field::new("month".into(), DataType::Boolean),
            Field::new("rf".into(), DataType::Float64),
            Field::new("rmw".into(), DataType::Float64),
            Field::new("smb".into(), DataType::Float64),
            Field::new("umd".into(), DataType::Float64),
        ])
    }

    pub fn to_row<'a>(self) -> Row<'a> {
        let days: i32 = (self.date.num_days_from_ce() - 719_163) as i32;
        Row::new(vec![
            self.cma.map_or(AnyValue::Null, AnyValue::Float64),
            AnyValue::Date(days),
            self.hml.map_or(AnyValue::Null, AnyValue::Float64),
            self.mktrf.map_or(AnyValue::Null, AnyValue::Float64),
            self.month.map_or(AnyValue::Null, AnyValue::Boolean),
            self.rf.map_or(AnyValue::Null, AnyValue::Float64),
            self.rmw.map_or(AnyValue::Null, AnyValue::Float64),
            self.smb.map_or(AnyValue::Null, AnyValue::Float64),
            self.umd.map_or(AnyValue::Null, AnyValue::Float64),
        ])
    }

    pub fn from_parquet(path: impl AsRef<Path>) -> Result<Vec<Self>, AppError> {
        let file = std::fs::File::open(path)?;
        let mut df = ParquetReader::new(file).finish()?;
        df = df
            .lazy()
            .with_columns([
                col("cma").cast(DataType::Float64),
                col("hml").cast(DataType::Float64),
                col("mktrf").cast(DataType::Float64),
                col("month").cast(DataType::Boolean),
                col("rf").cast(DataType::Float64),
                col("rmw").cast(DataType::Float64),
                col("smb").cast(DataType::Float64),
                col("umd").cast(DataType::Float64),
                col("date").cast(DataType::Date),
            ])
            .collect()?;

        let n = df.height();
        let origin_ce = 719_163i32;
        let date = df.column("date")?.date()?.clone();
        let month = df.column("month")?.bool()?.clone();

        let mut map: HashMap<&'static str, Float64Chunked> = HashMap::new();
        for &name in ["cma", "hml", "mktrf", "rf", "rmw", "smb", "umd"].iter() {
            map.insert(name, df.column(name)?.f64()?.clone());
        }

        let out: Vec<Self> = (0..n)
            .into_par_iter()
            .map(|i| {
                let nd = date
                    .phys
                    .get(i)
                    .and_then(|days| NaiveDate::from_num_days_from_ce_opt(days + origin_ce))
                    .unwrap_or_else(|| NaiveDate::from_ymd_opt(1970, 1, 1).unwrap());
                let get_f64 = |name: &str| -> Option<f64> { map.get(name).and_then(|a| a.get(i)) };
                Self {
                    cma: get_f64("cma"),
                    date: nd,
                    hml: get_f64("hml"),
                    mktrf: get_f64("mktrf"),
                    month: month.get(i),
                    rf: get_f64("rf"),
                    rmw: get_f64("rmw"),
                    smb: get_f64("smb"),
                    umd: get_f64("umd"),
                }
            })
            .collect();

        Ok(out)
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

    pub async fn create_result(
        data_vec: Vec<Self>,
        db: &Surreal<Db>,
        nsname: &str,
        dbname: &str,
        batch_size: usize,
        cores: usize,
    ) -> Result<usize, AppError> {
        db.use_ns(nsname).use_db(dbname).await?;
        Self::insert_vec_concurrent(db, data_vec, batch_size, cores).await
    }

    pub async fn read_range<'a>(
        conn: Arc<Mutex<Connection>>,
        date_range: (NaiveDate, NaiveDate),
    ) -> Result<Vec<Row<'a>>, AppError> {
        tokio::task::spawn_blocking(move || {
            let table = <Self as DuckCrudModel>::table();
            let sql = format!(
                r#"SELECT
    CAST(cma AS DOUBLE)   AS cma,
    CAST(date AS DATE)    AS date,
    CAST(hml AS DOUBLE)   AS hml,
    CAST(mktrf AS DOUBLE) AS mktrf,
    CAST(month AS BOOLEAN) AS month,
    CAST(rf AS DOUBLE)    AS rf,
    CAST(rmw AS DOUBLE)   AS rmw,
    CAST(smb AS DOUBLE)   AS smb,
    CAST(umd AS DOUBLE)   AS umd
FROM {table}
WHERE CAST(date AS DATE) BETWEEN DATE '{start}' AND DATE '{end}'
ORDER BY date"#,
                table = table,
                start = date_range.0.to_string(),
                end = date_range.1.to_string()
            );

            let conn_guard = conn.lock().expect("duckdb connection mutex poisoned");
            let mut stmt = conn_guard.prepare(sql.as_str())?;
            let mut reader = stmt.query_arrow([])?;
            let mut out: Vec<Row<'static>> = Vec::new();

            while let Some(batch) = reader.next() {
                let schema = batch.schema();
                let f = |name: &str| -> &Float64Array {
                    batch
                        .column(schema.index_of(name).unwrap())
                        .as_any()
                        .downcast_ref::<Float64Array>()
                        .unwrap()
                };
                let d = |name: &str| -> &Date32Array {
                    batch
                        .column(schema.index_of(name).unwrap())
                        .as_any()
                        .downcast_ref::<Date32Array>()
                        .unwrap()
                };
                let b = |name: &str| -> &BooleanArray {
                    batch
                        .column(schema.index_of(name).unwrap())
                        .as_any()
                        .downcast_ref::<BooleanArray>()
                        .unwrap()
                };

                let cma = f("cma");
                let date = d("date");
                let hml = f("hml");
                let mktrf = f("mktrf");
                let month = b("month");
                let rf = f("rf");
                let rmw = f("rmw");
                let smb = f("smb");
                let umd = f("umd");

                for row_i in 0..batch.num_rows() {
                    out.push(Row::new(vec![
                        if cma.is_null(row_i) {
                            AnyValue::Null
                        } else {
                            AnyValue::Float64(cma.value(row_i))
                        },
                        if date.is_null(row_i) {
                            AnyValue::Null
                        } else {
                            AnyValue::Date(date.value(row_i))
                        },
                        if hml.is_null(row_i) {
                            AnyValue::Null
                        } else {
                            AnyValue::Float64(hml.value(row_i))
                        },
                        if mktrf.is_null(row_i) {
                            AnyValue::Null
                        } else {
                            AnyValue::Float64(mktrf.value(row_i))
                        },
                        if month.is_null(row_i) {
                            AnyValue::Null
                        } else {
                            AnyValue::Boolean(month.value(row_i))
                        },
                        if rf.is_null(row_i) {
                            AnyValue::Null
                        } else {
                            AnyValue::Float64(rf.value(row_i))
                        },
                        if rmw.is_null(row_i) {
                            AnyValue::Null
                        } else {
                            AnyValue::Float64(rmw.value(row_i))
                        },
                        if smb.is_null(row_i) {
                            AnyValue::Null
                        } else {
                            AnyValue::Float64(smb.value(row_i))
                        },
                        if umd.is_null(row_i) {
                            AnyValue::Null
                        } else {
                            AnyValue::Float64(umd.value(row_i))
                        },
                    ]));
                }
            }

            Ok::<_, AppError>(out)
        })
        .await?
    }
}

/// Monthly Fama-French factors (plus UMD) and the risk-free rate.
///
/// Column order matches the upstream dataset:
/// `cma, date, dateff, hml, mktrf, month, rf, rmw, smb, umd, year`.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FamaFrenMthly {
    pub cma: Option<f64>,
    pub date: NaiveDate,
    pub dateff: NaiveDate,
    pub hml: Option<f64>,
    pub mktrf: Option<f64>,
    pub month: Option<i32>,
    pub rf: Option<f64>,
    pub rmw: Option<f64>,
    pub smb: Option<f64>,
    pub umd: Option<f64>,
    pub year: Option<i32>,
}

impl SurrealCrudModel for FamaFrenMthly {
    fn table() -> &'static str {
        "fama_french_monthly"
    }
    fn id_key(&self) -> Option<String> {
        Some(self.dateff.to_string())
    }
}

impl DuckCrudModel for FamaFrenMthly {
    fn table() -> &'static str {
        <Self as SurrealCrudModel>::table()
    }
    fn id_key(&self) -> Option<String> {
        <Self as SurrealCrudModel>::id_key(self)
    }
}

impl ToPolars for FamaFrenMthly {
    fn schema() -> Schema {
        FamaFrenMthly::polars_schema()
    }
}

impl FamaFrenMthly {
    pub fn polars_schema() -> Schema {
        Schema::from_iter(vec![
            Field::new("cma".into(), DataType::Float64),
            Field::new("date".into(), DataType::Date),
            Field::new("dateff".into(), DataType::Date),
            Field::new("hml".into(), DataType::Float64),
            Field::new("mktrf".into(), DataType::Float64),
            Field::new("month".into(), DataType::Int32),
            Field::new("rf".into(), DataType::Float64),
            Field::new("rmw".into(), DataType::Float64),
            Field::new("smb".into(), DataType::Float64),
            Field::new("umd".into(), DataType::Float64),
            Field::new("year".into(), DataType::Int32),
        ])
    }

    pub fn to_row<'a>(self) -> Row<'a> {
        let date_days: i32 = (self.date.num_days_from_ce() - 719_163) as i32;
        let dateff_days: i32 = (self.dateff.num_days_from_ce() - 719_163) as i32;
        Row::new(vec![
            self.cma.map_or(AnyValue::Null, AnyValue::Float64),
            AnyValue::Date(date_days),
            AnyValue::Date(dateff_days),
            self.hml.map_or(AnyValue::Null, AnyValue::Float64),
            self.mktrf.map_or(AnyValue::Null, AnyValue::Float64),
            self.month.map_or(AnyValue::Null, AnyValue::Int32),
            self.rf.map_or(AnyValue::Null, AnyValue::Float64),
            self.rmw.map_or(AnyValue::Null, AnyValue::Float64),
            self.smb.map_or(AnyValue::Null, AnyValue::Float64),
            self.umd.map_or(AnyValue::Null, AnyValue::Float64),
            self.year.map_or(AnyValue::Null, AnyValue::Int32),
        ])
    }

    pub fn from_parquet(path: impl AsRef<Path>) -> Result<Vec<Self>, AppError> {
        let file = std::fs::File::open(path)?;
        let mut df = ParquetReader::new(file).finish()?;
        df = df
            .lazy()
            .with_columns([
                col("cma").cast(DataType::Float64),
                col("hml").cast(DataType::Float64),
                col("mktrf").cast(DataType::Float64),
                col("rf").cast(DataType::Float64),
                col("rmw").cast(DataType::Float64),
                col("smb").cast(DataType::Float64),
                col("umd").cast(DataType::Float64),
                col("date").cast(DataType::Date),
                col("dateff").cast(DataType::Date),
                col("month").cast(DataType::Int32),
                col("year").cast(DataType::Int32),
            ])
            .collect()?;

        let n = df.height();
        let origin_ce = 719_163i32;
        let date = df.column("date")?.date()?.clone();
        let dateff = df.column("dateff")?.date()?.clone();
        let month = df.column("month")?.i32()?.clone();
        let year = df.column("year")?.i32()?.clone();

        let mut map: HashMap<&'static str, Float64Chunked> = HashMap::new();
        for &name in ["cma", "hml", "mktrf", "rf", "rmw", "smb", "umd"].iter() {
            map.insert(name, df.column(name)?.f64()?.clone());
        }

        let out: Vec<Self> = (0..n)
            .into_par_iter()
            .map(|i| {
                let date_nd = date
                    .phys
                    .get(i)
                    .and_then(|days| NaiveDate::from_num_days_from_ce_opt(days + origin_ce))
                    .unwrap_or_else(|| NaiveDate::from_ymd_opt(1970, 1, 1).unwrap());
                let dateff_nd = dateff
                    .phys
                    .get(i)
                    .and_then(|days| NaiveDate::from_num_days_from_ce_opt(days + origin_ce))
                    .unwrap_or_else(|| NaiveDate::from_ymd_opt(1970, 1, 1).unwrap());
                let get_f64 = |name: &str| -> Option<f64> { map.get(name).and_then(|a| a.get(i)) };

                Self {
                    cma: get_f64("cma"),
                    date: date_nd,
                    dateff: dateff_nd,
                    hml: get_f64("hml"),
                    mktrf: get_f64("mktrf"),
                    month: month.get(i),
                    rf: get_f64("rf"),
                    rmw: get_f64("rmw"),
                    smb: get_f64("smb"),
                    umd: get_f64("umd"),
                    year: year.get(i),
                }
            })
            .collect();

        Ok(out)
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

    pub async fn create_result(
        data_vec: Vec<Self>,
        db: &Surreal<Db>,
        nsname: &str,
        dbname: &str,
        batch_size: usize,
        cores: usize,
    ) -> Result<usize, AppError> {
        db.use_ns(nsname).use_db(dbname).await?;
        Self::insert_vec_concurrent(db, data_vec, batch_size, cores).await
    }

    pub async fn read_range<'a>(
        conn: Arc<Mutex<Connection>>,
        dateff_range: (NaiveDate, NaiveDate),
    ) -> Result<Vec<Row<'a>>, AppError> {
        tokio::task::spawn_blocking(move || {
            let table = <Self as DuckCrudModel>::table();
            let sql = format!(
                r#"SELECT
    CAST(cma AS DOUBLE)   AS cma,
    CAST(date AS DATE)    AS date,
    CAST(dateff AS DATE)  AS dateff,
    CAST(hml AS DOUBLE)   AS hml,
    CAST(mktrf AS DOUBLE) AS mktrf,
    CAST(month AS INTEGER) AS month,
    CAST(rf AS DOUBLE)    AS rf,
    CAST(rmw AS DOUBLE)   AS rmw,
    CAST(smb AS DOUBLE)   AS smb,
    CAST(umd AS DOUBLE)   AS umd,
    CAST(year AS INTEGER) AS year
FROM {table}
WHERE CAST(dateff AS DATE) BETWEEN DATE '{start}' AND DATE '{end}'
ORDER BY dateff"#,
                table = table,
                start = dateff_range.0.to_string(),
                end = dateff_range.1.to_string()
            );

            let conn_guard = conn.lock().expect("duckdb connection mutex poisoned");
            let mut stmt = conn_guard.prepare(sql.as_str())?;
            let mut reader = stmt.query_arrow([])?;
            let mut out: Vec<Row<'static>> = Vec::new();

            while let Some(batch) = reader.next() {
                let schema = batch.schema();
                let f = |name: &str| -> &Float64Array {
                    batch
                        .column(schema.index_of(name).unwrap())
                        .as_any()
                        .downcast_ref::<Float64Array>()
                        .unwrap()
                };
                let d = |name: &str| -> &Date32Array {
                    batch
                        .column(schema.index_of(name).unwrap())
                        .as_any()
                        .downcast_ref::<Date32Array>()
                        .unwrap()
                };
                let i32a = |name: &str| -> &Int32Array {
                    batch
                        .column(schema.index_of(name).unwrap())
                        .as_any()
                        .downcast_ref::<Int32Array>()
                        .unwrap()
                };

                let cma = f("cma");
                let date = d("date");
                let dateff = d("dateff");
                let hml = f("hml");
                let mktrf = f("mktrf");
                let month = i32a("month");
                let rf = f("rf");
                let rmw = f("rmw");
                let smb = f("smb");
                let umd = f("umd");
                let year = i32a("year");

                for row_i in 0..batch.num_rows() {
                    out.push(Row::new(vec![
                        if cma.is_null(row_i) {
                            AnyValue::Null
                        } else {
                            AnyValue::Float64(cma.value(row_i))
                        },
                        if date.is_null(row_i) {
                            AnyValue::Null
                        } else {
                            AnyValue::Date(date.value(row_i))
                        },
                        if dateff.is_null(row_i) {
                            AnyValue::Null
                        } else {
                            AnyValue::Date(dateff.value(row_i))
                        },
                        if hml.is_null(row_i) {
                            AnyValue::Null
                        } else {
                            AnyValue::Float64(hml.value(row_i))
                        },
                        if mktrf.is_null(row_i) {
                            AnyValue::Null
                        } else {
                            AnyValue::Float64(mktrf.value(row_i))
                        },
                        if month.is_null(row_i) {
                            AnyValue::Null
                        } else {
                            AnyValue::Int32(month.value(row_i))
                        },
                        if rf.is_null(row_i) {
                            AnyValue::Null
                        } else {
                            AnyValue::Float64(rf.value(row_i))
                        },
                        if rmw.is_null(row_i) {
                            AnyValue::Null
                        } else {
                            AnyValue::Float64(rmw.value(row_i))
                        },
                        if smb.is_null(row_i) {
                            AnyValue::Null
                        } else {
                            AnyValue::Float64(smb.value(row_i))
                        },
                        if umd.is_null(row_i) {
                            AnyValue::Null
                        } else {
                            AnyValue::Float64(umd.value(row_i))
                        },
                        if year.is_null(row_i) {
                            AnyValue::Null
                        } else {
                            AnyValue::Int32(year.value(row_i))
                        },
                    ]));
                }
            }

            Ok::<_, AppError>(out)
        })
        .await?
    }
}
