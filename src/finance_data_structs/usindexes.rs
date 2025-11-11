use super::{AppError, DuckCrudModel, SurrealCrudModel, ToPolars};
use arrow_array::Array;
use arrow_array::{Date32Array, Float64Array, Int64Array};
use chrono::{Datelike, NaiveDate};
use duckdb::Connection;
use polars::frame::row::Row;
use polars::prelude::*;
use rayon::prelude::*;
use serde::{Deserialize, Serialize};
use std::path::Path;
use std::sync::Arc;
use surrealdb::engine::local::Db;
use surrealdb::Surreal;

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct UsMarketIndex {
    pub date: NaiveDate,
    pub ewretd: Option<f64>,
    pub ewretx: Option<f64>,
    pub spindx: Option<f64>,
    pub sprtrn: Option<f64>,
    pub totcnt: Option<i32>,
    pub totval: Option<f64>,
    pub usdcnt: Option<i32>,
    pub usdval: Option<f64>,
    pub vwretd: Option<f64>,
    pub vwretx: Option<f64>,
}

impl SurrealCrudModel for UsMarketIndex {
    fn table() -> &'static str {
        "us_market_indexes_daily"
    }
    fn id_key(&self) -> Option<String> {
        Some(self.date.to_string())
    }
}

impl DuckCrudModel for UsMarketIndex {
    fn table() -> &'static str {
        // Keep naming consistent with other modules
        "us_market_indexes_daily"
    }
    fn id_key(&self) -> Option<String> {
        Some(self.date.to_string())
    }
}

impl UsMarketIndex {
    /// Read Parquet file and parse into typed structs.
    pub fn from_parquet(path: impl AsRef<Path>) -> Result<Vec<Self>, AppError> {
        let file = std::fs::File::open(path)?;
        let mut df = ParquetReader::new(file).finish()?;
        // Cast numeric columns to stable types
        let casts = vec![
            col("ewretd").cast(DataType::Float64),
            col("ewretx").cast(DataType::Float64),
            col("spindx").cast(DataType::Float64),
            col("sprtrn").cast(DataType::Float64),
            col("totcnt").cast(DataType::Int32),
            col("totval").cast(DataType::Float64),
            col("usdcnt").cast(DataType::Int32),
            col("usdval").cast(DataType::Float64),
            col("vwretd").cast(DataType::Float64),
            col("vwretx").cast(DataType::Float64),
        ];
        df = df.lazy().with_columns(casts).collect()?;

        let n = df.height();
        let date = df.column("date")?.date()?.clone();
        let ewretd = df.column("ewretd")?.f64()?.clone();
        let ewretx = df.column("ewretx")?.f64()?.clone();
        let spindx = df.column("spindx")?.f64()?.clone();
        let sprtrn = df.column("sprtrn")?.f64()?.clone();
        let totcnt = df.column("totcnt")?.i32()?.clone();
        let totval = df.column("totval")?.f64()?.clone();
        let usdcnt = df.column("usdcnt")?.i32()?.clone();
        let usdval = df.column("usdval")?.f64()?.clone();
        let vwretd = df.column("vwretd")?.f64()?.clone();
        let vwretx = df.column("vwretx")?.f64()?.clone();

        let origin_ce = 719_163i32;
        let out: Vec<Self> = (0..n)
            .into_par_iter()
            .map(|i| {
                let nd = date
                    .phys
                    .get(i)
                    .map(|days| {
                        chrono::NaiveDate::from_num_days_from_ce_opt(days + origin_ce).unwrap()
                    })
                    .unwrap_or_else(|| chrono::NaiveDate::from_ymd_opt(1970, 1, 1).unwrap());
                Self {
                    date: nd,
                    ewretd: ewretd.get(i),
                    ewretx: ewretx.get(i),
                    spindx: spindx.get(i),
                    sprtrn: sprtrn.get(i),
                    totcnt: totcnt.get(i),
                    totval: totval.get(i),
                    usdcnt: usdcnt.get(i),
                    usdval: usdval.get(i),
                    vwretd: vwretd.get(i),
                    vwretx: vwretx.get(i),
                }
            })
            .collect();

        Ok(out)
    }

    /// Convert to Polars Row (for DataFrame assembly).
    pub fn to_row<'a>(self) -> Row<'a> {
        let days: i32 = (self.date.num_days_from_ce() - 719_163) as i32;
        Row::new(vec![
            AnyValue::Date(days),
            self.ewretd.map_or(AnyValue::Null, AnyValue::Float64),
            self.ewretx.map_or(AnyValue::Null, AnyValue::Float64),
            self.spindx.map_or(AnyValue::Null, AnyValue::Float64),
            self.sprtrn.map_or(AnyValue::Null, AnyValue::Float64),
            self.totcnt.map_or(AnyValue::Null, AnyValue::Int32),
            self.totval.map_or(AnyValue::Null, AnyValue::Float64),
            self.usdcnt.map_or(AnyValue::Null, AnyValue::Int32),
            self.usdval.map_or(AnyValue::Null, AnyValue::Float64),
            self.vwretd.map_or(AnyValue::Null, AnyValue::Float64),
            self.vwretx.map_or(AnyValue::Null, AnyValue::Float64),
        ])
    }

    /// Ingest a Parquet file into DuckDB via table create/replace.
    pub async fn duck_from_parquet(
        conn: Arc<Connection>,
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

    /// Read rows for a date range from DuckDB and return as Polars Rows.
    pub async fn read_range<'a>(
        conn: Arc<Connection>,
        date_range: (NaiveDate, NaiveDate),
    ) -> Result<Vec<Row<'a>>, AppError> {
        tokio::task::block_in_place(move || {
            let sql = format!(
                "SELECT \
                    CAST(date AS DATE) AS date, \
                    CAST(ewretd AS DOUBLE) AS ewretd, \
                    CAST(ewretx AS DOUBLE) AS ewretx, \
                    CAST(spindx AS DOUBLE) AS spindx, \
                    CAST(sprtrn AS DOUBLE) AS sprtrn, \
                    CAST(totcnt AS BIGINT) AS totcnt, \
                    CAST(totval AS DOUBLE) AS totval, \
                    CAST(usdcnt AS BIGINT) AS usdcnt, \
                    CAST(usdval AS DOUBLE) AS usdval, \
                    CAST(vwretd AS DOUBLE) AS vwretd, \
                    CAST(vwretx AS DOUBLE) AS vwretx \
                 FROM {} \
                 WHERE CAST(date AS DATE) BETWEEN DATE '{}' AND DATE '{}' \
                 ORDER BY date",
                <Self as DuckCrudModel>::table(),
                date_range.0.to_string(),
                date_range.1.to_string()
            );

            let mut reader = conn.prepare(sql.as_str())?;
            let mut reader = reader.query_arrow([])?; // Arrow RecordBatchReader
            let mut out: Vec<Row<'static>> = Vec::new();

            while let Some(batch) = reader.next() {
                let schema = batch.schema();
                let date_idx = schema.index_of("date").unwrap();
                let date32 = batch
                    .column(date_idx)
                    .as_any()
                    .downcast_ref::<Date32Array>()
                    .unwrap();

                let f = |name: &str| -> &Float64Array {
                    batch
                        .column(schema.index_of(name).unwrap())
                        .as_any()
                        .downcast_ref::<Float64Array>()
                        .unwrap()
                };
                let i = |name: &str| -> &Int64Array {
                    batch
                        .column(schema.index_of(name).unwrap())
                        .as_any()
                        .downcast_ref::<Int64Array>()
                        .unwrap()
                };

                let ewretd = f("ewretd");
                let ewretx = f("ewretx");
                let spindx = f("spindx");
                let sprtrn = f("sprtrn");
                let totcnt = i("totcnt");
                let totval = f("totval");
                let usdcnt = i("usdcnt");
                let usdval = f("usdval");
                let vwretd = f("vwretd");
                let vwretx = f("vwretx");

                for row_i in 0..batch.num_rows() {
                    let nd = date32.value_as_date(row_i).unwrap();
                    let temp = Self {
                        date: nd,
                        ewretd: if ewretd.is_null(row_i) {
                            None
                        } else {
                            Some(ewretd.value(row_i))
                        },
                        ewretx: if ewretx.is_null(row_i) {
                            None
                        } else {
                            Some(ewretx.value(row_i))
                        },
                        spindx: if spindx.is_null(row_i) {
                            None
                        } else {
                            Some(spindx.value(row_i))
                        },
                        sprtrn: if sprtrn.is_null(row_i) {
                            None
                        } else {
                            Some(sprtrn.value(row_i))
                        },
                        totcnt: if totcnt.is_null(row_i) {
                            None
                        } else {
                            Some(totcnt.value(row_i) as i32)
                        },
                        totval: if totval.is_null(row_i) {
                            None
                        } else {
                            Some(totval.value(row_i))
                        },
                        usdcnt: if usdcnt.is_null(row_i) {
                            None
                        } else {
                            Some(usdcnt.value(row_i) as i32)
                        },
                        usdval: if usdval.is_null(row_i) {
                            None
                        } else {
                            Some(usdval.value(row_i))
                        },
                        vwretd: if vwretd.is_null(row_i) {
                            None
                        } else {
                            Some(vwretd.value(row_i))
                        },
                        vwretx: if vwretx.is_null(row_i) {
                            None
                        } else {
                            Some(vwretx.value(row_i))
                        },
                    };
                    out.push(temp.to_row());
                }
            }

            Ok::<Vec<Row>, AppError>(out)
        })
    }

    /// Insert into SurrealDB with batched concurrency.
    pub async fn create_result(
        data_vec: Vec<UsMarketIndex>,
        db: &Surreal<Db>,
        nsname: &str,
        dbname: &str,
        batch_size: usize,
        cores: usize,
    ) -> Result<usize, AppError> {
        db.use_ns(nsname).use_db(dbname).await?;
        UsMarketIndex::insert_vec_concurrent(db, data_vec, batch_size, cores).await
    }
}

impl ToPolars for UsMarketIndex {
    fn schema() -> Schema {
        Schema::from_iter([
            Field::new("date".into(), DataType::Date),
            Field::new("ewretd".into(), DataType::Float64),
            Field::new("ewretx".into(), DataType::Float64),
            Field::new("spindx".into(), DataType::Float64),
            Field::new("sprtrn".into(), DataType::Float64),
            Field::new("totcnt".into(), DataType::Int32),
            Field::new("totval".into(), DataType::Float64),
            Field::new("usdcnt".into(), DataType::Int32),
            Field::new("usdval".into(), DataType::Float64),
            Field::new("vwretd".into(), DataType::Float64),
            Field::new("vwretx".into(), DataType::Float64),
        ])
    }
}
