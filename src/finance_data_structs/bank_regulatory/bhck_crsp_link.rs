use super::super::{AppError, DuckCrudModel, SurrealCrudModel, ToPolars};
use arrow_array::{Array, Date32Array, Float64Array, StringArray};
use chrono::NaiveDate;
use duckdb::Connection;
use polars::frame::row::Row;
use polars::prelude::*;
use serde::{Deserialize, Serialize};
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct BhckCrspLink {
    pub rssd9001: Option<f64>,
    pub permco: Option<f64>,
    pub name: Option<String>,
    pub inst_type: Option<String>,
    pub dt_start: Option<NaiveDate>,
    pub dt_end: Option<NaiveDate>,
}

impl SurrealCrudModel for BhckCrspLink {
    fn table() -> &'static str {
        "bhck_crsp_link"
    }

    fn id_key(&self) -> Option<String> {
        match (self.rssd9001, self.permco, self.dt_start) {
            (Some(rssd9001), Some(permco), Some(dt_start)) => Some(format!("{rssd9001}:{permco}:{dt_start}")),
            _ => None,
        }
    }
}

impl DuckCrudModel for BhckCrspLink {
    fn table() -> &'static str {
        "bhck_crsp_link"
    }

    fn id_key(&self) -> Option<String> {
        <Self as SurrealCrudModel>::id_key(self)
    }
}

impl ToPolars for BhckCrspLink {
    fn schema() -> Schema {
        BhckCrspLink::polars_schema()
    }
}

impl BhckCrspLink {
    pub fn polars_schema() -> Schema {
        Schema::from_iter([
            Field::new("rssd9001".into(), DataType::Float64),
            Field::new("permco".into(), DataType::Float64),
            Field::new("name".into(), DataType::String),
            Field::new("inst_type".into(), DataType::String),
            Field::new("dt_start".into(), DataType::Date),
            Field::new("dt_end".into(), DataType::Date),
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

    /// Write a date-overlapping slice of the link table to a parquet.
    ///
    /// Uses an interval overlap predicate: `dt_start <= end AND dt_end >= start`.
    pub async fn read_range_to_parquet(
        conn: Arc<Mutex<Connection>>,
        date_range: (NaiveDate, NaiveDate),
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
    WHERE CAST(dt_start AS DATE) <= DATE '{end}'
      AND CAST(dt_end   AS DATE) >= DATE '{start}'
) TO '{out}' (FORMAT PARQUET);"#,
                table = table,
                start = date_range.0.to_string(),
                end = date_range.1.to_string(),
                out = out_sql
            );

            let conn_guard = conn.lock().expect("duckdb connection mutex poisoned");
            conn_guard.execute_batch(&sql)?;
            Ok::<PathBuf, AppError>(out_path)
        })
        .await?
    }

    /// Read rows overlapping a date range from DuckDB and return as Polars Rows.
    pub async fn read_range<'a>(
        conn: Arc<Mutex<Connection>>,
        date_range: (NaiveDate, NaiveDate),
    ) -> Result<Vec<Row<'a>>, AppError> {
        tokio::task::spawn_blocking(move || {
            let table = <Self as DuckCrudModel>::table();
            let sql = format!(
                r#"SELECT
    CAST(rssd9001 AS DOUBLE) AS rssd9001,
    CAST(permco   AS DOUBLE) AS permco,
    CAST(name     AS VARCHAR) AS name,
    CAST(inst_type AS VARCHAR) AS inst_type,
    CAST(dt_start AS DATE) AS dt_start,
    CAST(dt_end   AS DATE) AS dt_end
FROM {table}
WHERE CAST(dt_start AS DATE) <= DATE '{end}'
  AND CAST(dt_end   AS DATE) >= DATE '{start}'
ORDER BY rssd9001, permco, dt_start"#,
                table = table,
                start = date_range.0.to_string(),
                end = date_range.1.to_string(),
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
                let s = |name: &str| -> &StringArray {
                    batch
                        .column(schema.index_of(name).unwrap())
                        .as_any()
                        .downcast_ref::<StringArray>()
                        .unwrap()
                };
                let d = |name: &str| -> &Date32Array {
                    batch
                        .column(schema.index_of(name).unwrap())
                        .as_any()
                        .downcast_ref::<Date32Array>()
                        .unwrap()
                };

                let rssd9001 = f("rssd9001");
                let permco = f("permco");
                let name = s("name");
                let inst_type = s("inst_type");
                let dt_start = d("dt_start");
                let dt_end = d("dt_end");

                for row_i in 0..batch.num_rows() {
                    let mut vals: Vec<AnyValue<'static>> = Vec::with_capacity(6);
                    vals.push(if rssd9001.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(rssd9001.value(row_i))
                    });
                    vals.push(if permco.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(permco.value(row_i))
                    });
                    vals.push(if name.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::StringOwned(name.value(row_i).into())
                    });
                    vals.push(if inst_type.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::StringOwned(inst_type.value(row_i).into())
                    });
                    vals.push(if dt_start.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Date(dt_start.value(row_i))
                    });
                    vals.push(if dt_end.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Date(dt_end.value(row_i))
                    });
                    out.push(Row::new(vals));
                }
            }

            Ok::<Vec<Row>, AppError>(out)
        })
        .await?
    }
}

