pub mod bank_regulatory;
pub mod compustat;
pub mod crsp;
pub mod equity_factors;
pub mod global_equities;
pub mod global_fundamentals_compustat;
pub mod usindexes;
pub mod wdi;
pub mod world_indices;
use crate::error::AppError;
use duckdb::{params, Connection, OptionalExt};
use futures::{StreamExt, TryStreamExt};
use itertools::Itertools; // <-- brings .chunks() into scope
use polars::frame::row::Row;
use polars::prelude::*;
use serde::{de::DeserializeOwned, Deserialize, Serialize};
use std::{
    path::Path,
    sync::{Arc, Mutex},
};
use surrealdb::engine::local::Db;
use surrealdb::Surreal;
use uuid::Uuid;

/// Trait: provides a schema and helpers to build a DataFrame from `polars::Row`s.
pub trait ToPolars {
    /// Schema describing the target DataFrame.
    fn schema() -> Schema;

    /// Convert a slice of rows into a DataFrame using the trait’s schema.
    fn df_from_rows(rows: &[Row]) -> PolarsResult<DataFrame> {
        DataFrame::from_rows_and_schema(rows, &Self::schema())
    }

    /// Read a Parquet file into a `DataFrame` using Polars `scan_parquet` (lazy) + `collect`.
    ///
    /// Designed to accept the `PathBuf` returned by `read_range_to_parquet`.
    /// If calling from within a Tokio runtime (e.g. `#[tokio::test]`), prefer wrapping the call in
    /// `tokio::task::spawn_blocking` to avoid nested-runtime panics from Polars internals.
    fn df_from_parquet_scan<P: AsRef<Path>>(parquet_path: P) -> Result<DataFrame, AppError> {
        Self::df_from_parquet_scan_with_args(parquet_path, ScanArgsParquet::default())
    }

    /// Same as `df_from_parquet_scan`, but allows customizing `ScanArgsParquet`.
    fn df_from_parquet_scan_with_args<P: AsRef<Path>>(
        parquet_path: P,
        args: ScanArgsParquet,
    ) -> Result<DataFrame, AppError> {
        let parquet_path = parquet_path.as_ref();
        if !parquet_path.exists() {
            return Err(std::io::Error::new(
                std::io::ErrorKind::NotFound,
                format!("Parquet not found: {}", parquet_path.display()),
            )
            .into());
        }

        let path_str = parquet_path.to_string_lossy();
        let lf = LazyFrame::scan_parquet(PlPath::from_str(path_str.as_ref()), args)?;
        Ok(lf.collect()?)
    }

    /// Variant that accepts an iterator over rows (useful if you stream/build rows lazily).
    fn df_from_rows_iter<'a, I>(rows_iter: I) -> PolarsResult<DataFrame>
    where
        I: Iterator<Item = &'a Row<'a>>,
    {
        // Collect into a Vec to avoid relying on `from_rows_iter_and_schema` availability.
        let rows: Vec<Row> = rows_iter.cloned().collect();
        DataFrame::from_rows_and_schema(&rows, &Self::schema())
    }

    /// Produce an empty DataFrame with the right columns & dtypes.
    fn empty_df() -> DataFrame {
        DataFrame::empty_with_schema(&Self::schema())
    }
}
// Define an enum that will handle different error types
/// Generic CRUD trait for SurrealDB-backed data structures.
pub trait SurrealCrudModel: Sized + Clone + Serialize + for<'de> Deserialize<'de> {
    /// SurrealDB table name
    fn table() -> &'static str;
    /// Deterministic id for upsert; if None, a random id will be generated on create.
    fn id_key(&self) -> Option<String> {
        None
    }

    /// Create or update this record. If `id_key()` returns Some, performs an upsert; otherwise creates a new record.
    async fn upsert(&self, db: &Surreal<Db>) -> Result<(), AppError>
    where
        Self: Clone + Serialize + DeserializeOwned + Send + Sync + 'static,
    {
        let table = Self::table();
        if let Some(id) = self.id_key() {
            let _: Option<Self> = db.clone().update((table, id)).content(self.clone()).await?;
        } else {
            let _: Option<Self> = db.create(table).content(self.clone()).await?;
        }
        Ok(())
    }

    /// Read a record by id
    fn read<'a>(
        db: &'a Surreal<Db>,
        id: &'a str,
    ) -> std::pin::Pin<
        Box<dyn std::future::Future<Output = Result<Option<Self>, AppError>> + Send + 'a>,
    > {
        Box::pin(async move {
            db.select::<Option<Self>>((Self::table(), id))
                .await
                .map_err(Into::into)
        })
    }

    /// Delete a record by id
    fn delete<'a>(
        db: &'a Surreal<Db>,
        id: &'a str,
    ) -> std::pin::Pin<Box<dyn std::future::Future<Output = Result<(), AppError>> + Send + 'a>>
    {
        Box::pin(async move {
            let _ = db
                .delete::<Option<serde_json::Value>>((Self::table(), id))
                .await?;
            Ok(())
        })
    }

    async fn insert_vec_concurrent(
        db: &Surreal<Db>,
        items: Vec<Self>,
        chunk_size: usize,
        concurrency: usize,
    ) -> Result<usize, AppError>
    where
        Self: Serialize + Send + Sync + 'static, // no Clone needed here
    {
        let chunk_size = chunk_size.max(1);
        let concurrency = concurrency.max(1);

        // Consume `items` so elements MOVE into chunk Vecs (no per-item clone).
        let iter = items.into_iter();
        let chunks = iter.chunks(chunk_size);
        let mut batches: Vec<(usize, Vec<Self>)> = chunks
            .into_iter()
            .enumerate()
            .map(|(i, ch)| (i, ch.collect::<Vec<_>>())) // owns elements, no clones
            .collect();
        futures::stream::iter(batches.drain(..).map(|(i, batch)| {
            let db = db.clone();
            let n = batch.len();
            println!("{:?}", i);
            async move {
                // Move `batch` into bind (owned, 'static-friendly).
                db.query(format!(
                    "BEGIN TRANSACTION; INSERT INTO {} $data RETURN NONE; COMMIT;",
                    Self::table()
                ))
                .bind(("data", batch))
                .await
                .map_err(AppError::from)?;
                Ok::<usize, AppError>(n)
            }
        }))
        .buffer_unordered(concurrency)
        .try_fold(0usize, |acc, n| async move { Ok(acc + n) })
        .await
    }
}

/// Generic CRUD trait for DuckDB-backed data structures.
/// Storage model: `{table}(id TEXT PRIMARY KEY, doc JSON)`
pub trait DuckCrudModel: Sized + Clone + Serialize + for<'de> Deserialize<'de> {
    /// DuckDB table name
    fn table() -> &'static str;

    /// Deterministic id for upsert; if None, a random UUID is generated on create.
    fn id_key(&self) -> Option<String> {
        None
    }

    /// Ensure backing table exists (idempotent).
    fn ensure_table(conn: &Connection) -> Result<(), AppError> {
        let sql = format!(
            "CREATE TABLE IF NOT EXISTS {} (
                id  TEXT PRIMARY KEY,
                doc JSON
            );",
            Self::table()
        );
        conn.execute(&sql, [])?;
        Ok(())
    }

    /// Create or update this record (JSON doc, primary key on `id`).
    async fn upsert(&self, conn: Arc<Connection>) -> Result<String, AppError>
    where
        Self: Serialize + DeserializeOwned + Send + Sync + 'static,
    {
        let me = self.clone();
        tokio::task::block_in_place(move || {
            Self::ensure_table(&conn)?;
            let id = me.id_key().unwrap_or_else(|| Uuid::new_v4().to_string());
            let json = serde_json::to_string(&me)?;
            let sql = format!(
                "INSERT INTO {}(id, doc)
                 VALUES (?, ?)
                 ON CONFLICT(id) DO UPDATE SET doc = excluded.doc;",
                Self::table()
            );
            let mut stmt = conn.prepare(&sql)?;
            stmt.execute(params![id, json])?;
            Ok::<_, AppError>(id)
        })
    }

    /// Read a record by id.
    fn read<'a>(
        conn: Arc<Connection>,
        id: String,
    ) -> std::pin::Pin<Box<dyn std::future::Future<Output = Result<Option<Self>, AppError>> + 'a>>
    where
        Self: Serialize + DeserializeOwned + Send + 'static,
    {
        Box::pin(async move {
            tokio::task::block_in_place(move || {
                Self::ensure_table(&conn)?;
                let sql = format!("SELECT doc FROM {} WHERE id = ?;", Self::table());
                let mut stmt = conn.prepare(&sql)?;
                let row_opt = stmt
                    .query_row(params![id], |row| row.get::<_, String>(0))
                    .optional()?;
                match row_opt {
                    None => Ok(None),
                    Some(doc) => Ok(Some(serde_json::from_str::<Self>(&doc)?)),
                }
            })
        })
    }

    /// Delete a record by id.
    fn delete<'a>(
        conn: Arc<Connection>,
        id: String,
    ) -> std::pin::Pin<Box<dyn std::future::Future<Output = Result<(), AppError>> + 'a>> {
        Box::pin(async move {
            tokio::task::block_in_place(move || {
                Self::ensure_table(&conn)?;
                let sql = format!("DELETE FROM {} WHERE id = ?;", Self::table());
                conn.execute(&sql, params![id])?;
                Ok(())
            })
        })
    }
    //async fn store_duck_db(file_name: &str) -> Result<(), AppError>{let file_pth = Path::from(format!("{}.{}",file_name,"duckdb"));}
    /// 🔹 NEW: One-file Parquet ingest → JSON doc table with upsert.
    ///
    /// If `id_col` is `Some("col")`, uses that Parquet column (cast to TEXT) as `id`.
    /// If `None`, derives a stable id as `md5(to_json(struct_pack(*)))`.
    /// Returns the total rows processed from the file (inserted + updated).
    async fn upsert_from_parquet_one_file(
        conn: Arc<Mutex<Connection>>,
        parquet_path: impl AsRef<Path>,
        _id_col: Option<String>,
        table_opt: Option<String>,
    ) -> Result<usize, AppError> {
        let path = parquet_path.as_ref().to_string_lossy().to_string();
        let table_name = table_opt.unwrap_or_else(|| Self::table().to_string());

        tokio::task::spawn_blocking(move || {
            // Ensure table exists once
            let conn_guard = conn.lock().expect("duckdb connection mutex poisoned");
            Self::ensure_table(&conn_guard)?;
            let esc_path = path.replace('\'', "''");
            // Count rows, preferring Parquet metadata (avoids a full file scan) with a
            // compatibility fallback to `read_parquet()` for older/newer DuckDB schemas.
            let total: i64 = {
                // Newer DuckDB versions expose row counts at the row-group level.
                // Some versions expose `parquet_metadata()` at the column-chunk level,
                // so dedupe by `row_group_id` to avoid overcounting.
                let meta_sql = format!(
                    "SELECT COALESCE(SUM(row_group_num_rows), 0)
                     FROM (
                        SELECT DISTINCT row_group_id, row_group_num_rows
                        FROM parquet_metadata('{}')
                     ) t",
                    esc_path
                );
                match conn_guard.query_row(&meta_sql, [], |r| r.get::<_, i64>(0)) {
                    Ok(v) => v,
                    Err(_) => {
                        // Fallback (full scan): keeps behavior correct across DuckDB versions.
                        let count_sql =
                            format!("SELECT count(*) FROM read_parquet('{}')", esc_path);
                        conn_guard.query_row(&count_sql, [], |r| r.get::<_, i64>(0))?
                    }
                }
            };
            println!("Row Count {}", total);
            // Build one-shot UPSERT SQL
            let sql = format!(
                "CREATE OR REPLACE TABLE {} AS SELECT * FROM read_parquet('{}');",
                table_name, esc_path
            );
            // Manual transaction to avoid &mut borrow
            conn_guard.execute("BEGIN TRANSACTION", [])?;
            let res = (|| -> Result<(), AppError> {
                conn_guard.execute(&sql, [])?;
                Ok(())
            })();
            match res {
                Ok(()) => {
                    conn_guard.execute("COMMIT", [])?;
                    Ok::<usize, AppError>(total as usize)
                }
                Err(e) => {
                    let _ = conn_guard.execute("ROLLBACK", []);
                    Err(e)
                }
            }
        })
        .await?
    }
}
