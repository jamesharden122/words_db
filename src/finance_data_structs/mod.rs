pub mod compustat;
pub mod crsp;
pub mod world_indices;
use crate::error::AppError;
use duckdb::{params, Connection, OptionalExt};
use futures::{StreamExt, TryStreamExt};
use itertools::Itertools; // <-- brings .chunks() into scope
use serde::{de::DeserializeOwned, Deserialize, Serialize};
use std::{path::Path, sync::Arc};
use surrealdb::engine::local::Db;
use surrealdb::Surreal;
use tokio::task;
use uuid::Uuid;
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

    /// 🔹 NEW: One-file Parquet ingest → JSON doc table with upsert.
    ///
    /// If `id_col` is `Some("col")`, uses that Parquet column (cast to TEXT) as `id`.
    /// If `None`, derives a stable id as `md5(to_json(struct_pack(*)))`.
    /// Returns the total rows processed from the file (inserted + updated).
    async fn upsert_from_parquet_one_file(
        conn: Arc<Connection>,
        parquet_path: impl AsRef<Path>,
        _id_col: Option<String>,
        table_opt: Option<String>,
    ) -> Result<usize, AppError> {
        let path = parquet_path.as_ref().to_string_lossy().to_string();
        let table_name = table_opt.unwrap_or_else(|| Self::table().to_string());

        tokio::task::block_in_place(move || {
            // Ensure table exists once
            Self::ensure_table(&conn)?;
            // Count rows in the file (inline path so DuckDB can infer schema at prepare time)
            let esc_path = path.replace('\'', "''");
            let count_sql = format!("SELECT count(*) FROM read_parquet('{}')", esc_path);
            let total: i64 = conn.query_row(&count_sql, [], |r| r.get(0))?;
            println!("Row Count {}", total);
            // Build one-shot UPSERT SQL
            let sql = format!(
                "CREATE OR REPLACE TABLE {} AS SELECT * FROM read_parquet('{}');",
                table_name, esc_path
            );
            // Manual transaction to avoid &mut borrow
            conn.execute("BEGIN TRANSACTION", [])?;
            let res = (|| -> Result<(), AppError> {
                conn.execute(&sql, [])?;
                Ok(())
            })();
            match res {
                Ok(()) => {
                    conn.execute("COMMIT", [])?;
                    Ok::<usize, AppError>(total as usize)
                }
                Err(e) => {
                    let _ = conn.execute("ROLLBACK", []);
                    Err(e)
                }
            }
        })
    }
}
