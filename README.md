# words_db

Rust library for ingesting, storing, and analyzing financial index data. It
provides in-memory DuckDB and SurrealDB bootstraps, fast Parquet ingestion,
Arrow/Polars interop, and utilities for CRSP-style global daily indexes and
wide “world indices returns” tables.

## Features
- DuckDB JSON-doc tables and direct Parquet → table ingest.
- SurrealDB in-memory bootstrap with concurrent bulk inserts.
- Arrow RecordBatch streaming → Polars `Row` conversion.
- Typed models: `crsp::GlobalDailyIndex`, `world_indices::GlobalRets`.
- Ready-to-use Polars schema for `GlobalRets` and helpers to query date ranges.

## Layout
- `src/finance_data_structs/` — domain models and I/O helpers
  - `crsp.rs` (prices, metadata), `world_indices.rs` (returns), `compustat.rs`
- `src/schema.rs` — SurrealDB namespaces/tables
- `tests/` — DuckDB/SurrealDB ingestion and analysis examples
- `scripts/` — R/Python utilities (e.g., save Parquet)

## Quick Start
Requirements: Rust 1.75+, no external DBs needed (DuckDB bundled, SurrealDB
in-memory).

Build and test:
- `cargo build`
- `cargo test --release -- --nocapture`

Optionally tune threads for larger datasets:
- `RAYON_NUM_THREADS=14 POLARS_MAX_THREADS=14 cargo test --release`

## Examples
Ingest CRSP-style global indexes into DuckDB:
```rust
let conn = std::sync::Arc::new(words_db::start_duck_db("4GB", 14).await?);
words_db::finance_data_structs::crsp::GlobalDailyIndex
    ::duck_from_parquet(conn.clone(), "../data/raw_files/global_indexes_daily.parquet")
    .await?;
```

Query world returns and build a Polars DataFrame:
```rust
use words_db::finance_data_structs::world_indices::GlobalRets;
use chrono::NaiveDate;
let rows = GlobalRets::read_range(
    conn.clone(),
    (NaiveDate::from_ymd_opt(2020,1,1).unwrap(), NaiveDate::from_ymd_opt(2025,10,2).unwrap())
).await?;
let schema = GlobalRets::polars_schema();
let df = polars::prelude::DataFrame::from_rows_and_schema(&rows, &schema)?;
```

SurrealDB bootstrap and bulk insert (example):
```rust
let db = words_db::start_mem_db().await?;
use words_db::finance_data_structs::world_indices::GlobalRets;
let rets = GlobalRets::from_parquet("../data/raw_files/country_returns_wide.parquet")?;
let inserted = GlobalRets::create_rets_result(rets, &db, "indexes", "daily", 15_000, 14).await?;
```

Data files referenced by tests live outside the repo tree (e.g.
`../data/raw_files/...`). Tests auto-skip when inputs are missing.

## Contributing
See `AGENTS.md` for project structure, style, and PR guidelines.
