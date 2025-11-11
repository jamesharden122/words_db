use chrono::NaiveDate;
use polars::lazy::dsl::{pearson_corr, rolling_corr, spearman_rank_corr};
use polars::prelude::*;
use std::fs::File;
use std::path::Path;
use std::sync::Arc;
use words_db::finance_data_structs::crsp::{finance_tickers, GlobalDailyIndex};
use words_db::finance_data_structs::world_indices::GlobalRets;
use words_db::finance_data_structs::usindexes::UsMarketIndex;
use words_db::instantiatedb::duckdbinst::{DbType, persist_in_memory_to_file, open_duck_db_from_file};
const PARQUET_PATH: &str = "../data/raw_files/global_indexes_daily.parquet";

#[tokio::test(flavor = "multi_thread", worker_threads = 14)]
async fn duck_ingest_global_indexes_from_parquet() {
    let time = std::time::Instant::now();
    let conn = words_db::instantiatedb::duckdbinst::start_duck_db("4GB", 14)
        .await
        .expect("duckdb in-memory should start");
    let conn = Arc::new(conn);
    println!("Time Elapsed 1: {:?}", time.elapsed());
    let dbtype = DbType::GlobalDailyIndex;
    _ = dbtype.ingest(conn.clone(), "4GB")
        .await
        .expect("duck bootstrap with parquet should work");
    let mut stmt = conn.prepare("DESCRIBE  global_indexes_daily").unwrap();
    let mut rows = stmt.query([]).unwrap();
    while let Some(row) = rows.next().unwrap() {
        let name: String = row.get(0).unwrap();
        let dtype: String = row.get(1).unwrap();
        println!("{}: {}", name, dtype);
    }
    println!("Time Elapsed 2: {:?}", time.elapsed());

    let tic_vec = finance_tickers().unwrap();
    let data: Vec<polars::frame::row::Row> = GlobalDailyIndex::read_gdi_batch(
        conn.clone(),
        "tic".to_string(),
        tic_vec,
        (
            NaiveDate::from_ymd_opt(2020, 1, 1).unwrap(),
            NaiveDate::from_ymd_opt(2025, 10, 2).unwrap(),
        ),
    )
    .await
    .unwrap();
    println!("Time Elapsed 3: {:?}", time.elapsed());
    let schema = Schema::from_iter([
        Field::new("tic".into(), DataType::String),
        Field::new("datadate".into(), DataType::Date),
        Field::new("gvkeyx".into(), DataType::String),
        Field::new("conm".into(), DataType::String), // allow nulls
        Field::new("indextype".into(), DataType::String),
        Field::new("indexid".into(), DataType::String),
        Field::new("indexcat".into(), DataType::String), // allow nulls
        Field::new("idxiddesc".into(), DataType::String), // allow nulls
        Field::new("dvpsxd".into(), DataType::Float64),
        Field::new("newnum".into(), DataType::Int32),
        Field::new("oldnum".into(), DataType::Int32),
        Field::new("prccd".into(), DataType::Float64),
        Field::new("prccddiv".into(), DataType::Float64),
        Field::new("prccddivn".into(), DataType::Float64),
        Field::new("prchd".into(), DataType::Float64),
        Field::new("prcld".into(), DataType::Float64),
    ]);
    println!("Time Elapsed 4: {:?}", time.elapsed());
    let mut df = DataFrame::from_rows_and_schema(&data, &schema).unwrap();
    df = df.sort(["tic", "datadate"], Default::default()).unwrap();
    println!("DataFrame {:?}", df.head(Some(30)));
    println!("DataFrame Shape {:?}", df.shape());
    println!("Time Elapsed 5: {:?}", time.elapsed());
    let file = File::create("output.csv").unwrap();
    CsvWriter::new(file)
        .include_header(true)
        .finish(&mut df)
        .unwrap();
}

const WORLD_RETS_PARQUET_PATH: &str =
    "../data/raw_files/country_indexes/country_returns_wide.parquet";

#[tokio::test(flavor = "multi_thread", worker_threads = 14)]
async fn duck_ingest_world_indices_from_parquet() {
    let time = std::time::Instant::now();
    let conn = words_db::instantiatedb::duckdbinst::start_duck_db("4GB", 14)
        .await
        .expect("duckdb in-memory should start");
    let conn = Arc::new(conn);
    println!("[WORLD] Time Elapsed 1: {:?}", time.elapsed());
    let dbtype = DbType::GlobalRets;
    let _ = dbtype.ingest(conn.clone(), "4GB")
        .await
        .expect("duck bootstrap with parquet should work");
    let mut stmt = conn.prepare("DESCRIBE  global_sec_indexes_daily").unwrap();
    let mut rows = stmt.query([]).unwrap();
    while let Some(row) = rows.next().unwrap() {
        let name: String = row.get(0).unwrap();
        let dtype: String = row.get(1).unwrap();
        println!("[WORLD] {}: {}", name, dtype);
    }
    println!("[WORLD] Time Elapsed 2: {:?}", time.elapsed());

    let data: Vec<polars::frame::row::Row> = GlobalRets::read_range(
        conn.clone(),
        (
            NaiveDate::from_ymd_opt(2020, 1, 1).unwrap(),
            NaiveDate::from_ymd_opt(2025, 10, 2).unwrap(),
        ),
    )
    .await
    .unwrap();
    println!("[WORLD] fetched rows: {}", data.len());
    println!("[WORLD] Time Elapsed 3: {:?}", time.elapsed());
    let schema = GlobalRets::polars_schema();
    let mut df = DataFrame::from_rows_and_schema(&data, &schema).unwrap();
    df = df.sort(["date"], Default::default()).unwrap();
    let var_names: Vec<String> = df
        .get_column_names_owned()
        .into_iter()
        .map(|s| s.as_str().to_string())
        .filter(|s| s != "date")
        .collect();

    // Build a flat list of expressions for all pairwise correlations
    let mut exprs: Vec<Expr> = Vec::with_capacity(var_names.len() * var_names.len());
    for a in &var_names {
        for b in &var_names {
            exprs.push(pearson_corr(col(a.as_str()), col(b.as_str())).alias(format!("pearson{}-{}", a, b)));
        }
    }
    let df_one_row: DataFrame = df.lazy().select(exprs).collect().unwrap();
    // Turn the 1-row wide DF into an N x N correlation matrix DF
    let n = var_names.len();
    let mut corr_cols: Vec<Series> = Vec::with_capacity(n);
    for col_j in 0..n {
        let header = &var_names[col_j];
        let mut col_vals: Vec<f64> = Vec::with_capacity(n);
        for row_i in 0..n {
            let cname = format!("pearson{}-{}", var_names[row_i], var_names[col_j]);
            let v = df_one_row
                .column(&cname)
                .ok()
                .and_then(|s| s.f64().ok())
                .and_then(|ca| ca.get(0))
                .unwrap_or(f64::NAN);
            col_vals.push(v);
        }
        corr_cols.push(Series::new(header.as_str().into(), col_vals));
    }
    let corr_cols: Vec<Column> = corr_cols.into_iter().map(Column::from).collect();
    let mut corr_df = DataFrame::new(corr_cols).unwrap();
    println!("[WORLD] Corr shape: {:?}", corr_df.shape());
    println!("[WORLD] Corr (head): {:?}", corr_df.head(Some(10)));
    let file = File::create("world_corr_matrix.csv").unwrap();
    CsvWriter::new(file)
        .include_header(true)
        .finish(&mut corr_df)
        .unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 14)]
async fn duck_persist_and_reopen_from_file() {
    // Start in-memory DB and ingest from Parquet
    let conn = words_db::instantiatedb::duckdbinst::start_duck_db("4GB", 14).await.expect("duckdb in-memory start");
    let conn = Arc::new(conn);
    let rows = DbType::GlobalDailyIndex.ingest(conn.clone(), PARQUET_PATH).await.expect("ingest from parquet");
    assert!(rows > 0, "expected >0 rows ingested, got {}", rows);
    // Persist to a DuckDB file at the repository root
    let root = env!("CARGO_MANIFEST_DIR");
    let db_path = std::path::Path::new(root).join("duck_roundtrip.db").to_string_lossy().to_string();
    persist_in_memory_to_file(&conn, &db_path).expect("persist in-memory db to file");
    // Reopen and validate content
    let reopened = open_duck_db_from_file(&db_path, "4GB", 14).await.expect("open persisted duckdb file");
    let total: i64 = reopened.query_row("SELECT count(*) FROM global_indexes_daily", [],|r| r.get(0),).expect("count rows");
    assert!(total as usize >= rows, "reopened row count should be >= ingested");
    // Keep the DB file in the repository root.
}

// Ingest US market indexes parquet via DbType::UsMarket and assemble a DataFrame using ToPolars::df_from_rows
#[tokio::test(flavor = "multi_thread", worker_threads = 14)]
async fn duck_ingest_us_market_indexes_from_parquet() {
    // Expand ~ in path and guard if file missing (keep CI deterministic)
    let raw_path = "~/Dropbox/Desktop/tesero-sol/software_development/trading/data/raw_files/parqueut/crsp_ciz_sample/market_index/market_indexes_daily.parquet";
    let expanded = if raw_path.starts_with("~/") {
        if let Ok(home) = std::env::var("HOME") {
            format!("{}/{}", home, &raw_path[2..])
        } else {
            raw_path.to_string()
        }
    } else {
        raw_path.to_string()
    };
    if !std::path::Path::new(&expanded).exists() {
        eprintln!("US market parquet not found at {} — skipping test", expanded);
        return;
    }

    let conn = words_db::instantiatedb::duckdbinst::start_duck_db("4GB", 14)
        .await
        .expect("duckdb in-memory should start");
    let conn = Arc::new(conn);

    let rows = DbType::UsMarket
        .ingest(conn.clone(), &expanded)
        .await
        .expect("ingest us market parquet");
    assert!(rows > 0, "expected >0 rows ingested");

    // Read a small date range to exercise the path and build a DataFrame
    let start = NaiveDate::from_ymd_opt(2020, 1, 1).unwrap();
    let end = NaiveDate::from_ymd_opt(2020, 12, 31).unwrap();
    let data_rows = UsMarketIndex::read_range(conn.clone(), (start, end))
        .await
        .expect("read_range for us market");
    // Use ToPolars::df_from_rows
    let df = <UsMarketIndex as words_db::finance_data_structs::ToPolars>::df_from_rows(&data_rows)
        .expect("build polars DataFrame");
    println!("[US] DF shape: {:?}", df.shape());
    assert!(df.height() <= rows, "subset rows should be <= ingested rows");
}
