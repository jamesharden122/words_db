use chrono::NaiveDate;
use std::path::Path;
use std::sync::{Arc, Mutex};
use words_db::finance_data_structs::global_fundamentals_compustat::GlobalFundQtrly;
use words_db::finance_data_structs::{DuckCrudModel, ToPolars};
use words_db::instantiatedb::duckdbinst::{start_duck_db, DbType};

#[tokio::test(flavor = "multi_thread", worker_threads = 14)]
async fn duck_ingest_compustat_global_fundq_from_parquet() {
    // Keep CI deterministic: skip if local parquet is missing.
    let raw_path = "~/Dropbox/Desktop/tesero-sol/software_development/trading/data/raw_files/parquet/compustat/global_fundamentals/(datadate__gte=2025-06-01&datadate__lte=2025-11-30)_comp_g_fundq.parquet";
    let expanded = if raw_path.starts_with("~/") {
        if let Ok(home) = std::env::var("HOME") {
            format!("{}/{}", home, &raw_path[2..])
        } else {
            raw_path.to_string()
        }
    } else {
        raw_path.to_string()
    };
    if !Path::new(&expanded).exists() {
        eprintln!("compustat global fundamentals parquet not found at {expanded} — skipping test");
        return;
    }

    let conn = start_duck_db("4GB", 14)
        .await
        .expect("duckdb in-memory should start");
    let conn = Arc::new(Mutex::new(conn));

    let rows = DbType::GlobalFundQtrly
        .ingest(conn.clone(), &expanded)
        .await
        .expect("ingest compustat global fundq parquet");
    assert!(rows > 0, "expected >0 rows ingested");

    let table = <GlobalFundQtrly as DuckCrudModel>::table();
    let total: i64 = conn
        .lock()
        .unwrap()
        .query_row(&format!("SELECT count(*) FROM {table}"), [], |r| r.get(0))
        .expect("count rows in duck table");
    assert_eq!(
        total as usize, rows,
        "duck table row count should match parquet count"
    );

    // Exercise `read_range` + JSON→struct parsing.
    let min_date_str: String = conn
        .lock()
        .unwrap()
        .query_row(
            &format!("SELECT CAST(min(datadate) AS VARCHAR) FROM {table}"),
            [],
            |r| r.get(0),
        )
        .expect("min(datadate)");
    let min_date_token = min_date_str
        .split_whitespace()
        .next()
        .unwrap_or(min_date_str.as_str());
    let min_date = NaiveDate::parse_from_str(min_date_token, "%Y-%m-%d")
        .expect("parse min(datadate) as YYYY-MM-DD");

    let rows_range = GlobalFundQtrly::read_range(conn.clone(), (min_date, min_date))
        .await
        .expect("read_range should work");
    assert!(!rows_range.is_empty(), "expected non-empty date slice");
    let df = GlobalFundQtrly::df_from_rows(&rows_range).unwrap();
    let df = df
        .select([
            "conm", "gvkey", "fyr", "fqtr", "datadate", "datacqtr", "cstkq", "dlttq", "curcdq",
            "actq", "atq", "capxy", "ceqq", "invtq",
        ])
        .unwrap();
    println!("{:?}", df.head(Some(20)));
}
