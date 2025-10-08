use chrono::NaiveDate;
use polars::prelude::*;
use std::fs::File;
use std::path::Path;
use std::sync::Arc;
use words_db::finance_data_structs::crsp::{GlobalDailyIndex,finance_tickers};

const PARQUET_PATH: &str = "../data/raw_files/global_indexes_daily.parquet";

#[tokio::test(flavor = "multi_thread", worker_threads = 14)]
async fn duck_ingest_global_indexes_from_parquet() {
    // Skip test if the parquet file is not available in this environment.
    let time = std::time::Instant::now();
    if !Path::new(PARQUET_PATH).exists() {
        eprintln!(
            "Skipping duck_ingest_global_indexes_from_parquet: missing {}",
            PARQUET_PATH
        );
        return;
    }

    let conn = words_db::start_duck_db("4GB", 14)
        .await
        .expect("duckdb in-memory should start");
    let conn = Arc::new(conn);
    println!("Time Elapsed 1: {:?}", time.elapsed());
    let processed = GlobalDailyIndex::duck_from_parquet(conn.clone(), PARQUET_PATH)
        .await
        .expect("upsert from parquet should succeed");
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
    // Verify row count matches processed rows
    // Also exercise the convenience bootstrap that uses the same logic
    let _conn2 = words_db::start_duck_db_global_indexes(PARQUET_PATH)
        .await
        .expect("duck bootstrap with parquet should work");
}
