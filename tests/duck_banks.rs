use chrono::NaiveDate;
use polars::prelude::*;
use std::fs;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};
use std::time::Instant;
use words_db::finance_data_structs::bank_regulatory::{
    BhckLegacy1, BhckOther1, BhckSeries1, BhckSeries2,
};
use words_db::finance_data_structs::ToPolars;
use words_db::instantiatedb::duckdbinst::open_duck_db_from_file;
use words_db::instantiatedb::duckdbinst::{
    persist_selected_tables_to_file, start_duck_db, DbType,
};
use words_db::createdatasets::usbanks::{
    dly_securities_ds_from_db_files, fundamental_ds_from_db_files, mthly_securities_ds_from_db_files,
};

const BANK_REG_BASE: &str = "../data/raw_files/parquet/bank_regulatory/holding_company_financials";
const BHCK_CRSP_LINK_FILE: &str =
    "../data/raw_files/parquet/bank_regulatory/bhck_crsp_link/bhck_crsp_link_file.parquet";
const BHCK_OTHER: &str =
    "bhck_other/(rssd9999__gte=2000-01-01&rssd9999__lte=2010-01-01)_bhck_other.parquet";
const BHCK_SERIES1: &str =
    "bhck_series1/(rssd9999__gte=2000-01-01&rssd9999__lte=2010-01-01)_bhck_series1.parquet";
const BHCK_SERIES2: &str =
    "bhck_series2/(rssd9999__gte=2000-01-01&rssd9999__lte=2010-01-01)_bhck_series2.parquet";
const BCHK_LEGACY1: &str = "bhck_legacy/kuuy1wbug5dlu3zz.parquet";
const US_CRSP_DLY_FILE: &str = "../data/raw_files/parquet/crsp/us_bhc/daily_usbank_crsp.parquet";
const US_CRSP_MTHLY_FILE: &str =
    "../data/raw_files/parquet/crsp/us_bhc/monthly_usbank_crsp.parquet";

fn yyyymmdd_to_date(yyyymmdd: i64) -> NaiveDate {
    let year = (yyyymmdd / 10_000) as i32;
    let month = ((yyyymmdd / 100) % 100) as u32;
    let day = (yyyymmdd % 100) as u32;
    NaiveDate::from_ymd_opt(year, month, day)
        .unwrap_or_else(|| panic!("invalid yyyymmdd: {yyyymmdd}"))
}

fn bank_cases() -> [(DbType, &'static str, &'static str); 3] {
    [
        /*(
            DbType::BhckLegacy1,
            BCHK_LEGACY1,
            "bhck_legacy",
        ),*/
        (DbType::BhckOther1, BHCK_OTHER, "bhck_other"),
        (DbType::BhckSeries1, BHCK_SERIES1, "bhck_series1"),
        (DbType::BhckSeries2, BHCK_SERIES2, "bhck_series2"),
    ]
}

fn bank_crsp_cases() -> [(DbType, &'static str, &'static str); 3] {
    [
        (DbType::BhckCrspLink, BHCK_CRSP_LINK_FILE, "bhck_crsp_link"),
        (DbType::UsCrspDly, US_CRSP_DLY_FILE, "us_crsp_dly"),
        (DbType::UsCrspMthly, US_CRSP_MTHLY_FILE, "us_crsp_mthly"),
    ]
}

fn bank_duckdb_artifacts_dir(test_name: &str) -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("target")
        .join("test_artifacts")
        .join("duck_banks")
        .join(test_name)
}

async fn ingest_one_table_to_duckdb_file(
    dbtype: DbType,
    parquet_path: &Path,
    table: &str,
    out_file: &Path,
) {
    let conn = start_duck_db("35GB", 14)
        .await
        .expect("duckdb in-memory should start");
    let conn = Arc::new(Mutex::new(conn));

    let rows = dbtype
        .ingest(conn.clone(), &parquet_path.to_string_lossy())
        .await
        .unwrap_or_else(|e| panic!("ingest {table} parquet should work: {e:?}"));
    assert!(rows > 0, "expected >0 rows ingested for {table}");

    let out_str = out_file
        .to_str()
        .unwrap_or_else(|| panic!("non-utf8 output path: {}", out_file.display()));
    let guard = conn.lock().unwrap();
    persist_selected_tables_to_file(&*guard, out_str, vec![table.to_string()])
        .unwrap_or_else(|e| panic!("persist_selected_tables_to_file({table}) should work: {e:?}"));

    assert!(
        out_file.exists(),
        "expected output file {}",
        out_file.display()
    );
    let size = fs::metadata(out_file)
        .unwrap_or_else(|e| panic!("metadata({}) should work: {e}", out_file.display()))
        .len();
    assert!(size > 0, "expected non-empty duckdb file for {table}");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn duck_ingest_bank_regulatory_from_parquet() {
    let base = Path::new(env!("CARGO_MANIFEST_DIR")).join(BANK_REG_BASE);
    let cases = bank_cases();

    for (_, rel_path, _) in cases.iter() {
        let parquet_path = base.join(rel_path);
        if !parquet_path.exists() {
            eprintln!(
                "Bank regulatory parquet not found at {} — skipping test",
                parquet_path.display()
            );
            return;
        }
    }

    let conn = start_duck_db("35GB", 14)
        .await
        .expect("duckdb in-memory should start");
    let conn = Arc::new(Mutex::new(conn));

    for (dbtype, rel_path, table) in cases {
        let parquet_path = base.join(rel_path);
        let rows = dbtype
            .ingest(conn.clone(), &parquet_path.to_string_lossy())
            .await
            .unwrap_or_else(|e| panic!("ingest {table} parquet should work: {e:?}"));
        assert!(rows > 0, "expected >0 rows ingested for {table}");

        let count: i64 = {
            let guard = conn.lock().unwrap();
            let sql = format!("SELECT count(*) FROM {}", table);
            guard.query_row(&sql, [], |r| r.get(0)).unwrap()
        };
        assert!(count > 0, "expected >0 rows in DuckDB table {table}");

        let date_for_range: NaiveDate = match dbtype {
            DbType::BhckLegacy1 => {
                let yyyymmdd: i64 = {
                    let guard = conn.lock().unwrap();
                    let sql = format!(
                        r#"SELECT CAST(RSSD9999 AS BIGINT) AS d
FROM {table}
GROUP BY d
ORDER BY count(*) ASC, d ASC
LIMIT 1"#
                    );
                    guard.query_row(&sql, [], |r| r.get(0)).unwrap()
                };
                yyyymmdd_to_date(yyyymmdd)
            }
            DbType::BhckOther1 | DbType::BhckSeries1 | DbType::BhckSeries2 => {
                let date_str: String = {
                    let guard = conn.lock().unwrap();
                    let sql = format!(
                        r#"SELECT CAST(rssd9999 AS DATE) AS d
FROM {table}
GROUP BY d
ORDER BY count(*) ASC, d ASC
LIMIT 1"#
                    );
                    guard.query_row(&sql, [], |r| r.get(0)).unwrap()
                };
                NaiveDate::parse_from_str(&date_str, "%Y-%m-%d")
                    .unwrap_or_else(|e| panic!("failed to parse rssd9999 date '{date_str}': {e}"))
            }
            _ => unreachable!("bank_regulatory test only covers Bhck* tables"),
        };

        let range = (date_for_range, date_for_range);
        match dbtype {
            DbType::BhckLegacy1 => {
                let rows = BhckLegacy1::read_range(conn.clone(), range)
                    .await
                    .expect("BhckLegacy1::read_range");
                let df = <BhckLegacy1 as ToPolars>::df_from_rows(&rows)
                    .expect("BhckLegacy1 df_from_rows");
                assert!(df.height() > 0, "expected >0 rows in BhckLegacy1 df");
            }
            DbType::BhckOther1 => {
                let rows = BhckOther1::read_range(conn.clone(), range)
                    .await
                    .expect("BhckOther1::read_range");
                let df =
                    <BhckOther1 as ToPolars>::df_from_rows(&rows).expect("BhckOther1 df_from_rows");
                assert!(df.height() > 0, "expected >0 rows in BhckOther1 df");
            }
            DbType::BhckSeries1 => {
                let rows = BhckSeries1::read_range(conn.clone(), range)
                    .await
                    .expect("BhckSeries1::read_range");
                let df = <BhckSeries1 as ToPolars>::df_from_rows(&rows)
                    .expect("BhckSeries1 df_from_rows");
                assert!(df.height() > 0, "expected >0 rows in BhckSeries1 df");
            }
            DbType::BhckSeries2 => {
                let rows = BhckSeries2::read_range(conn.clone(), range)
                    .await
                    .expect("BhckSeries2::read_range");
                let df = <BhckSeries2 as ToPolars>::df_from_rows(&rows)
                    .expect("BhckSeries2 df_from_rows");
                assert!(df.height() > 0, "expected >0 rows in BhckSeries2 df");
            }
            _ => unreachable!("bank_regulatory test only covers Bhck* tables"),
        }
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn duck_bank_regulatory_read_range_to_parquet_then_scan() {
    let now = Instant::now();
    let base = Path::new(env!("CARGO_MANIFEST_DIR")).join(BANK_REG_BASE);
    let cases = bank_cases();

    for (_, rel_path, _) in cases.iter() {
        let parquet_path = base.join(rel_path);
        if !parquet_path.exists() {
            eprintln!(
                "Bank regulatory parquet not found at {} — skipping test",
                parquet_path.display()
            );
            return;
        }
    }

    let conn = start_duck_db("35GB", 14)
        .await
        .expect("duckdb in-memory should start");
    let conn = Arc::new(Mutex::new(conn));
    println!("{:?}", now.elapsed());
    for (dbtype, rel_path, table) in cases {
        let parquet_path = base.join(rel_path);
        let rows = dbtype
            .ingest(conn.clone(), &parquet_path.to_string_lossy())
            .await
            .unwrap_or_else(|e| panic!("ingest {table} parquet should work: {e:?}"));

        assert!(rows > 0, "expected >0 rows ingested for {table}");

        let date_for_range: NaiveDate = match dbtype {
            DbType::BhckLegacy1 => {
                let yyyymmdd: i64 = {
                    let guard = conn.lock().unwrap();
                    let sql = format!(
                        r#"SELECT CAST(RSSD9999 AS BIGINT) AS d
FROM {table}
GROUP BY d
ORDER BY count(*) ASC, d ASC
LIMIT 1"#
                    );
                    guard.query_row(&sql, [], |r| r.get(0)).unwrap()
                };
                yyyymmdd_to_date(yyyymmdd)
            }
            DbType::BhckOther1 | DbType::BhckSeries1 | DbType::BhckSeries2 => {
                let date_str: String = {
                    let guard = conn.lock().unwrap();
                    let sql = format!(
                        r#"SELECT CAST(rssd9999 AS DATE) AS d
FROM {table}
GROUP BY d
ORDER BY count(*) ASC, d ASC
LIMIT 1"#
                    );
                    guard.query_row(&sql, [], |r| r.get(0)).unwrap()
                };
                NaiveDate::parse_from_str(&date_str, "%Y-%m-%d")
                    .unwrap_or_else(|e| panic!("failed to parse rssd9999 date '{date_str}': {e}"))
            }
            _ => unreachable!("bank_regulatory test only covers Bhck* tables"),
        };

        let range = (date_for_range, date_for_range);
        let out_path: PathBuf =
            std::env::temp_dir().join(format!("words_db_{table}_{}_range.parquet", date_for_range));

        match dbtype {
            DbType::BhckLegacy1 => {
                let out = BhckLegacy1::read_range_to_parquet(conn.clone(), range, &out_path)
                    .await
                    .expect("BhckLegacy1::read_range_to_parquet");
                let args = ScanArgsParquet {
                    schema: Some(Arc::new(<BhckLegacy1 as ToPolars>::schema())),
                    ..Default::default()
                };
                println!("{:?}", now.elapsed());
                let height = tokio::task::spawn_blocking(move || {
                    let df = <BhckLegacy1 as ToPolars>::df_from_parquet_scan_with_args(out, args)
                        .expect("BhckLegacy1 df_from_parquet_scan_with_args");
                    df.height()
                })
                .await
                .expect("spawn_blocking join");
                assert!(
                    height > 0,
                    "expected >0 rows from scanned BhckLegacy1 parquet"
                );
            }
            DbType::BhckOther1 => {
                let out = BhckOther1::read_range_to_parquet(conn.clone(), range, &out_path)
                    .await
                    .expect("BhckOther1::read_range_to_parquet");
                let args = ScanArgsParquet {
                    schema: Some(Arc::new(<BhckOther1 as ToPolars>::schema())),
                    ..Default::default()
                };
                let height = tokio::task::spawn_blocking(move || {
                    let df = <BhckOther1 as ToPolars>::df_from_parquet_scan_with_args(out, args)
                        .expect("BhckOther1 df_from_parquet_scan_with_args");
                    df.height()
                })
                .await
                .expect("spawn_blocking join");
                assert!(
                    height > 0,
                    "expected >0 rows from scanned BhckOther1 parquet"
                );
            }
            DbType::BhckSeries1 => {
                let out = BhckSeries1::read_range_to_parquet(conn.clone(), range, &out_path)
                    .await
                    .expect("BhckSeries1::read_range_to_parquet");
                let args = ScanArgsParquet {
                    schema: Some(Arc::new(<BhckSeries1 as ToPolars>::schema())),
                    ..Default::default()
                };
                let height = tokio::task::spawn_blocking(move || {
                    let df = <BhckSeries1 as ToPolars>::df_from_parquet_scan_with_args(out, args)
                        .expect("BhckSeries1 df_from_parquet_scan_with_args");
                    df.height()
                })
                .await
                .expect("spawn_blocking join");
                assert!(
                    height > 0,
                    "expected >0 rows from scanned BhckSeries1 parquet"
                );
            }
            DbType::BhckSeries2 => {
                let out = BhckSeries2::read_range_to_parquet(conn.clone(), range, &out_path)
                    .await
                    .expect("BhckSeries2::read_range_to_parquet");
                let args = ScanArgsParquet {
                    schema: Some(Arc::new(<BhckSeries2 as ToPolars>::schema())),
                    ..Default::default()
                };
                let height = tokio::task::spawn_blocking(move || {
                    let df = <BhckSeries2 as ToPolars>::df_from_parquet_scan_with_args(out, args)
                        .expect("BhckSeries2 df_from_parquet_scan_with_args");
                    df.height()
                })
                .await
                .expect("spawn_blocking join");
                assert!(
                    height > 0,
                    "expected >0 rows from scanned BhckSeries2 parquet"
                );
            }
            _ => unreachable!("bank_regulatory test only covers Bhck* tables"),
        }
        println!("{:?}", now.elapsed());
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn duck_bank_regulatory_dump_each_table_to_duckdb_files() {
    let base = Path::new(env!("CARGO_MANIFEST_DIR")).join(BANK_REG_BASE);
    let cases = bank_cases();

    for (_, rel_path, _) in cases.iter() {
        let parquet_path = base.join(rel_path);
        if !parquet_path.exists() {
            eprintln!(
                "Bank regulatory parquet not found at {} — skipping test",
                parquet_path.display()
            );
            return;
        }
    }

    let out_dir = bank_duckdb_artifacts_dir("dump_each_table");
    fs::create_dir_all(&out_dir).expect("create artifacts dir");

    for (dbtype, rel_path, table) in cases {
        let parquet_path = base.join(rel_path);
        let out_file = out_dir.join(format!("{table}.duckdb"));
        ingest_one_table_to_duckdb_file(dbtype, &parquet_path, table, &out_file).await;
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn duck_bank_crsp_dump_each_table_to_duckdb_files() {
    let base = Path::new(env!("CARGO_MANIFEST_DIR"));
    let cases = bank_crsp_cases();

    for (_, rel_path, _) in cases.iter() {
        let parquet_path = base.join(rel_path);
        if !parquet_path.exists() {
            eprintln!(
                "Bank/CRSP parquet not found at {} — skipping test",
                parquet_path.display()
            );
            return;
        }
    }

    let out_dir = bank_duckdb_artifacts_dir("dump_bank_crsp_each_table");
    fs::create_dir_all(&out_dir).expect("create artifacts dir");

    for (dbtype, rel_path, table) in cases {
        let parquet_path = base.join(rel_path);
        let out_file = out_dir.join(format!("{table}.duckdb"));
        ingest_one_table_to_duckdb_file(dbtype, &parquet_path, table, &out_file).await;
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn duck_createdatasets_usbanks_builds_from_duckdb_artifacts() {
    // Prereq: run `duck_bank_regulatory_dump_each_table_to_duckdb_files` first.
    let bank_out_dir = bank_duckdb_artifacts_dir("dump_each_table");

    let bhck_other = bank_out_dir.join("bhck_other.duckdb");
    let bhck_series1 = bank_out_dir.join("bhck_series1.duckdb");
    let bhck_series2 = bank_out_dir.join("bhck_series2.duckdb");

    for p in [&bhck_other, &bhck_series1, &bhck_series2] {
        if !p.exists() {
            eprintln!(
                "DuckDB file not found at {} — skipping test (run dump test first)",
                p.display()
            );
            return;
        }
    }

    let out_dir = bank_duckdb_artifacts_dir("createdatasets_usbanks");
    fs::create_dir_all(&out_dir).expect("create artifacts dir");

    // Build bank fundamentals from attached bank regulatory tables.
    let fundamentals_db = fundamental_ds_from_db_files(
        "35GB",
        14,
        bhck_other
            .to_str()
            .unwrap_or_else(|| panic!("non-utf8 path: {}", bhck_other.display())),
        bhck_series1
            .to_str()
            .unwrap_or_else(|| panic!("non-utf8 path: {}", bhck_series1.display())),
        bhck_series2
            .to_str()
            .unwrap_or_else(|| panic!("non-utf8 path: {}", bhck_series2.display())),
        out_dir.clone(),
    )
    .await
    .expect("fundamental_ds_from_db_files should succeed");
    assert!(
        fundamentals_db.exists(),
        "expected output db file {}",
        fundamentals_db.display()
    );

    let conn = open_duck_db_from_file(
        fundamentals_db
            .to_str()
            .unwrap_or_else(|| panic!("non-utf8 db path: {}", fundamentals_db.display())),
        "35GB",
        14,
    )
    .await
    .expect("open output db");
    let row_count: i64 = conn
        .query_row("SELECT count(*) FROM bank_fundamentals", [], |r| r.get(0))
        .expect("count bank_fundamentals");
    assert!(row_count > 0, "expected >0 rows in bank_fundamentals");

    // Optional: build securities datasets if those DuckDB artifacts exist.
    let bank_crsp_dir = bank_duckdb_artifacts_dir("dump_bank_crsp_each_table");
    let bhck_crsp_link = bank_crsp_dir.join("bhck_crsp_link.duckdb");
    let us_crsp_mthly = bank_crsp_dir.join("us_crsp_mthly.duckdb");
    let us_crsp_dly = bank_crsp_dir.join("us_crsp_dly.duckdb");

    if bhck_crsp_link.exists() && us_crsp_mthly.exists() {
        let out = mthly_securities_ds_from_db_files(
            "35GB",
            14,
            bhck_crsp_link
                .to_str()
                .unwrap_or_else(|| panic!("non-utf8 path: {}", bhck_crsp_link.display())),
            us_crsp_mthly
                .to_str()
                .unwrap_or_else(|| panic!("non-utf8 path: {}", us_crsp_mthly.display())),
            out_dir.clone(),
        )
        .await
        .expect("mthly_securities_ds_from_db_files");
        assert!(out.exists(), "expected output db file {}", out.display());
    }

    if bhck_crsp_link.exists() && us_crsp_dly.exists() {
        let out = dly_securities_ds_from_db_files(
            "35GB",
            14,
            bhck_crsp_link
                .to_str()
                .unwrap_or_else(|| panic!("non-utf8 path: {}", bhck_crsp_link.display())),
            us_crsp_dly
                .to_str()
                .unwrap_or_else(|| panic!("non-utf8 path: {}", us_crsp_dly.display())),
            out_dir,
        )
        .await
        .expect("dly_securities_ds_from_db_files");
        assert!(out.exists(), "expected output db file {}", out.display());
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn duck_bank_regulatory_read_each_table_from_duckdb_files() {
    let cases = bank_cases();
    let out_dir = bank_duckdb_artifacts_dir("dump_each_table");

    for (_, _, table) in cases.iter() {
        let out_file = out_dir.join(format!("{table}.duckdb"));
        if !out_file.exists() {
            eprintln!(
                "DuckDB file not found at {} — skipping test (run dump test first)",
                out_file.display()
            );
            return;
        }
    }

    for (_, _, table) in cases {
        let out_file = out_dir.join(format!("{table}.duckdb"));
        let db_path = out_file
            .to_str()
            .unwrap_or_else(|| panic!("non-utf8 db path: {}", out_file.display()));
        let open_start = Instant::now();
        let conn = open_duck_db_from_file(db_path, "35GB", 14)
            .await
            .unwrap_or_else(|e| panic!("open_duck_db_from_file({db_path}) should work: {e:?}"));
        let open_time = open_start.elapsed();
        let conn = Arc::new(Mutex::new(conn));

        let table = table.to_string();
        let table_for_load = table.clone();
        let (load_time, count): (std::time::Duration, i64) =
            tokio::task::spawn_blocking(move || {
                let load_start = Instant::now();
                let guard = conn.lock().expect("duckdb connection mutex poisoned");

                // Copy the whole DB file into an in-memory attached database to benchmark
                // full-file load time. Since each `.duckdb` file contains a single table, loading
                // that table into an attached `:memory:` database is sufficient.
                let _ = guard.execute_batch("DETACH mem;");
                guard
                    .execute_batch("ATTACH ':memory:' AS mem;")
                    .expect("ATTACH ':memory:' AS mem");

                let table_ident = format!("\"{}\"", table_for_load.replace('\"', "\"\""));
                guard
                    .execute_batch(&format!(
                        "CREATE OR REPLACE TABLE mem.{t} AS SELECT * FROM main.{t};",
                        t = table_ident
                    ))
                    .expect("CTAS main -> mem");

                let sql = format!("SELECT count(*) FROM mem.{table_ident}");
                let count: i64 = guard
                    .query_row(&sql, [], |r| r.get(0))
                    .expect("count query");
                guard.execute_batch("DETACH mem;").expect("DETACH mem");

                (load_start.elapsed(), count)
            })
            .await
            .expect("spawn_blocking join");

        println!(
            "[duckdb file->mem] table={table} file={} open_time={open_time:?} load_time={load_time:?} rows={count}",
            out_file.display()
        );
        assert!(
            count > 0,
            "expected >0 rows in {table} from {}",
            out_file.display()
        );
    }
}
