use chrono::NaiveDate;
use polars::lazy::dsl::{pearson_corr, rolling_corr, spearman_rank_corr};
use polars::prelude::*;
use std::fs::File;
use std::path::Path;
use std::sync::{Arc, Mutex};
use words_db::finance_data_structs::crsp::{finance_tickers, GlobalDailyIndex};
use words_db::finance_data_structs::equity_factors::EquityFactorsMonthly;
use words_db::finance_data_structs::global_equities::GlobalEquitiesMonthly;
use words_db::finance_data_structs::usindexes::UsMarketIndex;
use words_db::finance_data_structs::wdi::WdiWide;
use words_db::finance_data_structs::world_indices::GlobalRets;
use words_db::instantiatedb::duckdbinst::{
    open_duck_db_from_file, persist_in_memory_to_file, DbType,
};
const PARQUET_PATH: &str = "../data/raw_files/global_indexes_daily.parquet";

#[tokio::test(flavor = "multi_thread", worker_threads = 14)]
async fn duck_ingest_global_indexes_from_parquet() {
    let parquet_path = Path::new(env!("CARGO_MANIFEST_DIR")).join(PARQUET_PATH);
    if !parquet_path.exists() {
        eprintln!(
            "Global indexes parquet not found at {} — skipping test",
            parquet_path.display()
        );
        return;
    }
    let parquet_path_str = parquet_path.to_string_lossy().to_string();

    let time = std::time::Instant::now();
    let conn = words_db::instantiatedb::duckdbinst::start_duck_db("4GB", 14)
        .await
        .expect("duckdb in-memory should start");
    let conn = Arc::new(Mutex::new(conn));
    println!("Time Elapsed 1: {:?}", time.elapsed());
    let dbtype = DbType::GlobalDailyIndex;
    _ = dbtype
        .ingest(conn.clone(), &parquet_path_str)
        .await
        .expect("duck bootstrap with parquet should work");
    {
        let guard = conn.lock().unwrap();
        let mut stmt = guard.prepare("DESCRIBE  global_indexes_daily").unwrap();
        let mut rows = stmt.query([]).unwrap();
        while let Some(row) = rows.next().unwrap() {
            let name: String = row.get(0).unwrap();
            let dtype: String = row.get(1).unwrap();
            println!("{}: {}", name, dtype);
        }
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
    let parquet_path = Path::new(env!("CARGO_MANIFEST_DIR")).join(WORLD_RETS_PARQUET_PATH);
    if !parquet_path.exists() {
        eprintln!(
            "World returns parquet not found at {} — skipping test",
            parquet_path.display()
        );
        return;
    }
    let parquet_path_str = parquet_path.to_string_lossy().to_string();

    let time = std::time::Instant::now();
    let conn = words_db::instantiatedb::duckdbinst::start_duck_db("4GB", 14)
        .await
        .expect("duckdb in-memory should start");
    let conn = Arc::new(Mutex::new(conn));
    println!("[WORLD] Time Elapsed 1: {:?}", time.elapsed());
    let dbtype = DbType::GlobalRets;
    let _ = dbtype
        .ingest(conn.clone(), &parquet_path_str)
        .await
        .expect("duck bootstrap with parquet should work");
    {
        let guard = conn.lock().unwrap();
        let mut stmt = guard.prepare("DESCRIBE  global_sec_indexes_daily").unwrap();
        let mut rows = stmt.query([]).unwrap();
        while let Some(row) = rows.next().unwrap() {
            let name: String = row.get(0).unwrap();
            let dtype: String = row.get(1).unwrap();
            println!("[WORLD] {}: {}", name, dtype);
        }
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
            exprs.push(
                pearson_corr(col(a.as_str()), col(b.as_str())).alias(format!("pearson{}-{}", a, b)),
            );
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
    let parquet_path = Path::new(env!("CARGO_MANIFEST_DIR")).join(PARQUET_PATH);
    if !parquet_path.exists() {
        eprintln!(
            "Global indexes parquet not found at {} — skipping test",
            parquet_path.display()
        );
        return;
    }
    let parquet_path_str = parquet_path.to_string_lossy().to_string();

    // Start in-memory DB and ingest from Parquet
    let conn = words_db::instantiatedb::duckdbinst::start_duck_db("4GB", 14)
        .await
        .expect("duckdb in-memory start");
    let conn = Arc::new(Mutex::new(conn));
    let rows = DbType::GlobalDailyIndex
        .ingest(conn.clone(), &parquet_path_str)
        .await
        .expect("ingest from parquet");
    assert!(rows > 0, "expected >0 rows ingested, got {}", rows);
    // Persist to a DuckDB file at the repository root
    let root = env!("CARGO_MANIFEST_DIR");
    let db_path = std::path::Path::new(root)
        .join("duck_roundtrip.db")
        .to_string_lossy()
        .to_string();
    persist_in_memory_to_file(&conn.lock().unwrap(), &db_path)
        .expect("persist in-memory db to file");
    // Reopen and validate content
    let reopened = open_duck_db_from_file(&db_path, "4GB", 14)
        .await
        .expect("open persisted duckdb file");
    let total: i64 = reopened
        .query_row("SELECT count(*) FROM global_indexes_daily", [], |r| {
            r.get(0)
        })
        .expect("count rows");
    assert!(
        total as usize >= rows,
        "reopened row count should be >= ingested"
    );
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
        eprintln!(
            "US market parquet not found at {} — skipping test",
            expanded
        );
        return;
    }

    let conn = words_db::instantiatedb::duckdbinst::start_duck_db("4GB", 14)
        .await
        .expect("duckdb in-memory should start");
    let conn = Arc::new(Mutex::new(conn));

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
    assert!(
        df.height() <= rows,
        "subset rows should be <= ingested rows"
    );
}

// Ingest WDI (World Development Indicators) wide parquet and query a subset via WdiWide
#[tokio::test(flavor = "multi_thread", worker_threads = 14)]
async fn duck_ingest_wdi_from_parquet() {
    // Path resolution: prefer env var, otherwise skip test
    let raw_path = "~/Dropbox/Desktop/tesero-sol/software_development/trading/data/raw_files/parquet/macro/world_indicators/tidy_wdi.parquet";
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
        eprintln!("WDI parquet not found at {} — skipping test", expanded);
        return;
    }

    let conn = words_db::instantiatedb::duckdbinst::start_duck_db("4GB", 14)
        .await
        .expect("duckdb in-memory should start");
    let conn = Arc::new(Mutex::new(conn));

    // Ingest into default table `wdi_wide`
    let rows = DbType::WdiWide
        .ingest(conn.clone(), &expanded)
        .await
        .expect("ingest wdi parquet");
    assert!(rows > 0, "expected >0 WDI rows ingested");

    // Fetch a classic indicator for the requested country set
    let countries = [
        "USA", "AUS", "AUT", "CHL", "CHN", "COL", "DNK", "FRA", "DEU", "HUN", "IDN", "ITA", "JPN",
        "MEX", "NLD", "NZL", "PRT", "SGP", "ZAF", "KOR", "SWE", "TUR", "GBR", "HKG",
    ];

    let ind_code_arr = [
        "NY.GDP.MKTP.CD",
        "NY.GDP.MKTP.KD.ZG",
        "SL.UEM.TOTL.ZS",
        "NY.ADJ.NNTY.KD.ZG",
        "NY.GSR.NFCY.CN",
        "NE.CON.PRVT.PC.KD",
        "SL.EMP.WORK.ZS",
        "SM.POP.NETM",
        "EG.ELC.RNEW.ZS",
        "FR.INR.RINR",
        "FB.BNK.CAPA.ZS",
        "BX.KLT.DINV.CD.WD",
        "BX.KLT.DINV.WD.GD.ZS",
        "CM.MKT.LCAP.GD.ZS",
        "CM.MKT.LCAP.CD",
    ];
    for code in ind_code_arr.iter() {
        let data_rows = WdiWide::read_indicator_countries(
            conn.clone(),
            "wdi_wide",
            &code,
            (2019, 2025),
            &countries,
        )
        .await
        .expect("read_indicator_countries for WDI");
        // 2. Define Base Fields
        let mut schema_fields = vec![
            Field::new("indicator_name".into(), DataType::String),
            Field::new("indicator_code".into(), DataType::String),
            Field::new("year".into(), DataType::Int32),
        ];
        // 3. Dynamically add Country Fields
        // This iterates over your array and creates a Float64 field for each one
        for country_code in countries.iter() {
            schema_fields.push(Field::new((*country_code).into(), DataType::Float64));
        }
        // 4. Create the Schema
        let schema = Schema::from_iter(schema_fields);
        let df = DataFrame::from_rows_and_schema(&data_rows, &schema).unwrap();
        println!("{:?}", df.head(Some(20)));
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 14)]
async fn duck_ingest_monthly_from_parquet() {
    // Base dir and files provided by user
    let base_dir =
        "~/Dropbox/Desktop/tesero-sol/software_development/trading/data/raw_files/parquet/";
    let factors_rel = "factors/global/monthly/2020_2025_parquet.parquet";
    let equities_rel = "compustat/global_securities/monthly/(datadate__gte=2020-01-01&datadate__lte=2025-01-01)_comp_global_monthly.parquet";

    // Expand ~ and join paths
    let expand_home = |p: &str| -> String {
        if p.starts_with("~/") {
            if let Ok(home) = std::env::var("HOME") {
                format!("{}/{}", home, &p[2..])
            } else {
                p.to_string()
            }
        } else {
            p.to_string()
        }
    };
    let base = expand_home(base_dir);
    let factors_path = std::path::Path::new(&base).join(factors_rel);
    let equities_path = std::path::Path::new(&base).join(equities_rel);

    // Skip test if files not present (keeps CI deterministic)
    if !factors_path.exists() {
        eprintln!(
            "Factors parquet not found at {} — skipping test",
            factors_path.to_string_lossy()
        );
        return;
    }
    if !equities_path.exists() {
        eprintln!(
            "Securities monthly parquet not found at {} — skipping test",
            equities_path.to_string_lossy()
        );
        return;
    }

    // Start DuckDB
    let conn = words_db::instantiatedb::duckdbinst::start_duck_db("24GB", 14)
        .await
        .expect("duckdb in-memory should start");
    let conn = Arc::new(Mutex::new(conn));
    let now = std::time::Instant::now();
    // Ingest Equity Factors Monthly
    let n_fac = DbType::EquityFactorsMonthly
        .ingest(conn.clone(), &factors_path.to_string_lossy())
        .await
        .expect("ingest equity factors monthly parquet");
    assert!(n_fac > 0, "expected >0 rows ingested for factors");
    println!("{:?}", now.elapsed());
    // Ingest Global Equities Monthly (securities)
    let n_eq = DbType::GlobalEquitiesMonthly
        .ingest(conn.clone(), &equities_path.to_string_lossy())
        .await
        .expect("ingest global equities monthly parquet");
    assert!(n_eq > 0, "expected >0 rows ingested for equities");
    println!("{:?}", now.elapsed());
    // Simple checks: describe tables
    // Read small windows to exercise the paths
    let start = NaiveDate::from_ymd_opt(2020, 1, 1).unwrap();
    let end = NaiveDate::from_ymd_opt(2020, 3, 31).unwrap();

    // GlobalEquitiesMonthly typed rows → DataFrame
    let eq_rows = GlobalEquitiesMonthly::read_range(conn.clone(), (start, end))
        .await
        .expect("read_range equities monthly");
    let eq_df =
        <GlobalEquitiesMonthly as words_db::finance_data_structs::ToPolars>::df_from_rows(&eq_rows)
            .expect("equities monthly df_from_rows");
    println!("[equities] DF shape: {:?}", eq_df.shape());

    //EquityFactorsMonthly: JSON doc rows → DataFrame
    let fac_rows = EquityFactorsMonthly::read_range(conn.clone(), (start, end))
        .await
        .expect("read_range_json factors monthly");
    let fac_df =
        <EquityFactorsMonthly as words_db::finance_data_structs::ToPolars>::df_from_rows(&fac_rows)
            .expect("factors monthly df_from_rows");
    println!("[factors] DF shape: {:?}", fac_df.shape());
    println!("{:?}", now.elapsed());
}
