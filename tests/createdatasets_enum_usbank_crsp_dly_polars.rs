use polars::prelude::*;
use std::path::{Path, PathBuf};
use std::time::{SystemTime, UNIX_EPOCH};
use words_db::createdatasets::usbanks::{BankCrspDly, BankCrspPaths};
use words_db::createdatasets::{CreateDuckFls, MergeDuckFls};
use words_db::finance_data_structs::get_polars_df_from_sql;
use words_db::instantiatedb::duckdbinst::open_duck_db_from_file;

fn test_artifacts_dir(test_name: &str) -> PathBuf {
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("system time")
        .as_nanos();
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("target")
        .join("test_artifacts")
        .join(test_name)
        .join(format!("{}_{}", std::process::id(), nanos))
}

fn expand_tilde(path: &str) -> PathBuf {
    if let Some(rest) = path.strip_prefix("~/") {
        if let Ok(home) = std::env::var("HOME") {
            return Path::new(&home).join(rest);
        }
    }
    PathBuf::from(path)
}

fn require_exists(path: &Path) -> bool {
    if !path.exists() {
        eprintln!(
            "Required parquet not found at {} — skipping test",
            path.display()
        );
        return false;
    }
    true
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn create_and_merge_usbank_crsp_dly_enum_to_polars_df() {
    let root = test_artifacts_dir("createdatasets_enum_usbank_crsp_dly_polars");
    let duck_dir = root.join("duckdb_files");
    let merge_dir = root.join("merged");

    // Use the same parquet inputs as `tests/test_datasets.rs:test_create_securities_duckdb_files`.
    let bhck_crsp = expand_tilde("~/Dropbox/Desktop/tesero-sol/software_development/trading/data/raw_files/parquet/bank_regulatory/bhck_crsp_link/bhck_crsp_link_file.parquet");
    let crsp_mthly = expand_tilde("~/Dropbox/Desktop/tesero-sol/software_development/trading/data/raw_files/parquet/crsp/us_bhc/monthly_usbank_crsp.parquet");
    let ff_mthly = expand_tilde("~/Dropbox/Desktop/tesero-sol/software_development/trading/data/raw_files/parquet/factors/us/monthly_ff_factors.parquet");
    let crsp_dly = expand_tilde("~/Dropbox/Desktop/tesero-sol/software_development/trading/data/raw_files/parquet/crsp/us_bhc/daily_usbank_crsp.parquet");
    let ff_dly = expand_tilde("~/Dropbox/Desktop/tesero-sol/software_development/trading/data/raw_files/parquet/factors/us/daily_ff_factors.parquet");

    for p in [&bhck_crsp, &crsp_mthly, &ff_mthly, &crsp_dly, &ff_dly] {
        if !require_exists(p) {
            return;
        }
    }

    // 1) Create the underlying DuckDB files via the enum option.
    let create = CreateDuckFls::UsBankCrsp(BankCrspPaths {
        bhck_crsp: Some(vec![bhck_crsp.to_string_lossy().to_string()]),
        crsp_mthly: Some(vec![crsp_mthly.to_string_lossy().to_string()]),
        ff_mthly: Some(vec![ff_mthly.to_string_lossy().to_string()]),
        crsp_dly: Some(vec![crsp_dly.to_string_lossy().to_string()]),
        ff_dly: Some(vec![ff_dly.to_string_lossy().to_string()]),
    });
    create
        .create_db_files(&duck_dir)
        .await
        .expect("CreateDuckFls::UsBankCrsp should succeed");

    let bhck_crsp_duck = duck_dir.join("bhck_crsp_link.duckdb");
    let us_crsp_dly_duck = duck_dir.join("us_crsp_dly.duckdb");
    let ff_dly_duck = duck_dir.join("fama_french_daily.duckdb");
    for p in [&bhck_crsp_duck, &us_crsp_dly_duck, &ff_dly_duck] {
        assert!(p.exists(), "expected duckdb file at {}", p.display());
    }

    // 2) Merge the daily dataset via the enum option.
    let merge = MergeDuckFls::UsBankCrspDly(BankCrspDly {
        bhck_crsp: bhck_crsp_duck.to_string_lossy().to_string(),
        crsp_dly: us_crsp_dly_duck.to_string_lossy().to_string(),
        ff_dly: ff_dly_duck.to_string_lossy().to_string(),
    });
    let out_db = merge
        .merge_db_files(&merge_dir, "20GB", 10)
        .await
        .expect("MergeDuckFls::UsBankCrspDly should succeed");
    assert!(
        out_db.exists(),
        "expected merged duckdb at {}",
        out_db.display()
    );

    // 3) Read the merged table into Polars via get_polars_df_from_sql.
    let conn = open_duck_db_from_file(out_db.to_str().expect("utf-8 path"), "1GB", 2)
        .await
        .expect("open merged duckdb");
    let mut chunks = get_polars_df_from_sql(
        &conn,
        "SELECT * FROM bank_securities_dly ORDER BY permco, date",
    )
    .await
    .expect("get_polars_df_from_sql should succeed");

    assert!(!chunks.is_empty(), "expected at least one Arrow batch");
    let mut df = chunks.remove(0);
    for c in chunks {
        df.vstack_mut(&c).expect("vstack polars chunks");
    }
    println!("columns: {:?}", df.get_column_names());
    println!("{:?}", df.height());
    println!("{:?}", df.head(Some(30)));
}
