use std::path::{Path, PathBuf};

use words_db::createdatasets::usbanks::{
    dly_securities_ds_from_db_files, fundamental_ds_from_db_files,
};
use words_db::instantiatedb::duckdbinst::open_duck_db_from_file;

fn target_tmp_dir() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("target")
        .join("tmp")
}

fn require_exists(path: &Path) -> bool {
    if !path.exists() {
        eprintln!(
            "Required DuckDB file not found at {} — skipping test",
            path.display()
        );
        return false;
    }
    true
}

#[tokio::test(flavor = "multi_thread", worker_threads = 14)]
async fn fundamental_ds_from_target_tmp_duckdb_files() {
    let base = target_tmp_dir().join("bank-fundamentals-data");
    //let bhck_legacy = base.join("bhck_legacy.duckdb");
    let bhck_other = base.join("bhck_other.duckdb");
    let bhck_series1 = base.join("bhck_series1.duckdb");
    let bhck_series2 = base.join("bhck_series2.duckdb");

    for p in [
        //&bhck_legacy,
        &bhck_other,
        &bhck_series1,
        &bhck_series2,
    ] {
        if !require_exists(p) {
            return;
        }
    }

    let out_dir = target_tmp_dir()
        .join("test-output")
        .join("fundamental_ds_from_db_files");
    let out_db = fundamental_ds_from_db_files(
        "50GB",
        14,
        // `bhck_legacy` is currently unused by `fundamental_ds_from_db_files` (see underscore
        // param name), but the API expects it.
        "",
        //bhck_legacy.to_str().expect("bhck_legacy path is utf-8"),
        bhck_other.to_str().expect("bhck_other path is utf-8"),
        bhck_series1.to_str().expect("bhck_series1 path is utf-8"),
        bhck_series2.to_str().expect("bhck_series2 path is utf-8"),
        out_dir,
    )
    .await
    .expect("fundamental_ds_from_db_files should succeed");
    assert!(
        out_db.exists(),
        "expected output duckdb at {}",
        out_db.display()
    )
}

#[tokio::test(flavor = "multi_thread", worker_threads = 14)]
async fn dly_securities_ds_from_target_tmp_duckdb_files() {
    let base = target_tmp_dir().join("securities-data");
    let bhck_crsp_link = base.join("bhck_crsp_link.duckdb");
    let us_crsp_dly = base.join("us_crsp_dly.duckdb");
    let fama_french_daily = base.join("fama_french_daily.duckdb");

    for p in [&bhck_crsp_link, &us_crsp_dly, &fama_french_daily] {
        if !require_exists(p) {
            return;
        }
    }

    let out_dir = target_tmp_dir()
        .join("test-output")
        .join("dly_securities_ds_from_db_files");
    let out_db = dly_securities_ds_from_db_files(
        "20GB",
        4,
        bhck_crsp_link
            .to_str()
            .expect("bhck_crsp_link path is utf-8"),
        us_crsp_dly.to_str().expect("us_crsp_dly path is utf-8"),
        fama_french_daily
            .to_str()
            .expect("fama_french_daily path is utf-8"),
        out_dir,
    )
    .await
    .expect("dly_securities_ds_from_db_files should succeed");
    assert!(
        out_db.exists(),
        "expected output duckdb at {}",
        out_db.display()
    );

    let conn = open_duck_db_from_file(out_db.to_str().expect("output path is utf-8"), "20GB", 14)
        .await
        .expect("open output duckdb");
    let one: i32 = conn
        .query_row("SELECT 1 FROM bank_securities_dly LIMIT 1", [], |r| {
            r.get(0)
        })
        .expect("bank_securities_dly should be non-empty");
    assert_eq!(one, 1);
}
