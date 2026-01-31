use std::{
    fs,
    path::{Path, PathBuf},
};
use words_db::createdatasets::usbanks::{
    create_fundamental_duckdb_files, create_securities_duckdb_files,
};

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
async fn test_create_fundamental_duckdb_files() {
    let base = expand_tilde("~/Dropbox/Desktop/tesero-sol/software_development/trading/data/raw_files/parquet/bank_regulatory/holding_company_financials");

    let other_dir = base.join("bhck_other");
    let s1_dir = base.join("bhck_series1");
    let s2_dir = base.join("bhck_series2");
    let legacy_dir = base.join("bhck_legacy");

    let other_files = ["bhck_other.parquet"];
    let s1_files = ["bhck_series1.parquet"];
    let s2_files = ["bhck_series2.parquet"];

    let other_paths: Vec<PathBuf> = other_files.iter().map(|f| other_dir.join(f)).collect();
    let s1_paths: Vec<PathBuf> = s1_files.iter().map(|f| s1_dir.join(f)).collect();
    let s2_paths: Vec<PathBuf> = s2_files.iter().map(|f| s2_dir.join(f)).collect();
    let legacy_paths: Vec<PathBuf> = vec![legacy_dir.join("kuuy1wbug5dlu3zz.parquet")];
    let tmp_pth = "/home/yakaman/Dropbox/Desktop/tesero-sol/software_development/trading/words_db/target/tmp/bank-fundamentals-data/";
    let dir = Path::new(tmp_pth);
    if !dir.exists() {
        fs::create_dir(dir).unwrap();
    }

    for p in other_paths
        .iter()
        .chain(s1_paths.iter())
        .chain(s2_paths.iter())
        .chain(legacy_paths.iter())
    {
        if !require_exists(p) {
            return;
        }
    }

    let other_strs: Vec<String> = other_paths
        .iter()
        .map(|p| p.to_string_lossy().to_string())
        .collect();
    let s1_strs: Vec<String> = s1_paths
        .iter()
        .map(|p| p.to_string_lossy().to_string())
        .collect();
    let s2_strs: Vec<String> = s2_paths
        .iter()
        .map(|p| p.to_string_lossy().to_string())
        .collect();
    let legacy_strs: Vec<String> = legacy_paths
        .iter()
        .map(|p| p.to_string_lossy().to_string())
        .collect();

    create_fundamental_duckdb_files(
        Some(other_strs),
        Some(s1_strs),
        Some(s2_strs),
        Some(legacy_strs),
        dir,
    )
    .await
    .expect("create_fundamental_duckdb_files should succeed");

    // New logic: output file stem is the table name (one `.duckdb` per dataset dir).
    let expected = [
        dir.join("bhck_other.duckdb"),
        dir.join("bhck_series1.duckdb"),
        dir.join("bhck_series2.duckdb"),
        dir.join("bhck_legacy.duckdb"),
    ];
    for out in expected {
        assert!(out.exists(), "expected output duckdb at {}", out.display());
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_create_securities_duckdb_files() {
    let bhck_crsp = expand_tilde("~/Dropbox/Desktop/tesero-sol/software_development/trading/data/raw_files/parquet/bank_regulatory/bhck_crsp_link/bhck_crsp_link_file.parquet");
    let crsp_mthly = expand_tilde("~/Dropbox/Desktop/tesero-sol/software_development/trading/data/raw_files/parquet/crsp/us_bhc/monthly_usbank_crsp.parquet");
    let ff_mthly = expand_tilde("~/Dropbox/Desktop/tesero-sol/software_development/trading/data/raw_files/parquet/factors/us/monthly_ff_factors.parquet");
    let crsp_dly = expand_tilde("~/Dropbox/Desktop/tesero-sol/software_development/trading/data/raw_files/parquet/crsp/us_bhc/daily_usbank_crsp.parquet");
    let ff_dly = expand_tilde("~/Dropbox/Desktop/tesero-sol/software_development/trading/data/raw_files/parquet/factors/us/daily_ff_factors.parquet");
    let tmp_pth = "/home/yakaman/Dropbox/Desktop/tesero-sol/software_development/trading/words_db/target/tmp/securities-data/";
    let dir = Path::new(tmp_pth);
    if !dir.exists() {
        fs::create_dir(dir).unwrap();
    }
    for p in [&bhck_crsp, &crsp_mthly, &ff_mthly, &crsp_dly, &ff_dly] {
        if !require_exists(p) {
            return;
        }
    }

    create_securities_duckdb_files(
        Some(vec![bhck_crsp.to_string_lossy().to_string()]),
        Some(vec![crsp_mthly.to_string_lossy().to_string()]),
        Some(vec![ff_mthly.to_string_lossy().to_string()]),
        Some(vec![crsp_dly.to_string_lossy().to_string()]),
        Some(vec![ff_dly.to_string_lossy().to_string()]),
        dir,
    )
    .await
    .expect("create_securities_duckdb_files should succeed");

    for parquet in [&bhck_crsp, &crsp_mthly, &ff_mthly, &crsp_dly, &ff_dly] {
        let out = match parquet.file_name().and_then(|n| n.to_str()) {
            Some("bhck_crsp_link_file.parquet") => dir.join("bhck_crsp_link.duckdb"),
            Some("monthly_usbank_crsp.parquet") => dir.join("us_crsp_mthly.duckdb"),
            Some("monthly_ff_factors.parquet") => dir.join("fama_french_monthly.duckdb"),
            Some("daily_usbank_crsp.parquet") => dir.join("us_crsp_dly.duckdb"),
            Some("daily_ff_factors.parquet") => dir.join("fama_french_daily.duckdb"),
            _ => parquet.with_extension("duckdb"),
        };
        assert!(out.exists(), "expected output duckdb at {}", out.display());
    }
}
