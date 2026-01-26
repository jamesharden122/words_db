use std::path::{Path, PathBuf};
use std::time::{SystemTime, UNIX_EPOCH};
use words_db::finance_data_structs::*;
use words_db::instantiatedb::surrealdbinst::{start_mem_db, start_mem_db_no_table};

fn expand_home(raw: &str) -> String {
    if let Some(rest) = raw.strip_prefix("~/") {
        if let Ok(home) = std::env::var("HOME") {
            return format!("{}/{}", home, rest);
        }
    }
    raw.to_string()
}

fn resolve_path_in_repo(raw: &str) -> PathBuf {
    let expanded = expand_home(raw);
    let p = PathBuf::from(expanded);
    if p.is_absolute() {
        p
    } else {
        PathBuf::from(env!("CARGO_MANIFEST_DIR")).join(p)
    }
}
//RAYON_NUM_THREADS=14 POLARS_MAX_THREADS=14 cargo test start_mem_db_works --release -- --no-capture --test-threads=1
#[tokio::test(flavor = "multi_thread", worker_threads = 15)]
async fn start_mem_db_works() {
    println!("{:?}", "checkpoint 1");
    let db = start_mem_db().await.expect("db should start");
    println!("{:?}", "checkpoint 2");

    // sanity: we should be able to switch to the DB created by schema bootstrapping
    db.use_ns("indexes")
        .use_db("daily")
        .await
        .expect("indexes/daily exists");
    println!("{:?}", "checkpoint 3");

    // Optional ingest: runs only when the Parquet is available (keeps CI deterministic).
    let parquet_raw = std::env::var("WORDS_DB_GLOBAL_INDEXES_PARQUET")
        .unwrap_or_else(|_| "../data/raw_files/global_indexes_daily.parquet".to_string());
    let parquet_path = resolve_path_in_repo(&parquet_raw);
    if !parquet_path.exists() {
        eprintln!(
            "Global indexes parquet not found at {} — skipping ingest",
            parquet_path.display()
        );
        return;
    }

    let df_vec = crsp::GlobalDailyIndex::from_parquet(&parquet_path)
        .expect("global indexes parquet should load");
    println!("{:?}", "checkpoint 4");
    let time = std::time::Instant::now();
    println!(
        "The data was uploaded: {:?}",
        crsp::GlobalDailyIndex::create_gdi_result(df_vec, &db, "indexes", "daily", 15000, 15)
            .await
            .expect("bulk insert should succeed")
    );
    println!("{:?}", "checkpoint 5");
    let duration = time.elapsed();
    println!("duration: {:?}", duration);

    let backup_path = Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("target")
        .join("global_daily_backup.sql");
    db.export(backup_path.to_str().expect("utf-8 path"))
        .await
        .expect("export should succeed");
}

#[tokio::test]
async fn import_mem_db_works() {
    // Self-contained export → import roundtrip (schema-focused, deterministic).
    let export_path = {
        let nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("system time")
            .as_nanos();
        Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("target")
            .join(format!(
                "surreal_roundtrip_{}_{}.sql",
                std::process::id(),
                nanos
            ))
    };

    let db = start_mem_db().await.expect("db should start");
    db.export(export_path.to_str().expect("utf-8 path"))
        .await
        .expect("export should succeed");

    // Import into a fresh DB without tables; export contains schema definitions.
    let db2 = start_mem_db_no_table().await.expect("db should start");
    db2.import(export_path.to_str().expect("utf-8 path"))
        .await
        .expect("import should succeed");
    db2.use_ns("indexes")
        .use_db("daily")
        .await
        .expect("indexes/daily exists");
    db2.query("INFO FOR TABLE global;")
        .await
        .expect("imported schema should include `global` table");
}
