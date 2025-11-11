use words_db::finance_data_structs::*;
use words_db::instantiatedb::surrealdbinst::{start_mem_db, start_mem_db_no_table};

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
    // Simple sanity query
    let df_vec =
        crsp::GlobalDailyIndex::from_parquet("../../data/raw_files/global_indexes_daily.parquet")
            .unwrap();
    println!("{:?}", "checkpoint 4");
    let time = std::time::Instant::now();
    println!(
        "The data was uploaded: {:?}",
        crsp::GlobalDailyIndex::create_gdi_result(df_vec, &db, "indexes", "daily", 15000, 15)
            .await
            .unwrap()
    );
    println!("{:?}", "checkpoint 5");
    let duration = time.elapsed();
    println!("duration: {:?}", duration);

    db.export("../global_daily_backup.sql").await.unwrap();
}

#[tokio::test]
async fn import_mem_db_works() {
    println!("{:?}", "checkpoint 1");
    let db = start_mem_db_no_table().await.expect("db should start");
    println!("{:?}", "checkpoint 2");
    let time = std::time::Instant::now();
    db.import("../global_daily_backup.sql").await.unwrap();
    let duration = time.elapsed();
    println!("duration: {:?}", duration);
}
