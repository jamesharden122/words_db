use super::*;
use surrealdb::engine::local::{Db, Mem};
use surrealdb::Surreal;
/// Start and return a new in-memory SurrealDB connection and apply base schema.
pub async fn start_mem_db() -> surrealdb::Result<Surreal<Db>> {
    let config = surrealdb::opt::Config::default().strict();
    let db = Surreal::new::<Mem>(config).await?;
    schema::apply_base_schema(&db).await?;
    Ok(db)
}

/// Start and return a new in-memory SurrealDB connection and apply base schema.
pub async fn start_mem_db_no_table() -> surrealdb::Result<Surreal<Db>> {
    let config = surrealdb::opt::Config::default().strict();
    let db = Surreal::new::<Mem>(config).await?;
    schema::apply_base_schema_no_table(&db).await?;
    Ok(db)
}


pub async fn start_mem_db_global_indexes(
    global_index_pth: &str,
    batch_size: usize,
    cores: usize,
) -> surrealdb::Result<Surreal<Db>> {
    let db = start_mem_db().await.expect("db should start");
    db.use_ns("indexes")
        .use_db("daily")
        .await
        .expect("indexes/daily exists");
    let df_vec = crsp::GlobalDailyIndex::from_parquet(global_index_pth).unwrap();
    println!("{:?}", "checkpoint 4");
    let time = std::time::Instant::now();
    println!(
        "The data was uploaded: {:?}",
        crsp::GlobalDailyIndex::create_gdi_result(
            df_vec, &db, "indexes", "daily", batch_size, cores
        )
        .await
        .unwrap()
    );
    let duration = time.elapsed();
    println!("duration: {:?}", duration);
    db.export("../global_daily_backup.sql").await.unwrap();
    Ok(db)
}

async fn import_mem_db(backup_file: &str) -> surrealdb::Result<Surreal<Db>> {
    let db = start_mem_db_no_table().await.expect("db should start");
    let time = std::time::Instant::now();
    db.import(backup_file).await.unwrap();
    let duration = time.elapsed();
    println!("duration: {:?}", duration);
    Ok(db)
}