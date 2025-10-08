use surrealdb::engine::local::Db;
use surrealdb::Surreal;

pub async fn apply_base_schema(db: &Surreal<Db>) -> surrealdb::Result<()> {
    // ---- compustat / quarterly ----
    db.query("DEFINE NAMESPACE compustat;").await?;
    db.use_ns("compustat").await?;
    db.query("DEFINE DATABASE quarterly;").await?;
    db.use_db("quarterly").await?;
    db.query(
        r#"
        DEFINE TABLE compustat SCHEMALESS PERMISSIONS NONE;
        "#,
    )
    .await?;

    // ---- indexes / daily ----
    db.query("DEFINE NAMESPACE indexes;").await?;
    db.use_ns("indexes").await?;
    db.query("DEFINE DATABASE daily;").await?;
    db.use_db("daily").await?;
    db.query(
        r#"
        DEFINE TABLE global SCHEMALESS PERMISSIONS NONE;    
        "#,
    )
    .await?;

    Ok(())
}

pub async fn apply_base_schema_no_table(db: &Surreal<Db>) -> surrealdb::Result<()> {
    // ---- compustat / quarterly ----
    db.query("DEFINE NAMESPACE compustat;").await?;
    db.use_ns("compustat").await?;
    db.query("DEFINE DATABASE quarterly;").await?;
    db.use_db("quarterly").await?;
    // ---- indexes / daily ----
    db.query("DEFINE NAMESPACE indexes;").await?;
    db.use_ns("indexes").await?;
    db.query("DEFINE DATABASE daily;").await?;
    db.use_db("daily").await?;
    Ok(())
}
