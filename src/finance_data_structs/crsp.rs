use super::{AppError, DuckCrudModel, SurrealCrudModel, ToPolars};
use arrow_array::{Array, Date32Array, Float64Array, Int64Array, StringArray};
use chrono::{Datelike, NaiveDate};
use duckdb::Connection;
use itertools::izip;
use polars::frame::row::Row;
use polars::prelude::*;
use rayon::prelude::*;
use serde::{Deserialize, Serialize};
use std::path::Path;
use std::sync::Arc;
use surrealdb::engine::local::Db;
use surrealdb::Surreal;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GlobalDailyIndex {
    pub tic: String,
    pub datadate: NaiveDate,
    pub gvkeyx: String,
    pub conm: String,
    pub indextype: String,
    pub indexid: String,
    pub indexcat: Option<String>,
    pub idxiddesc: Option<String>,
    pub dvpsxd: Option<f64>,
    pub newnum: Option<i32>,
    pub oldnum: Option<i32>,
    pub prccd: Option<f64>,
    pub prccddiv: Option<f64>,
    pub prccddivn: Option<f64>,
    pub prchd: Option<f64>,
    pub prcld: Option<f64>,
}

impl SurrealCrudModel for GlobalDailyIndex {
    fn table() -> &'static str {
        "global"
    }
    fn id_key(&self) -> Option<String> {
        Some(format!("{}:{}", self.tic, self.datadate))
    }
}

impl DuckCrudModel for GlobalDailyIndex {
    fn table() -> &'static str {
        // Keep table naming consistent with SurrealDB for simplicity
        "global_indexes_daily"
    }
    fn id_key(&self) -> Option<String> {
        Some(format!("{}:{}", self.tic, self.datadate))
    }
}

impl GlobalDailyIndex {
    pub fn to_row<'a>(self) -> Row<'a> {
        let days: i32 = (self.datadate.num_days_from_ce() - 719_163) as i32;
        Row::new(vec![
            AnyValue::StringOwned(self.tic.into()), // String
            AnyValue::Date(days),                   // NaiveDate -> string (e.g., "2025-10-02")
            AnyValue::StringOwned(self.gvkeyx.into()),
            AnyValue::StringOwned(self.conm.into()),
            AnyValue::StringOwned(self.indextype.into()),
            AnyValue::StringOwned(self.indexid.into()),
            self.indexcat
                .map(|val| AnyValue::StringOwned(val.into()))
                .unwrap_or(AnyValue::Null),
            self.idxiddesc
                .map(|val| AnyValue::StringOwned(val.into()))
                .unwrap_or(AnyValue::Null),
            self.dvpsxd.map_or(AnyValue::Null, AnyValue::Float64),
            self.newnum.map_or(AnyValue::Null, AnyValue::Int32),
            self.oldnum.map_or(AnyValue::Null, AnyValue::Int32),
            self.prccd.map_or(AnyValue::Null, AnyValue::Float64),
            self.prccddiv.map_or(AnyValue::Null, AnyValue::Float64),
            self.prccddivn.map_or(AnyValue::Null, AnyValue::Float64),
            self.prchd.map_or(AnyValue::Null, AnyValue::Float64),
            self.prcld.map_or(AnyValue::Null, AnyValue::Float64),
        ])
    }
    pub fn from_parquet(path: impl AsRef<Path>) -> Result<Vec<Self>, AppError> {
        let file = std::fs::File::open(path)?;
        let mut df = ParquetReader::new(file).finish()?;
        df = df
            .lazy()
            .with_columns([
                col("dvpsxd").cast(DataType::Float64),
                col("prccd").cast(DataType::Float64),
                col("prccddiv").cast(DataType::Float64),
                col("prccddivn").cast(DataType::Float64),
                col("prchd").cast(DataType::Float64),
                col("prcld").cast(DataType::Float64),
            ])
            .collect()?;

        let n = df.height();
        println!("df height: {}", n);
        // Pull once
        let tic = df.column("tic")?.str()?.clone();
        let gvkeyx = df.column("gvkeyx")?.str()?.clone();
        let conm = df.column("conm")?.str()?.clone();
        let indextype = df.column("indextype")?.str()?.clone();
        let indexid = df.column("indexid")?.str()?.clone();
        let indexcat = df.column("indexcat")?.str()?.clone();
        let idxiddesc = df.column("idxiddesc")?.str()?.clone();
        let datadate = df.column("datadate")?.date()?.clone();

        let dvpsxd = df.column("dvpsxd")?.f64()?.clone();
        let newnum = df.column("newnum")?.i32()?.clone();
        let oldnum = df.column("oldnum")?.i32()?.clone();
        let prccd = df.column("prccd")?.f64()?.clone();
        let prccddiv = df.column("prccddiv")?.f64()?.clone();
        let prccddivn = df.column("prccddivn")?.f64()?.clone();
        let prchd = df.column("prchd")?.f64()?.clone();
        let prcld = df.column("prcld")?.f64()?.clone();
        let start = std::time::Instant::now();
        let origin_ce = 719_163i32;
        let iter = izip!(
            tic.into_iter(),
            gvkeyx.into_iter(),
            conm.into_iter(),
            indextype.into_iter(),
            indexid.into_iter(),
            indexcat.into_iter(),
            idxiddesc.into_iter(),
            datadate.phys.into_iter(), // Option<i32> days since epoch
            dvpsxd.into_iter(),
            newnum.into_iter(),
            oldnum.into_iter(),
            prccd.into_iter(),
            prccddiv.into_iter(),
            prccddivn.into_iter(),
            prchd.into_iter(),
            prcld.into_iter()
        );
        let duration = start.elapsed();
        println!("Time taken: {:?}", duration);
        let out = iter
            .enumerate()
            .par_bridge()
            .map(
                |(
                    _i,
                    (
                        tic,
                        gvkeyx,
                        conm,
                        indextype,
                        indexid,
                        indexcat,
                        idxiddesc,
                        dd,
                        dvpsxd,
                        newnum,
                        oldnum,
                        prccd,
                        prccddiv,
                        prccddivn,
                        prchd,
                        prcld,
                    ),
                )| {
                    let nd = dd
                        .map(|days| {
                            chrono::NaiveDate::from_num_days_from_ce_opt(days + origin_ce).unwrap()
                        })
                        .unwrap_or_else(|| chrono::NaiveDate::from_ymd_opt(1970, 1, 1).unwrap());

                    Self {
                        tic: tic.unwrap_or("").to_string(),
                        datadate: nd,
                        gvkeyx: gvkeyx.unwrap_or("").to_string(),
                        conm: conm.unwrap_or("").to_string(),
                        indextype: indextype.unwrap_or("").to_string(),
                        indexid: indexid.unwrap_or("").to_string(),
                        indexcat: indexcat.map(|val| val.to_string()),
                        idxiddesc: idxiddesc.map(|val| val.to_string()),
                        dvpsxd: dvpsxd,
                        newnum,
                        oldnum,
                        prccd,
                        prccddiv,
                        prccddivn,
                        prchd,
                        prcld,
                    }
                },
            )
            .collect();
        let duration = start.elapsed();
        println!("Time taken: {:?}", duration);
        Ok(out)
    }

    /// Ingest a Parquet file of global indexes into DuckDB JSON-doc table via upsert.
    /// Returns total rows processed from the Parquet file.
    pub async fn duck_from_parquet(
        conn: Arc<Connection>,
        parquet_path: impl AsRef<Path>,
    ) -> Result<usize, AppError> {
        // Use stable content hash as id; if you want a composed id
        // like "tic:datadate", extend the trait helper to accept an expression.
        <Self as DuckCrudModel>::upsert_from_parquet_one_file(
            conn,
            parquet_path,
            None,
            Some(<Self as DuckCrudModel>::table().into()),
        )
        .await
    }

    pub async fn create_gdi_result(
        data_vec: Vec<GlobalDailyIndex>,
        db: &Surreal<Db>,
        nsname: &str,
        dbname: &str,
        batch_size: usize,
        cores: usize,
    ) -> Result<usize, AppError> {
        db.use_ns(nsname).use_db(dbname).await?;
        GlobalDailyIndex::insert_vec_concurrent(&db, data_vec, batch_size, cores).await
    }

    /// Read a record by id.
    pub async fn read_gdi_batch<'a>(
        conn: Arc<Connection>,
        filter_col: String,
        filter_vals: Vec<String>,
        date_range: (NaiveDate, NaiveDate),
    ) -> Result<Vec<Row<'a>>, AppError> {
        tokio::task::block_in_place(move || {
            if filter_vals.is_empty() {
                return Ok(Vec::new());
            }
            let values = filter_vals
                .iter()
                .map(|s| format!("('{}')", s.replace('\'', "''")))
                .collect::<Vec<_>>()
                .join(",");

            let sql = format!(
                "
            WITH wanted(val) AS (VALUES {})
            SELECT
            tic,
            datadate,
            gvkeyx,
            conm,
            indextype,
            indexid,
            COALESCE(indexcat, '')  AS indexcat,
            COALESCE(idxiddesc, '') AS idxiddesc,
            CAST(dvpsxd    AS DOUBLE) AS dvpsxd,
            CAST(newnum    AS BIGINT) AS newnum,
            CAST(oldnum    AS BIGINT) AS oldnum,
            CAST(prccd     AS DOUBLE) AS prccd,
            CAST(prccddiv  AS DOUBLE) AS prccddiv,
            CAST(prccddivn AS DOUBLE) AS prccddivn,
            CAST(prchd     AS DOUBLE) AS prchd,
            CAST(prcld     AS DOUBLE) AS prcld 
            FROM {} t 
            JOIN wanted w ON t.{} = w.val
            WHERE datadate BETWEEN DATE '{}' AND DATE '{}'
            ",
                values,
                <Self as DuckCrudModel>::table(),
                filter_col,
                date_range.0.to_string(),
                date_range.1.to_string()
            );

            let mut reader = conn.prepare(sql.as_str())?;
            let mut reader = reader.query_arrow([])?; // Arrow RecordBatchReader
            let mut out: Vec<Row<'static>> = Vec::new();
            while let Some(batch) = reader.next() {
                // ---- Downcast columns once per batch ----
                let tic = batch
                    .column(0)
                    .as_any()
                    .downcast_ref::<StringArray>()
                    .unwrap();
                let date32 = batch
                    .column(1)
                    .as_any()
                    .downcast_ref::<Date32Array>()
                    .unwrap();
                let gvkeyx = batch
                    .column(2)
                    .as_any()
                    .downcast_ref::<StringArray>()
                    .unwrap();
                let conm = batch
                    .column(3)
                    .as_any()
                    .downcast_ref::<StringArray>()
                    .unwrap();
                let indextype = batch
                    .column(4)
                    .as_any()
                    .downcast_ref::<StringArray>()
                    .unwrap();
                let indexid = batch
                    .column(5)
                    .as_any()
                    .downcast_ref::<StringArray>()
                    .unwrap();
                let indexcat = batch
                    .column(6)
                    .as_any()
                    .downcast_ref::<StringArray>()
                    .unwrap();
                let idxiddesc = batch
                    .column(7)
                    .as_any()
                    .downcast_ref::<StringArray>()
                    .unwrap();
                let dvpsxd = batch
                    .column(8)
                    .as_any()
                    .downcast_ref::<Float64Array>()
                    .unwrap();
                let newnum = batch
                    .column(9)
                    .as_any()
                    .downcast_ref::<Int64Array>()
                    .unwrap();
                let oldnum = batch
                    .column(10)
                    .as_any()
                    .downcast_ref::<Int64Array>()
                    .unwrap();
                let prccd = batch
                    .column(11)
                    .as_any()
                    .downcast_ref::<Float64Array>()
                    .unwrap();
                let prccddiv = batch
                    .column(12)
                    .as_any()
                    .downcast_ref::<Float64Array>()
                    .unwrap();
                let prccddivn = batch
                    .column(13)
                    .as_any()
                    .downcast_ref::<Float64Array>()
                    .unwrap();
                let prchd = batch
                    .column(14)
                    .as_any()
                    .downcast_ref::<Float64Array>()
                    .unwrap();
                let prcld = batch
                    .column(15)
                    .as_any()
                    .downcast_ref::<Float64Array>()
                    .unwrap();

                // ---- Parallel map rows in this batch ----
                let part: Vec<Row> = (0..batch.num_rows())
                    .into_par_iter()
                    .map(|i| {
                        // HANDLE NULLS as needed; below assumes NOT NULL. If nullable, check `is_null(i)` and use Option<> fields.
                        let datadate = date32.value_as_date(i).unwrap(); // or handle None if nullable
                        let temp = Self {
                            tic: tic.value(i).to_string(),
                            datadate,
                            gvkeyx: gvkeyx.value(i).to_string(),
                            conm: conm.value(i).to_string(),
                            indextype: indextype.value(i).to_string(),
                            indexid: indexid.value(i).to_string(),
                            indexcat: Some(indexcat.value(i).to_string()),
                            idxiddesc: Some(idxiddesc.value(i).to_string()),
                            dvpsxd: Some(dvpsxd.value(i)),
                            newnum: Some(newnum.value(i) as i32),
                            oldnum: Some(oldnum.value(i) as i32),
                            prccd: Some(prccd.value(i)),
                            prccddiv: Some(prccddiv.value(i)),
                            prccddivn: Some(prccddivn.value(i)),
                            prchd: Some(prchd.value(i)),
                            prcld: Some(prcld.value(i)),
                        };

                        temp.to_row()
                    })
                    .collect();

                out.extend(part);
            }

            Ok::<Vec<Row>, AppError>(out)
        })
    }
}

impl ToPolars for GlobalDailyIndex {
    fn schema() -> Schema {
        Schema::from_iter([
            Field::new("tic".into(), DataType::String),
            Field::new("datadate".into(), DataType::Date),
            Field::new("gvkeyx".into(), DataType::String),
            Field::new("conm".into(), DataType::String),
            Field::new("indextype".into(), DataType::String),
            Field::new("indexid".into(), DataType::String),
            Field::new("indexcat".into(), DataType::String),
            Field::new("idxiddesc".into(), DataType::String),
            Field::new("dvpsxd".into(), DataType::Float64),
            Field::new("newnum".into(), DataType::Int32),
            Field::new("oldnum".into(), DataType::Int32),
            Field::new("prccd".into(), DataType::Float64),
            Field::new("prccddiv".into(), DataType::Float64),
            Field::new("prccddivn".into(), DataType::Float64),
            Field::new("prchd".into(), DataType::Float64),
            Field::new("prcld".into(), DataType::Float64),
        ])
    }
}

pub fn normalize_date_str(s: &str) -> Result<NaiveDate, AppError> {
    NaiveDate::parse_from_str(s, "%Y-%m-%d")
        .or_else(|_| NaiveDate::parse_from_str(s, "%Y-%m-%dT%H:%M:%S%:z"))
        .map_err(AppError::ChronoError)
}


pub fn finance_tickers() -> Option<Vec<String>>{
        let tic_vec = vec![
        "I3MLT002".to_string(),
        "I3SWE001".to_string(),
        "I6UNK001".to_string(),
        "I3DEU003".to_string(),
        "I3GBR049".to_string(),
        "I3GBR068".to_string(),
        "I3GBR001".to_string(),
        "I2IND004".to_string(),
        "I3GBR050".to_string(),
        "I3AUT005".to_string(),
        "I3GBR057".to_string(),
        "I3GBR059".to_string(),
        "I3TUR001".to_string(),
        "I3FRA006".to_string(),
        "I3FIN003".to_string(),
        "I2IDN003".to_string(),
        "I2PAK002".to_string(),
        "I2MYS002".to_string(),
        "I3ESP003".to_string(),
        "I2KOR003".to_string(),
        "I3GBR053".to_string(),
        "I4MEX005".to_string(),
        "I2AUS006".to_string(),
        "I3DNK014".to_string(),
        "I6UNK082".to_string(),
        "I2HKG008".to_string(),
        "I3ITA028".to_string(),
        "I2JPN004".to_string(),
        "I2HKG004".to_string(),
        "I2JPN008".to_string(),
        "I3NLD013".to_string(),
        "I2NZL002".to_string(),
        "I2SGP002".to_string(),
        "I3ESP004".to_string(),
        "I2AUS004".to_string(),
        "I3SWE003".to_string(),
        "I3CHE002".to_string(),
        "I3GBR062".to_string(),
        "I6UNK083".to_string(),
        "I2SGP005".to_string(),
        "I2THA002".to_string(),
        "I2JPN006".to_string(),
        "I3CHE018".to_string(),
        "I2TWN002".to_string(),
        "I2JPN013".to_string(),
        "I2JPN014".to_string(),
        "I2JPN015".to_string(),
        "I3AUT004".to_string(),
        "I3BEL003".to_string(),
        "I3BEL006".to_string(),
        "I4BRA002".to_string(),
        "I5CAN003".to_string(),
        "I4CHL002".to_string(),
        "I3FIN021".to_string(),
        "I3FRA001".to_string(),
        "I3FRA004".to_string(),
        "I3DEU001".to_string(),
        "I3DEU008".to_string(),
        "I3GRC020".to_string(),
        "I6UNK107".to_string(),
        "I3IRL002".to_string(),
        "I2AUS002".to_string(),
        "I3AUT001".to_string(),
        "I3BEL001".to_string(),
        "I3BEL002".to_string(),
        "I2AUS010".to_string(),
        "I2CHN003".to_string(),
        "I2CHN004".to_string(),
        "I3FIN022".to_string(),
        "I3FRA005".to_string(),
        "I2IND001".to_string(),
        "I3IRL004".to_string(),
        "I2JPN007".to_string(),
        "I2JPN012".to_string(),
        "I4MEX003".to_string(),
        "I3NLD014".to_string(),
        "I1NGA001".to_string(),
        "I3NOR004".to_string(),
        "I3POL001".to_string(),
        "I3CHE019".to_string(),
        "I4VEN002".to_string(),
        "I2AUS005".to_string(),
        "I3AUT003".to_string(),
        "I3BEL005".to_string(),
        "I5CAN004".to_string(),
        "I3DNK001".to_string(),
        "I4MEX001".to_string(),
        "I3NOR002".to_string(),
        "I2NZL001".to_string(),
        "I1ZAF002".to_string(),
        "I2SGP001".to_string(),
        "I3ESP002".to_string(),
        "I3SWE002".to_string(),
        "I3CHE001".to_string(),
        "I3GBR052".to_string(),
        "I3GBR054".to_string(),
        "I3GBR067".to_string(),
        "I3GBR055".to_string(),
        "I2HKG003".to_string(),
        "I3DNK002".to_string(),
        "I3AUT006".to_string(),
        "I1ISR002".to_string(),
        "I3ROM001".to_string(),
        "I3HRV001".to_string(),
        "I3SVK001".to_string(),
        "I3PRT002".to_string(),
        "I2KOR004".to_string(),
        "I3DEU005".to_string(),
        "I6UNK002".to_string(),
        "I6UNK003".to_string(),
        "I6UNK004".to_string(),
        "I6UNK005".to_string(),
        "I6UNK015".to_string(),
        "I6UNK016".to_string(),
        "I6UNK018".to_string(),
        "I6UNK019".to_string(),
        "I6UNK017".to_string(),
        "I6UNK020".to_string(),
        "I6UNK021".to_string(),
        "I6UNK022".to_string(),
        "I6UNK023".to_string(),
        "I6UNK006".to_string(),
        "I6UNK027".to_string(),
        "I6UNK026".to_string(),
        "I6UNK025".to_string(),
        "I6UNK024".to_string(),
        "I6UNK007".to_string(),
        "I6UNK059".to_string(),
        "I6UNK058".to_string(),
        "I6UNK008".to_string(),
        "I3BEL004".to_string(),
        "I6UNK084".to_string(),
        "I6UNK085".to_string(),
        "I6UNK113".to_string(),
        "I2JPN011".to_string(),
        "I3GBR065".to_string(),
        "I6UNK112".to_string(),
        "I6UNK111".to_string(),
        "I1ISR001".to_string(),
        "I3LUX001".to_string(),
        "I6UNK110".to_string(),
        "I6UNK127".to_string(),
        "I3DNK013".to_string(),
        "I6UNK057".to_string(),
        "I2PHL003".to_string(),
        "I1ZAF003".to_string(),
        "I1ZAF001".to_string(),
        "I2KHG010".to_string(),
        "I3GBR066".to_string(),
        "I3BGR001".to_string(),
        "I6UNK029".to_string(),
        "I6UNK030".to_string(),
        "I6UNK031".to_string(),
        "I6UNK032".to_string(),
        "I6UNK033".to_string(),
        "I6UNK034".to_string(),
        "I6UNK035".to_string(),
        "I6UNK036".to_string(),
        "I6UNK037".to_string(),
        "I6UNK038".to_string(),
        "I6UNK039".to_string(),
        "I6UNK040".to_string(),
        "I6UNK041".to_string(),
        "I6UNK042".to_string(),
        "I6UNK043".to_string(),
        "I6UNK044".to_string(),
        "I6UNK045".to_string(),
        "I6UNK046".to_string(),
        "I6UNK047".to_string(),
        "I6UNK052".to_string(),
        "I2CHN001".to_string(),
        "I6UNK055".to_string(),
        "I3ISL001".to_string(),
        "I1MAR001".to_string(),
        "I6UNK108".to_string(),
        "I2AUS007".to_string(),
        "I2JPN010".to_string(),
        "I2AUS008".to_string(),
        "I3GBR083".to_string(),
        "I3GBR085".to_string(),
        "I2IND006".to_string(),
        "I6UNK132".to_string(),
        "I6UNK134".to_string(),
        "I6UNK135".to_string(),
        "I6UNK136".to_string(),
        "I6UNK138".to_string(),
        "I6UNK139".to_string(),
        "I6UNK140".to_string(),
        "I6UNK141".to_string(),
        "I6UNK142".to_string(),
        "I6UNK143".to_string(),
        "I6UNK144".to_string(),
        "I6UNK147".to_string(),
        "I6UNK148".to_string(),
        "I6UNK150".to_string(),
        "I6UNK153".to_string(),
        "I6UNK155".to_string(),
        "I6UNK156".to_string(),
        "I6UNK157".to_string(),
        "I6UNK158".to_string(),
        "I6UNK159".to_string(),
        "I6UNK160".to_string(),
        "I6UNK161".to_string(),
        "I6UNK162".to_string(),
        "I6UNK163".to_string(),
        "I6UNK164".to_string(),
        "I6UNK168".to_string(),
        "I6UNK170".to_string(),
        "I6UNK171".to_string(),
        "I6UNK172".to_string(),
        "I6UNK173".to_string(),
        "I6UNK175".to_string(),
        "I4BRA003".to_string(),
        "I4CHL003".to_string(),
        "I4COL002".to_string(),
        "I2PHL005".to_string(),
        "I3PRT014".to_string(),
        "I2THA003".to_string(),
        "I3GBR097".to_string(),
        "I4JAM001".to_string(),
        "I3GBR082".to_string(),
        "I2IND007".to_string(),
        "I2PHL004".to_string(),
        "I3PRT015".to_string(),
        "I3EST002".to_string(),
        "I2JPN020".to_string(),
        "I2JPN021".to_string(),
        "I2JPN022".to_string(),
        "I2JPN023".to_string(),
        "I2JPN024".to_string(),
        "I2JPN018".to_string(),
        "I2JPN025".to_string(),
        "I2JPN026".to_string(),
        "I2JPN027".to_string(),
        "I2JPN028".to_string(),
        "I2JPN029".to_string(),
        "I2JPN030".to_string(),
        "I2JPN031".to_string(),
        "I2JPN032".to_string(),
        "I2JPN033".to_string(),
        "I2JPN034".to_string(),
        "I2JPN044".to_string(),
        "I2JPN045".to_string(),
        "I2JPN046".to_string(),
        "I2JPN047".to_string(),
        "I2JPN048".to_string(),
        "I2JPN049".to_string(),
        "I2JPN050".to_string(),
        "I2JPN051".to_string(),
        "I3HUN001".to_string(),
        "I1SAU001".to_string(),
        "I3SWE004".to_string(),
        "I3GBR056".to_string(),
        "I4ARG003".to_string(),
        "I3NLD012".to_string(),
        "I3ESP005".to_string(),
        "I3NOR006".to_string(),
        "I6UNK051".to_string(),
        "I6UNK086".to_string(),
        "I2LKA001".to_string(),
        "I2JPN001".to_string(),
        "I3IRL001".to_string(),
        "I3FIN002".to_string(),
        "I3FRA002".to_string(),
        "I3DEU007".to_string(),
        "I3ITA003".to_string(),
        "I6UNK028".to_string(),
        "I6UNK048".to_string(),
        "I6UNK049".to_string(),
        "I6UNK050".to_string(),
        "I6UNK053".to_string(),
        "I6UNK054".to_string(),
        "I6UNK056".to_string(),
        "I3GRC021".to_string(),
        "I2IDN004".to_string(),
        "I2NZL003".to_string(),
        "I3CZE002".to_string(),
        "I2SGP007".to_string(),
        "I2MYS004".to_string(),
        "I2CHN006".to_string(),
        "I2AUS012".to_string(),
        "I3NLD015".to_string(),
        "I3TUR002".to_string(),
        "I3ITA029".to_string(),
        "I2BGD002".to_string(),
        "I6UNK081".to_string(),
        "I3CHE017".to_string(),
        "I3NOR007".to_string(),
        "I3DEU009".to_string(),
        "I3DEU010".to_string(),
        "I3DEU011".to_string(),
        "I3DEU012".to_string(),
        "I3DEU014".to_string(),
        "I3DEU016".to_string(),
        "I3DEU017".to_string(),
        "I3DEU019".to_string(),
        "I3DEU020".to_string(),
        "I3DEU021".to_string(),
        "I3DEU022".to_string(),
        "I3DEU023".to_string(),
        "I3DEU024".to_string(),
        "I3DEU025".to_string(),
        "I3DEU026".to_string(),
        "I3DEU027".to_string(),
        "I3DEU028".to_string(),
        "I6UNK118".to_string(),
        "I6UNK119".to_string(),
        "I6USA004".to_string(),
        "I6UNK121".to_string(),
        "I6UNK125".to_string(),
        "I6USA005".to_string(),
        "I6USA006".to_string(),
        "I6USA007".to_string(),
        "I6USA008".to_string(),
        "I2AUS009".to_string(),
        "I3BEL007".to_string(),
        "I3BEL008".to_string(),
        "I3ESP035".to_string(),
        "I3ESP036".to_string(),
        "I3ESP037".to_string(),
        "I3ESP039".to_string(),
        "I3ESP041".to_string(),
        "I3ESP042".to_string(),
        "I3ESP043".to_string(),
        "I3ESP044".to_string(),
        "I3ESP045".to_string(),
        "I3ESP046".to_string(),
        "I3ESP047".to_string(),
        "I3ESP048".to_string(),
        "I3ESP049".to_string(),
        "I3ESP050".to_string(),
        "I3ESP051".to_string(),
        "I3ESP052".to_string(),
        "I3ESP053".to_string(),
        "I3ESP054".to_string(),
        "I3ESP055".to_string(),
        "I3ESP057".to_string(),
        "I3ESP058".to_string(),
        "I3ESP059".to_string(),
        "I3ESP060".to_string(),
        "I3GBR073".to_string(),
        "I3GBR072".to_string(),
        "I3GBR074".to_string(),
        "I3GBR075".to_string(),
        "I3GBR076".to_string(),
        "I3GBR081".to_string(),
        "I3GBR077".to_string(),
        "I2IND005".to_string(),
        "I2KOR005".to_string(),
        "I3GBR112".to_string(),
        "I3GBR113".to_string(),
        "I3DEU029".to_string(),
        "I2CHN005".to_string(),
        "I3GBR003".to_string(),
        "I3GBR004".to_string(),
        "I3GBR005".to_string(),
        "I3GBR006".to_string(),
        "I3GBR007".to_string(),
        "I3GBR008".to_string(),
        "I3GBR009".to_string(),
        "I3GBR010".to_string(),
        "I3GBR011".to_string(),
        "I3GBR012".to_string(),
        "I3GBR013".to_string(),
        "I3GBR014".to_string(),
        "I3GBR016".to_string(),
        "I3GBR017".to_string(),
        "I3GBR018".to_string(),
        "I3GBR019".to_string(),
        "I3GBR020".to_string(),
        "I3GBR069".to_string(),
        "I3GBR021".to_string(),
        "I3GBR022".to_string(),
        "I3GBR023".to_string(),
        "I3GBR047".to_string(),
        "I3GBR024".to_string(),
        "I3GBR025".to_string(),
        "I3GBR026".to_string(),
        "I3GBR027".to_string(),
        "I3GBR028".to_string(),
        "I3GBR029".to_string(),
        "I3GBR048".to_string(),
        "I3GBR030".to_string(),
        "I3GBR032".to_string(),
        "I2HKG001".to_string(),
        "I2HKG002".to_string(),
        "I2HKG005".to_string(),
        "I2HKG006".to_string(),
        "I3GBR033".to_string(),
        "I3GBR035".to_string(),
        "I3GBR037".to_string(),
        "I3GBR039".to_string(),
        "I3GBR040".to_string(),
        "I3GBR042".to_string(),
        "I3GBR043".to_string(),
        "I3GBR044".to_string(),
        "I3GBR045".to_string(),
        "I2KOR001".to_string(),
        "I2TWN001".to_string(),
        "I3DEU004".to_string(),
        "I3FIN020".to_string(),
        "I2JPN043".to_string(),
        "I2JPN042".to_string(),
        "I2JPN041".to_string(),
        "I2JPN040".to_string(),
        "I6UNK146".to_string(),
        "I6UNK133".to_string(),
        "I2JPN038".to_string(),
        "I2JPN037".to_string(),
        "I2JPN036".to_string(),
        "I2JPN035".to_string(),
        "I6UNK166".to_string(),
        "I2JPN039".to_string(),
        "I6UNK130".to_string(),
        "I6UNK131".to_string(),
        "I6UNK149".to_string(),
        "I6UNK151".to_string(),
        "I6UNK152".to_string(),
        "I6UNK167".to_string(),
        "I6UNK169".to_string(),
        "I3GBR099".to_string(),
        "I3GBR100".to_string(),
        "I3GBR101".to_string(),
        "I3GBR102".to_string(),
        "I3GBR103".to_string(),
        "I3GBR104".to_string(),
        "I3GBR105".to_string(),
        "I3GBR106".to_string(),
        "I3GBR107".to_string(),
        "I3GBR108".to_string(),
        "I3GBR109".to_string(),
        "I3GBR110".to_string(),
        "I3GBR111".to_string(),
        "I3GBR114".to_string(),
        "I3GBR115".to_string(),
        "I3GBR116".to_string(),
        "I3GBR117".to_string(),
        "I3GBR098".to_string(),
    ];
    Some(tic_vec)
}
