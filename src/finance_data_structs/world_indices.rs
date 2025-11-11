use super::{AppError, DuckCrudModel, SurrealCrudModel, ToPolars};
use arrow_array::Array;
use arrow_array::{Date32Array, Float64Array};
use chrono::{Datelike, NaiveDate};
use duckdb::Connection;
use polars::frame::row::Row;
use polars::prelude::*;
use rayon::prelude::*;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::path::Path;
use std::sync::Arc;
use surrealdb::engine::local::Db;
use surrealdb::Surreal;

// Column name lists for GlobalRets schema and parsing
const PORTRET_COLS: [&str; 38] = [
    "aus_portret",
    "aut_portret",
    "bel_portret",
    "bra_portret",
    "che_portret",
    "chl_portret",
    "chn_portret",
    "col_portret",
    "deu_portret",
    "dnk_portret",
    "egy_portret",
    "esp_portret",
    "fin_portret",
    "fra_portret",
    "gbr_portret",
    "grc_portret",
    "hkg_portret",
    "hun_portret",
    "idn_portret",
    "ind_portret",
    "irl_portret",
    "ita_portret",
    "jpn_portret",
    "kor_portret",
    "mex_portret",
    "mys_portret",
    "nld_portret",
    "nor_portret",
    "nzl_portret",
    "phl_portret",
    "pol_portret",
    "prt_portret",
    "sgp_portret",
    "swe_portret",
    "tha_portret",
    "tur_portret",
    "twn_portret",
    "zaf_portret",
];
const PORTRETX_COLS: [&str; 38] = [
    "aus_portretx",
    "aut_portretx",
    "bel_portretx",
    "bra_portretx",
    "che_portretx",
    "chl_portretx",
    "chn_portretx",
    "col_portretx",
    "deu_portretx",
    "dnk_portretx",
    "egy_portretx",
    "esp_portretx",
    "fin_portretx",
    "fra_portretx",
    "gbr_portretx",
    "grc_portretx",
    "hkg_portretx",
    "hun_portretx",
    "idn_portretx",
    "ind_portretx",
    "irl_portretx",
    "ita_portretx",
    "jpn_portretx",
    "kor_portretx",
    "mex_portretx",
    "mys_portretx",
    "nld_portretx",
    "nor_portretx",
    "nzl_portretx",
    "phl_portretx",
    "pol_portretx",
    "prt_portretx",
    "sgp_portretx",
    "swe_portretx",
    "tha_portretx",
    "tur_portretx",
    "twn_portretx",
    "zaf_portretx",
];

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct GlobalRets {
    pub date: NaiveDate,
    pub aus_portret: Option<f64>,
    pub aut_portret: Option<f64>,
    pub bel_portret: Option<f64>,
    pub bra_portret: Option<f64>,
    pub che_portret: Option<f64>,
    pub chl_portret: Option<f64>,
    pub chn_portret: Option<f64>,
    pub col_portret: Option<f64>,
    pub deu_portret: Option<f64>,
    pub dnk_portret: Option<f64>,
    pub egy_portret: Option<f64>,
    pub esp_portret: Option<f64>,
    pub fin_portret: Option<f64>,
    pub fra_portret: Option<f64>,
    pub gbr_portret: Option<f64>,
    pub grc_portret: Option<f64>,
    pub hkg_portret: Option<f64>,
    pub hun_portret: Option<f64>,
    pub idn_portret: Option<f64>,
    pub ind_portret: Option<f64>,
    pub irl_portret: Option<f64>,
    pub ita_portret: Option<f64>,
    pub jpn_portret: Option<f64>,
    pub kor_portret: Option<f64>,
    pub mex_portret: Option<f64>,
    pub mys_portret: Option<f64>,
    pub nld_portret: Option<f64>,
    pub nor_portret: Option<f64>,
    pub nzl_portret: Option<f64>,
    pub phl_portret: Option<f64>,
    pub pol_portret: Option<f64>,
    pub prt_portret: Option<f64>,
    pub sgp_portret: Option<f64>,
    pub swe_portret: Option<f64>,
    pub tha_portret: Option<f64>,
    pub tur_portret: Option<f64>,
    pub twn_portret: Option<f64>,
    pub zaf_portret: Option<f64>,
    pub aus_portretx: Option<f64>,
    pub aut_portretx: Option<f64>,
    pub bel_portretx: Option<f64>,
    pub bra_portretx: Option<f64>,
    pub che_portretx: Option<f64>,
    pub chl_portretx: Option<f64>,
    pub chn_portretx: Option<f64>,
    pub col_portretx: Option<f64>,
    pub deu_portretx: Option<f64>,
    pub dnk_portretx: Option<f64>,
    pub egy_portretx: Option<f64>,
    pub esp_portretx: Option<f64>,
    pub fin_portretx: Option<f64>,
    pub fra_portretx: Option<f64>,
    pub gbr_portretx: Option<f64>,
    pub grc_portretx: Option<f64>,
    pub hkg_portretx: Option<f64>,
    pub hun_portretx: Option<f64>,
    pub idn_portretx: Option<f64>,
    pub ind_portretx: Option<f64>,
    pub irl_portretx: Option<f64>,
    pub ita_portretx: Option<f64>,
    pub jpn_portretx: Option<f64>,
    pub kor_portretx: Option<f64>,
    pub mex_portretx: Option<f64>,
    pub mys_portretx: Option<f64>,
    pub nld_portretx: Option<f64>,
    pub nor_portretx: Option<f64>,
    pub nzl_portretx: Option<f64>,
    pub phl_portretx: Option<f64>,
    pub pol_portretx: Option<f64>,
    pub prt_portretx: Option<f64>,
    pub sgp_portretx: Option<f64>,
    pub swe_portretx: Option<f64>,
    pub tha_portretx: Option<f64>,
    pub tur_portretx: Option<f64>,
    pub twn_portretx: Option<f64>,
    pub zaf_portretx: Option<f64>,
}

impl SurrealCrudModel for GlobalRets {
    fn table() -> &'static str {
        "global"
    }
    fn id_key(&self) -> Option<String> {
        Some(self.date.to_string())
    }
}

impl DuckCrudModel for GlobalRets {
    fn table() -> &'static str {
        // Keep table naming consistent with SurrealDB for simplicity
        "global_sec_indexes_daily"
    }
    fn id_key(&self) -> Option<String> {
        Some(self.date.to_string())
    }
}

impl GlobalRets {
    pub fn from_parquet(path: impl AsRef<Path>) -> Result<Vec<Self>, AppError> {
        let file = std::fs::File::open(path)?;
        let mut df = ParquetReader::new(file).finish()?;
        // Ensure numeric columns are f64
        let casts: Vec<Expr> = PORTRET_COLS
            .iter()
            .chain(PORTRETX_COLS.iter())
            .map(|n| col(*n).cast(DataType::Float64))
            .collect();
        df = df.lazy().with_columns(casts).collect()?;

        let n = df.height();
        println!("rets df height: {}", n);
        let date = df.column("date")?.date()?.clone();
        let mut map: HashMap<&'static str, Float64Chunked> = HashMap::new();
        for &name in PORTRET_COLS.iter().chain(PORTRETX_COLS.iter()) {
            map.insert(name, df.column(name)?.f64()?.clone());
        }

        let origin_ce = 719_163i32;
        let out: Vec<Self> = (0..n)
            .into_par_iter()
            .map(|i| {
                let nd = date
                    .phys
                    .get(i)
                    .map(|days| {
                        chrono::NaiveDate::from_num_days_from_ce_opt(days + origin_ce).unwrap()
                    })
                    .unwrap_or_else(|| chrono::NaiveDate::from_ymd_opt(1970, 1, 1).unwrap());
                let get_f64 = |name: &str| -> Option<f64> { map.get(name).and_then(|a| a.get(i)) };
                Self {
                    date: nd,
                    aus_portret: get_f64("aus_portret"),
                    aut_portret: get_f64("aut_portret"),
                    bel_portret: get_f64("bel_portret"),
                    bra_portret: get_f64("bra_portret"),
                    che_portret: get_f64("che_portret"),
                    chl_portret: get_f64("chl_portret"),
                    chn_portret: get_f64("chn_portret"),
                    col_portret: get_f64("col_portret"),
                    deu_portret: get_f64("deu_portret"),
                    dnk_portret: get_f64("dnk_portret"),
                    egy_portret: get_f64("egy_portret"),
                    esp_portret: get_f64("esp_portret"),
                    fin_portret: get_f64("fin_portret"),
                    fra_portret: get_f64("fra_portret"),
                    gbr_portret: get_f64("gbr_portret"),
                    grc_portret: get_f64("grc_portret"),
                    hkg_portret: get_f64("hkg_portret"),
                    hun_portret: get_f64("hun_portret"),
                    idn_portret: get_f64("idn_portret"),
                    ind_portret: get_f64("ind_portret"),
                    irl_portret: get_f64("irl_portret"),
                    ita_portret: get_f64("ita_portret"),
                    jpn_portret: get_f64("jpn_portret"),
                    kor_portret: get_f64("kor_portret"),
                    mex_portret: get_f64("mex_portret"),
                    mys_portret: get_f64("mys_portret"),
                    nld_portret: get_f64("nld_portret"),
                    nor_portret: get_f64("nor_portret"),
                    nzl_portret: get_f64("nzl_portret"),
                    phl_portret: get_f64("phl_portret"),
                    pol_portret: get_f64("pol_portret"),
                    prt_portret: get_f64("prt_portret"),
                    sgp_portret: get_f64("sgp_portret"),
                    swe_portret: get_f64("swe_portret"),
                    tha_portret: get_f64("tha_portret"),
                    tur_portret: get_f64("tur_portret"),
                    twn_portret: get_f64("twn_portret"),
                    zaf_portret: get_f64("zaf_portret"),
                    aus_portretx: get_f64("aus_portretx"),
                    aut_portretx: get_f64("aut_portretx"),
                    bel_portretx: get_f64("bel_portretx"),
                    bra_portretx: get_f64("bra_portretx"),
                    che_portretx: get_f64("che_portretx"),
                    chl_portretx: get_f64("chl_portretx"),
                    chn_portretx: get_f64("chn_portretx"),
                    col_portretx: get_f64("col_portretx"),
                    deu_portretx: get_f64("deu_portretx"),
                    dnk_portretx: get_f64("dnk_portretx"),
                    egy_portretx: get_f64("egy_portretx"),
                    esp_portretx: get_f64("esp_portretx"),
                    fin_portretx: get_f64("fin_portretx"),
                    fra_portretx: get_f64("fra_portretx"),
                    gbr_portretx: get_f64("gbr_portretx"),
                    grc_portretx: get_f64("grc_portretx"),
                    hkg_portretx: get_f64("hkg_portretx"),
                    hun_portretx: get_f64("hun_portretx"),
                    idn_portretx: get_f64("idn_portretx"),
                    ind_portretx: get_f64("ind_portretx"),
                    irl_portretx: get_f64("irl_portretx"),
                    ita_portretx: get_f64("ita_portretx"),
                    jpn_portretx: get_f64("jpn_portretx"),
                    kor_portretx: get_f64("kor_portretx"),
                    mex_portretx: get_f64("mex_portretx"),
                    mys_portretx: get_f64("mys_portretx"),
                    nld_portretx: get_f64("nld_portretx"),
                    nor_portretx: get_f64("nor_portretx"),
                    nzl_portretx: get_f64("nzl_portretx"),
                    phl_portretx: get_f64("phl_portretx"),
                    pol_portretx: get_f64("pol_portretx"),
                    prt_portretx: get_f64("prt_portretx"),
                    sgp_portretx: get_f64("sgp_portretx"),
                    swe_portretx: get_f64("swe_portretx"),
                    tha_portretx: get_f64("tha_portretx"),
                    tur_portretx: get_f64("tur_portretx"),
                    twn_portretx: get_f64("twn_portretx"),
                    zaf_portretx: get_f64("zaf_portretx"),
                }
            })
            .collect();

        Ok(out)
    }

    pub fn polars_schema() -> Schema {
        let mut fields: Vec<Field> =
            Vec::with_capacity(1 + PORTRET_COLS.len() + PORTRETX_COLS.len());
        fields.push(Field::new("date".into(), DataType::Date));
        for name in PORTRET_COLS.iter() {
            fields.push(Field::new((*name).into(), DataType::Float64));
        }
        for name in PORTRETX_COLS.iter() {
            fields.push(Field::new((*name).into(), DataType::Float64));
        }
        Schema::from_iter(fields)
    }

    pub fn to_row<'a>(self) -> Row<'a> {
        let days: i32 = (self.date.num_days_from_ce() - 719_163) as i32;
        Row::new(vec![
            AnyValue::Date(days),
            self.aus_portret.map_or(AnyValue::Null, AnyValue::Float64),
            self.aut_portret.map_or(AnyValue::Null, AnyValue::Float64),
            self.bel_portret.map_or(AnyValue::Null, AnyValue::Float64),
            self.bra_portret.map_or(AnyValue::Null, AnyValue::Float64),
            self.che_portret.map_or(AnyValue::Null, AnyValue::Float64),
            self.chl_portret.map_or(AnyValue::Null, AnyValue::Float64),
            self.chn_portret.map_or(AnyValue::Null, AnyValue::Float64),
            self.col_portret.map_or(AnyValue::Null, AnyValue::Float64),
            self.deu_portret.map_or(AnyValue::Null, AnyValue::Float64),
            self.dnk_portret.map_or(AnyValue::Null, AnyValue::Float64),
            self.egy_portret.map_or(AnyValue::Null, AnyValue::Float64),
            self.esp_portret.map_or(AnyValue::Null, AnyValue::Float64),
            self.fin_portret.map_or(AnyValue::Null, AnyValue::Float64),
            self.fra_portret.map_or(AnyValue::Null, AnyValue::Float64),
            self.gbr_portret.map_or(AnyValue::Null, AnyValue::Float64),
            self.grc_portret.map_or(AnyValue::Null, AnyValue::Float64),
            self.hkg_portret.map_or(AnyValue::Null, AnyValue::Float64),
            self.hun_portret.map_or(AnyValue::Null, AnyValue::Float64),
            self.idn_portret.map_or(AnyValue::Null, AnyValue::Float64),
            self.ind_portret.map_or(AnyValue::Null, AnyValue::Float64),
            self.irl_portret.map_or(AnyValue::Null, AnyValue::Float64),
            self.ita_portret.map_or(AnyValue::Null, AnyValue::Float64),
            self.jpn_portret.map_or(AnyValue::Null, AnyValue::Float64),
            self.kor_portret.map_or(AnyValue::Null, AnyValue::Float64),
            self.mex_portret.map_or(AnyValue::Null, AnyValue::Float64),
            self.mys_portret.map_or(AnyValue::Null, AnyValue::Float64),
            self.nld_portret.map_or(AnyValue::Null, AnyValue::Float64),
            self.nor_portret.map_or(AnyValue::Null, AnyValue::Float64),
            self.nzl_portret.map_or(AnyValue::Null, AnyValue::Float64),
            self.phl_portret.map_or(AnyValue::Null, AnyValue::Float64),
            self.pol_portret.map_or(AnyValue::Null, AnyValue::Float64),
            self.prt_portret.map_or(AnyValue::Null, AnyValue::Float64),
            self.sgp_portret.map_or(AnyValue::Null, AnyValue::Float64),
            self.swe_portret.map_or(AnyValue::Null, AnyValue::Float64),
            self.tha_portret.map_or(AnyValue::Null, AnyValue::Float64),
            self.tur_portret.map_or(AnyValue::Null, AnyValue::Float64),
            self.twn_portret.map_or(AnyValue::Null, AnyValue::Float64),
            self.zaf_portret.map_or(AnyValue::Null, AnyValue::Float64),
            self.aus_portretx.map_or(AnyValue::Null, AnyValue::Float64),
            self.aut_portretx.map_or(AnyValue::Null, AnyValue::Float64),
            self.bel_portretx.map_or(AnyValue::Null, AnyValue::Float64),
            self.bra_portretx.map_or(AnyValue::Null, AnyValue::Float64),
            self.che_portretx.map_or(AnyValue::Null, AnyValue::Float64),
            self.chl_portretx.map_or(AnyValue::Null, AnyValue::Float64),
            self.chn_portretx.map_or(AnyValue::Null, AnyValue::Float64),
            self.col_portretx.map_or(AnyValue::Null, AnyValue::Float64),
            self.deu_portretx.map_or(AnyValue::Null, AnyValue::Float64),
            self.dnk_portretx.map_or(AnyValue::Null, AnyValue::Float64),
            self.egy_portretx.map_or(AnyValue::Null, AnyValue::Float64),
            self.esp_portretx.map_or(AnyValue::Null, AnyValue::Float64),
            self.fin_portretx.map_or(AnyValue::Null, AnyValue::Float64),
            self.fra_portretx.map_or(AnyValue::Null, AnyValue::Float64),
            self.gbr_portretx.map_or(AnyValue::Null, AnyValue::Float64),
            self.grc_portretx.map_or(AnyValue::Null, AnyValue::Float64),
            self.hkg_portretx.map_or(AnyValue::Null, AnyValue::Float64),
            self.hun_portretx.map_or(AnyValue::Null, AnyValue::Float64),
            self.idn_portretx.map_or(AnyValue::Null, AnyValue::Float64),
            self.ind_portretx.map_or(AnyValue::Null, AnyValue::Float64),
            self.irl_portretx.map_or(AnyValue::Null, AnyValue::Float64),
            self.ita_portretx.map_or(AnyValue::Null, AnyValue::Float64),
            self.jpn_portretx.map_or(AnyValue::Null, AnyValue::Float64),
            self.kor_portretx.map_or(AnyValue::Null, AnyValue::Float64),
            self.mex_portretx.map_or(AnyValue::Null, AnyValue::Float64),
            self.mys_portretx.map_or(AnyValue::Null, AnyValue::Float64),
            self.nld_portretx.map_or(AnyValue::Null, AnyValue::Float64),
            self.nor_portretx.map_or(AnyValue::Null, AnyValue::Float64),
            self.nzl_portretx.map_or(AnyValue::Null, AnyValue::Float64),
            self.phl_portretx.map_or(AnyValue::Null, AnyValue::Float64),
            self.pol_portretx.map_or(AnyValue::Null, AnyValue::Float64),
            self.prt_portretx.map_or(AnyValue::Null, AnyValue::Float64),
            self.sgp_portretx.map_or(AnyValue::Null, AnyValue::Float64),
            self.swe_portretx.map_or(AnyValue::Null, AnyValue::Float64),
            self.tha_portretx.map_or(AnyValue::Null, AnyValue::Float64),
            self.tur_portretx.map_or(AnyValue::Null, AnyValue::Float64),
            self.twn_portretx.map_or(AnyValue::Null, AnyValue::Float64),
            self.zaf_portretx.map_or(AnyValue::Null, AnyValue::Float64),
        ])
    }

    /// Ingest a Parquet file of world index returns into DuckDB via table create/replace.
    /// Returns total rows processed from the Parquet file.
    pub async fn duck_from_parquet(
        conn: Arc<Connection>,
        parquet_path: impl AsRef<Path>,
    ) -> Result<usize, AppError> {
        <Self as DuckCrudModel>::upsert_from_parquet_one_file(
            conn,
            parquet_path,
            None,
            Some(<Self as DuckCrudModel>::table().into()),
        )
        .await
    }

    /// Read rows for a given date range and return them as Polars rows.
    /// Follows the `to_row` pattern: build GlobalRets per row then convert.
    pub async fn read_range<'a>(
        conn: Arc<Connection>,
        date_range: (NaiveDate, NaiveDate),
    ) -> Result<Vec<Row<'a>>, AppError> {
        tokio::task::block_in_place(move || {
            let sql = format!(
                "SELECT * REPLACE (CAST(date AS DATE) AS date) \
                 FROM {} \
                 WHERE CAST(date AS DATE) BETWEEN DATE '{}' AND DATE '{}' \
                 ORDER BY date",
                <Self as DuckCrudModel>::table(),
                date_range.0.to_string(),
                date_range.1.to_string()
            );

            let mut reader = conn.prepare(sql.as_str())?;
            let mut reader = reader.query_arrow([])?; // Arrow RecordBatchReader
            let mut out: Vec<Row<'static>> = Vec::new();

            while let Some(batch) = reader.next() {
                let schema = batch.schema();
                let date_idx = schema.index_of("date").unwrap();
                let date32 = batch
                    .column(date_idx)
                    .as_any()
                    .downcast_ref::<Date32Array>()
                    .unwrap();
                let mut get_f64 = |name: &str, i: usize| -> Option<f64> {
                    let idx = schema.index_of(name).unwrap();
                    let arr = batch
                        .column(idx)
                        .as_any()
                        .downcast_ref::<Float64Array>()
                        .unwrap();
                    if arr.is_null(i) {
                        None
                    } else {
                        Some(arr.value(i))
                    }
                };

                for i in 0..batch.num_rows() {
                    let nd = date32.value_as_date(i).unwrap();
                    let temp = Self {
                        date: nd,
                        aus_portret: get_f64("aus_portret", i),
                        aut_portret: get_f64("aut_portret", i),
                        bel_portret: get_f64("bel_portret", i),
                        bra_portret: get_f64("bra_portret", i),
                        che_portret: get_f64("che_portret", i),
                        chl_portret: get_f64("chl_portret", i),
                        chn_portret: get_f64("chn_portret", i),
                        col_portret: get_f64("col_portret", i),
                        deu_portret: get_f64("deu_portret", i),
                        dnk_portret: get_f64("dnk_portret", i),
                        egy_portret: get_f64("egy_portret", i),
                        esp_portret: get_f64("esp_portret", i),
                        fin_portret: get_f64("fin_portret", i),
                        fra_portret: get_f64("fra_portret", i),
                        gbr_portret: get_f64("gbr_portret", i),
                        grc_portret: get_f64("grc_portret", i),
                        hkg_portret: get_f64("hkg_portret", i),
                        hun_portret: get_f64("hun_portret", i),
                        idn_portret: get_f64("idn_portret", i),
                        ind_portret: get_f64("ind_portret", i),
                        irl_portret: get_f64("irl_portret", i),
                        ita_portret: get_f64("ita_portret", i),
                        jpn_portret: get_f64("jpn_portret", i),
                        kor_portret: get_f64("kor_portret", i),
                        mex_portret: get_f64("mex_portret", i),
                        mys_portret: get_f64("mys_portret", i),
                        nld_portret: get_f64("nld_portret", i),
                        nor_portret: get_f64("nor_portret", i),
                        nzl_portret: get_f64("nzl_portret", i),
                        phl_portret: get_f64("phl_portret", i),
                        pol_portret: get_f64("pol_portret", i),
                        prt_portret: get_f64("prt_portret", i),
                        sgp_portret: get_f64("sgp_portret", i),
                        swe_portret: get_f64("swe_portret", i),
                        tha_portret: get_f64("tha_portret", i),
                        tur_portret: get_f64("tur_portret", i),
                        twn_portret: get_f64("twn_portret", i),
                        zaf_portret: get_f64("zaf_portret", i),
                        aus_portretx: get_f64("aus_portretx", i),
                        aut_portretx: get_f64("aut_portretx", i),
                        bel_portretx: get_f64("bel_portretx", i),
                        bra_portretx: get_f64("bra_portretx", i),
                        che_portretx: get_f64("che_portretx", i),
                        chl_portretx: get_f64("chl_portretx", i),
                        chn_portretx: get_f64("chn_portretx", i),
                        col_portretx: get_f64("col_portretx", i),
                        deu_portretx: get_f64("deu_portretx", i),
                        dnk_portretx: get_f64("dnk_portretx", i),
                        egy_portretx: get_f64("egy_portretx", i),
                        esp_portretx: get_f64("esp_portretx", i),
                        fin_portretx: get_f64("fin_portretx", i),
                        fra_portretx: get_f64("fra_portretx", i),
                        gbr_portretx: get_f64("gbr_portretx", i),
                        grc_portretx: get_f64("grc_portretx", i),
                        hkg_portretx: get_f64("hkg_portretx", i),
                        hun_portretx: get_f64("hun_portretx", i),
                        idn_portretx: get_f64("idn_portretx", i),
                        ind_portretx: get_f64("ind_portretx", i),
                        irl_portretx: get_f64("irl_portretx", i),
                        ita_portretx: get_f64("ita_portretx", i),
                        jpn_portretx: get_f64("jpn_portretx", i),
                        kor_portretx: get_f64("kor_portretx", i),
                        mex_portretx: get_f64("mex_portretx", i),
                        mys_portretx: get_f64("mys_portretx", i),
                        nld_portretx: get_f64("nld_portretx", i),
                        nor_portretx: get_f64("nor_portretx", i),
                        nzl_portretx: get_f64("nzl_portretx", i),
                        phl_portretx: get_f64("phl_portretx", i),
                        pol_portretx: get_f64("pol_portretx", i),
                        prt_portretx: get_f64("prt_portretx", i),
                        sgp_portretx: get_f64("sgp_portretx", i),
                        swe_portretx: get_f64("swe_portretx", i),
                        tha_portretx: get_f64("tha_portretx", i),
                        tur_portretx: get_f64("tur_portretx", i),
                        twn_portretx: get_f64("twn_portretx", i),
                        zaf_portretx: get_f64("zaf_portretx", i),
                    };
                    out.push(temp.to_row());
                }
            }

            Ok::<Vec<Row>, AppError>(out)
        })
    }

    pub async fn create_rets_result(
        data_vec: Vec<GlobalRets>,
        db: &Surreal<Db>,
        nsname: &str,
        dbname: &str,
        batch_size: usize,
        cores: usize,
    ) -> Result<usize, AppError> {
        db.use_ns(nsname).use_db(dbname).await?;
        GlobalRets::insert_vec_concurrent(&db, data_vec, batch_size, cores).await
    }
}

impl ToPolars for GlobalRets {
    fn schema() -> Schema {
        Self::polars_schema()
    }
}
