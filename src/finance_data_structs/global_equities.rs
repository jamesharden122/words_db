use super::{AppError, DuckCrudModel, SurrealCrudModel, ToPolars};
use arrow_array::{Array, Date32Array, Float64Array, Int64Array, StringArray};
use chrono::{Datelike, NaiveDate};
use duckdb::Connection;
use polars::frame::row::Row;
use polars::prelude::*;
use serde::{Deserialize, Serialize};
use std::path::Path;
use std::sync::{Arc, Mutex};
use surrealdb::engine::local::Db;
use surrealdb::Surreal;

/// Wide daily-equities record drawn from global listings data (prices, dividends, id codes).
///
/// Notes on typing:
/// - Many identifier/code columns are strings and may contain NAs; use `Option<String>`.
/// - Dates are optional `NaiveDate`.
/// - Numeric price/qty fields use `Option<f64>` except obvious counters where `i32/i64` fit.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GlobalEquities {
    // Identifiers / classification
    pub permno: Option<i32>,
    pub permco: Option<i32>,
    pub gvkey: Option<i32>,
    pub iid: Option<String>,
    pub cusip: Option<String>,
    pub isin: Option<String>,
    pub sedol: Option<String>,
    pub issuno: Option<String>,
    pub conm: Option<String>,
    pub tpci: Option<String>,
    pub secstat: Option<String>,
    pub epf: Option<String>,
    pub fic: Option<String>,
    pub loc: Option<String>,
    pub curcdd: Option<String>,
    pub curcddv: Option<String>,
    pub exchg: Option<i32>,
    pub hexcd: Option<i32>,
    pub gind: Option<String>,
    pub gsubind: Option<String>,

    // Dates (announcement/record/pay etc.)
    pub date: Option<NaiveDate>,
    pub datadate: Option<NaiveDate>,
    pub anncdate: Option<NaiveDate>,
    pub recorddate: Option<NaiveDate>,
    pub paydate: Option<NaiveDate>,
    pub cheqvpaydate: Option<NaiveDate>,
    pub divdpaydate: Option<NaiveDate>,
    pub divrcpaydate: Option<NaiveDate>,
    pub divsppaydate: Option<NaiveDate>,

    // Times / flags
    pub divdtm: Option<String>,
    pub cheqvtm: Option<String>,
    pub divsptm: Option<String>,
    pub monthend: Option<i32>,

    // Prices / quotes
    pub prc: Option<f64>,
    pub openprc: Option<f64>,
    pub bid: Option<f64>,
    pub ask: Option<f64>,
    pub bidlo: Option<f64>,
    pub askhi: Option<f64>,
    pub prccd: Option<f64>,
    pub prchd: Option<f64>,
    pub prcld: Option<f64>,
    pub prcod: Option<f64>,
    pub prcstd: Option<i32>,
    pub cfacpr: Option<f64>,
    pub cfacshr: Option<f64>,
    pub ajexdi: Option<f64>,

    // Returns / turnover
    pub ret: Option<f64>,
    pub retx: Option<f64>,
    pub trfd: Option<f64>,
    pub vol: Option<i64>,
    pub shrout: Option<i64>,
    pub numtrd: Option<i64>,
    pub cshoc: Option<f64>,
    pub cshtrd: Option<i64>,
    pub qunit: Option<i32>,

    // Cash equivalents / dividends (amounts are in local currency context)
    pub cheqv: Option<f64>,
    pub cheqvgross: Option<f64>,
    pub cheqvnet: Option<f64>,
    pub div: Option<f64>,
    pub divd: Option<f64>,
    pub divdgross: Option<f64>,
    pub divdnet: Option<f64>,
    pub divgross: Option<f64>,
    pub divnet: Option<f64>,
    pub divrc: Option<f64>,
    pub divrcgross: Option<f64>,
    pub divrcnet: Option<f64>,
    pub divsp: Option<f64>,
    pub divspgross: Option<f64>,
    pub divspnet: Option<f64>,

    // Splits
    pub split: Option<f64>,
    pub splitf: Option<f64>,
}

impl SurrealCrudModel for GlobalEquities {
    fn table() -> &'static str {
        "global_equities_daily"
    }
    fn id_key(&self) -> Option<String> {
        match (&self.isin, &self.datadate, &self.iid) {
            (Some(isin), Some(d), Some(iid)) => Some(format!("{}:{}:{}", isin, d, iid)),
            (Some(isin), Some(d), None) => Some(format!("{}:{}", isin, d)),
            _ => None,
        }
    }
}

impl DuckCrudModel for GlobalEquities {
    fn table() -> &'static str {
        "global_equities_daily"
    }
    fn id_key(&self) -> Option<String> {
        <Self as SurrealCrudModel>::id_key(self)
    }
}

impl GlobalEquities {
    /// Non-destructive convenience ingest from a single Parquet file using DuckDB.
    /// Uses the typed-table approach consistent with other modules in this crate.
    pub async fn duck_from_parquet(
        conn: Arc<Mutex<Connection>>,
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

    /// Batched concurrent insert into SurrealDB.
    pub async fn create_result(
        data_vec: Vec<GlobalEquities>,
        db: &Surreal<Db>,
        nsname: &str,
        dbname: &str,
        batch_size: usize,
        cores: usize,
    ) -> Result<usize, AppError> {
        db.use_ns(nsname).use_db(dbname).await?;
        GlobalEquities::insert_vec_concurrent(db, data_vec, batch_size, cores).await
    }
}

impl GlobalEquities {
    /// Read rows for a given datadate range and return them as Polars Rows.
    pub async fn read_range<'a>(
        conn: Arc<Mutex<Connection>>,
        date_range: (NaiveDate, NaiveDate),
    ) -> Result<Vec<Row<'a>>, AppError> {
        tokio::task::spawn_blocking(move || {
            let table = <Self as DuckCrudModel>::table();
            let sql = format!(
                "SELECT \
                    CAST(permno AS BIGINT)    AS permno, \
                    CAST(permco AS BIGINT)    AS permco, \
                    CAST(gvkey  AS BIGINT)    AS gvkey, \
                    CAST(iid    AS VARCHAR)   AS iid, \
                    CAST(cusip  AS VARCHAR)   AS cusip, \
                    CAST(isin   AS VARCHAR)   AS isin, \
                    CAST(sedol  AS VARCHAR)   AS sedol, \
                    CAST(issuno AS VARCHAR)   AS issuno, \
                    CAST(conm   AS VARCHAR)   AS conm, \
                    CAST(tpci   AS VARCHAR)   AS tpci, \
                    CAST(secstat AS VARCHAR)  AS secstat, \
                    CAST(epf     AS VARCHAR)  AS epf, \
                    CAST(fic     AS VARCHAR)  AS fic, \
                    CAST(loc     AS VARCHAR)  AS loc, \
                    CAST(curcdd  AS VARCHAR)  AS curcdd, \
                    CAST(curcddv AS VARCHAR)  AS curcddv, \
                    CAST(exchg   AS BIGINT)   AS exchg, \
                    CAST(hexcd   AS BIGINT)   AS hexcd, \
                    CAST(gind    AS VARCHAR)  AS gind, \
                    CAST(gsubind AS VARCHAR)  AS gsubind, \
                    CAST(date        AS DATE) AS date, \
                    CAST(datadate    AS DATE) AS datadate, \
                    CAST(anncdate    AS DATE) AS anncdate, \
                    CAST(recorddate  AS DATE) AS recorddate, \
                    CAST(paydate     AS DATE) AS paydate, \
                    CAST(cheqvpaydate AS DATE) AS cheqvpaydate, \
                    CAST(divdpaydate  AS DATE) AS divdpaydate, \
                    CAST(divrcpaydate AS DATE) AS divrcpaydate, \
                    CAST(divsppaydate AS DATE) AS divsppaydate, \
                    CAST(divdtm   AS VARCHAR)  AS divdtm, \
                    CAST(cheqvtm  AS VARCHAR)  AS cheqvtm, \
                    CAST(divsptm  AS VARCHAR)  AS divsptm, \
                    CAST(monthend AS BIGINT)   AS monthend, \
                    CAST(prc      AS DOUBLE)   AS prc, \
                    CAST(openprc  AS DOUBLE)   AS openprc, \
                    CAST(bid      AS DOUBLE)   AS bid, \
                    CAST(ask      AS DOUBLE)   AS ask, \
                    CAST(bidlo    AS DOUBLE)   AS bidlo, \
                    CAST(askhi    AS DOUBLE)   AS askhi, \
                    CAST(prccd    AS DOUBLE)   AS prccd, \
                    CAST(prchd    AS DOUBLE)   AS prchd, \
                    CAST(prcld    AS DOUBLE)   AS prcld, \
                    CAST(prcod    AS DOUBLE)   AS prcod, \
                    CAST(prcstd   AS BIGINT)   AS prcstd, \
                    CAST(cfacpr   AS DOUBLE)   AS cfacpr, \
                    CAST(cfacshr  AS DOUBLE)   AS cfacshr, \
                    CAST(ajexdi   AS DOUBLE)   AS ajexdi, \
                    CAST(ret      AS DOUBLE)   AS ret, \
                    CAST(retx     AS DOUBLE)   AS retx, \
                    CAST(trfd     AS DOUBLE)   AS trfd, \
                    CAST(vol      AS BIGINT)   AS vol, \
                    CAST(shrout   AS BIGINT)   AS shrout, \
                    CAST(numtrd   AS BIGINT)   AS numtrd, \
                    CAST(cshoc    AS DOUBLE)   AS cshoc, \
                    CAST(cshtrd   AS BIGINT)   AS cshtrd, \
                    CAST(qunit    AS BIGINT)   AS qunit, \
                    CAST(cheqv       AS DOUBLE) AS cheqv, \
                    CAST(cheqvgross  AS DOUBLE) AS cheqvgross, \
                    CAST(cheqvnet    AS DOUBLE) AS cheqvnet, \
                    CAST(div         AS DOUBLE) AS div, \
                    CAST(divd        AS DOUBLE) AS divd, \
                    CAST(divdgross   AS DOUBLE) AS divdgross, \
                    CAST(divdnet     AS DOUBLE) AS divdnet, \
                    CAST(divgross    AS DOUBLE) AS divgross, \
                    CAST(divnet      AS DOUBLE) AS divnet, \
                    CAST(divrc       AS DOUBLE) AS divrc, \
                    CAST(divrcgross  AS DOUBLE) AS divrcgross, \
                    CAST(divrcnet    AS DOUBLE) AS divrcnet, \
                    CAST(divsp       AS DOUBLE) AS divsp, \
                    CAST(divspgross  AS DOUBLE) AS divspgross, \
                    CAST(divspnet    AS DOUBLE) AS divspnet, \
                    CAST(split       AS DOUBLE) AS split, \
                    CAST(splitf      AS DOUBLE) AS splitf \
                FROM {table} \
                WHERE CAST(datadate AS DATE) BETWEEN DATE '{start}' AND DATE '{end}' \
                ORDER BY datadate",
                table = table,
                start = date_range.0.to_string(),
                end = date_range.1.to_string()
            );

            let conn_guard = conn.lock().expect("duckdb connection mutex poisoned");
            let mut stmt = conn_guard.prepare(sql.as_str())?;
            let mut reader = stmt.query_arrow([])?;
            let mut out: Vec<Row<'static>> = Vec::new();

            while let Some(batch) = reader.next() {
                let schema = batch.schema();
                // Helper to get typed arrays
                let s = |name: &str| -> &StringArray {
                    batch
                        .column(schema.index_of(name).unwrap())
                        .as_any()
                        .downcast_ref::<StringArray>()
                        .unwrap()
                };
                let f = |name: &str| -> &Float64Array {
                    batch
                        .column(schema.index_of(name).unwrap())
                        .as_any()
                        .downcast_ref::<Float64Array>()
                        .unwrap()
                };
                let i = |name: &str| -> &Int64Array {
                    batch
                        .column(schema.index_of(name).unwrap())
                        .as_any()
                        .downcast_ref::<Int64Array>()
                        .unwrap()
                };
                let d = |name: &str| -> &Date32Array {
                    batch
                        .column(schema.index_of(name).unwrap())
                        .as_any()
                        .downcast_ref::<Date32Array>()
                        .unwrap()
                };

                // Pre-bind arrays
                let permno = i("permno");
                let permco = i("permco");
                let gvkey = i("gvkey");
                let iid = s("iid");
                let cusip = s("cusip");
                let isin = s("isin");
                let sedol = s("sedol");
                let issuno = s("issuno");
                let conm = s("conm");
                let tpci = s("tpci");
                let secstat = s("secstat");
                let epf = s("epf");
                let fic = s("fic");
                let loc = s("loc");
                let curcdd = s("curcdd");
                let curcddv = s("curcddv");
                let exchg = i("exchg");
                let hexcd = i("hexcd");
                let gind = s("gind");
                let gsubind = s("gsubind");

                let date = d("date");
                let datadate = d("datadate");
                let anncdate = d("anncdate");
                let recorddate = d("recorddate");
                let paydate = d("paydate");
                let cheqvpaydate = d("cheqvpaydate");
                let divdpaydate = d("divdpaydate");
                let divrcpaydate = d("divrcpaydate");
                let divsppaydate = d("divsppaydate");

                let divdtm = s("divdtm");
                let cheqvtm = s("cheqvtm");
                let divsptm = s("divsptm");
                let monthend = i("monthend");

                let prc = f("prc");
                let openprc = f("openprc");
                let bid = f("bid");
                let ask = f("ask");
                let bidlo = f("bidlo");
                let askhi = f("askhi");
                let prccd = f("prccd");
                let prchd = f("prchd");
                let prcld = f("prcld");
                let prcod = f("prcod");
                let prcstd = i("prcstd");
                let cfacpr = f("cfacpr");
                let cfacshr = f("cfacshr");
                let ajexdi = f("ajexdi");
                let ret = f("ret");
                let retx = f("retx");
                let trfd = f("trfd");
                let vol = i("vol");
                let shrout = i("shrout");
                let numtrd = i("numtrd");
                let cshoc = f("cshoc");
                let cshtrd = i("cshtrd");
                let qunit = i("qunit");
                let cheqv = f("cheqv");
                let cheqvgross = f("cheqvgross");
                let cheqvnet = f("cheqvnet");
                let div = f("div");
                let divd = f("divd");
                let divdgross = f("divdgross");
                let divdnet = f("divdnet");
                let divgross = f("divgross");
                let divnet = f("divnet");
                let divrc = f("divrc");
                let divrcgross = f("divrcgross");
                let divrcnet = f("divrcnet");
                let divsp = f("divsp");
                let divspgross = f("divspgross");
                let divspnet = f("divspnet");
                let split = f("split");
                let splitf = f("splitf");

                for row_i in 0..batch.num_rows() {
                    // Helper closures to get Option<T>
                    let gs = |arr: &StringArray| -> Option<String> {
                        if arr.is_null(row_i) {
                            None
                        } else {
                            Some(arr.value(row_i).to_string())
                        }
                    };
                    let gi32 = |arr: &Int64Array| -> Option<i32> {
                        if arr.is_null(row_i) {
                            None
                        } else {
                            Some(arr.value(row_i) as i32)
                        }
                    };
                    let gi64 = |arr: &Int64Array| -> Option<i64> {
                        if arr.is_null(row_i) {
                            None
                        } else {
                            Some(arr.value(row_i))
                        }
                    };
                    let gf = |arr: &Float64Array| -> Option<f64> {
                        if arr.is_null(row_i) {
                            None
                        } else {
                            Some(arr.value(row_i))
                        }
                    };
                    let gd = |arr: &Date32Array| -> Option<NaiveDate> {
                        if arr.is_null(row_i) {
                            None
                        } else {
                            arr.value_as_date(row_i)
                        }
                    };

                    let temp = Self {
                        permno: gi32(permno),
                        permco: gi32(permco),
                        gvkey: gi32(gvkey),
                        iid: gs(iid),
                        cusip: gs(cusip),
                        isin: gs(isin),
                        sedol: gs(sedol),
                        issuno: gs(issuno),
                        conm: gs(conm),
                        tpci: gs(tpci),
                        secstat: gs(secstat),
                        epf: gs(epf),
                        fic: gs(fic),
                        loc: gs(loc),
                        curcdd: gs(curcdd),
                        curcddv: gs(curcddv),
                        exchg: gi32(exchg),
                        hexcd: gi32(hexcd),
                        gind: gs(gind),
                        gsubind: gs(gsubind),
                        date: gd(date),
                        datadate: gd(datadate),
                        anncdate: gd(anncdate),
                        recorddate: gd(recorddate),
                        paydate: gd(paydate),
                        cheqvpaydate: gd(cheqvpaydate),
                        divdpaydate: gd(divdpaydate),
                        divrcpaydate: gd(divrcpaydate),
                        divsppaydate: gd(divsppaydate),
                        divdtm: gs(divdtm),
                        cheqvtm: gs(cheqvtm),
                        divsptm: gs(divsptm),
                        monthend: gi32(monthend),
                        prc: gf(prc),
                        openprc: gf(openprc),
                        bid: gf(bid),
                        ask: gf(ask),
                        bidlo: gf(bidlo),
                        askhi: gf(askhi),
                        prccd: gf(prccd),
                        prchd: gf(prchd),
                        prcld: gf(prcld),
                        prcod: gf(prcod),
                        prcstd: gi32(prcstd),
                        cfacpr: gf(cfacpr),
                        cfacshr: gf(cfacshr),
                        ajexdi: gf(ajexdi),
                        ret: gf(ret),
                        retx: gf(retx),
                        trfd: gf(trfd),
                        vol: gi64(vol),
                        shrout: gi64(shrout),
                        numtrd: gi64(numtrd),
                        cshoc: gf(cshoc),
                        cshtrd: gi64(cshtrd),
                        qunit: gi32(qunit),
                        cheqv: gf(cheqv),
                        cheqvgross: gf(cheqvgross),
                        cheqvnet: gf(cheqvnet),
                        div: gf(div),
                        divd: gf(divd),
                        divdgross: gf(divdgross),
                        divdnet: gf(divdnet),
                        divgross: gf(divgross),
                        divnet: gf(divnet),
                        divrc: gf(divrc),
                        divrcgross: gf(divrcgross),
                        divrcnet: gf(divrcnet),
                        divsp: gf(divsp),
                        divspgross: gf(divspgross),
                        divspnet: gf(divspnet),
                        split: gf(split),
                        splitf: gf(splitf),
                    };
                    out.push(temp.to_row());
                }
            }

            Ok::<Vec<Row>, AppError>(out)
        })
        .await?
    }
}

impl GlobalEquities {
    fn date_to_any(d: Option<NaiveDate>) -> AnyValue<'static> {
        match d {
            Some(nd) => {
                let days: i32 = (nd.num_days_from_ce() - 719_163) as i32;
                AnyValue::Date(days)
            }
            None => AnyValue::Null,
        }
    }

    /// Convert to Polars Row following `polars_schema` column order.
    pub fn to_row<'a>(self) -> Row<'a> {
        Row::new(vec![
            self.permno.map_or(AnyValue::Null, |v| AnyValue::Int32(v)),
            self.permco.map_or(AnyValue::Null, |v| AnyValue::Int32(v)),
            self.gvkey.map_or(AnyValue::Null, |v| AnyValue::Int32(v)),
            self.iid
                .map(|v| AnyValue::StringOwned(v.into()))
                .unwrap_or(AnyValue::Null),
            self.cusip
                .map(|v| AnyValue::StringOwned(v.into()))
                .unwrap_or(AnyValue::Null),
            self.isin
                .map(|v| AnyValue::StringOwned(v.into()))
                .unwrap_or(AnyValue::Null),
            self.sedol
                .map(|v| AnyValue::StringOwned(v.into()))
                .unwrap_or(AnyValue::Null),
            self.issuno
                .map(|v| AnyValue::StringOwned(v.into()))
                .unwrap_or(AnyValue::Null),
            self.conm
                .map(|v| AnyValue::StringOwned(v.into()))
                .unwrap_or(AnyValue::Null),
            self.tpci
                .map(|v| AnyValue::StringOwned(v.into()))
                .unwrap_or(AnyValue::Null),
            self.secstat
                .map(|v| AnyValue::StringOwned(v.into()))
                .unwrap_or(AnyValue::Null),
            self.epf
                .map(|v| AnyValue::StringOwned(v.into()))
                .unwrap_or(AnyValue::Null),
            self.fic
                .map(|v| AnyValue::StringOwned(v.into()))
                .unwrap_or(AnyValue::Null),
            self.loc
                .map(|v| AnyValue::StringOwned(v.into()))
                .unwrap_or(AnyValue::Null),
            self.curcdd
                .map(|v| AnyValue::StringOwned(v.into()))
                .unwrap_or(AnyValue::Null),
            self.curcddv
                .map(|v| AnyValue::StringOwned(v.into()))
                .unwrap_or(AnyValue::Null),
            self.exchg.map_or(AnyValue::Null, |v| AnyValue::Int32(v)),
            self.hexcd.map_or(AnyValue::Null, |v| AnyValue::Int32(v)),
            self.gind
                .map(|v| AnyValue::StringOwned(v.into()))
                .unwrap_or(AnyValue::Null),
            self.gsubind
                .map(|v| AnyValue::StringOwned(v.into()))
                .unwrap_or(AnyValue::Null),
            Self::date_to_any(self.date),
            Self::date_to_any(self.datadate),
            Self::date_to_any(self.anncdate),
            Self::date_to_any(self.recorddate),
            Self::date_to_any(self.paydate),
            Self::date_to_any(self.cheqvpaydate),
            Self::date_to_any(self.divdpaydate),
            Self::date_to_any(self.divrcpaydate),
            Self::date_to_any(self.divsppaydate),
            self.divdtm
                .map(|v| AnyValue::StringOwned(v.into()))
                .unwrap_or(AnyValue::Null),
            self.cheqvtm
                .map(|v| AnyValue::StringOwned(v.into()))
                .unwrap_or(AnyValue::Null),
            self.divsptm
                .map(|v| AnyValue::StringOwned(v.into()))
                .unwrap_or(AnyValue::Null),
            self.monthend.map_or(AnyValue::Null, |v| AnyValue::Int32(v)),
            self.prc.map_or(AnyValue::Null, AnyValue::Float64),
            self.openprc.map_or(AnyValue::Null, AnyValue::Float64),
            self.bid.map_or(AnyValue::Null, AnyValue::Float64),
            self.ask.map_or(AnyValue::Null, AnyValue::Float64),
            self.bidlo.map_or(AnyValue::Null, AnyValue::Float64),
            self.askhi.map_or(AnyValue::Null, AnyValue::Float64),
            self.prccd.map_or(AnyValue::Null, AnyValue::Float64),
            self.prchd.map_or(AnyValue::Null, AnyValue::Float64),
            self.prcld.map_or(AnyValue::Null, AnyValue::Float64),
            self.prcod.map_or(AnyValue::Null, AnyValue::Float64),
            self.prcstd.map_or(AnyValue::Null, |v| AnyValue::Int32(v)),
            self.cfacpr.map_or(AnyValue::Null, AnyValue::Float64),
            self.cfacshr.map_or(AnyValue::Null, AnyValue::Float64),
            self.ajexdi.map_or(AnyValue::Null, AnyValue::Float64),
            self.ret.map_or(AnyValue::Null, AnyValue::Float64),
            self.retx.map_or(AnyValue::Null, AnyValue::Float64),
            self.trfd.map_or(AnyValue::Null, AnyValue::Float64),
            self.vol.map_or(AnyValue::Null, |v| AnyValue::Int64(v)),
            self.shrout.map_or(AnyValue::Null, |v| AnyValue::Int64(v)),
            self.numtrd.map_or(AnyValue::Null, |v| AnyValue::Int64(v)),
            self.cshoc.map_or(AnyValue::Null, AnyValue::Float64),
            self.cshtrd.map_or(AnyValue::Null, |v| AnyValue::Int64(v)),
            self.qunit.map_or(AnyValue::Null, |v| AnyValue::Int32(v)),
            self.cheqv.map_or(AnyValue::Null, AnyValue::Float64),
            self.cheqvgross.map_or(AnyValue::Null, AnyValue::Float64),
            self.cheqvnet.map_or(AnyValue::Null, AnyValue::Float64),
            self.div.map_or(AnyValue::Null, AnyValue::Float64),
            self.divd.map_or(AnyValue::Null, AnyValue::Float64),
            self.divdgross.map_or(AnyValue::Null, AnyValue::Float64),
            self.divdnet.map_or(AnyValue::Null, AnyValue::Float64),
            self.divgross.map_or(AnyValue::Null, AnyValue::Float64),
            self.divnet.map_or(AnyValue::Null, AnyValue::Float64),
            self.divrc.map_or(AnyValue::Null, AnyValue::Float64),
            self.divrcgross.map_or(AnyValue::Null, AnyValue::Float64),
            self.divrcnet.map_or(AnyValue::Null, AnyValue::Float64),
            self.divsp.map_or(AnyValue::Null, AnyValue::Float64),
            self.divspgross.map_or(AnyValue::Null, AnyValue::Float64),
            self.divspnet.map_or(AnyValue::Null, AnyValue::Float64),
            self.split.map_or(AnyValue::Null, AnyValue::Float64),
            self.splitf.map_or(AnyValue::Null, AnyValue::Float64),
        ])
    }

    pub fn polars_schema() -> Schema {
        Schema::from_iter([
            Field::new("permno".into(), DataType::Int32),
            Field::new("permco".into(), DataType::Int32),
            Field::new("gvkey".into(), DataType::Int32),
            Field::new("iid".into(), DataType::String),
            Field::new("cusip".into(), DataType::String),
            Field::new("isin".into(), DataType::String),
            Field::new("sedol".into(), DataType::String),
            Field::new("issuno".into(), DataType::String),
            Field::new("conm".into(), DataType::String),
            Field::new("tpci".into(), DataType::String),
            Field::new("secstat".into(), DataType::String),
            Field::new("epf".into(), DataType::String),
            Field::new("fic".into(), DataType::String),
            Field::new("loc".into(), DataType::String),
            Field::new("curcdd".into(), DataType::String),
            Field::new("curcddv".into(), DataType::String),
            Field::new("exchg".into(), DataType::Int32),
            Field::new("hexcd".into(), DataType::Int32),
            Field::new("gind".into(), DataType::String),
            Field::new("gsubind".into(), DataType::String),
            Field::new("date".into(), DataType::Date),
            Field::new("datadate".into(), DataType::Date),
            Field::new("anncdate".into(), DataType::Date),
            Field::new("recorddate".into(), DataType::Date),
            Field::new("paydate".into(), DataType::Date),
            Field::new("cheqvpaydate".into(), DataType::Date),
            Field::new("divdpaydate".into(), DataType::Date),
            Field::new("divrcpaydate".into(), DataType::Date),
            Field::new("divsppaydate".into(), DataType::Date),
            Field::new("divdtm".into(), DataType::String),
            Field::new("cheqvtm".into(), DataType::String),
            Field::new("divsptm".into(), DataType::String),
            Field::new("monthend".into(), DataType::Int32),
            Field::new("prc".into(), DataType::Float64),
            Field::new("openprc".into(), DataType::Float64),
            Field::new("bid".into(), DataType::Float64),
            Field::new("ask".into(), DataType::Float64),
            Field::new("bidlo".into(), DataType::Float64),
            Field::new("askhi".into(), DataType::Float64),
            Field::new("prccd".into(), DataType::Float64),
            Field::new("prchd".into(), DataType::Float64),
            Field::new("prcld".into(), DataType::Float64),
            Field::new("prcod".into(), DataType::Float64),
            Field::new("prcstd".into(), DataType::Int32),
            Field::new("cfacpr".into(), DataType::Float64),
            Field::new("cfacshr".into(), DataType::Float64),
            Field::new("ajexdi".into(), DataType::Float64),
            Field::new("ret".into(), DataType::Float64),
            Field::new("retx".into(), DataType::Float64),
            Field::new("trfd".into(), DataType::Float64),
            Field::new("vol".into(), DataType::Int64),
            Field::new("shrout".into(), DataType::Int64),
            Field::new("numtrd".into(), DataType::Int64),
            Field::new("cshoc".into(), DataType::Float64),
            Field::new("cshtrd".into(), DataType::Int64),
            Field::new("qunit".into(), DataType::Int32),
            Field::new("cheqv".into(), DataType::Float64),
            Field::new("cheqvgross".into(), DataType::Float64),
            Field::new("cheqvnet".into(), DataType::Float64),
            Field::new("div".into(), DataType::Float64),
            Field::new("divd".into(), DataType::Float64),
            Field::new("divdgross".into(), DataType::Float64),
            Field::new("divdnet".into(), DataType::Float64),
            Field::new("divgross".into(), DataType::Float64),
            Field::new("divnet".into(), DataType::Float64),
            Field::new("divrc".into(), DataType::Float64),
            Field::new("divrcgross".into(), DataType::Float64),
            Field::new("divrcnet".into(), DataType::Float64),
            Field::new("divsp".into(), DataType::Float64),
            Field::new("divspgross".into(), DataType::Float64),
            Field::new("divspnet".into(), DataType::Float64),
            Field::new("split".into(), DataType::Float64),
            Field::new("splitf".into(), DataType::Float64),
        ])
    }
}

impl ToPolars for GlobalEquities {
    fn schema() -> Schema {
        Self::polars_schema()
    }
}

/// Monthly global equities record (Compustat Global monthly security dataset).
///
/// Column set mirrors the provided monthly sample (R `str(df_sec)`), keeping
/// string/number/date optionality consistent with ingest behavior.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GlobalEquitiesMonthly {
    pub ajexm: Option<f64>,
    pub ajpm: Option<f64>,
    pub conm: Option<String>,
    pub cshtrm: Option<f64>,
    pub curcddvm: Option<String>,
    pub curcdm: Option<String>,
    pub datadate: Option<NaiveDate>,
    pub dvpspm: Option<f64>,
    pub dvpspm_fn: Option<String>,
    pub dvpsxm: Option<f64>,
    pub dvpsxm_fn: Option<String>,
    pub epf: Option<String>,
    pub exchg: Option<i32>,
    pub fic: Option<String>,
    pub gvkey: Option<String>,
    pub iid: Option<String>,
    pub isalrt: Option<String>,
    pub isin: Option<String>,
    pub loc: Option<String>,
    pub prccm: Option<f64>,
    pub prchm: Option<f64>,
    pub prclm: Option<f64>,
    pub secstat: Option<String>,
    pub sedol: Option<String>,
    pub tpci: Option<String>,
}

impl SurrealCrudModel for GlobalEquitiesMonthly {
    fn table() -> &'static str {
        "global_equities_monthly"
    }
    fn id_key(&self) -> Option<String> {
        match (&self.isin, &self.datadate, &self.iid) {
            (Some(isin), Some(d), Some(iid)) => Some(format!("{}:{}:{}", isin, d, iid)),
            (Some(isin), Some(d), None) => Some(format!("{}:{}", isin, d)),
            _ => None,
        }
    }
}

impl DuckCrudModel for GlobalEquitiesMonthly {
    fn table() -> &'static str {
        "global_equities_monthly"
    }
    fn id_key(&self) -> Option<String> {
        <Self as SurrealCrudModel>::id_key(self)
    }
}

impl GlobalEquitiesMonthly {
    /// Convenience ingest: create/replace DuckDB table from a Parquet file.
    pub async fn duck_from_parquet(
        conn: Arc<Mutex<Connection>>,
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

    /// Batched concurrent insert into SurrealDB.
    pub async fn create_result(
        data_vec: Vec<GlobalEquitiesMonthly>,
        db: &Surreal<Db>,
        nsname: &str,
        dbname: &str,
        batch_size: usize,
        cores: usize,
    ) -> Result<usize, AppError> {
        db.use_ns(nsname).use_db(dbname).await?;
        GlobalEquitiesMonthly::insert_vec_concurrent(db, data_vec, batch_size, cores).await
    }
}

impl GlobalEquitiesMonthly {
    /// Read rows for a given datadate range and return them as Polars Rows.
    pub async fn read_range<'a>(
        conn: Arc<Mutex<Connection>>,
        date_range: (NaiveDate, NaiveDate),
    ) -> Result<Vec<Row<'a>>, AppError> {
        tokio::task::spawn_blocking(move || {
            let table = <Self as DuckCrudModel>::table();
            let sql = format!(
                "SELECT \
                    CAST(ajexm AS DOUBLE)      AS ajexm, \
                    CAST(ajpm AS DOUBLE)       AS ajpm, \
                    CAST(conm AS VARCHAR)       AS conm, \
                    CAST(cshtrm AS DOUBLE)     AS cshtrm, \
                    CAST(curcddvm AS VARCHAR)   AS curcddvm, \
                    CAST(curcdm AS VARCHAR)     AS curcdm, \
                    CAST(datadate AS DATE)      AS datadate, \
                    CAST(dvpspm AS DOUBLE)     AS dvpspm, \
                    CAST(dvpspm_fn AS VARCHAR)  AS dvpspm_fn, \
                    CAST(dvpsxm AS DOUBLE)     AS dvpsxm, \
                    CAST(dvpsxm_fn AS VARCHAR)  AS dvpsxm_fn, \
                    CAST(epf AS VARCHAR)         AS epf, \
                    CAST(exchg AS BIGINT)        AS exchg, \
                    CAST(fic AS VARCHAR)         AS fic, \
                    CAST(gvkey AS VARCHAR)       AS gvkey, \
                    CAST(iid AS VARCHAR)         AS iid, \
                    CAST(isalrt AS VARCHAR)      AS isalrt, \
                    CAST(isin AS VARCHAR)        AS isin, \
                    CAST(loc AS VARCHAR)         AS loc, \
                    CAST(prccm AS DOUBLE)       AS prccm, \
                    CAST(prchm AS DOUBLE)       AS prchm, \
                    CAST(prclm AS DOUBLE)       AS prclm, \
                    CAST(secstat AS VARCHAR)     AS secstat, \
                    CAST(sedol AS VARCHAR)       AS sedol, \
                    CAST(tpci AS VARCHAR)        AS tpci \
                FROM {table} \
                WHERE CAST(datadate AS DATE) BETWEEN DATE '{start}' AND DATE '{end}' \
                ORDER BY datadate",
                table = table,
                start = date_range.0.to_string(),
                end = date_range.1.to_string()
            );

            let conn_guard = conn.lock().expect("duckdb connection mutex poisoned");
            let mut stmt = conn_guard.prepare(sql.as_str())?;
            let mut reader = stmt.query_arrow([])?;
            let mut out: Vec<Row<'static>> = Vec::new();

            while let Some(batch) = reader.next() {
                let schema = batch.schema();
                let s = |name: &str| -> &StringArray {
                    batch
                        .column(schema.index_of(name).unwrap())
                        .as_any()
                        .downcast_ref::<StringArray>()
                        .unwrap()
                };
                let f = |name: &str| -> &Float64Array {
                    batch
                        .column(schema.index_of(name).unwrap())
                        .as_any()
                        .downcast_ref::<Float64Array>()
                        .unwrap()
                };
                let i = |name: &str| -> &Int64Array {
                    batch
                        .column(schema.index_of(name).unwrap())
                        .as_any()
                        .downcast_ref::<Int64Array>()
                        .unwrap()
                };
                let d = |name: &str| -> &Date32Array {
                    batch
                        .column(schema.index_of(name).unwrap())
                        .as_any()
                        .downcast_ref::<Date32Array>()
                        .unwrap()
                };

                // Pre-bind arrays in SELECT order
                let ajexm = f("ajexm");
                let ajpm = f("ajpm");
                let conm = s("conm");
                let cshtrm = f("cshtrm");
                let curcddvm = s("curcddvm");
                let curcdm = s("curcdm");
                let datadate = d("datadate");
                let dvpspm = f("dvpspm");
                let dvpspm_fn = s("dvpspm_fn");
                let dvpsxm = f("dvpsxm");
                let dvpsxm_fn = s("dvpsxm_fn");
                let epf = s("epf");
                let exchg = i("exchg");
                let fic = s("fic");
                let gvkey = s("gvkey");
                let iid = s("iid");
                let isalrt = s("isalrt");
                let isin = s("isin");
                let loc = s("loc");
                let prccm = f("prccm");
                let prchm = f("prchm");
                let prclm = f("prclm");
                let secstat = s("secstat");
                let sedol = s("sedol");
                let tpci = s("tpci");

                for row_i in 0..batch.num_rows() {
                    let gs = |arr: &StringArray| -> Option<String> {
                        if arr.is_null(row_i) {
                            None
                        } else {
                            Some(arr.value(row_i).to_string())
                        }
                    };
                    let gf = |arr: &Float64Array| -> Option<f64> {
                        if arr.is_null(row_i) {
                            None
                        } else {
                            Some(arr.value(row_i))
                        }
                    };
                    let gi32 = |arr: &Int64Array| -> Option<i32> {
                        if arr.is_null(row_i) {
                            None
                        } else {
                            Some(arr.value(row_i) as i32)
                        }
                    };
                    let gd = |arr: &Date32Array| -> Option<NaiveDate> {
                        if arr.is_null(row_i) {
                            None
                        } else {
                            arr.value_as_date(row_i)
                        }
                    };

                    let temp = Self {
                        ajexm: gf(ajexm),
                        ajpm: gf(ajpm),
                        conm: gs(conm),
                        cshtrm: gf(cshtrm),
                        curcddvm: gs(curcddvm),
                        curcdm: gs(curcdm),
                        datadate: gd(datadate),
                        dvpspm: gf(dvpspm),
                        dvpspm_fn: gs(dvpspm_fn),
                        dvpsxm: gf(dvpsxm),
                        dvpsxm_fn: gs(dvpsxm_fn),
                        epf: gs(epf),
                        exchg: gi32(exchg),
                        fic: gs(fic),
                        gvkey: gs(gvkey),
                        iid: gs(iid),
                        isalrt: gs(isalrt),
                        isin: gs(isin),
                        loc: gs(loc),
                        prccm: gf(prccm),
                        prchm: gf(prchm),
                        prclm: gf(prclm),
                        secstat: gs(secstat),
                        sedol: gs(sedol),
                        tpci: gs(tpci),
                    };
                    out.push(temp.to_row());
                }
            }

            Ok::<Vec<Row>, AppError>(out)
        })
        .await?
    }
}

impl GlobalEquitiesMonthly {
    fn date_to_any(d: Option<NaiveDate>) -> AnyValue<'static> {
        match d {
            Some(nd) => {
                let days: i32 = (nd.num_days_from_ce() - 719_163) as i32;
                AnyValue::Date(days)
            }
            None => AnyValue::Null,
        }
    }

    /// Convert to Polars Row following `polars_schema` column order.
    pub fn to_row<'a>(self) -> Row<'a> {
        Row::new(vec![
            self.ajexm.map_or(AnyValue::Null, AnyValue::Float64),
            self.ajpm.map_or(AnyValue::Null, AnyValue::Float64),
            self.conm
                .map(|v| AnyValue::StringOwned(v.into()))
                .unwrap_or(AnyValue::Null),
            self.cshtrm.map_or(AnyValue::Null, AnyValue::Float64),
            self.curcddvm
                .map(|v| AnyValue::StringOwned(v.into()))
                .unwrap_or(AnyValue::Null),
            self.curcdm
                .map(|v| AnyValue::StringOwned(v.into()))
                .unwrap_or(AnyValue::Null),
            Self::date_to_any(self.datadate),
            self.dvpspm.map_or(AnyValue::Null, AnyValue::Float64),
            self.dvpspm_fn
                .map(|v| AnyValue::StringOwned(v.into()))
                .unwrap_or(AnyValue::Null),
            self.dvpsxm.map_or(AnyValue::Null, AnyValue::Float64),
            self.dvpsxm_fn
                .map(|v| AnyValue::StringOwned(v.into()))
                .unwrap_or(AnyValue::Null),
            self.epf
                .map(|v| AnyValue::StringOwned(v.into()))
                .unwrap_or(AnyValue::Null),
            self.exchg.map_or(AnyValue::Null, |v| AnyValue::Int32(v)),
            self.fic
                .map(|v| AnyValue::StringOwned(v.into()))
                .unwrap_or(AnyValue::Null),
            self.gvkey
                .map(|v| AnyValue::StringOwned(v.into()))
                .unwrap_or(AnyValue::Null),
            self.iid
                .map(|v| AnyValue::StringOwned(v.into()))
                .unwrap_or(AnyValue::Null),
            self.isalrt
                .map(|v| AnyValue::StringOwned(v.into()))
                .unwrap_or(AnyValue::Null),
            self.isin
                .map(|v| AnyValue::StringOwned(v.into()))
                .unwrap_or(AnyValue::Null),
            self.loc
                .map(|v| AnyValue::StringOwned(v.into()))
                .unwrap_or(AnyValue::Null),
            self.prccm.map_or(AnyValue::Null, AnyValue::Float64),
            self.prchm.map_or(AnyValue::Null, AnyValue::Float64),
            self.prclm.map_or(AnyValue::Null, AnyValue::Float64),
            self.secstat
                .map(|v| AnyValue::StringOwned(v.into()))
                .unwrap_or(AnyValue::Null),
            self.sedol
                .map(|v| AnyValue::StringOwned(v.into()))
                .unwrap_or(AnyValue::Null),
            self.tpci
                .map(|v| AnyValue::StringOwned(v.into()))
                .unwrap_or(AnyValue::Null),
        ])
    }

    pub fn polars_schema() -> Schema {
        Schema::from_iter([
            Field::new("ajexm".into(), DataType::Float64),
            Field::new("ajpm".into(), DataType::Float64),
            Field::new("conm".into(), DataType::String),
            Field::new("cshtrm".into(), DataType::Float64),
            Field::new("curcddvm".into(), DataType::String),
            Field::new("curcdm".into(), DataType::String),
            Field::new("datadate".into(), DataType::Date),
            Field::new("dvpspm".into(), DataType::Float64),
            Field::new("dvpspm_fn".into(), DataType::String),
            Field::new("dvpsxm".into(), DataType::Float64),
            Field::new("dvpsxm_fn".into(), DataType::String),
            Field::new("epf".into(), DataType::String),
            Field::new("exchg".into(), DataType::Int32),
            Field::new("fic".into(), DataType::String),
            Field::new("gvkey".into(), DataType::String),
            Field::new("iid".into(), DataType::String),
            Field::new("isalrt".into(), DataType::String),
            Field::new("isin".into(), DataType::String),
            Field::new("loc".into(), DataType::String),
            Field::new("prccm".into(), DataType::Float64),
            Field::new("prchm".into(), DataType::Float64),
            Field::new("prclm".into(), DataType::Float64),
            Field::new("secstat".into(), DataType::String),
            Field::new("sedol".into(), DataType::String),
            Field::new("tpci".into(), DataType::String),
        ])
    }
}

impl ToPolars for GlobalEquitiesMonthly {
    fn schema() -> Schema {
        Self::polars_schema()
    }
}
