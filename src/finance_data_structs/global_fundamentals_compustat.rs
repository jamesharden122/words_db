use super::{AppError, DuckCrudModel, SurrealCrudModel};
use arrow_array::{Array, BooleanArray, Date32Array, Float64Array, StringArray};
use chrono::{Datelike, NaiveDate};
use duckdb::Connection;
use polars::frame::row::Row;
use polars::prelude::*;
use rayon::prelude::*;
use serde::{Deserialize, Serialize};
use std::path::Path;
use std::sync::{Arc, Mutex};

/// Compustat Global Fundamentals Quarterly (fundq) wide panel.
///
/// Mirrors the column names from the WRDS/Compustat extract (`str()` output in R).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GlobalFundQtrly {
    pub accdq: Option<f64>,
    pub accliy: Option<f64>,
    pub accoq: Option<f64>,
    pub acctstdq: Option<String>,
    pub acoq: Option<f64>,
    pub acoxq: Option<f64>,
    pub acqdisny: Option<bool>,
    pub acqdisoy: Option<bool>,
    pub actq: Option<f64>,
    pub adpacq: Option<bool>,
    pub adpacy: Option<bool>,
    pub amq: Option<f64>,
    pub amy: Option<f64>,
    pub ancq: Option<f64>,
    pub aolochy: Option<f64>,
    pub aoq: Option<f64>,
    pub aotq: Option<f64>,
    pub apalchy: Option<bool>,
    pub apchy: Option<f64>,
    pub apoq: Option<f64>,
    pub apq: Option<f64>,
    pub aqcy: Option<f64>,
    pub artfsq: Option<f64>,
    pub asdisy: Option<bool>,
    pub asinvy: Option<bool>,
    pub atochy: Option<f64>,
    pub atq: Option<f64>,
    pub autxrq: Option<bool>,
    pub autxry: Option<f64>,
    pub bcefq: Option<bool>,
    pub bcefy: Option<bool>,
    pub bctq: Option<f64>,
    pub bcty: Option<f64>,
    pub bdiq: Option<f64>,
    pub bdiy: Option<f64>,
    pub bsprq: Option<String>,
    pub capcstq: Option<bool>,
    pub capcsty: Option<bool>,
    pub capfly: Option<bool>,
    pub capr1q: Option<f64>,
    pub capr2q: Option<f64>,
    pub capr3q: Option<f64>,
    pub capsq: Option<f64>,
    pub capxfiy: Option<bool>,
    pub capxy: Option<f64>,
    pub caq: Option<f64>,
    pub ceqq: Option<f64>,
    pub cfbdq: Option<f64>,
    pub cfbdy: Option<f64>,
    pub cfereq: Option<f64>,
    pub cferey: Option<f64>,
    pub cflaothy: Option<f64>,
    pub cfoq: Option<f64>,
    pub cfoy: Option<f64>,
    pub cfpdoq: Option<bool>,
    pub cfpdoy: Option<f64>,
    pub chechy: Option<f64>,
    pub chenfdy: Option<bool>,
    pub cheq: Option<f64>,
    pub chq: Option<f64>,
    pub chsq: Option<f64>,
    pub cltq: Option<bool>,
    pub cogsq: Option<f64>,
    pub cogsy: Option<f64>,
    pub compstq: Option<String>,
    pub conm: Option<String>,
    pub consol: Option<String>,
    pub costat: Option<String>,
    pub cstkq: Option<f64>,
    pub curcdq: Option<String>,
    pub datacqtr: Option<String>,
    pub datadate: Option<NaiveDate>,
    pub datafmt: Option<String>,
    pub datafqtr: Option<String>,
    pub dcsfdy: Option<bool>,
    pub dcufdy: Option<bool>,
    pub dfpacq: Option<f64>,
    pub dfxaq: Option<f64>,
    pub dfxay: Option<f64>,
    pub dispochy: Option<bool>,
    pub ditq: Option<f64>,
    pub dity: Option<f64>,
    pub dlcchy: Option<f64>,
    pub dlcq: Option<f64>,
    pub dltisy: Option<bool>,
    pub dltry: Option<bool>,
    pub dlttq: Option<f64>,
    pub docy: Option<bool>,
    pub dpactq: Option<f64>,
    pub dpcy: Option<f64>,
    pub dpq: Option<f64>,
    pub dptbq: Option<f64>,
    pub dptcq: Option<f64>,
    pub dpy: Option<f64>,
    pub dvpdpq: Option<f64>,
    pub dvpdpy: Option<f64>,
    pub dvrecy: Option<bool>,
    pub dvrreq: Option<bool>,
    pub dvrrey: Option<f64>,
    pub dvtq: Option<f64>,
    pub dvty: Option<f64>,
    pub dvy: Option<f64>,
    pub eieacy: Option<f64>,
    pub eqdivpy: Option<bool>,
    pub eqrtq: Option<f64>,
    pub eroq: Option<f64>,
    pub esubq: Option<f64>,
    pub esuby: Option<f64>,
    pub exchg: Option<f64>,
    pub exresy: Option<bool>,
    pub exreuy: Option<bool>,
    pub exrey: Option<f64>,
    pub fcaq: Option<f64>,
    pub fcay: Option<f64>,
    pub fdateq: Option<NaiveDate>,
    pub feaq: Option<f64>,
    pub felq: Option<f64>,
    pub fiaoy: Option<f64>,
    pub fic: Option<String>,
    pub fincfy: Option<f64>,
    pub finincy: Option<bool>,
    pub finley: Option<bool>,
    pub finrey: Option<bool>,
    pub finvaoy: Option<bool>,
    pub fopoy: Option<f64>,
    pub fqtr: Option<f64>,
    pub fsrcopoy: Option<bool>,
    pub fsrcopty: Option<bool>,
    pub fsrcoy: Option<bool>,
    pub fsrcty: Option<bool>,
    pub fuseoy: Option<bool>,
    pub fusety: Option<bool>,
    pub fyearq: Option<f64>,
    pub fyr: Option<f64>,
    pub gdwlamq: Option<bool>,
    pub gdwlamy: Option<f64>,
    pub gdwlq: Option<f64>,
    pub gpq: Option<f64>,
    pub gpy: Option<f64>,
    pub gvkey: Option<String>,
    pub iatiq: Option<f64>,
    pub ibcy: Option<f64>,
    pub ibkiq: Option<f64>,
    pub ibkiy: Option<f64>,
    pub ibmiiq: Option<f64>,
    pub ibmiiy: Option<f64>,
    pub ibq: Option<f64>,
    pub iby: Option<f64>,
    pub iditq: Option<f64>,
    pub idity: Option<f64>,
    pub iid: Option<String>,
    pub iireq: Option<f64>,
    pub iirey: Option<f64>,
    pub iitq: Option<f64>,
    pub iity: Option<f64>,
    pub indfmt: Option<String>,
    pub intandy: Option<bool>,
    pub intanpy: Option<f64>,
    pub intanq: Option<f64>,
    pub intcq: Option<f64>,
    pub intcy: Option<f64>,
    pub intfacty: Option<f64>,
    pub intfly: Option<bool>,
    pub intiacty: Option<f64>,
    pub intoacty: Option<f64>,
    pub intpdy: Option<bool>,
    pub intrcy: Option<bool>,
    pub invchy: Option<f64>,
    pub invdspy: Option<bool>,
    pub invsvcy: Option<bool>,
    pub invtq: Option<f64>,
    pub iobdq: Option<f64>,
    pub iobdy: Option<f64>,
    pub ioiq: Option<f64>,
    pub ioiy: Option<f64>,
    pub ioreq: Option<f64>,
    pub iorey: Option<f64>,
    pub ipq: Option<f64>,
    pub iptiq: Option<f64>,
    pub iptiy: Option<f64>,
    pub isgtq: Option<f64>,
    pub isgty: Option<f64>,
    pub isin: Option<String>,
    pub istq: Option<f64>,
    pub ivacoy: Option<f64>,
    pub ivaeqq: Option<f64>,
    pub ivaoq: Option<f64>,
    pub ivchy: Option<f64>,
    pub iviq: Option<f64>,
    pub iviy: Option<f64>,
    pub ivncfy: Option<f64>,
    pub ivptq: Option<f64>,
    pub ivstchy: Option<f64>,
    pub ivstq: Option<f64>,
    pub ivtfsq: Option<f64>,
    pub lcabgq: Option<f64>,
    pub lcacuq: Option<f64>,
    pub lcoq: Option<f64>,
    pub lcoxq: Option<f64>,
    pub lctq: Option<f64>,
    pub liqresny: Option<bool>,
    pub liqresoy: Option<bool>,
    pub lltq: Option<f64>,
    pub lndepy: Option<bool>,
    pub lnincy: Option<bool>,
    pub lnmdy: Option<bool>,
    pub lnrepy: Option<bool>,
    pub loc: Option<String>,
    pub loq: Option<f64>,
    pub lseq: Option<f64>,
    pub lsq: Option<f64>,
    pub ltdchy: Option<f64>,
    pub ltdlchy: Option<bool>,
    pub ltloy: Option<bool>,
    pub ltmibq: Option<f64>,
    pub ltq: Option<f64>,
    pub mibnq: Option<f64>,
    pub mibq: Option<bool>,
    pub mibtq: Option<f64>,
    pub micy: Option<f64>,
    pub miiq: Option<f64>,
    pub miiy: Option<f64>,
    pub miseqy: Option<f64>,
    pub mtlq: Option<bool>,
    pub ncfliqy: Option<bool>,
    pub neqmiy: Option<bool>,
    pub nitq: Option<bool>,
    pub nity: Option<f64>,
    pub noasuby: Option<bool>,
    pub nopioq: Option<f64>,
    pub nopioy: Option<f64>,
    pub nopiq: Option<f64>,
    pub nopiy: Option<f64>,
    pub oancfcy: Option<bool>,
    pub oancfdy: Option<bool>,
    pub oancfy: Option<f64>,
    pub oiadpq: Option<f64>,
    pub oiadpy: Option<f64>,
    pub oibdpq: Option<f64>,
    pub oibdpy: Option<f64>,
    pub opprfty: Option<bool>,
    pub oproq: Option<f64>,
    pub oproy: Option<f64>,
    pub pclq: Option<f64>,
    pub pcly: Option<f64>,
    pub pdateq: Option<NaiveDate>,
    pub pdq: Option<f64>,
    pub pdsa: Option<f64>,
    pub pdytd: Option<f64>,
    pub piq: Option<f64>,
    pub piy: Option<f64>,
    pub pliachy: Option<bool>,
    pub popsrc: Option<String>,
    pub ppentq: Option<f64>,
    pub prcq: Option<bool>,
    pub prosaiy: Option<bool>,
    pub prstkcy: Option<bool>,
    pub prvy: Option<f64>,
    pub psfixy: Option<f64>,
    pub pstkq: Option<f64>,
    pub ptranq: Option<f64>,
    pub ptrany: Option<f64>,
    pub purtshry: Option<f64>,
    pub pvoq: Option<f64>,
    pub pvoy: Option<f64>,
    pub pvtq: Option<f64>,
    pub ratiq: Option<f64>,
    pub rawmsmq: Option<bool>,
    pub rawmsmy: Option<bool>,
    pub recchy: Option<f64>,
    pub reccoq: Option<f64>,
    pub rectoq: Option<f64>,
    pub rectq: Option<f64>,
    pub rectrq: Option<f64>,
    pub reitq: Option<f64>,
    pub reity: Option<f64>,
    pub req: Option<f64>,
    pub revtq: Option<f64>,
    pub revty: Option<f64>,
    pub risq: Option<f64>,
    pub risy: Option<f64>,
    pub rltq: Option<f64>,
    pub rp: Option<String>,
    pub rvlrvq: Option<f64>,
    pub rvtiq: Option<f64>,
    pub rvutxq: Option<bool>,
    pub rvy: Option<f64>,
    pub saaq: Option<f64>,
    pub saleq: Option<f64>,
    pub saley: Option<f64>,
    pub salq: Option<f64>,
    pub sbdcq: Option<bool>,
    pub scfq: Option<f64>,
    pub scoq: Option<bool>,
    pub scq: Option<f64>,
    pub sctq: Option<f64>,
    pub sedol: Option<String>,
    pub seqq: Option<f64>,
    pub shrcapy: Option<bool>,
    pub sivy: Option<f64>,
    pub spiq: Option<f64>,
    pub spiy: Option<f64>,
    pub sppchy: Option<bool>,
    pub sppivy: Option<f64>,
    pub srcq: Option<f64>,
    pub ssnpq: Option<bool>,
    pub sstky: Option<f64>,
    pub staltq: Option<bool>,
    pub stfixay: Option<bool>,
    pub stinvy: Option<bool>,
    pub stkchq: Option<bool>,
    pub stkchy: Option<bool>,
    pub subdisy: Option<bool>,
    pub subpury: Option<bool>,
    pub tdsgq: Option<f64>,
    pub tdsgy: Option<f64>,
    pub tdstq: Option<f64>,
    pub teqq: Option<f64>,
    pub transaq: Option<f64>,
    pub tstkq: Option<f64>,
    pub txdbq: Option<f64>,
    pub txdcy: Option<f64>,
    pub txopy: Option<f64>,
    pub txtq: Option<f64>,
    pub txty: Option<f64>,
    pub txy: Option<bool>,
    pub unnpq: Option<f64>,
    pub updq: Option<f64>,
    pub wcapchcy: Option<bool>,
    pub wcapchy: Option<bool>,
    pub wcapopcy: Option<f64>,
    pub wcapsay: Option<bool>,
    pub wcapsuy: Option<bool>,
    pub wcapsy: Option<bool>,
    pub wcapty: Option<bool>,
    pub wcapuy: Option<bool>,
    pub xagtq: Option<f64>,
    pub xagty: Option<f64>,
    pub xbdtq: Option<f64>,
    pub xbdty: Option<f64>,
    pub xcomiq: Option<f64>,
    pub xcomiy: Option<f64>,
    pub xcomq: Option<bool>,
    pub xcomy: Option<bool>,
    pub xdvreq: Option<bool>,
    pub xdvrey: Option<bool>,
    pub xidocy: Option<bool>,
    pub xintq: Option<f64>,
    pub xinty: Option<f64>,
    pub xioq: Option<f64>,
    pub xioy: Option<f64>,
    pub xiq: Option<f64>,
    pub xiviq: Option<bool>,
    pub xiviy: Option<bool>,
    pub xivreq: Option<f64>,
    pub xivrey: Option<f64>,
    pub xiy: Option<f64>,
    pub xobdq: Option<f64>,
    pub xobdy: Option<f64>,
    pub xoiq: Option<f64>,
    pub xoiy: Option<f64>,
    pub xoproq: Option<f64>,
    pub xoproy: Option<f64>,
    pub xoprq: Option<f64>,
    pub xopry: Option<f64>,
    pub xoreq: Option<f64>,
    pub xorey: Option<f64>,
    pub xppq: Option<f64>,
    pub xretq: Option<f64>,
    pub xrety: Option<f64>,
    pub xsgaq: Option<f64>,
    pub xsgay: Option<f64>,
    pub xsq: Option<f64>,
    pub xstoq: Option<bool>,
    pub xstoy: Option<f64>,
    pub xstq: Option<f64>,
    pub xsty: Option<f64>,
    pub xsy: Option<f64>,
    pub xtq: Option<f64>,
    pub xty: Option<f64>,
}

impl SurrealCrudModel for GlobalFundQtrly {
    fn table() -> &'static str {
        // Namespace `compustat` / db `quarterly` uses a single schemaless table by default.
        "compustat"
    }
    fn id_key(&self) -> Option<String> {
        match (&self.gvkey, &self.iid, &self.datadate) {
            (Some(gvkey), Some(iid), Some(datadate)) => Some(format!("{gvkey}:{iid}:{datadate}")),
            (Some(gvkey), None, Some(datadate)) => Some(format!("{gvkey}:{datadate}")),
            _ => None,
        }
    }
}

impl DuckCrudModel for GlobalFundQtrly {
    fn table() -> &'static str {
        "comp_g_fundq"
    }
    fn id_key(&self) -> Option<String> {
        <Self as SurrealCrudModel>::id_key(self)
    }
}

impl GlobalFundQtrly {
    /// Create/replace DuckDB table from a Parquet file (full schema preserved in DuckDB).
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

    pub async fn read_range<'a>(
        conn: Arc<Mutex<Connection>>,
        date_range: (NaiveDate, NaiveDate),
    ) -> Result<Vec<Row<'a>>, AppError> {
        tokio::task::spawn_blocking(move || {
            let table = <Self as DuckCrudModel>::table();
            let sql = format!(
                "SELECT \
                    CAST(accdq AS DOUBLE) AS accdq, \
                    CAST(accliy AS DOUBLE) AS accliy, \
                    CAST(accoq AS DOUBLE) AS accoq, \
                    CAST(acctstdq AS VARCHAR) AS acctstdq, \
                    CAST(acoq AS DOUBLE) AS acoq, \
                    CAST(acoxq AS DOUBLE) AS acoxq, \
                    CAST(acqdisny AS BOOLEAN) AS acqdisny, \
                    CAST(acqdisoy AS BOOLEAN) AS acqdisoy, \
                    CAST(actq AS DOUBLE) AS actq, \
                    CAST(adpacq AS BOOLEAN) AS adpacq, \
                    CAST(adpacy AS BOOLEAN) AS adpacy, \
                    CAST(amq AS DOUBLE) AS amq, \
                    CAST(amy AS DOUBLE) AS amy, \
                    CAST(ancq AS DOUBLE) AS ancq, \
                    CAST(aolochy AS DOUBLE) AS aolochy, \
                    CAST(aoq AS DOUBLE) AS aoq, \
                    CAST(aotq AS DOUBLE) AS aotq, \
                    CAST(apalchy AS BOOLEAN) AS apalchy, \
                    CAST(apchy AS DOUBLE) AS apchy, \
                    CAST(apoq AS DOUBLE) AS apoq, \
                    CAST(apq AS DOUBLE) AS apq, \
                    CAST(aqcy AS DOUBLE) AS aqcy, \
                    CAST(artfsq AS DOUBLE) AS artfsq, \
                    CAST(asdisy AS BOOLEAN) AS asdisy, \
                    CAST(asinvy AS BOOLEAN) AS asinvy, \
                    CAST(atochy AS DOUBLE) AS atochy, \
                    CAST(atq AS DOUBLE) AS atq, \
                    CAST(autxrq AS BOOLEAN) AS autxrq, \
                    CAST(autxry AS DOUBLE) AS autxry, \
                    CAST(bcefq AS BOOLEAN) AS bcefq, \
                    CAST(bcefy AS BOOLEAN) AS bcefy, \
                    CAST(bctq AS DOUBLE) AS bctq, \
                    CAST(bcty AS DOUBLE) AS bcty, \
                    CAST(bdiq AS DOUBLE) AS bdiq, \
                    CAST(bdiy AS DOUBLE) AS bdiy, \
                    CAST(bsprq AS VARCHAR) AS bsprq, \
                    CAST(capcstq AS BOOLEAN) AS capcstq, \
                    CAST(capcsty AS BOOLEAN) AS capcsty, \
                    CAST(capfly AS BOOLEAN) AS capfly, \
                    CAST(capr1q AS DOUBLE) AS capr1q, \
                    CAST(capr2q AS DOUBLE) AS capr2q, \
                    CAST(capr3q AS DOUBLE) AS capr3q, \
                    CAST(capsq AS DOUBLE) AS capsq, \
                    CAST(capxfiy AS BOOLEAN) AS capxfiy, \
                    CAST(capxy AS DOUBLE) AS capxy, \
                    CAST(caq AS DOUBLE) AS caq, \
                    CAST(ceqq AS DOUBLE) AS ceqq, \
                    CAST(cfbdq AS DOUBLE) AS cfbdq, \
                    CAST(cfbdy AS DOUBLE) AS cfbdy, \
                    CAST(cfereq AS DOUBLE) AS cfereq, \
                    CAST(cferey AS DOUBLE) AS cferey, \
                    CAST(cflaothy AS DOUBLE) AS cflaothy, \
                    CAST(cfoq AS DOUBLE) AS cfoq, \
                    CAST(cfoy AS DOUBLE) AS cfoy, \
                    CAST(cfpdoq AS BOOLEAN) AS cfpdoq, \
                    CAST(cfpdoy AS DOUBLE) AS cfpdoy, \
                    CAST(chechy AS DOUBLE) AS chechy, \
                    CAST(chenfdy AS BOOLEAN) AS chenfdy, \
                    CAST(cheq AS DOUBLE) AS cheq, \
                    CAST(chq AS DOUBLE) AS chq, \
                    CAST(chsq AS DOUBLE) AS chsq, \
                    CAST(cltq AS BOOLEAN) AS cltq, \
                    CAST(cogsq AS DOUBLE) AS cogsq, \
                    CAST(cogsy AS DOUBLE) AS cogsy, \
                    CAST(compstq AS VARCHAR) AS compstq, \
                    CAST(conm AS VARCHAR) AS conm, \
                    CAST(consol AS VARCHAR) AS consol, \
                    CAST(costat AS VARCHAR) AS costat, \
                    CAST(cstkq AS DOUBLE) AS cstkq, \
                    CAST(curcdq AS VARCHAR) AS curcdq, \
                    CAST(datacqtr AS VARCHAR) AS datacqtr, \
                    CAST(datadate AS DATE) AS datadate, \
                    CAST(datafmt AS VARCHAR) AS datafmt, \
                    CAST(datafqtr AS VARCHAR) AS datafqtr, \
                    CAST(dcsfdy AS BOOLEAN) AS dcsfdy, \
                    CAST(dcufdy AS BOOLEAN) AS dcufdy, \
                    CAST(dfpacq AS DOUBLE) AS dfpacq, \
                    CAST(dfxaq AS DOUBLE) AS dfxaq, \
                    CAST(dfxay AS DOUBLE) AS dfxay, \
                    CAST(dispochy AS BOOLEAN) AS dispochy, \
                    CAST(ditq AS DOUBLE) AS ditq, \
                    CAST(dity AS DOUBLE) AS dity, \
                    CAST(dlcchy AS DOUBLE) AS dlcchy, \
                    CAST(dlcq AS DOUBLE) AS dlcq, \
                    CAST(dltisy AS BOOLEAN) AS dltisy, \
                    CAST(dltry AS BOOLEAN) AS dltry, \
                    CAST(dlttq AS DOUBLE) AS dlttq, \
                    CAST(docy AS BOOLEAN) AS docy, \
                    CAST(dpactq AS DOUBLE) AS dpactq, \
                    CAST(dpcy AS DOUBLE) AS dpcy, \
                    CAST(dpq AS DOUBLE) AS dpq, \
                    CAST(dptbq AS DOUBLE) AS dptbq, \
                    CAST(dptcq AS DOUBLE) AS dptcq, \
                    CAST(dpy AS DOUBLE) AS dpy, \
                    CAST(dvpdpq AS DOUBLE) AS dvpdpq, \
                    CAST(dvpdpy AS DOUBLE) AS dvpdpy, \
                    CAST(dvrecy AS BOOLEAN) AS dvrecy, \
                    CAST(dvrreq AS BOOLEAN) AS dvrreq, \
                    CAST(dvrrey AS DOUBLE) AS dvrrey, \
                    CAST(dvtq AS DOUBLE) AS dvtq, \
                    CAST(dvty AS DOUBLE) AS dvty, \
                    CAST(dvy AS DOUBLE) AS dvy, \
                    CAST(eieacy AS DOUBLE) AS eieacy, \
                    CAST(eqdivpy AS BOOLEAN) AS eqdivpy, \
                    CAST(eqrtq AS DOUBLE) AS eqrtq, \
                    CAST(eroq AS DOUBLE) AS eroq, \
                    CAST(esubq AS DOUBLE) AS esubq, \
                    CAST(esuby AS DOUBLE) AS esuby, \
                    CAST(exchg AS DOUBLE) AS exchg, \
                    CAST(exresy AS BOOLEAN) AS exresy, \
                    CAST(exreuy AS BOOLEAN) AS exreuy, \
                    CAST(exrey AS DOUBLE) AS exrey, \
                    CAST(fcaq AS DOUBLE) AS fcaq, \
                    CAST(fcay AS DOUBLE) AS fcay, \
                    CAST(fdateq AS DATE) AS fdateq, \
                    CAST(feaq AS DOUBLE) AS feaq, \
                    CAST(felq AS DOUBLE) AS felq, \
                    CAST(fiaoy AS DOUBLE) AS fiaoy, \
                    CAST(fic AS VARCHAR) AS fic, \
                    CAST(fincfy AS DOUBLE) AS fincfy, \
                    CAST(finincy AS BOOLEAN) AS finincy, \
                    CAST(finley AS BOOLEAN) AS finley, \
                    CAST(finrey AS BOOLEAN) AS finrey, \
                    CAST(finvaoy AS BOOLEAN) AS finvaoy, \
                    CAST(fopoy AS DOUBLE) AS fopoy, \
                    CAST(fqtr AS DOUBLE) AS fqtr, \
                    CAST(fsrcopoy AS BOOLEAN) AS fsrcopoy, \
                    CAST(fsrcopty AS BOOLEAN) AS fsrcopty, \
                    CAST(fsrcoy AS BOOLEAN) AS fsrcoy, \
                    CAST(fsrcty AS BOOLEAN) AS fsrcty, \
                    CAST(fuseoy AS BOOLEAN) AS fuseoy, \
                    CAST(fusety AS BOOLEAN) AS fusety, \
                    CAST(fyearq AS DOUBLE) AS fyearq, \
                    CAST(fyr AS DOUBLE) AS fyr, \
                    CAST(gdwlamq AS BOOLEAN) AS gdwlamq, \
                    CAST(gdwlamy AS DOUBLE) AS gdwlamy, \
                    CAST(gdwlq AS DOUBLE) AS gdwlq, \
                    CAST(gpq AS DOUBLE) AS gpq, \
                    CAST(gpy AS DOUBLE) AS gpy, \
                    CAST(gvkey AS VARCHAR) AS gvkey, \
                    CAST(iatiq AS DOUBLE) AS iatiq, \
                    CAST(ibcy AS DOUBLE) AS ibcy, \
                    CAST(ibkiq AS DOUBLE) AS ibkiq, \
                    CAST(ibkiy AS DOUBLE) AS ibkiy, \
                    CAST(ibmiiq AS DOUBLE) AS ibmiiq, \
                    CAST(ibmiiy AS DOUBLE) AS ibmiiy, \
                    CAST(ibq AS DOUBLE) AS ibq, \
                    CAST(iby AS DOUBLE) AS iby, \
                    CAST(iditq AS DOUBLE) AS iditq, \
                    CAST(idity AS DOUBLE) AS idity, \
                    CAST(iid AS VARCHAR) AS iid, \
                    CAST(iireq AS DOUBLE) AS iireq, \
                    CAST(iirey AS DOUBLE) AS iirey, \
                    CAST(iitq AS DOUBLE) AS iitq, \
                    CAST(iity AS DOUBLE) AS iity, \
                    CAST(indfmt AS VARCHAR) AS indfmt, \
                    CAST(intandy AS BOOLEAN) AS intandy, \
                    CAST(intanpy AS DOUBLE) AS intanpy, \
                    CAST(intanq AS DOUBLE) AS intanq, \
                    CAST(intcq AS DOUBLE) AS intcq, \
                    CAST(intcy AS DOUBLE) AS intcy, \
                    CAST(intfacty AS DOUBLE) AS intfacty, \
                    CAST(intfly AS BOOLEAN) AS intfly, \
                    CAST(intiacty AS DOUBLE) AS intiacty, \
                    CAST(intoacty AS DOUBLE) AS intoacty, \
                    CAST(intpdy AS BOOLEAN) AS intpdy, \
                    CAST(intrcy AS BOOLEAN) AS intrcy, \
                    CAST(invchy AS DOUBLE) AS invchy, \
                    CAST(invdspy AS BOOLEAN) AS invdspy, \
                    CAST(invsvcy AS BOOLEAN) AS invsvcy, \
                    CAST(invtq AS DOUBLE) AS invtq, \
                    CAST(iobdq AS DOUBLE) AS iobdq, \
                    CAST(iobdy AS DOUBLE) AS iobdy, \
                    CAST(ioiq AS DOUBLE) AS ioiq, \
                    CAST(ioiy AS DOUBLE) AS ioiy, \
                    CAST(ioreq AS DOUBLE) AS ioreq, \
                    CAST(iorey AS DOUBLE) AS iorey, \
                    CAST(ipq AS DOUBLE) AS ipq, \
                    CAST(iptiq AS DOUBLE) AS iptiq, \
                    CAST(iptiy AS DOUBLE) AS iptiy, \
                    CAST(isgtq AS DOUBLE) AS isgtq, \
                    CAST(isgty AS DOUBLE) AS isgty, \
                    CAST(isin AS VARCHAR) AS isin, \
                    CAST(istq AS DOUBLE) AS istq, \
                    CAST(ivacoy AS DOUBLE) AS ivacoy, \
                    CAST(ivaeqq AS DOUBLE) AS ivaeqq, \
                    CAST(ivaoq AS DOUBLE) AS ivaoq, \
                    CAST(ivchy AS DOUBLE) AS ivchy, \
                    CAST(iviq AS DOUBLE) AS iviq, \
                    CAST(iviy AS DOUBLE) AS iviy, \
                    CAST(ivncfy AS DOUBLE) AS ivncfy, \
                    CAST(ivptq AS DOUBLE) AS ivptq, \
                    CAST(ivstchy AS DOUBLE) AS ivstchy, \
                    CAST(ivstq AS DOUBLE) AS ivstq, \
                    CAST(ivtfsq AS DOUBLE) AS ivtfsq, \
                    CAST(lcabgq AS DOUBLE) AS lcabgq, \
                    CAST(lcacuq AS DOUBLE) AS lcacuq, \
                    CAST(lcoq AS DOUBLE) AS lcoq, \
                    CAST(lcoxq AS DOUBLE) AS lcoxq, \
                    CAST(lctq AS DOUBLE) AS lctq, \
                    CAST(liqresny AS BOOLEAN) AS liqresny, \
                    CAST(liqresoy AS BOOLEAN) AS liqresoy, \
                    CAST(lltq AS DOUBLE) AS lltq, \
                    CAST(lndepy AS BOOLEAN) AS lndepy, \
                    CAST(lnincy AS BOOLEAN) AS lnincy, \
                    CAST(lnmdy AS BOOLEAN) AS lnmdy, \
                    CAST(lnrepy AS BOOLEAN) AS lnrepy, \
                    CAST(loc AS VARCHAR) AS loc, \
                    CAST(loq AS DOUBLE) AS loq, \
                    CAST(lseq AS DOUBLE) AS lseq, \
                    CAST(lsq AS DOUBLE) AS lsq, \
                    CAST(ltdchy AS DOUBLE) AS ltdchy, \
                    CAST(ltdlchy AS BOOLEAN) AS ltdlchy, \
                    CAST(ltloy AS BOOLEAN) AS ltloy, \
                    CAST(ltmibq AS DOUBLE) AS ltmibq, \
                    CAST(ltq AS DOUBLE) AS ltq, \
                    CAST(mibnq AS DOUBLE) AS mibnq, \
                    CAST(mibq AS BOOLEAN) AS mibq, \
                    CAST(mibtq AS DOUBLE) AS mibtq, \
                    CAST(micy AS DOUBLE) AS micy, \
                    CAST(miiq AS DOUBLE) AS miiq, \
                    CAST(miiy AS DOUBLE) AS miiy, \
                    CAST(miseqy AS DOUBLE) AS miseqy, \
                    CAST(mtlq AS BOOLEAN) AS mtlq, \
                    CAST(ncfliqy AS BOOLEAN) AS ncfliqy, \
                    CAST(neqmiy AS BOOLEAN) AS neqmiy, \
                    CAST(nitq AS BOOLEAN) AS nitq, \
                    CAST(nity AS DOUBLE) AS nity, \
                    CAST(noasuby AS BOOLEAN) AS noasuby, \
                    CAST(nopioq AS DOUBLE) AS nopioq, \
                    CAST(nopioy AS DOUBLE) AS nopioy, \
                    CAST(nopiq AS DOUBLE) AS nopiq, \
                    CAST(nopiy AS DOUBLE) AS nopiy, \
                    CAST(oancfcy AS BOOLEAN) AS oancfcy, \
                    CAST(oancfdy AS BOOLEAN) AS oancfdy, \
                    CAST(oancfy AS DOUBLE) AS oancfy, \
                    CAST(oiadpq AS DOUBLE) AS oiadpq, \
                    CAST(oiadpy AS DOUBLE) AS oiadpy, \
                    CAST(oibdpq AS DOUBLE) AS oibdpq, \
                    CAST(oibdpy AS DOUBLE) AS oibdpy, \
                    CAST(opprfty AS BOOLEAN) AS opprfty, \
                    CAST(oproq AS DOUBLE) AS oproq, \
                    CAST(oproy AS DOUBLE) AS oproy, \
                    CAST(pclq AS DOUBLE) AS pclq, \
                    CAST(pcly AS DOUBLE) AS pcly, \
                    CAST(pdateq AS DATE) AS pdateq, \
                    CAST(pdq AS DOUBLE) AS pdq, \
                    CAST(pdsa AS DOUBLE) AS pdsa, \
                    CAST(pdytd AS DOUBLE) AS pdytd, \
                    CAST(piq AS DOUBLE) AS piq, \
                    CAST(piy AS DOUBLE) AS piy, \
                    CAST(pliachy AS BOOLEAN) AS pliachy, \
                    CAST(popsrc AS VARCHAR) AS popsrc, \
                    CAST(ppentq AS DOUBLE) AS ppentq, \
                    CAST(prcq AS BOOLEAN) AS prcq, \
                    CAST(prosaiy AS BOOLEAN) AS prosaiy, \
                    CAST(prstkcy AS BOOLEAN) AS prstkcy, \
                    CAST(prvy AS DOUBLE) AS prvy, \
                    CAST(psfixy AS DOUBLE) AS psfixy, \
                    CAST(pstkq AS DOUBLE) AS pstkq, \
                    CAST(ptranq AS DOUBLE) AS ptranq, \
                    CAST(ptrany AS DOUBLE) AS ptrany, \
                    CAST(purtshry AS DOUBLE) AS purtshry, \
                    CAST(pvoq AS DOUBLE) AS pvoq, \
                    CAST(pvoy AS DOUBLE) AS pvoy, \
                    CAST(pvtq AS DOUBLE) AS pvtq, \
                    CAST(ratiq AS DOUBLE) AS ratiq, \
                    CAST(rawmsmq AS BOOLEAN) AS rawmsmq, \
                    CAST(rawmsmy AS BOOLEAN) AS rawmsmy, \
                    CAST(recchy AS DOUBLE) AS recchy, \
                    CAST(reccoq AS DOUBLE) AS reccoq, \
                    CAST(rectoq AS DOUBLE) AS rectoq, \
                    CAST(rectq AS DOUBLE) AS rectq, \
                    CAST(rectrq AS DOUBLE) AS rectrq, \
                    CAST(reitq AS DOUBLE) AS reitq, \
                    CAST(reity AS DOUBLE) AS reity, \
                    CAST(req AS DOUBLE) AS req, \
                    CAST(revtq AS DOUBLE) AS revtq, \
                    CAST(revty AS DOUBLE) AS revty, \
                    CAST(risq AS DOUBLE) AS risq, \
                    CAST(risy AS DOUBLE) AS risy, \
                    CAST(rltq AS DOUBLE) AS rltq, \
                    CAST(rp AS VARCHAR) AS rp, \
                    CAST(rvlrvq AS DOUBLE) AS rvlrvq, \
                    CAST(rvtiq AS DOUBLE) AS rvtiq, \
                    CAST(rvutxq AS BOOLEAN) AS rvutxq, \
                    CAST(rvy AS DOUBLE) AS rvy, \
                    CAST(saaq AS DOUBLE) AS saaq, \
                    CAST(saleq AS DOUBLE) AS saleq, \
                    CAST(saley AS DOUBLE) AS saley, \
                    CAST(salq AS DOUBLE) AS salq, \
                    CAST(sbdcq AS BOOLEAN) AS sbdcq, \
                    CAST(scfq AS DOUBLE) AS scfq, \
                    CAST(scoq AS BOOLEAN) AS scoq, \
                    CAST(scq AS DOUBLE) AS scq, \
                    CAST(sctq AS DOUBLE) AS sctq, \
                    CAST(sedol AS VARCHAR) AS sedol, \
                    CAST(seqq AS DOUBLE) AS seqq, \
                    CAST(shrcapy AS BOOLEAN) AS shrcapy, \
                    CAST(sivy AS DOUBLE) AS sivy, \
                    CAST(spiq AS DOUBLE) AS spiq, \
                    CAST(spiy AS DOUBLE) AS spiy, \
                    CAST(sppchy AS BOOLEAN) AS sppchy, \
                    CAST(sppivy AS DOUBLE) AS sppivy, \
                    CAST(srcq AS DOUBLE) AS srcq, \
                    CAST(ssnpq AS BOOLEAN) AS ssnpq, \
                    CAST(sstky AS DOUBLE) AS sstky, \
                    CAST(staltq AS BOOLEAN) AS staltq, \
                    CAST(stfixay AS BOOLEAN) AS stfixay, \
                    CAST(stinvy AS BOOLEAN) AS stinvy, \
                    CAST(stkchq AS BOOLEAN) AS stkchq, \
                    CAST(stkchy AS BOOLEAN) AS stkchy, \
                    CAST(subdisy AS BOOLEAN) AS subdisy, \
                    CAST(subpury AS BOOLEAN) AS subpury, \
                    CAST(tdsgq AS DOUBLE) AS tdsgq, \
                    CAST(tdsgy AS DOUBLE) AS tdsgy, \
                    CAST(tdstq AS DOUBLE) AS tdstq, \
                    CAST(teqq AS DOUBLE) AS teqq, \
                    CAST(transaq AS DOUBLE) AS transaq, \
                    CAST(tstkq AS DOUBLE) AS tstkq, \
                    CAST(txdbq AS DOUBLE) AS txdbq, \
                    CAST(txdcy AS DOUBLE) AS txdcy, \
                    CAST(txopy AS DOUBLE) AS txopy, \
                    CAST(txtq AS DOUBLE) AS txtq, \
                    CAST(txty AS DOUBLE) AS txty, \
                    CAST(txy AS BOOLEAN) AS txy, \
                    CAST(unnpq AS DOUBLE) AS unnpq, \
                    CAST(updq AS DOUBLE) AS updq, \
                    CAST(wcapchcy AS BOOLEAN) AS wcapchcy, \
                    CAST(wcapchy AS BOOLEAN) AS wcapchy, \
                    CAST(wcapopcy AS DOUBLE) AS wcapopcy, \
                    CAST(wcapsay AS BOOLEAN) AS wcapsay, \
                    CAST(wcapsuy AS BOOLEAN) AS wcapsuy, \
                    CAST(wcapsy AS BOOLEAN) AS wcapsy, \
                    CAST(wcapty AS BOOLEAN) AS wcapty, \
                    CAST(wcapuy AS BOOLEAN) AS wcapuy, \
                    CAST(xagtq AS DOUBLE) AS xagtq, \
                    CAST(xagty AS DOUBLE) AS xagty, \
                    CAST(xbdtq AS DOUBLE) AS xbdtq, \
                    CAST(xbdty AS DOUBLE) AS xbdty, \
                    CAST(xcomiq AS DOUBLE) AS xcomiq, \
                    CAST(xcomiy AS DOUBLE) AS xcomiy, \
                    CAST(xcomq AS BOOLEAN) AS xcomq, \
                    CAST(xcomy AS BOOLEAN) AS xcomy, \
                    CAST(xdvreq AS BOOLEAN) AS xdvreq, \
                    CAST(xdvrey AS BOOLEAN) AS xdvrey, \
                    CAST(xidocy AS BOOLEAN) AS xidocy, \
                    CAST(xintq AS DOUBLE) AS xintq, \
                    CAST(xinty AS DOUBLE) AS xinty, \
                    CAST(xioq AS DOUBLE) AS xioq, \
                    CAST(xioy AS DOUBLE) AS xioy, \
                    CAST(xiq AS DOUBLE) AS xiq, \
                    CAST(xiviq AS BOOLEAN) AS xiviq, \
                    CAST(xiviy AS BOOLEAN) AS xiviy, \
                    CAST(xivreq AS DOUBLE) AS xivreq, \
                    CAST(xivrey AS DOUBLE) AS xivrey, \
                    CAST(xiy AS DOUBLE) AS xiy, \
                    CAST(xobdq AS DOUBLE) AS xobdq, \
                    CAST(xobdy AS DOUBLE) AS xobdy, \
                    CAST(xoiq AS DOUBLE) AS xoiq, \
                    CAST(xoiy AS DOUBLE) AS xoiy, \
                    CAST(xoproq AS DOUBLE) AS xoproq, \
                    CAST(xoproy AS DOUBLE) AS xoproy, \
                    CAST(xoprq AS DOUBLE) AS xoprq, \
                    CAST(xopry AS DOUBLE) AS xopry, \
                    CAST(xoreq AS DOUBLE) AS xoreq, \
                    CAST(xorey AS DOUBLE) AS xorey, \
                    CAST(xppq AS DOUBLE) AS xppq, \
                    CAST(xretq AS DOUBLE) AS xretq, \
                    CAST(xrety AS DOUBLE) AS xrety, \
                    CAST(xsgaq AS DOUBLE) AS xsgaq, \
                    CAST(xsgay AS DOUBLE) AS xsgay, \
                    CAST(xsq AS DOUBLE) AS xsq, \
                    CAST(xstoq AS BOOLEAN) AS xstoq, \
                    CAST(xstoy AS DOUBLE) AS xstoy, \
                    CAST(xstq AS DOUBLE) AS xstq, \
                    CAST(xsty AS DOUBLE) AS xsty, \
                    CAST(xsy AS DOUBLE) AS xsy, \
                    CAST(xtq AS DOUBLE) AS xtq, \
                    CAST(xty AS DOUBLE) AS xty \
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
                let b = |name: &str| -> &BooleanArray {
                    batch
                        .column(schema.index_of(name).unwrap())
                        .as_any()
                        .downcast_ref::<BooleanArray>()
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
                let accdq = f("accdq");
                let accliy = f("accliy");
                let accoq = f("accoq");
                let acctstdq = s("acctstdq");
                let acoq = f("acoq");
                let acoxq = f("acoxq");
                let acqdisny = b("acqdisny");
                let acqdisoy = b("acqdisoy");
                let actq = f("actq");
                let adpacq = b("adpacq");
                let adpacy = b("adpacy");
                let amq = f("amq");
                let amy = f("amy");
                let ancq = f("ancq");
                let aolochy = f("aolochy");
                let aoq = f("aoq");
                let aotq = f("aotq");
                let apalchy = b("apalchy");
                let apchy = f("apchy");
                let apoq = f("apoq");
                let apq = f("apq");
                let aqcy = f("aqcy");
                let artfsq = f("artfsq");
                let asdisy = b("asdisy");
                let asinvy = b("asinvy");
                let atochy = f("atochy");
                let atq = f("atq");
                let autxrq = b("autxrq");
                let autxry = f("autxry");
                let bcefq = b("bcefq");
                let bcefy = b("bcefy");
                let bctq = f("bctq");
                let bcty = f("bcty");
                let bdiq = f("bdiq");
                let bdiy = f("bdiy");
                let bsprq = s("bsprq");
                let capcstq = b("capcstq");
                let capcsty = b("capcsty");
                let capfly = b("capfly");
                let capr1q = f("capr1q");
                let capr2q = f("capr2q");
                let capr3q = f("capr3q");
                let capsq = f("capsq");
                let capxfiy = b("capxfiy");
                let capxy = f("capxy");
                let caq = f("caq");
                let ceqq = f("ceqq");
                let cfbdq = f("cfbdq");
                let cfbdy = f("cfbdy");
                let cfereq = f("cfereq");
                let cferey = f("cferey");
                let cflaothy = f("cflaothy");
                let cfoq = f("cfoq");
                let cfoy = f("cfoy");
                let cfpdoq = b("cfpdoq");
                let cfpdoy = f("cfpdoy");
                let chechy = f("chechy");
                let chenfdy = b("chenfdy");
                let cheq = f("cheq");
                let chq = f("chq");
                let chsq = f("chsq");
                let cltq = b("cltq");
                let cogsq = f("cogsq");
                let cogsy = f("cogsy");
                let compstq = s("compstq");
                let conm = s("conm");
                let consol = s("consol");
                let costat = s("costat");
                let cstkq = f("cstkq");
                let curcdq = s("curcdq");
                let datacqtr = s("datacqtr");
                let datadate = d("datadate");
                let datafmt = s("datafmt");
                let datafqtr = s("datafqtr");
                let dcsfdy = b("dcsfdy");
                let dcufdy = b("dcufdy");
                let dfpacq = f("dfpacq");
                let dfxaq = f("dfxaq");
                let dfxay = f("dfxay");
                let dispochy = b("dispochy");
                let ditq = f("ditq");
                let dity = f("dity");
                let dlcchy = f("dlcchy");
                let dlcq = f("dlcq");
                let dltisy = b("dltisy");
                let dltry = b("dltry");
                let dlttq = f("dlttq");
                let docy = b("docy");
                let dpactq = f("dpactq");
                let dpcy = f("dpcy");
                let dpq = f("dpq");
                let dptbq = f("dptbq");
                let dptcq = f("dptcq");
                let dpy = f("dpy");
                let dvpdpq = f("dvpdpq");
                let dvpdpy = f("dvpdpy");
                let dvrecy = b("dvrecy");
                let dvrreq = b("dvrreq");
                let dvrrey = f("dvrrey");
                let dvtq = f("dvtq");
                let dvty = f("dvty");
                let dvy = f("dvy");
                let eieacy = f("eieacy");
                let eqdivpy = b("eqdivpy");
                let eqrtq = f("eqrtq");
                let eroq = f("eroq");
                let esubq = f("esubq");
                let esuby = f("esuby");
                let exchg = f("exchg");
                let exresy = b("exresy");
                let exreuy = b("exreuy");
                let exrey = f("exrey");
                let fcaq = f("fcaq");
                let fcay = f("fcay");
                let fdateq = d("fdateq");
                let feaq = f("feaq");
                let felq = f("felq");
                let fiaoy = f("fiaoy");
                let fic = s("fic");
                let fincfy = f("fincfy");
                let finincy = b("finincy");
                let finley = b("finley");
                let finrey = b("finrey");
                let finvaoy = b("finvaoy");
                let fopoy = f("fopoy");
                let fqtr = f("fqtr");
                let fsrcopoy = b("fsrcopoy");
                let fsrcopty = b("fsrcopty");
                let fsrcoy = b("fsrcoy");
                let fsrcty = b("fsrcty");
                let fuseoy = b("fuseoy");
                let fusety = b("fusety");
                let fyearq = f("fyearq");
                let fyr = f("fyr");
                let gdwlamq = b("gdwlamq");
                let gdwlamy = f("gdwlamy");
                let gdwlq = f("gdwlq");
                let gpq = f("gpq");
                let gpy = f("gpy");
                let gvkey = s("gvkey");
                let iatiq = f("iatiq");
                let ibcy = f("ibcy");
                let ibkiq = f("ibkiq");
                let ibkiy = f("ibkiy");
                let ibmiiq = f("ibmiiq");
                let ibmiiy = f("ibmiiy");
                let ibq = f("ibq");
                let iby = f("iby");
                let iditq = f("iditq");
                let idity = f("idity");
                let iid = s("iid");
                let iireq = f("iireq");
                let iirey = f("iirey");
                let iitq = f("iitq");
                let iity = f("iity");
                let indfmt = s("indfmt");
                let intandy = b("intandy");
                let intanpy = f("intanpy");
                let intanq = f("intanq");
                let intcq = f("intcq");
                let intcy = f("intcy");
                let intfacty = f("intfacty");
                let intfly = b("intfly");
                let intiacty = f("intiacty");
                let intoacty = f("intoacty");
                let intpdy = b("intpdy");
                let intrcy = b("intrcy");
                let invchy = f("invchy");
                let invdspy = b("invdspy");
                let invsvcy = b("invsvcy");
                let invtq = f("invtq");
                let iobdq = f("iobdq");
                let iobdy = f("iobdy");
                let ioiq = f("ioiq");
                let ioiy = f("ioiy");
                let ioreq = f("ioreq");
                let iorey = f("iorey");
                let ipq = f("ipq");
                let iptiq = f("iptiq");
                let iptiy = f("iptiy");
                let isgtq = f("isgtq");
                let isgty = f("isgty");
                let isin = s("isin");
                let istq = f("istq");
                let ivacoy = f("ivacoy");
                let ivaeqq = f("ivaeqq");
                let ivaoq = f("ivaoq");
                let ivchy = f("ivchy");
                let iviq = f("iviq");
                let iviy = f("iviy");
                let ivncfy = f("ivncfy");
                let ivptq = f("ivptq");
                let ivstchy = f("ivstchy");
                let ivstq = f("ivstq");
                let ivtfsq = f("ivtfsq");
                let lcabgq = f("lcabgq");
                let lcacuq = f("lcacuq");
                let lcoq = f("lcoq");
                let lcoxq = f("lcoxq");
                let lctq = f("lctq");
                let liqresny = b("liqresny");
                let liqresoy = b("liqresoy");
                let lltq = f("lltq");
                let lndepy = b("lndepy");
                let lnincy = b("lnincy");
                let lnmdy = b("lnmdy");
                let lnrepy = b("lnrepy");
                let loc = s("loc");
                let loq = f("loq");
                let lseq = f("lseq");
                let lsq = f("lsq");
                let ltdchy = f("ltdchy");
                let ltdlchy = b("ltdlchy");
                let ltloy = b("ltloy");
                let ltmibq = f("ltmibq");
                let ltq = f("ltq");
                let mibnq = f("mibnq");
                let mibq = b("mibq");
                let mibtq = f("mibtq");
                let micy = f("micy");
                let miiq = f("miiq");
                let miiy = f("miiy");
                let miseqy = f("miseqy");
                let mtlq = b("mtlq");
                let ncfliqy = b("ncfliqy");
                let neqmiy = b("neqmiy");
                let nitq = b("nitq");
                let nity = f("nity");
                let noasuby = b("noasuby");
                let nopioq = f("nopioq");
                let nopioy = f("nopioy");
                let nopiq = f("nopiq");
                let nopiy = f("nopiy");
                let oancfcy = b("oancfcy");
                let oancfdy = b("oancfdy");
                let oancfy = f("oancfy");
                let oiadpq = f("oiadpq");
                let oiadpy = f("oiadpy");
                let oibdpq = f("oibdpq");
                let oibdpy = f("oibdpy");
                let opprfty = b("opprfty");
                let oproq = f("oproq");
                let oproy = f("oproy");
                let pclq = f("pclq");
                let pcly = f("pcly");
                let pdateq = d("pdateq");
                let pdq = f("pdq");
                let pdsa = f("pdsa");
                let pdytd = f("pdytd");
                let piq = f("piq");
                let piy = f("piy");
                let pliachy = b("pliachy");
                let popsrc = s("popsrc");
                let ppentq = f("ppentq");
                let prcq = b("prcq");
                let prosaiy = b("prosaiy");
                let prstkcy = b("prstkcy");
                let prvy = f("prvy");
                let psfixy = f("psfixy");
                let pstkq = f("pstkq");
                let ptranq = f("ptranq");
                let ptrany = f("ptrany");
                let purtshry = f("purtshry");
                let pvoq = f("pvoq");
                let pvoy = f("pvoy");
                let pvtq = f("pvtq");
                let ratiq = f("ratiq");
                let rawmsmq = b("rawmsmq");
                let rawmsmy = b("rawmsmy");
                let recchy = f("recchy");
                let reccoq = f("reccoq");
                let rectoq = f("rectoq");
                let rectq = f("rectq");
                let rectrq = f("rectrq");
                let reitq = f("reitq");
                let reity = f("reity");
                let req = f("req");
                let revtq = f("revtq");
                let revty = f("revty");
                let risq = f("risq");
                let risy = f("risy");
                let rltq = f("rltq");
                let rp = s("rp");
                let rvlrvq = f("rvlrvq");
                let rvtiq = f("rvtiq");
                let rvutxq = b("rvutxq");
                let rvy = f("rvy");
                let saaq = f("saaq");
                let saleq = f("saleq");
                let saley = f("saley");
                let salq = f("salq");
                let sbdcq = b("sbdcq");
                let scfq = f("scfq");
                let scoq = b("scoq");
                let scq = f("scq");
                let sctq = f("sctq");
                let sedol = s("sedol");
                let seqq = f("seqq");
                let shrcapy = b("shrcapy");
                let sivy = f("sivy");
                let spiq = f("spiq");
                let spiy = f("spiy");
                let sppchy = b("sppchy");
                let sppivy = f("sppivy");
                let srcq = f("srcq");
                let ssnpq = b("ssnpq");
                let sstky = f("sstky");
                let staltq = b("staltq");
                let stfixay = b("stfixay");
                let stinvy = b("stinvy");
                let stkchq = b("stkchq");
                let stkchy = b("stkchy");
                let subdisy = b("subdisy");
                let subpury = b("subpury");
                let tdsgq = f("tdsgq");
                let tdsgy = f("tdsgy");
                let tdstq = f("tdstq");
                let teqq = f("teqq");
                let transaq = f("transaq");
                let tstkq = f("tstkq");
                let txdbq = f("txdbq");
                let txdcy = f("txdcy");
                let txopy = f("txopy");
                let txtq = f("txtq");
                let txty = f("txty");
                let txy = b("txy");
                let unnpq = f("unnpq");
                let updq = f("updq");
                let wcapchcy = b("wcapchcy");
                let wcapchy = b("wcapchy");
                let wcapopcy = f("wcapopcy");
                let wcapsay = b("wcapsay");
                let wcapsuy = b("wcapsuy");
                let wcapsy = b("wcapsy");
                let wcapty = b("wcapty");
                let wcapuy = b("wcapuy");
                let xagtq = f("xagtq");
                let xagty = f("xagty");
                let xbdtq = f("xbdtq");
                let xbdty = f("xbdty");
                let xcomiq = f("xcomiq");
                let xcomiy = f("xcomiy");
                let xcomq = b("xcomq");
                let xcomy = b("xcomy");
                let xdvreq = b("xdvreq");
                let xdvrey = b("xdvrey");
                let xidocy = b("xidocy");
                let xintq = f("xintq");
                let xinty = f("xinty");
                let xioq = f("xioq");
                let xioy = f("xioy");
                let xiq = f("xiq");
                let xiviq = b("xiviq");
                let xiviy = b("xiviy");
                let xivreq = f("xivreq");
                let xivrey = f("xivrey");
                let xiy = f("xiy");
                let xobdq = f("xobdq");
                let xobdy = f("xobdy");
                let xoiq = f("xoiq");
                let xoiy = f("xoiy");
                let xoproq = f("xoproq");
                let xoproy = f("xoproy");
                let xoprq = f("xoprq");
                let xopry = f("xopry");
                let xoreq = f("xoreq");
                let xorey = f("xorey");
                let xppq = f("xppq");
                let xretq = f("xretq");
                let xrety = f("xrety");
                let xsgaq = f("xsgaq");
                let xsgay = f("xsgay");
                let xsq = f("xsq");
                let xstoq = b("xstoq");
                let xstoy = f("xstoy");
                let xstq = f("xstq");
                let xsty = f("xsty");
                let xsy = f("xsy");
                let xtq = f("xtq");
                let xty = f("xty");

                let rows: Vec<Row<'static>> = (0..batch.num_rows())
                    .into_par_iter()
                    .map(|row_i| {
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
                        let gb = |arr: &BooleanArray| -> Option<bool> {
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
                            accdq: gf(accdq),
                            accliy: gf(accliy),
                            accoq: gf(accoq),
                            acctstdq: gs(acctstdq),
                            acoq: gf(acoq),
                            acoxq: gf(acoxq),
                            acqdisny: gb(acqdisny),
                            acqdisoy: gb(acqdisoy),
                            actq: gf(actq),
                            adpacq: gb(adpacq),
                            adpacy: gb(adpacy),
                            amq: gf(amq),
                            amy: gf(amy),
                            ancq: gf(ancq),
                            aolochy: gf(aolochy),
                            aoq: gf(aoq),
                            aotq: gf(aotq),
                            apalchy: gb(apalchy),
                            apchy: gf(apchy),
                            apoq: gf(apoq),
                            apq: gf(apq),
                            aqcy: gf(aqcy),
                            artfsq: gf(artfsq),
                            asdisy: gb(asdisy),
                            asinvy: gb(asinvy),
                            atochy: gf(atochy),
                            atq: gf(atq),
                            autxrq: gb(autxrq),
                            autxry: gf(autxry),
                            bcefq: gb(bcefq),
                            bcefy: gb(bcefy),
                            bctq: gf(bctq),
                            bcty: gf(bcty),
                            bdiq: gf(bdiq),
                            bdiy: gf(bdiy),
                            bsprq: gs(bsprq),
                            capcstq: gb(capcstq),
                            capcsty: gb(capcsty),
                            capfly: gb(capfly),
                            capr1q: gf(capr1q),
                            capr2q: gf(capr2q),
                            capr3q: gf(capr3q),
                            capsq: gf(capsq),
                            capxfiy: gb(capxfiy),
                            capxy: gf(capxy),
                            caq: gf(caq),
                            ceqq: gf(ceqq),
                            cfbdq: gf(cfbdq),
                            cfbdy: gf(cfbdy),
                            cfereq: gf(cfereq),
                            cferey: gf(cferey),
                            cflaothy: gf(cflaothy),
                            cfoq: gf(cfoq),
                            cfoy: gf(cfoy),
                            cfpdoq: gb(cfpdoq),
                            cfpdoy: gf(cfpdoy),
                            chechy: gf(chechy),
                            chenfdy: gb(chenfdy),
                            cheq: gf(cheq),
                            chq: gf(chq),
                            chsq: gf(chsq),
                            cltq: gb(cltq),
                            cogsq: gf(cogsq),
                            cogsy: gf(cogsy),
                            compstq: gs(compstq),
                            conm: gs(conm),
                            consol: gs(consol),
                            costat: gs(costat),
                            cstkq: gf(cstkq),
                            curcdq: gs(curcdq),
                            datacqtr: gs(datacqtr),
                            datadate: gd(datadate),
                            datafmt: gs(datafmt),
                            datafqtr: gs(datafqtr),
                            dcsfdy: gb(dcsfdy),
                            dcufdy: gb(dcufdy),
                            dfpacq: gf(dfpacq),
                            dfxaq: gf(dfxaq),
                            dfxay: gf(dfxay),
                            dispochy: gb(dispochy),
                            ditq: gf(ditq),
                            dity: gf(dity),
                            dlcchy: gf(dlcchy),
                            dlcq: gf(dlcq),
                            dltisy: gb(dltisy),
                            dltry: gb(dltry),
                            dlttq: gf(dlttq),
                            docy: gb(docy),
                            dpactq: gf(dpactq),
                            dpcy: gf(dpcy),
                            dpq: gf(dpq),
                            dptbq: gf(dptbq),
                            dptcq: gf(dptcq),
                            dpy: gf(dpy),
                            dvpdpq: gf(dvpdpq),
                            dvpdpy: gf(dvpdpy),
                            dvrecy: gb(dvrecy),
                            dvrreq: gb(dvrreq),
                            dvrrey: gf(dvrrey),
                            dvtq: gf(dvtq),
                            dvty: gf(dvty),
                            dvy: gf(dvy),
                            eieacy: gf(eieacy),
                            eqdivpy: gb(eqdivpy),
                            eqrtq: gf(eqrtq),
                            eroq: gf(eroq),
                            esubq: gf(esubq),
                            esuby: gf(esuby),
                            exchg: gf(exchg),
                            exresy: gb(exresy),
                            exreuy: gb(exreuy),
                            exrey: gf(exrey),
                            fcaq: gf(fcaq),
                            fcay: gf(fcay),
                            fdateq: gd(fdateq),
                            feaq: gf(feaq),
                            felq: gf(felq),
                            fiaoy: gf(fiaoy),
                            fic: gs(fic),
                            fincfy: gf(fincfy),
                            finincy: gb(finincy),
                            finley: gb(finley),
                            finrey: gb(finrey),
                            finvaoy: gb(finvaoy),
                            fopoy: gf(fopoy),
                            fqtr: gf(fqtr),
                            fsrcopoy: gb(fsrcopoy),
                            fsrcopty: gb(fsrcopty),
                            fsrcoy: gb(fsrcoy),
                            fsrcty: gb(fsrcty),
                            fuseoy: gb(fuseoy),
                            fusety: gb(fusety),
                            fyearq: gf(fyearq),
                            fyr: gf(fyr),
                            gdwlamq: gb(gdwlamq),
                            gdwlamy: gf(gdwlamy),
                            gdwlq: gf(gdwlq),
                            gpq: gf(gpq),
                            gpy: gf(gpy),
                            gvkey: gs(gvkey),
                            iatiq: gf(iatiq),
                            ibcy: gf(ibcy),
                            ibkiq: gf(ibkiq),
                            ibkiy: gf(ibkiy),
                            ibmiiq: gf(ibmiiq),
                            ibmiiy: gf(ibmiiy),
                            ibq: gf(ibq),
                            iby: gf(iby),
                            iditq: gf(iditq),
                            idity: gf(idity),
                            iid: gs(iid),
                            iireq: gf(iireq),
                            iirey: gf(iirey),
                            iitq: gf(iitq),
                            iity: gf(iity),
                            indfmt: gs(indfmt),
                            intandy: gb(intandy),
                            intanpy: gf(intanpy),
                            intanq: gf(intanq),
                            intcq: gf(intcq),
                            intcy: gf(intcy),
                            intfacty: gf(intfacty),
                            intfly: gb(intfly),
                            intiacty: gf(intiacty),
                            intoacty: gf(intoacty),
                            intpdy: gb(intpdy),
                            intrcy: gb(intrcy),
                            invchy: gf(invchy),
                            invdspy: gb(invdspy),
                            invsvcy: gb(invsvcy),
                            invtq: gf(invtq),
                            iobdq: gf(iobdq),
                            iobdy: gf(iobdy),
                            ioiq: gf(ioiq),
                            ioiy: gf(ioiy),
                            ioreq: gf(ioreq),
                            iorey: gf(iorey),
                            ipq: gf(ipq),
                            iptiq: gf(iptiq),
                            iptiy: gf(iptiy),
                            isgtq: gf(isgtq),
                            isgty: gf(isgty),
                            isin: gs(isin),
                            istq: gf(istq),
                            ivacoy: gf(ivacoy),
                            ivaeqq: gf(ivaeqq),
                            ivaoq: gf(ivaoq),
                            ivchy: gf(ivchy),
                            iviq: gf(iviq),
                            iviy: gf(iviy),
                            ivncfy: gf(ivncfy),
                            ivptq: gf(ivptq),
                            ivstchy: gf(ivstchy),
                            ivstq: gf(ivstq),
                            ivtfsq: gf(ivtfsq),
                            lcabgq: gf(lcabgq),
                            lcacuq: gf(lcacuq),
                            lcoq: gf(lcoq),
                            lcoxq: gf(lcoxq),
                            lctq: gf(lctq),
                            liqresny: gb(liqresny),
                            liqresoy: gb(liqresoy),
                            lltq: gf(lltq),
                            lndepy: gb(lndepy),
                            lnincy: gb(lnincy),
                            lnmdy: gb(lnmdy),
                            lnrepy: gb(lnrepy),
                            loc: gs(loc),
                            loq: gf(loq),
                            lseq: gf(lseq),
                            lsq: gf(lsq),
                            ltdchy: gf(ltdchy),
                            ltdlchy: gb(ltdlchy),
                            ltloy: gb(ltloy),
                            ltmibq: gf(ltmibq),
                            ltq: gf(ltq),
                            mibnq: gf(mibnq),
                            mibq: gb(mibq),
                            mibtq: gf(mibtq),
                            micy: gf(micy),
                            miiq: gf(miiq),
                            miiy: gf(miiy),
                            miseqy: gf(miseqy),
                            mtlq: gb(mtlq),
                            ncfliqy: gb(ncfliqy),
                            neqmiy: gb(neqmiy),
                            nitq: gb(nitq),
                            nity: gf(nity),
                            noasuby: gb(noasuby),
                            nopioq: gf(nopioq),
                            nopioy: gf(nopioy),
                            nopiq: gf(nopiq),
                            nopiy: gf(nopiy),
                            oancfcy: gb(oancfcy),
                            oancfdy: gb(oancfdy),
                            oancfy: gf(oancfy),
                            oiadpq: gf(oiadpq),
                            oiadpy: gf(oiadpy),
                            oibdpq: gf(oibdpq),
                            oibdpy: gf(oibdpy),
                            opprfty: gb(opprfty),
                            oproq: gf(oproq),
                            oproy: gf(oproy),
                            pclq: gf(pclq),
                            pcly: gf(pcly),
                            pdateq: gd(pdateq),
                            pdq: gf(pdq),
                            pdsa: gf(pdsa),
                            pdytd: gf(pdytd),
                            piq: gf(piq),
                            piy: gf(piy),
                            pliachy: gb(pliachy),
                            popsrc: gs(popsrc),
                            ppentq: gf(ppentq),
                            prcq: gb(prcq),
                            prosaiy: gb(prosaiy),
                            prstkcy: gb(prstkcy),
                            prvy: gf(prvy),
                            psfixy: gf(psfixy),
                            pstkq: gf(pstkq),
                            ptranq: gf(ptranq),
                            ptrany: gf(ptrany),
                            purtshry: gf(purtshry),
                            pvoq: gf(pvoq),
                            pvoy: gf(pvoy),
                            pvtq: gf(pvtq),
                            ratiq: gf(ratiq),
                            rawmsmq: gb(rawmsmq),
                            rawmsmy: gb(rawmsmy),
                            recchy: gf(recchy),
                            reccoq: gf(reccoq),
                            rectoq: gf(rectoq),
                            rectq: gf(rectq),
                            rectrq: gf(rectrq),
                            reitq: gf(reitq),
                            reity: gf(reity),
                            req: gf(req),
                            revtq: gf(revtq),
                            revty: gf(revty),
                            risq: gf(risq),
                            risy: gf(risy),
                            rltq: gf(rltq),
                            rp: gs(rp),
                            rvlrvq: gf(rvlrvq),
                            rvtiq: gf(rvtiq),
                            rvutxq: gb(rvutxq),
                            rvy: gf(rvy),
                            saaq: gf(saaq),
                            saleq: gf(saleq),
                            saley: gf(saley),
                            salq: gf(salq),
                            sbdcq: gb(sbdcq),
                            scfq: gf(scfq),
                            scoq: gb(scoq),
                            scq: gf(scq),
                            sctq: gf(sctq),
                            sedol: gs(sedol),
                            seqq: gf(seqq),
                            shrcapy: gb(shrcapy),
                            sivy: gf(sivy),
                            spiq: gf(spiq),
                            spiy: gf(spiy),
                            sppchy: gb(sppchy),
                            sppivy: gf(sppivy),
                            srcq: gf(srcq),
                            ssnpq: gb(ssnpq),
                            sstky: gf(sstky),
                            staltq: gb(staltq),
                            stfixay: gb(stfixay),
                            stinvy: gb(stinvy),
                            stkchq: gb(stkchq),
                            stkchy: gb(stkchy),
                            subdisy: gb(subdisy),
                            subpury: gb(subpury),
                            tdsgq: gf(tdsgq),
                            tdsgy: gf(tdsgy),
                            tdstq: gf(tdstq),
                            teqq: gf(teqq),
                            transaq: gf(transaq),
                            tstkq: gf(tstkq),
                            txdbq: gf(txdbq),
                            txdcy: gf(txdcy),
                            txopy: gf(txopy),
                            txtq: gf(txtq),
                            txty: gf(txty),
                            txy: gb(txy),
                            unnpq: gf(unnpq),
                            updq: gf(updq),
                            wcapchcy: gb(wcapchcy),
                            wcapchy: gb(wcapchy),
                            wcapopcy: gf(wcapopcy),
                            wcapsay: gb(wcapsay),
                            wcapsuy: gb(wcapsuy),
                            wcapsy: gb(wcapsy),
                            wcapty: gb(wcapty),
                            wcapuy: gb(wcapuy),
                            xagtq: gf(xagtq),
                            xagty: gf(xagty),
                            xbdtq: gf(xbdtq),
                            xbdty: gf(xbdty),
                            xcomiq: gf(xcomiq),
                            xcomiy: gf(xcomiy),
                            xcomq: gb(xcomq),
                            xcomy: gb(xcomy),
                            xdvreq: gb(xdvreq),
                            xdvrey: gb(xdvrey),
                            xidocy: gb(xidocy),
                            xintq: gf(xintq),
                            xinty: gf(xinty),
                            xioq: gf(xioq),
                            xioy: gf(xioy),
                            xiq: gf(xiq),
                            xiviq: gb(xiviq),
                            xiviy: gb(xiviy),
                            xivreq: gf(xivreq),
                            xivrey: gf(xivrey),
                            xiy: gf(xiy),
                            xobdq: gf(xobdq),
                            xobdy: gf(xobdy),
                            xoiq: gf(xoiq),
                            xoiy: gf(xoiy),
                            xoproq: gf(xoproq),
                            xoproy: gf(xoproy),
                            xoprq: gf(xoprq),
                            xopry: gf(xopry),
                            xoreq: gf(xoreq),
                            xorey: gf(xorey),
                            xppq: gf(xppq),
                            xretq: gf(xretq),
                            xrety: gf(xrety),
                            xsgaq: gf(xsgaq),
                            xsgay: gf(xsgay),
                            xsq: gf(xsq),
                            xstoq: gb(xstoq),
                            xstoy: gf(xstoy),
                            xstq: gf(xstq),
                            xsty: gf(xsty),
                            xsy: gf(xsy),
                            xtq: gf(xtq),
                            xty: gf(xty),
                        };
                        let row: Row<'static> = temp.to_row();
                        row
                    })
                    .collect();
                out.extend(rows);
            }

            Ok::<Vec<Row<'static>>, AppError>(out)
        })
        .await?
    }

    fn date_to_any<'a>(d: Option<NaiveDate>) -> AnyValue<'a> {
        match d {
            Some(nd) => {
                let days: i32 = (nd.num_days_from_ce() - 719_163) as i32;
                AnyValue::Date(days)
            }
            None => AnyValue::Null,
        }
    }

    fn bool_to_any<'a>(b: Option<bool>) -> AnyValue<'a> {
        b.map_or(AnyValue::Null, AnyValue::Boolean)
    }

    fn string_to_any<'a>(s: Option<String>) -> AnyValue<'a> {
        s.map(|v| AnyValue::StringOwned(v.into()))
            .unwrap_or(AnyValue::Null)
    }

    pub fn to_row<'a>(self) -> Row<'a> {
        let mut vals: Vec<AnyValue<'a>> = Vec::with_capacity(380);
        vals.push(self.accdq.map_or(AnyValue::Null, AnyValue::Float64));
        vals.push(self.accliy.map_or(AnyValue::Null, AnyValue::Float64));
        vals.push(self.accoq.map_or(AnyValue::Null, AnyValue::Float64));
        vals.push(Self::string_to_any(self.acctstdq));
        vals.push(self.acoq.map_or(AnyValue::Null, AnyValue::Float64));
        vals.push(self.acoxq.map_or(AnyValue::Null, AnyValue::Float64));
        vals.push(Self::bool_to_any(self.acqdisny));
        vals.push(Self::bool_to_any(self.acqdisoy));
        vals.push(self.actq.map_or(AnyValue::Null, AnyValue::Float64));
        vals.push(Self::bool_to_any(self.adpacq));
        vals.push(Self::bool_to_any(self.adpacy));
        vals.push(self.amq.map_or(AnyValue::Null, AnyValue::Float64));
        vals.push(self.amy.map_or(AnyValue::Null, AnyValue::Float64));
        vals.push(self.ancq.map_or(AnyValue::Null, AnyValue::Float64));
        vals.push(self.aolochy.map_or(AnyValue::Null, AnyValue::Float64));
        vals.push(self.aoq.map_or(AnyValue::Null, AnyValue::Float64));
        vals.push(self.aotq.map_or(AnyValue::Null, AnyValue::Float64));
        vals.push(Self::bool_to_any(self.apalchy));
        vals.push(self.apchy.map_or(AnyValue::Null, AnyValue::Float64));
        vals.push(self.apoq.map_or(AnyValue::Null, AnyValue::Float64));
        vals.push(self.apq.map_or(AnyValue::Null, AnyValue::Float64));
        vals.push(self.aqcy.map_or(AnyValue::Null, AnyValue::Float64));
        vals.push(self.artfsq.map_or(AnyValue::Null, AnyValue::Float64));
        vals.push(Self::bool_to_any(self.asdisy));
        vals.push(Self::bool_to_any(self.asinvy));
        vals.push(self.atochy.map_or(AnyValue::Null, AnyValue::Float64));
        vals.push(self.atq.map_or(AnyValue::Null, AnyValue::Float64));
        vals.push(Self::bool_to_any(self.autxrq));
        vals.push(self.autxry.map_or(AnyValue::Null, AnyValue::Float64));
        vals.push(Self::bool_to_any(self.bcefq));
        vals.push(Self::bool_to_any(self.bcefy));
        vals.push(self.bctq.map_or(AnyValue::Null, AnyValue::Float64));
        vals.push(self.bcty.map_or(AnyValue::Null, AnyValue::Float64));
        vals.push(self.bdiq.map_or(AnyValue::Null, AnyValue::Float64));
        vals.push(self.bdiy.map_or(AnyValue::Null, AnyValue::Float64));
        vals.push(Self::string_to_any(self.bsprq));
        vals.push(Self::bool_to_any(self.capcstq));
        vals.push(Self::bool_to_any(self.capcsty));
        vals.push(Self::bool_to_any(self.capfly));
        vals.push(self.capr1q.map_or(AnyValue::Null, AnyValue::Float64));
        vals.push(self.capr2q.map_or(AnyValue::Null, AnyValue::Float64));
        vals.push(self.capr3q.map_or(AnyValue::Null, AnyValue::Float64));
        vals.push(self.capsq.map_or(AnyValue::Null, AnyValue::Float64));
        vals.push(Self::bool_to_any(self.capxfiy));
        vals.push(self.capxy.map_or(AnyValue::Null, AnyValue::Float64));
        vals.push(self.caq.map_or(AnyValue::Null, AnyValue::Float64));
        vals.push(self.ceqq.map_or(AnyValue::Null, AnyValue::Float64));
        vals.push(self.cfbdq.map_or(AnyValue::Null, AnyValue::Float64));
        vals.push(self.cfbdy.map_or(AnyValue::Null, AnyValue::Float64));
        vals.push(self.cfereq.map_or(AnyValue::Null, AnyValue::Float64));
        vals.push(self.cferey.map_or(AnyValue::Null, AnyValue::Float64));
        vals.push(self.cflaothy.map_or(AnyValue::Null, AnyValue::Float64));
        vals.push(self.cfoq.map_or(AnyValue::Null, AnyValue::Float64));
        vals.push(self.cfoy.map_or(AnyValue::Null, AnyValue::Float64));
        vals.push(Self::bool_to_any(self.cfpdoq));
        vals.push(self.cfpdoy.map_or(AnyValue::Null, AnyValue::Float64));
        vals.push(self.chechy.map_or(AnyValue::Null, AnyValue::Float64));
        vals.push(Self::bool_to_any(self.chenfdy));
        vals.push(self.cheq.map_or(AnyValue::Null, AnyValue::Float64));
        vals.push(self.chq.map_or(AnyValue::Null, AnyValue::Float64));
        vals.push(self.chsq.map_or(AnyValue::Null, AnyValue::Float64));
        vals.push(Self::bool_to_any(self.cltq));
        vals.push(self.cogsq.map_or(AnyValue::Null, AnyValue::Float64));
        vals.push(self.cogsy.map_or(AnyValue::Null, AnyValue::Float64));
        vals.push(Self::string_to_any(self.compstq));
        vals.push(Self::string_to_any(self.conm));
        vals.push(Self::string_to_any(self.consol));
        vals.push(Self::string_to_any(self.costat));
        vals.push(self.cstkq.map_or(AnyValue::Null, AnyValue::Float64));
        vals.push(Self::string_to_any(self.curcdq));
        vals.push(Self::string_to_any(self.datacqtr));
        vals.push(Self::date_to_any(self.datadate));
        vals.push(Self::string_to_any(self.datafmt));
        vals.push(Self::string_to_any(self.datafqtr));
        vals.push(Self::bool_to_any(self.dcsfdy));
        vals.push(Self::bool_to_any(self.dcufdy));
        vals.push(self.dfpacq.map_or(AnyValue::Null, AnyValue::Float64));
        vals.push(self.dfxaq.map_or(AnyValue::Null, AnyValue::Float64));
        vals.push(self.dfxay.map_or(AnyValue::Null, AnyValue::Float64));
        vals.push(Self::bool_to_any(self.dispochy));
        vals.push(self.ditq.map_or(AnyValue::Null, AnyValue::Float64));
        vals.push(self.dity.map_or(AnyValue::Null, AnyValue::Float64));
        vals.push(self.dlcchy.map_or(AnyValue::Null, AnyValue::Float64));
        vals.push(self.dlcq.map_or(AnyValue::Null, AnyValue::Float64));
        vals.push(Self::bool_to_any(self.dltisy));
        vals.push(Self::bool_to_any(self.dltry));
        vals.push(self.dlttq.map_or(AnyValue::Null, AnyValue::Float64));
        vals.push(Self::bool_to_any(self.docy));
        vals.push(self.dpactq.map_or(AnyValue::Null, AnyValue::Float64));
        vals.push(self.dpcy.map_or(AnyValue::Null, AnyValue::Float64));
        vals.push(self.dpq.map_or(AnyValue::Null, AnyValue::Float64));
        vals.push(self.dptbq.map_or(AnyValue::Null, AnyValue::Float64));
        vals.push(self.dptcq.map_or(AnyValue::Null, AnyValue::Float64));
        vals.push(self.dpy.map_or(AnyValue::Null, AnyValue::Float64));
        vals.push(self.dvpdpq.map_or(AnyValue::Null, AnyValue::Float64));
        vals.push(self.dvpdpy.map_or(AnyValue::Null, AnyValue::Float64));
        vals.push(Self::bool_to_any(self.dvrecy));
        vals.push(Self::bool_to_any(self.dvrreq));
        vals.push(self.dvrrey.map_or(AnyValue::Null, AnyValue::Float64));
        vals.push(self.dvtq.map_or(AnyValue::Null, AnyValue::Float64));
        vals.push(self.dvty.map_or(AnyValue::Null, AnyValue::Float64));
        vals.push(self.dvy.map_or(AnyValue::Null, AnyValue::Float64));
        vals.push(self.eieacy.map_or(AnyValue::Null, AnyValue::Float64));
        vals.push(Self::bool_to_any(self.eqdivpy));
        vals.push(self.eqrtq.map_or(AnyValue::Null, AnyValue::Float64));
        vals.push(self.eroq.map_or(AnyValue::Null, AnyValue::Float64));
        vals.push(self.esubq.map_or(AnyValue::Null, AnyValue::Float64));
        vals.push(self.esuby.map_or(AnyValue::Null, AnyValue::Float64));
        vals.push(self.exchg.map_or(AnyValue::Null, AnyValue::Float64));
        vals.push(Self::bool_to_any(self.exresy));
        vals.push(Self::bool_to_any(self.exreuy));
        vals.push(self.exrey.map_or(AnyValue::Null, AnyValue::Float64));
        vals.push(self.fcaq.map_or(AnyValue::Null, AnyValue::Float64));
        vals.push(self.fcay.map_or(AnyValue::Null, AnyValue::Float64));
        vals.push(Self::date_to_any(self.fdateq));
        vals.push(self.feaq.map_or(AnyValue::Null, AnyValue::Float64));
        vals.push(self.felq.map_or(AnyValue::Null, AnyValue::Float64));
        vals.push(self.fiaoy.map_or(AnyValue::Null, AnyValue::Float64));
        vals.push(Self::string_to_any(self.fic));
        vals.push(self.fincfy.map_or(AnyValue::Null, AnyValue::Float64));
        vals.push(Self::bool_to_any(self.finincy));
        vals.push(Self::bool_to_any(self.finley));
        vals.push(Self::bool_to_any(self.finrey));
        vals.push(Self::bool_to_any(self.finvaoy));
        vals.push(self.fopoy.map_or(AnyValue::Null, AnyValue::Float64));
        vals.push(self.fqtr.map_or(AnyValue::Null, AnyValue::Float64));
        vals.push(Self::bool_to_any(self.fsrcopoy));
        vals.push(Self::bool_to_any(self.fsrcopty));
        vals.push(Self::bool_to_any(self.fsrcoy));
        vals.push(Self::bool_to_any(self.fsrcty));
        vals.push(Self::bool_to_any(self.fuseoy));
        vals.push(Self::bool_to_any(self.fusety));
        vals.push(self.fyearq.map_or(AnyValue::Null, AnyValue::Float64));
        vals.push(self.fyr.map_or(AnyValue::Null, AnyValue::Float64));
        vals.push(Self::bool_to_any(self.gdwlamq));
        vals.push(self.gdwlamy.map_or(AnyValue::Null, AnyValue::Float64));
        vals.push(self.gdwlq.map_or(AnyValue::Null, AnyValue::Float64));
        vals.push(self.gpq.map_or(AnyValue::Null, AnyValue::Float64));
        vals.push(self.gpy.map_or(AnyValue::Null, AnyValue::Float64));
        vals.push(Self::string_to_any(self.gvkey));
        vals.push(self.iatiq.map_or(AnyValue::Null, AnyValue::Float64));
        vals.push(self.ibcy.map_or(AnyValue::Null, AnyValue::Float64));
        vals.push(self.ibkiq.map_or(AnyValue::Null, AnyValue::Float64));
        vals.push(self.ibkiy.map_or(AnyValue::Null, AnyValue::Float64));
        vals.push(self.ibmiiq.map_or(AnyValue::Null, AnyValue::Float64));
        vals.push(self.ibmiiy.map_or(AnyValue::Null, AnyValue::Float64));
        vals.push(self.ibq.map_or(AnyValue::Null, AnyValue::Float64));
        vals.push(self.iby.map_or(AnyValue::Null, AnyValue::Float64));
        vals.push(self.iditq.map_or(AnyValue::Null, AnyValue::Float64));
        vals.push(self.idity.map_or(AnyValue::Null, AnyValue::Float64));
        vals.push(Self::string_to_any(self.iid));
        vals.push(self.iireq.map_or(AnyValue::Null, AnyValue::Float64));
        vals.push(self.iirey.map_or(AnyValue::Null, AnyValue::Float64));
        vals.push(self.iitq.map_or(AnyValue::Null, AnyValue::Float64));
        vals.push(self.iity.map_or(AnyValue::Null, AnyValue::Float64));
        vals.push(Self::string_to_any(self.indfmt));
        vals.push(Self::bool_to_any(self.intandy));
        vals.push(self.intanpy.map_or(AnyValue::Null, AnyValue::Float64));
        vals.push(self.intanq.map_or(AnyValue::Null, AnyValue::Float64));
        vals.push(self.intcq.map_or(AnyValue::Null, AnyValue::Float64));
        vals.push(self.intcy.map_or(AnyValue::Null, AnyValue::Float64));
        vals.push(self.intfacty.map_or(AnyValue::Null, AnyValue::Float64));
        vals.push(Self::bool_to_any(self.intfly));
        vals.push(self.intiacty.map_or(AnyValue::Null, AnyValue::Float64));
        vals.push(self.intoacty.map_or(AnyValue::Null, AnyValue::Float64));
        vals.push(Self::bool_to_any(self.intpdy));
        vals.push(Self::bool_to_any(self.intrcy));
        vals.push(self.invchy.map_or(AnyValue::Null, AnyValue::Float64));
        vals.push(Self::bool_to_any(self.invdspy));
        vals.push(Self::bool_to_any(self.invsvcy));
        vals.push(self.invtq.map_or(AnyValue::Null, AnyValue::Float64));
        vals.push(self.iobdq.map_or(AnyValue::Null, AnyValue::Float64));
        vals.push(self.iobdy.map_or(AnyValue::Null, AnyValue::Float64));
        vals.push(self.ioiq.map_or(AnyValue::Null, AnyValue::Float64));
        vals.push(self.ioiy.map_or(AnyValue::Null, AnyValue::Float64));
        vals.push(self.ioreq.map_or(AnyValue::Null, AnyValue::Float64));
        vals.push(self.iorey.map_or(AnyValue::Null, AnyValue::Float64));
        vals.push(self.ipq.map_or(AnyValue::Null, AnyValue::Float64));
        vals.push(self.iptiq.map_or(AnyValue::Null, AnyValue::Float64));
        vals.push(self.iptiy.map_or(AnyValue::Null, AnyValue::Float64));
        vals.push(self.isgtq.map_or(AnyValue::Null, AnyValue::Float64));
        vals.push(self.isgty.map_or(AnyValue::Null, AnyValue::Float64));
        vals.push(Self::string_to_any(self.isin));
        vals.push(self.istq.map_or(AnyValue::Null, AnyValue::Float64));
        vals.push(self.ivacoy.map_or(AnyValue::Null, AnyValue::Float64));
        vals.push(self.ivaeqq.map_or(AnyValue::Null, AnyValue::Float64));
        vals.push(self.ivaoq.map_or(AnyValue::Null, AnyValue::Float64));
        vals.push(self.ivchy.map_or(AnyValue::Null, AnyValue::Float64));
        vals.push(self.iviq.map_or(AnyValue::Null, AnyValue::Float64));
        vals.push(self.iviy.map_or(AnyValue::Null, AnyValue::Float64));
        vals.push(self.ivncfy.map_or(AnyValue::Null, AnyValue::Float64));
        vals.push(self.ivptq.map_or(AnyValue::Null, AnyValue::Float64));
        vals.push(self.ivstchy.map_or(AnyValue::Null, AnyValue::Float64));
        vals.push(self.ivstq.map_or(AnyValue::Null, AnyValue::Float64));
        vals.push(self.ivtfsq.map_or(AnyValue::Null, AnyValue::Float64));
        vals.push(self.lcabgq.map_or(AnyValue::Null, AnyValue::Float64));
        vals.push(self.lcacuq.map_or(AnyValue::Null, AnyValue::Float64));
        vals.push(self.lcoq.map_or(AnyValue::Null, AnyValue::Float64));
        vals.push(self.lcoxq.map_or(AnyValue::Null, AnyValue::Float64));
        vals.push(self.lctq.map_or(AnyValue::Null, AnyValue::Float64));
        vals.push(Self::bool_to_any(self.liqresny));
        vals.push(Self::bool_to_any(self.liqresoy));
        vals.push(self.lltq.map_or(AnyValue::Null, AnyValue::Float64));
        vals.push(Self::bool_to_any(self.lndepy));
        vals.push(Self::bool_to_any(self.lnincy));
        vals.push(Self::bool_to_any(self.lnmdy));
        vals.push(Self::bool_to_any(self.lnrepy));
        vals.push(Self::string_to_any(self.loc));
        vals.push(self.loq.map_or(AnyValue::Null, AnyValue::Float64));
        vals.push(self.lseq.map_or(AnyValue::Null, AnyValue::Float64));
        vals.push(self.lsq.map_or(AnyValue::Null, AnyValue::Float64));
        vals.push(self.ltdchy.map_or(AnyValue::Null, AnyValue::Float64));
        vals.push(Self::bool_to_any(self.ltdlchy));
        vals.push(Self::bool_to_any(self.ltloy));
        vals.push(self.ltmibq.map_or(AnyValue::Null, AnyValue::Float64));
        vals.push(self.ltq.map_or(AnyValue::Null, AnyValue::Float64));
        vals.push(self.mibnq.map_or(AnyValue::Null, AnyValue::Float64));
        vals.push(Self::bool_to_any(self.mibq));
        vals.push(self.mibtq.map_or(AnyValue::Null, AnyValue::Float64));
        vals.push(self.micy.map_or(AnyValue::Null, AnyValue::Float64));
        vals.push(self.miiq.map_or(AnyValue::Null, AnyValue::Float64));
        vals.push(self.miiy.map_or(AnyValue::Null, AnyValue::Float64));
        vals.push(self.miseqy.map_or(AnyValue::Null, AnyValue::Float64));
        vals.push(Self::bool_to_any(self.mtlq));
        vals.push(Self::bool_to_any(self.ncfliqy));
        vals.push(Self::bool_to_any(self.neqmiy));
        vals.push(Self::bool_to_any(self.nitq));
        vals.push(self.nity.map_or(AnyValue::Null, AnyValue::Float64));
        vals.push(Self::bool_to_any(self.noasuby));
        vals.push(self.nopioq.map_or(AnyValue::Null, AnyValue::Float64));
        vals.push(self.nopioy.map_or(AnyValue::Null, AnyValue::Float64));
        vals.push(self.nopiq.map_or(AnyValue::Null, AnyValue::Float64));
        vals.push(self.nopiy.map_or(AnyValue::Null, AnyValue::Float64));
        vals.push(Self::bool_to_any(self.oancfcy));
        vals.push(Self::bool_to_any(self.oancfdy));
        vals.push(self.oancfy.map_or(AnyValue::Null, AnyValue::Float64));
        vals.push(self.oiadpq.map_or(AnyValue::Null, AnyValue::Float64));
        vals.push(self.oiadpy.map_or(AnyValue::Null, AnyValue::Float64));
        vals.push(self.oibdpq.map_or(AnyValue::Null, AnyValue::Float64));
        vals.push(self.oibdpy.map_or(AnyValue::Null, AnyValue::Float64));
        vals.push(Self::bool_to_any(self.opprfty));
        vals.push(self.oproq.map_or(AnyValue::Null, AnyValue::Float64));
        vals.push(self.oproy.map_or(AnyValue::Null, AnyValue::Float64));
        vals.push(self.pclq.map_or(AnyValue::Null, AnyValue::Float64));
        vals.push(self.pcly.map_or(AnyValue::Null, AnyValue::Float64));
        vals.push(Self::date_to_any(self.pdateq));
        vals.push(self.pdq.map_or(AnyValue::Null, AnyValue::Float64));
        vals.push(self.pdsa.map_or(AnyValue::Null, AnyValue::Float64));
        vals.push(self.pdytd.map_or(AnyValue::Null, AnyValue::Float64));
        vals.push(self.piq.map_or(AnyValue::Null, AnyValue::Float64));
        vals.push(self.piy.map_or(AnyValue::Null, AnyValue::Float64));
        vals.push(Self::bool_to_any(self.pliachy));
        vals.push(Self::string_to_any(self.popsrc));
        vals.push(self.ppentq.map_or(AnyValue::Null, AnyValue::Float64));
        vals.push(Self::bool_to_any(self.prcq));
        vals.push(Self::bool_to_any(self.prosaiy));
        vals.push(Self::bool_to_any(self.prstkcy));
        vals.push(self.prvy.map_or(AnyValue::Null, AnyValue::Float64));
        vals.push(self.psfixy.map_or(AnyValue::Null, AnyValue::Float64));
        vals.push(self.pstkq.map_or(AnyValue::Null, AnyValue::Float64));
        vals.push(self.ptranq.map_or(AnyValue::Null, AnyValue::Float64));
        vals.push(self.ptrany.map_or(AnyValue::Null, AnyValue::Float64));
        vals.push(self.purtshry.map_or(AnyValue::Null, AnyValue::Float64));
        vals.push(self.pvoq.map_or(AnyValue::Null, AnyValue::Float64));
        vals.push(self.pvoy.map_or(AnyValue::Null, AnyValue::Float64));
        vals.push(self.pvtq.map_or(AnyValue::Null, AnyValue::Float64));
        vals.push(self.ratiq.map_or(AnyValue::Null, AnyValue::Float64));
        vals.push(Self::bool_to_any(self.rawmsmq));
        vals.push(Self::bool_to_any(self.rawmsmy));
        vals.push(self.recchy.map_or(AnyValue::Null, AnyValue::Float64));
        vals.push(self.reccoq.map_or(AnyValue::Null, AnyValue::Float64));
        vals.push(self.rectoq.map_or(AnyValue::Null, AnyValue::Float64));
        vals.push(self.rectq.map_or(AnyValue::Null, AnyValue::Float64));
        vals.push(self.rectrq.map_or(AnyValue::Null, AnyValue::Float64));
        vals.push(self.reitq.map_or(AnyValue::Null, AnyValue::Float64));
        vals.push(self.reity.map_or(AnyValue::Null, AnyValue::Float64));
        vals.push(self.req.map_or(AnyValue::Null, AnyValue::Float64));
        vals.push(self.revtq.map_or(AnyValue::Null, AnyValue::Float64));
        vals.push(self.revty.map_or(AnyValue::Null, AnyValue::Float64));
        vals.push(self.risq.map_or(AnyValue::Null, AnyValue::Float64));
        vals.push(self.risy.map_or(AnyValue::Null, AnyValue::Float64));
        vals.push(self.rltq.map_or(AnyValue::Null, AnyValue::Float64));
        vals.push(Self::string_to_any(self.rp));
        vals.push(self.rvlrvq.map_or(AnyValue::Null, AnyValue::Float64));
        vals.push(self.rvtiq.map_or(AnyValue::Null, AnyValue::Float64));
        vals.push(Self::bool_to_any(self.rvutxq));
        vals.push(self.rvy.map_or(AnyValue::Null, AnyValue::Float64));
        vals.push(self.saaq.map_or(AnyValue::Null, AnyValue::Float64));
        vals.push(self.saleq.map_or(AnyValue::Null, AnyValue::Float64));
        vals.push(self.saley.map_or(AnyValue::Null, AnyValue::Float64));
        vals.push(self.salq.map_or(AnyValue::Null, AnyValue::Float64));
        vals.push(Self::bool_to_any(self.sbdcq));
        vals.push(self.scfq.map_or(AnyValue::Null, AnyValue::Float64));
        vals.push(Self::bool_to_any(self.scoq));
        vals.push(self.scq.map_or(AnyValue::Null, AnyValue::Float64));
        vals.push(self.sctq.map_or(AnyValue::Null, AnyValue::Float64));
        vals.push(Self::string_to_any(self.sedol));
        vals.push(self.seqq.map_or(AnyValue::Null, AnyValue::Float64));
        vals.push(Self::bool_to_any(self.shrcapy));
        vals.push(self.sivy.map_or(AnyValue::Null, AnyValue::Float64));
        vals.push(self.spiq.map_or(AnyValue::Null, AnyValue::Float64));
        vals.push(self.spiy.map_or(AnyValue::Null, AnyValue::Float64));
        vals.push(Self::bool_to_any(self.sppchy));
        vals.push(self.sppivy.map_or(AnyValue::Null, AnyValue::Float64));
        vals.push(self.srcq.map_or(AnyValue::Null, AnyValue::Float64));
        vals.push(Self::bool_to_any(self.ssnpq));
        vals.push(self.sstky.map_or(AnyValue::Null, AnyValue::Float64));
        vals.push(Self::bool_to_any(self.staltq));
        vals.push(Self::bool_to_any(self.stfixay));
        vals.push(Self::bool_to_any(self.stinvy));
        vals.push(Self::bool_to_any(self.stkchq));
        vals.push(Self::bool_to_any(self.stkchy));
        vals.push(Self::bool_to_any(self.subdisy));
        vals.push(Self::bool_to_any(self.subpury));
        vals.push(self.tdsgq.map_or(AnyValue::Null, AnyValue::Float64));
        vals.push(self.tdsgy.map_or(AnyValue::Null, AnyValue::Float64));
        vals.push(self.tdstq.map_or(AnyValue::Null, AnyValue::Float64));
        vals.push(self.teqq.map_or(AnyValue::Null, AnyValue::Float64));
        vals.push(self.transaq.map_or(AnyValue::Null, AnyValue::Float64));
        vals.push(self.tstkq.map_or(AnyValue::Null, AnyValue::Float64));
        vals.push(self.txdbq.map_or(AnyValue::Null, AnyValue::Float64));
        vals.push(self.txdcy.map_or(AnyValue::Null, AnyValue::Float64));
        vals.push(self.txopy.map_or(AnyValue::Null, AnyValue::Float64));
        vals.push(self.txtq.map_or(AnyValue::Null, AnyValue::Float64));
        vals.push(self.txty.map_or(AnyValue::Null, AnyValue::Float64));
        vals.push(Self::bool_to_any(self.txy));
        vals.push(self.unnpq.map_or(AnyValue::Null, AnyValue::Float64));
        vals.push(self.updq.map_or(AnyValue::Null, AnyValue::Float64));
        vals.push(Self::bool_to_any(self.wcapchcy));
        vals.push(Self::bool_to_any(self.wcapchy));
        vals.push(self.wcapopcy.map_or(AnyValue::Null, AnyValue::Float64));
        vals.push(Self::bool_to_any(self.wcapsay));
        vals.push(Self::bool_to_any(self.wcapsuy));
        vals.push(Self::bool_to_any(self.wcapsy));
        vals.push(Self::bool_to_any(self.wcapty));
        vals.push(Self::bool_to_any(self.wcapuy));
        vals.push(self.xagtq.map_or(AnyValue::Null, AnyValue::Float64));
        vals.push(self.xagty.map_or(AnyValue::Null, AnyValue::Float64));
        vals.push(self.xbdtq.map_or(AnyValue::Null, AnyValue::Float64));
        vals.push(self.xbdty.map_or(AnyValue::Null, AnyValue::Float64));
        vals.push(self.xcomiq.map_or(AnyValue::Null, AnyValue::Float64));
        vals.push(self.xcomiy.map_or(AnyValue::Null, AnyValue::Float64));
        vals.push(Self::bool_to_any(self.xcomq));
        vals.push(Self::bool_to_any(self.xcomy));
        vals.push(Self::bool_to_any(self.xdvreq));
        vals.push(Self::bool_to_any(self.xdvrey));
        vals.push(Self::bool_to_any(self.xidocy));
        vals.push(self.xintq.map_or(AnyValue::Null, AnyValue::Float64));
        vals.push(self.xinty.map_or(AnyValue::Null, AnyValue::Float64));
        vals.push(self.xioq.map_or(AnyValue::Null, AnyValue::Float64));
        vals.push(self.xioy.map_or(AnyValue::Null, AnyValue::Float64));
        vals.push(self.xiq.map_or(AnyValue::Null, AnyValue::Float64));
        vals.push(Self::bool_to_any(self.xiviq));
        vals.push(Self::bool_to_any(self.xiviy));
        vals.push(self.xivreq.map_or(AnyValue::Null, AnyValue::Float64));
        vals.push(self.xivrey.map_or(AnyValue::Null, AnyValue::Float64));
        vals.push(self.xiy.map_or(AnyValue::Null, AnyValue::Float64));
        vals.push(self.xobdq.map_or(AnyValue::Null, AnyValue::Float64));
        vals.push(self.xobdy.map_or(AnyValue::Null, AnyValue::Float64));
        vals.push(self.xoiq.map_or(AnyValue::Null, AnyValue::Float64));
        vals.push(self.xoiy.map_or(AnyValue::Null, AnyValue::Float64));
        vals.push(self.xoproq.map_or(AnyValue::Null, AnyValue::Float64));
        vals.push(self.xoproy.map_or(AnyValue::Null, AnyValue::Float64));
        vals.push(self.xoprq.map_or(AnyValue::Null, AnyValue::Float64));
        vals.push(self.xopry.map_or(AnyValue::Null, AnyValue::Float64));
        vals.push(self.xoreq.map_or(AnyValue::Null, AnyValue::Float64));
        vals.push(self.xorey.map_or(AnyValue::Null, AnyValue::Float64));
        vals.push(self.xppq.map_or(AnyValue::Null, AnyValue::Float64));
        vals.push(self.xretq.map_or(AnyValue::Null, AnyValue::Float64));
        vals.push(self.xrety.map_or(AnyValue::Null, AnyValue::Float64));
        vals.push(self.xsgaq.map_or(AnyValue::Null, AnyValue::Float64));
        vals.push(self.xsgay.map_or(AnyValue::Null, AnyValue::Float64));
        vals.push(self.xsq.map_or(AnyValue::Null, AnyValue::Float64));
        vals.push(Self::bool_to_any(self.xstoq));
        vals.push(self.xstoy.map_or(AnyValue::Null, AnyValue::Float64));
        vals.push(self.xstq.map_or(AnyValue::Null, AnyValue::Float64));
        vals.push(self.xsty.map_or(AnyValue::Null, AnyValue::Float64));
        vals.push(self.xsy.map_or(AnyValue::Null, AnyValue::Float64));
        vals.push(self.xtq.map_or(AnyValue::Null, AnyValue::Float64));
        vals.push(self.xty.map_or(AnyValue::Null, AnyValue::Float64));
        Row::new(vals)
    }
}
