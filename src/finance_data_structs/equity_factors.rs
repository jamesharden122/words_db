use super::{AppError, DuckCrudModel, SurrealCrudModel, ToPolars};
use arrow_array::{Array, Date32Array, Float64Array, Int64Array, StringArray};
use chrono::{Datelike, NaiveDate};
use duckdb::Connection;
use polars::frame::row::Row;
use polars::prelude::*;
use rayon::prelude::*;
use serde::{Deserialize, Serialize};
use std::path::Path;
use std::sync::{Arc, Mutex};
use surrealdb::engine::local::Db;
use surrealdb::Surreal;

/// Monthly Equity Factors (wide per-security factor panel).
///
/// Notes:
/// - The raw Parquet has ~443 columns. Instead of hand-typing all columns
///   into the Rust struct, we keep only key identifiers for Surreal `id_key`
///   and reading by date. For data analysis in DuckDB/Polars, use
///   `duck_from_parquet` to materialize the full table and query all columns.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EquityFactorsMonthly {
    // Core identifiers / dates
    pub gvkey: Option<i32>,
    pub iid: Option<String>,
    pub permno: Option<i32>,
    pub permco: Option<i32>,
    pub id: Option<i64>,
    pub date: Option<NaiveDate>,
    pub eom: Option<NaiveDate>,
    pub excntry: Option<String>,
    pub size_grp: Option<String>,

    // A few common measures (present in the sample)
    pub me: Option<f64>,
    pub ret_exc: Option<f64>,
    pub ret: Option<f64>,
    pub prc: Option<f64>,

    // Added fields
    pub obs_main: Option<f64>,
    pub exch_main: Option<f64>,
    pub common: Option<f64>,
    pub primary_sec: Option<f64>,
    pub me_company: Option<f64>,
    pub ret_exc_lead1m: Option<f64>,
    pub ret_local: Option<f64>,
    pub ret_lag_dif: Option<f64>,
    pub prc_local: Option<f64>,
    pub prc_high: Option<f64>,
    pub prc_low: Option<f64>,
    pub bidask: Option<f64>,
    pub curcd: Option<String>,
    pub fx: Option<f64>,
    pub gics: Option<f64>,
    pub naics: Option<f64>,
    pub sic: Option<f64>,
    pub ff49: Option<f64>,
    pub dolvol: Option<f64>,
    pub shares: Option<f64>,
    pub tvol: Option<f64>,
    pub adjfct: Option<f64>,
    pub comp_tpci: Option<String>,
    pub crsp_shrcd: Option<f64>,
    pub comp_exchg: Option<f64>,
    pub crsp_exchcd: Option<f64>,
    pub source_crsp: Option<f64>,
    pub market_equity: Option<f64>,
    pub div12m_me: Option<f64>,
    pub chcsho_12m: Option<f64>,
    pub eqnpo_12m: Option<f64>,
    pub ret_1_0: Option<f64>,
    pub ret_3_1: Option<f64>,
    pub ret_6_1: Option<f64>,
    pub ret_9_1: Option<f64>,
    pub ret_12_1: Option<f64>,
    pub ret_12_7: Option<f64>,
    pub ret_60_12: Option<f64>,
    pub seas_1_1an: Option<f64>,
    pub seas_1_1na: Option<f64>,
    pub seas_2_5an: Option<f64>,
    pub seas_2_5na: Option<f64>,
    pub seas_6_10an: Option<f64>,
    pub seas_6_10na: Option<f64>,
    pub seas_11_15an: Option<f64>,
    pub seas_11_15na: Option<f64>,
    pub seas_16_20an: Option<f64>,
    pub seas_16_20na: Option<f64>,
    pub at_gr1: Option<f64>,
    pub sale_gr1: Option<f64>,
    pub capx_gr1: Option<f64>,
    pub inv_gr1: Option<f64>,
    pub debt_gr3: Option<f64>,
    pub sale_gr3: Option<f64>,
    pub capx_gr3: Option<f64>,
    pub inv_gr1a: Option<f64>,
    pub lti_gr1a: Option<f64>,
    pub sti_gr1a: Option<f64>,
    pub coa_gr1a: Option<f64>,
    pub col_gr1a: Option<f64>,
    pub cowc_gr1a: Option<f64>,
    pub ncoa_gr1a: Option<f64>,
    pub ncol_gr1a: Option<f64>,
    pub nncoa_gr1a: Option<f64>,
    pub fnl_gr1a: Option<f64>,
    pub nfna_gr1a: Option<f64>,
    pub tax_gr1a: Option<f64>,
    pub be_gr1a: Option<f64>,
    pub ebit_sale: Option<f64>,
    pub gp_at: Option<f64>,
    pub cop_at: Option<f64>,
    pub ope_be: Option<f64>,
    pub ni_be: Option<f64>,
    pub ebit_bev: Option<f64>,
    pub netis_at: Option<f64>,
    pub eqnetis_at: Option<f64>,
    pub dbnetis_at: Option<f64>,
    pub oaccruals_at: Option<f64>,
    pub oaccruals_ni: Option<f64>,
    pub taccruals_at: Option<f64>,
    pub taccruals_ni: Option<f64>,
    pub noa_at: Option<f64>,
    pub opex_at: Option<f64>,
    pub at_turnover: Option<f64>,
    pub sale_bev: Option<f64>,
    pub rd_sale: Option<f64>,
    pub cash_at: Option<f64>,
    pub sale_emp_gr1: Option<f64>,
    pub emp_gr1: Option<f64>,
    pub ni_inc8q: Option<f64>,
    pub noa_gr1a: Option<f64>,
    pub ppeinv_gr1a: Option<f64>,
    pub lnoa_gr1a: Option<f64>,
    pub capx_gr2: Option<f64>,
    pub saleq_gr1: Option<f64>,
    pub niq_be: Option<f64>,
    pub niq_at: Option<f64>,
    pub niq_be_chg1: Option<f64>,
    pub niq_at_chg1: Option<f64>,
    pub rd5_at: Option<f64>,
    pub dsale_dinv: Option<f64>,
    pub dsale_drec: Option<f64>,
    pub dgp_dsale: Option<f64>,
    pub dsale_dsga: Option<f64>,
    pub saleq_su: Option<f64>,
    pub niq_su: Option<f64>,
    pub capex_abn: Option<f64>,
    pub op_atl1: Option<f64>,
    pub gp_atl1: Option<f64>,
    pub ope_bel1: Option<f64>,
    pub cop_atl1: Option<f64>,
    pub pi_nix: Option<f64>,
    pub ocf_at: Option<f64>,
    pub op_at: Option<f64>,
    pub ocf_at_chg1: Option<f64>,
    pub at_be: Option<f64>,
    pub ocfq_saleq_std: Option<f64>,
    pub tangibility: Option<f64>,
    pub earnings_variability: Option<f64>,
    pub aliq_at: Option<f64>,
    pub f_score: Option<f64>,
    pub o_score: Option<f64>,
    pub z_score: Option<f64>,
    pub kz_index: Option<f64>,
    pub ni_ar1: Option<f64>,
    pub ni_ivol: Option<f64>,
    pub at_me: Option<f64>,
    pub be_me: Option<f64>,
    pub debt_me: Option<f64>,
    pub netdebt_me: Option<f64>,
    pub sale_me: Option<f64>,
    pub ni_me: Option<f64>,
    pub ocf_me: Option<f64>,
    pub fcf_me: Option<f64>,
    pub eqpo_me: Option<f64>,
    pub eqnpo_me: Option<f64>,
    pub rd_me: Option<f64>,
    pub ival_me: Option<f64>,
    pub bev_mev: Option<f64>,
    pub ebitda_mev: Option<f64>,
    pub aliq_mat: Option<f64>,
    pub eq_dur: Option<f64>,
    pub beta_60m: Option<f64>,
    pub resff3_12_1: Option<f64>,
    pub resff3_6_1: Option<f64>,
    pub mispricing_mgmt: Option<f64>,
    pub mispricing_perf: Option<f64>,
    pub ivol_capm_21d: Option<f64>,
    pub iskew_capm_21d: Option<f64>,
    pub coskew_21d: Option<f64>,
    pub beta_dimson_21d: Option<f64>,
    pub ivol_ff3_21d: Option<f64>,
    pub iskew_ff3_21d: Option<f64>,
    pub ivol_hxz4_21d: Option<f64>,
    pub iskew_hxz4_21d: Option<f64>,
    pub rmax5_21d: Option<f64>,
    pub rmax1_21d: Option<f64>,
    pub rvol_21d: Option<f64>,
    pub rskew_21d: Option<f64>,
    pub zero_trades_21d: Option<f64>,
    pub dolvol_126d: Option<f64>,
    pub dolvol_var_126d: Option<f64>,
    pub turnover_126d: Option<f64>,
    pub turnover_var_126d: Option<f64>,
    pub zero_trades_126d: Option<f64>,
    pub zero_trades_252d: Option<f64>,
    pub ami_126d: Option<f64>,
    pub ivol_capm_252d: Option<f64>,
    pub prc_highprc_252d: Option<f64>,
    pub betadown_252d: Option<f64>,
    pub bidaskhl_21d: Option<f64>,
    pub corr_1260d: Option<f64>,
    pub betabab_1260d: Option<f64>,
    pub rmax5_rvol_21d: Option<f64>,
    pub age: Option<f64>,
    pub qmj: Option<f64>,
    pub qmj_prof: Option<f64>,
    pub qmj_growth: Option<f64>,
    pub qmj_safety: Option<f64>,
    pub enterprise_value: Option<f64>,
    pub book_equity: Option<f64>,
    pub assets: Option<f64>,
    pub sales: Option<f64>,
    pub net_income: Option<f64>,
    pub div1m_me: Option<f64>,
    pub div3m_me: Option<f64>,
    pub div6m_me: Option<f64>,
    pub divspc1m_me: Option<f64>,
    pub divspc12m_me: Option<f64>,
    pub chcsho_1m: Option<f64>,
    pub chcsho_3m: Option<f64>,
    pub chcsho_6m: Option<f64>,
    pub eqnpo_1m: Option<f64>,
    pub eqnpo_3m: Option<f64>,
    pub eqnpo_6m: Option<f64>,
    pub ret_2_0: Option<f64>,
    pub ret_3_0: Option<f64>,
    pub ret_6_0: Option<f64>,
    pub ret_9_0: Option<f64>,
    pub ret_12_0: Option<f64>,
    pub ret_18_1: Option<f64>,
    pub ret_24_1: Option<f64>,
    pub ret_24_12: Option<f64>,
    pub ret_36_1: Option<f64>,
    pub ret_36_12: Option<f64>,
    pub ret_48_12: Option<f64>,
    pub ret_48_1: Option<f64>,
    pub ret_60_1: Option<f64>,
    pub ret_60_36: Option<f64>,
    pub ca_gr1: Option<f64>,
    pub nca_gr1: Option<f64>,
    pub lt_gr1: Option<f64>,
    pub cl_gr1: Option<f64>,
    pub ncl_gr1: Option<f64>,
    pub be_gr1: Option<f64>,
    pub pstk_gr1: Option<f64>,
    pub debt_gr1: Option<f64>,
    pub cogs_gr1: Option<f64>,
    pub sga_gr1: Option<f64>,
    pub opex_gr1: Option<f64>,
    pub at_gr3: Option<f64>,
    pub ca_gr3: Option<f64>,
    pub nca_gr3: Option<f64>,
    pub lt_gr3: Option<f64>,
    pub cl_gr3: Option<f64>,
    pub ncl_gr3: Option<f64>,
    pub be_gr3: Option<f64>,
    pub pstk_gr3: Option<f64>,
    pub cogs_gr3: Option<f64>,
    pub sga_gr3: Option<f64>,
    pub opex_gr3: Option<f64>,
    pub cash_gr1a: Option<f64>,
    pub rec_gr1a: Option<f64>,
    pub ppeg_gr1a: Option<f64>,
    pub intan_gr1a: Option<f64>,
    pub debtst_gr1a: Option<f64>,
    pub ap_gr1a: Option<f64>,
    pub txp_gr1a: Option<f64>,
    pub debtlt_gr1a: Option<f64>,
    pub txditc_gr1a: Option<f64>,
    pub oa_gr1a: Option<f64>,
    pub ol_gr1a: Option<f64>,
    pub fna_gr1a: Option<f64>,
    pub gp_gr1a: Option<f64>,
    pub ebitda_gr1a: Option<f64>,
    pub ebit_gr1a: Option<f64>,
    pub ope_gr1a: Option<f64>,
    pub ni_gr1a: Option<f64>,
    pub nix_gr1a: Option<f64>,
    pub dp_gr1a: Option<f64>,
    pub fincf_gr1a: Option<f64>,
    pub ocf_gr1a: Option<f64>,
    pub fcf_gr1a: Option<f64>,
    pub nwc_gr1a: Option<f64>,
    pub eqnetis_gr1a: Option<f64>,
    pub dltnetis_gr1a: Option<f64>,
    pub dstnetis_gr1a: Option<f64>,
    pub dbnetis_gr1a: Option<f64>,
    pub netis_gr1a: Option<f64>,
    pub eqnpo_gr1a: Option<f64>,
    pub eqbb_gr1a: Option<f64>,
    pub eqis_gr1a: Option<f64>,
    pub div_gr1a: Option<f64>,
    pub eqpo_gr1a: Option<f64>,
    pub capx_gr1a: Option<f64>,
    pub cash_gr3a: Option<f64>,
    pub inv_gr3a: Option<f64>,
    pub rec_gr3a: Option<f64>,
    pub ppeg_gr3a: Option<f64>,
    pub lti_gr3a: Option<f64>,
    pub intan_gr3a: Option<f64>,
    pub debtst_gr3a: Option<f64>,
    pub ap_gr3a: Option<f64>,
    pub txp_gr3a: Option<f64>,
    pub debtlt_gr3a: Option<f64>,
    pub txditc_gr3a: Option<f64>,
    pub coa_gr3a: Option<f64>,
    pub col_gr3a: Option<f64>,
    pub cowc_gr3a: Option<f64>,
    pub ncoa_gr3a: Option<f64>,
    pub ncol_gr3a: Option<f64>,
    pub nncoa_gr3a: Option<f64>,
    pub oa_gr3a: Option<f64>,
    pub ol_gr3a: Option<f64>,
    pub fna_gr3a: Option<f64>,
    pub fnl_gr3a: Option<f64>,
    pub nfna_gr3a: Option<f64>,
    pub gp_gr3a: Option<f64>,
    pub ebitda_gr3a: Option<f64>,
    pub ebit_gr3a: Option<f64>,
    pub ope_gr3a: Option<f64>,
    pub ni_gr3a: Option<f64>,
    pub nix_gr3a: Option<f64>,
    pub dp_gr3a: Option<f64>,
    pub fincf_gr3a: Option<f64>,
    pub ocf_gr3a: Option<f64>,
    pub fcf_gr3a: Option<f64>,
    pub nwc_gr3a: Option<f64>,
    pub eqnetis_gr3a: Option<f64>,
    pub dltnetis_gr3a: Option<f64>,
    pub dstnetis_gr3a: Option<f64>,
    pub dbnetis_gr3a: Option<f64>,
    pub netis_gr3a: Option<f64>,
    pub eqnpo_gr3a: Option<f64>,
    pub tax_gr3a: Option<f64>,
    pub eqbb_gr3a: Option<f64>,
    pub eqis_gr3a: Option<f64>,
    pub div_gr3a: Option<f64>,
    pub eqpo_gr3a: Option<f64>,
    pub capx_gr3a: Option<f64>,
    pub capx_at: Option<f64>,
    pub rd_at: Option<f64>,
    pub spi_at: Option<f64>,
    pub xido_at: Option<f64>,
    pub nri_at: Option<f64>,
    pub gp_sale: Option<f64>,
    pub ebitda_sale: Option<f64>,
    pub pi_sale: Option<f64>,
    pub ni_sale: Option<f64>,
    pub nix_sale: Option<f64>,
    pub ocf_sale: Option<f64>,
    pub fcf_sale: Option<f64>,
    pub ebitda_at: Option<f64>,
    pub ebit_at: Option<f64>,
    pub fi_at: Option<f64>,
    pub ni_at: Option<f64>,
    pub nix_be: Option<f64>,
    pub ocf_be: Option<f64>,
    pub fcf_be: Option<f64>,
    pub gp_bev: Option<f64>,
    pub ebitda_bev: Option<f64>,
    pub fi_bev: Option<f64>,
    pub cop_bev: Option<f64>,
    pub gp_ppen: Option<f64>,
    pub ebitda_ppen: Option<f64>,
    pub fcf_ppen: Option<f64>,
    pub fincf_at: Option<f64>,
    pub eqis_at: Option<f64>,
    pub dltnetis_at: Option<f64>,
    pub dstnetis_at: Option<f64>,
    pub eqnpo_at: Option<f64>,
    pub eqbb_at: Option<f64>,
    pub div_at: Option<f64>,
    pub be_bev: Option<f64>,
    pub debt_bev: Option<f64>,
    pub cash_bev: Option<f64>,
    pub pstk_bev: Option<f64>,
    pub debtlt_bev: Option<f64>,
    pub debtst_bev: Option<f64>,
    pub int_debt: Option<f64>,
    pub int_debtlt: Option<f64>,
    pub ebitda_debt: Option<f64>,
    pub profit_cl: Option<f64>,
    pub ocf_cl: Option<f64>,
    pub ocf_debt: Option<f64>,
    pub cash_lt: Option<f64>,
    pub inv_act: Option<f64>,
    pub rec_act: Option<f64>,
    pub debtst_debt: Option<f64>,
    pub cl_lt: Option<f64>,
    pub debtlt_debt: Option<f64>,
    pub lt_ppen: Option<f64>,
    pub debtlt_be: Option<f64>,
    pub nwc_at: Option<f64>,
    pub fcf_ocf: Option<f64>,
    pub debt_at: Option<f64>,
    pub debt_be: Option<f64>,
    pub ebit_int: Option<f64>,
    pub inv_days: Option<f64>,
    pub rec_days: Option<f64>,
    pub ap_days: Option<f64>,
    pub cash_conversion: Option<f64>,
    pub cash_cl: Option<f64>,
    pub caliq_cl: Option<f64>,
    pub ca_cl: Option<f64>,
    pub inv_turnover: Option<f64>,
    pub rec_turnover: Option<f64>,
    pub ap_turnover: Option<f64>,
    pub adv_sale: Option<f64>,
    pub staff_sale: Option<f64>,
    pub sale_be: Option<f64>,
    pub div_ni: Option<f64>,
    pub sale_nwc: Option<f64>,
    pub tax_pi: Option<f64>,
    pub ni_emp: Option<f64>,
    pub sale_emp: Option<f64>,
    pub niq_saleq_std: Option<f64>,
    pub roeq_be_std: Option<f64>,
    pub roe_be_std: Option<f64>,
    pub intrinsic_value: Option<f64>,
    pub gpoa_ch5: Option<f64>,
    pub roe_ch5: Option<f64>,
    pub roa_ch5: Option<f64>,
    pub cfoa_ch5: Option<f64>,
    pub gmar_ch5: Option<f64>,
    pub cash_me: Option<f64>,
    pub gp_me: Option<f64>,
    pub ebitda_me: Option<f64>,
    pub ebit_me: Option<f64>,
    pub ope_me: Option<f64>,
    pub nix_me: Option<f64>,
    pub cop_me: Option<f64>,
    pub div_me: Option<f64>,
    pub eqbb_me: Option<f64>,
    pub eqis_me: Option<f64>,
    pub eqnetis_me: Option<f64>,
    pub at_mev: Option<f64>,
    pub ppen_mev: Option<f64>,
    pub be_mev: Option<f64>,
    pub cash_mev: Option<f64>,
    pub sale_mev: Option<f64>,
    pub gp_mev: Option<f64>,
    pub ebit_mev: Option<f64>,
    pub cop_mev: Option<f64>,
    pub ocf_mev: Option<f64>,
    pub fcf_mev: Option<f64>,
    pub debt_mev: Option<f64>,
    pub pstk_mev: Option<f64>,
    pub debtlt_mev: Option<f64>,
    pub debtst_mev: Option<f64>,
    pub dltnetis_mev: Option<f64>,
    pub dstnetis_mev: Option<f64>,
    pub dbnetis_mev: Option<f64>,
    pub netis_mev: Option<f64>,
    pub fincf_mev: Option<f64>,
    pub ivol_capm_60m: Option<f64>,
    pub beta_21d: Option<f64>,
    pub beta_252d: Option<f64>,
    pub rvol_252d: Option<f64>,
    pub rvolhl_21d: Option<f64>,
}

impl SurrealCrudModel for EquityFactorsMonthly {
    fn table() -> &'static str {
        "equity_factors_monthly"
    }
    fn id_key(&self) -> Option<String> {
        match (&self.gvkey, &self.iid, &self.eom) {
            (Some(gv), Some(iid), Some(eom)) => Some(format!("{}:{}:{}", gv, iid, eom)),
            (Some(gv), None, Some(eom)) => Some(format!("{}:{}", gv, eom)),
            _ => self.id.map(|x| x.to_string()),
        }
    }
}

impl DuckCrudModel for EquityFactorsMonthly {
    fn table() -> &'static str {
        "equity_factors_monthly"
    }
    fn id_key(&self) -> Option<String> {
        <Self as SurrealCrudModel>::id_key(self)
    }
}

impl EquityFactorsMonthly {
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

    /// Batched concurrent insert into SurrealDB.
    pub async fn create_result(
        data_vec: Vec<EquityFactorsMonthly>,
        db: &Surreal<Db>,
        nsname: &str,
        dbname: &str,
        batch_size: usize,
        cores: usize,
    ) -> Result<usize, AppError> {
        db.use_ns(nsname).use_db(dbname).await?;
        EquityFactorsMonthly::insert_vec_concurrent(db, data_vec, batch_size, cores).await
    }
}

impl EquityFactorsMonthly {
    /// Read rows for a given eom range, returned as a single `doc` JSON column.
    /// This avoids enumerating ~443 typed columns and provides a flexible payload.
    pub async fn read_range<'a>(
        conn: Arc<Mutex<Connection>>,
        date_range: (NaiveDate, NaiveDate),
    ) -> Result<Vec<Row<'a>>, AppError> {
        tokio::task::spawn_blocking(move || {
            let table = <Self as DuckCrudModel>::table();
            let sql = format!(
                "SELECT \
                    CAST(gvkey   AS BIGINT)   AS gvkey, \
                    CAST(iid     AS VARCHAR)  AS iid, \
                    CAST(permno  AS BIGINT)   AS permno, \
                    CAST(permco  AS BIGINT)   AS permco, \
                    CAST(id      AS BIGINT)   AS id, \
                    CAST(date    AS DATE)     AS date, \
                    CAST(eom     AS DATE)     AS eom, \
                    CAST(excntry AS VARCHAR)  AS excntry, \
                    CAST(size_grp AS VARCHAR) AS size_grp, \
                    CAST(me      AS DOUBLE)   AS me, \
                    CAST(ret_exc AS DOUBLE)   AS ret_exc, \
                    CAST(ret     AS DOUBLE)   AS ret, \
                    CAST(prc     AS DOUBLE)   AS prc, \
                    CAST(obs_main AS DOUBLE) AS obs_main, \
                    CAST(exch_main AS DOUBLE) AS exch_main, \
                    CAST(common AS DOUBLE) AS common, \
                    CAST(primary_sec AS DOUBLE) AS primary_sec, \
                    CAST(me_company AS DOUBLE) AS me_company, \
                    CAST(ret_exc_lead1m AS DOUBLE) AS ret_exc_lead1m, \
                    CAST(ret_local AS DOUBLE) AS ret_local, \
                    CAST(ret_lag_dif AS DOUBLE) AS ret_lag_dif, \
                    CAST(prc_local AS DOUBLE) AS prc_local, \
                    CAST(prc_high AS DOUBLE) AS prc_high, \
                    CAST(prc_low AS DOUBLE) AS prc_low, \
                    CAST(bidask AS DOUBLE) AS bidask, \
                    CAST(curcd AS VARCHAR) AS curcd, \
                    CAST(fx AS DOUBLE) AS fx, \
                    CAST(gics AS DOUBLE) AS gics, \
                    CAST(naics AS DOUBLE) AS naics, \
                    CAST(sic AS DOUBLE) AS sic, \
                    CAST(ff49 AS DOUBLE) AS ff49, \
                    CAST(dolvol AS DOUBLE) AS dolvol, \
                    CAST(shares AS DOUBLE) AS shares, \
                    CAST(tvol AS DOUBLE) AS tvol, \
                    CAST(adjfct AS DOUBLE) AS adjfct, \
                    CAST(comp_tpci AS VARCHAR) AS comp_tpci, \
                    CAST(crsp_shrcd AS DOUBLE) AS crsp_shrcd, \
                    CAST(comp_exchg AS DOUBLE) AS comp_exchg, \
                    CAST(crsp_exchcd AS DOUBLE) AS crsp_exchcd, \
                    CAST(source_crsp AS DOUBLE) AS source_crsp, \
                    CAST(market_equity AS DOUBLE) AS market_equity, \
                    CAST(div12m_me AS DOUBLE) AS div12m_me, \
                    CAST(chcsho_12m AS DOUBLE) AS chcsho_12m, \
                    CAST(eqnpo_12m AS DOUBLE) AS eqnpo_12m, \
                    CAST(ret_1_0 AS DOUBLE) AS ret_1_0, \
                    CAST(ret_3_1 AS DOUBLE) AS ret_3_1, \
                    CAST(ret_6_1 AS DOUBLE) AS ret_6_1, \
                    CAST(ret_9_1 AS DOUBLE) AS ret_9_1, \
                    CAST(ret_12_1 AS DOUBLE) AS ret_12_1, \
                    CAST(ret_12_7 AS DOUBLE) AS ret_12_7, \
                    CAST(ret_60_12 AS DOUBLE) AS ret_60_12, \
                    CAST(seas_1_1an AS DOUBLE) AS seas_1_1an, \
                    CAST(seas_1_1na AS DOUBLE) AS seas_1_1na, \
                    CAST(seas_2_5an AS DOUBLE) AS seas_2_5an, \
                    CAST(seas_2_5na AS DOUBLE) AS seas_2_5na, \
                    CAST(seas_6_10an AS DOUBLE) AS seas_6_10an, \
                    CAST(seas_6_10na AS DOUBLE) AS seas_6_10na, \
                    CAST(seas_11_15an AS DOUBLE) AS seas_11_15an, \
                    CAST(seas_11_15na AS DOUBLE) AS seas_11_15na, \
                    CAST(seas_16_20an AS DOUBLE) AS seas_16_20an, \
                    CAST(seas_16_20na AS DOUBLE) AS seas_16_20na, \
                    CAST(at_gr1 AS DOUBLE) AS at_gr1, \
                    CAST(sale_gr1 AS DOUBLE) AS sale_gr1, \
                    CAST(capx_gr1 AS DOUBLE) AS capx_gr1, \
                    CAST(inv_gr1 AS DOUBLE) AS inv_gr1, \
                    CAST(debt_gr3 AS DOUBLE) AS debt_gr3, \
                    CAST(sale_gr3 AS DOUBLE) AS sale_gr3, \
                    CAST(capx_gr3 AS DOUBLE) AS capx_gr3, \
                    CAST(inv_gr1a AS DOUBLE) AS inv_gr1a, \
                    CAST(lti_gr1a AS DOUBLE) AS lti_gr1a, \
                    CAST(sti_gr1a AS DOUBLE) AS sti_gr1a, \
                    CAST(coa_gr1a AS DOUBLE) AS coa_gr1a, \
                    CAST(col_gr1a AS DOUBLE) AS col_gr1a, \
                    CAST(cowc_gr1a AS DOUBLE) AS cowc_gr1a, \
                    CAST(ncoa_gr1a AS DOUBLE) AS ncoa_gr1a, \
                    CAST(ncol_gr1a AS DOUBLE) AS ncol_gr1a, \
                    CAST(nncoa_gr1a AS DOUBLE) AS nncoa_gr1a, \
                    CAST(fnl_gr1a AS DOUBLE) AS fnl_gr1a, \
                    CAST(nfna_gr1a AS DOUBLE) AS nfna_gr1a, \
                    CAST(tax_gr1a AS DOUBLE) AS tax_gr1a, \
                    CAST(be_gr1a AS DOUBLE) AS be_gr1a, \
                    CAST(ebit_sale AS DOUBLE) AS ebit_sale, \
                    CAST(gp_at AS DOUBLE) AS gp_at, \
                    CAST(cop_at AS DOUBLE) AS cop_at, \
                    CAST(ope_be AS DOUBLE) AS ope_be, \
                    CAST(ni_be AS DOUBLE) AS ni_be, \
                    CAST(ebit_bev AS DOUBLE) AS ebit_bev, \
                    CAST(netis_at AS DOUBLE) AS netis_at, \
                    CAST(eqnetis_at AS DOUBLE) AS eqnetis_at, \
                    CAST(dbnetis_at AS DOUBLE) AS dbnetis_at, \
                    CAST(oaccruals_at AS DOUBLE) AS oaccruals_at, \
                    CAST(oaccruals_ni AS DOUBLE) AS oaccruals_ni, \
                    CAST(taccruals_at AS DOUBLE) AS taccruals_at, \
                    CAST(taccruals_ni AS DOUBLE) AS taccruals_ni, \
                    CAST(noa_at AS DOUBLE) AS noa_at, \
                    CAST(opex_at AS DOUBLE) AS opex_at, \
                    CAST(at_turnover AS DOUBLE) AS at_turnover, \
                    CAST(sale_bev AS DOUBLE) AS sale_bev, \
                    CAST(rd_sale AS DOUBLE) AS rd_sale, \
                    CAST(cash_at AS DOUBLE) AS cash_at, \
                    CAST(sale_emp_gr1 AS DOUBLE) AS sale_emp_gr1, \
                    CAST(emp_gr1 AS DOUBLE) AS emp_gr1, \
                    CAST(ni_inc8q AS DOUBLE) AS ni_inc8q, \
                    CAST(noa_gr1a AS DOUBLE) AS noa_gr1a, \
                    CAST(ppeinv_gr1a AS DOUBLE) AS ppeinv_gr1a, \
                    CAST(lnoa_gr1a AS DOUBLE) AS lnoa_gr1a, \
                    CAST(capx_gr2 AS DOUBLE) AS capx_gr2, \
                    CAST(saleq_gr1 AS DOUBLE) AS saleq_gr1, \
                    CAST(niq_be AS DOUBLE) AS niq_be, \
                    CAST(niq_at AS DOUBLE) AS niq_at, \
                    CAST(niq_be_chg1 AS DOUBLE) AS niq_be_chg1, \
                    CAST(niq_at_chg1 AS DOUBLE) AS niq_at_chg1, \
                    CAST(rd5_at AS DOUBLE) AS rd5_at, \
                    CAST(dsale_dinv AS DOUBLE) AS dsale_dinv, \
                    CAST(dsale_drec AS DOUBLE) AS dsale_drec, \
                    CAST(dgp_dsale AS DOUBLE) AS dgp_dsale, \
                    CAST(dsale_dsga AS DOUBLE) AS dsale_dsga, \
                    CAST(saleq_su AS DOUBLE) AS saleq_su, \
                    CAST(niq_su AS DOUBLE) AS niq_su, \
                    CAST(capex_abn AS DOUBLE) AS capex_abn, \
                    CAST(op_atl1 AS DOUBLE) AS op_atl1, \
                    CAST(gp_atl1 AS DOUBLE) AS gp_atl1, \
                    CAST(ope_bel1 AS DOUBLE) AS ope_bel1, \
                    CAST(cop_atl1 AS DOUBLE) AS cop_atl1, \
                    CAST(pi_nix AS DOUBLE) AS pi_nix, \
                    CAST(ocf_at AS DOUBLE) AS ocf_at, \
                    CAST(op_at AS DOUBLE) AS op_at, \
                    CAST(ocf_at_chg1 AS DOUBLE) AS ocf_at_chg1, \
                    CAST(at_be AS DOUBLE) AS at_be, \
                    CAST(ocfq_saleq_std AS DOUBLE) AS ocfq_saleq_std, \
                    CAST(tangibility AS DOUBLE) AS tangibility, \
                    CAST(earnings_variability AS DOUBLE) AS earnings_variability, \
                    CAST(aliq_at AS DOUBLE) AS aliq_at, \
                    CAST(f_score AS DOUBLE) AS f_score, \
                    CAST(o_score AS DOUBLE) AS o_score, \
                    CAST(z_score AS DOUBLE) AS z_score, \
                    CAST(kz_index AS DOUBLE) AS kz_index, \
                    CAST(ni_ar1 AS DOUBLE) AS ni_ar1, \
                    CAST(ni_ivol AS DOUBLE) AS ni_ivol, \
                    CAST(at_me AS DOUBLE) AS at_me, \
                    CAST(be_me AS DOUBLE) AS be_me, \
                    CAST(debt_me AS DOUBLE) AS debt_me, \
                    CAST(netdebt_me AS DOUBLE) AS netdebt_me, \
                    CAST(sale_me AS DOUBLE) AS sale_me, \
                    CAST(ni_me AS DOUBLE) AS ni_me, \
                    CAST(ocf_me AS DOUBLE) AS ocf_me, \
                    CAST(fcf_me AS DOUBLE) AS fcf_me, \
                    CAST(eqpo_me AS DOUBLE) AS eqpo_me, \
                    CAST(eqnpo_me AS DOUBLE) AS eqnpo_me, \
                    CAST(rd_me AS DOUBLE) AS rd_me, \
                    CAST(ival_me AS DOUBLE) AS ival_me, \
                    CAST(bev_mev AS DOUBLE) AS bev_mev, \
                    CAST(ebitda_mev AS DOUBLE) AS ebitda_mev, \
                    CAST(aliq_mat AS DOUBLE) AS aliq_mat, \
                    CAST(eq_dur AS DOUBLE) AS eq_dur, \
                    CAST(beta_60m AS DOUBLE) AS beta_60m, \
                    CAST(resff3_12_1 AS DOUBLE) AS resff3_12_1, \
                    CAST(resff3_6_1 AS DOUBLE) AS resff3_6_1, \
                    CAST(mispricing_mgmt AS DOUBLE) AS mispricing_mgmt, \
                    CAST(mispricing_perf AS DOUBLE) AS mispricing_perf, \
                    CAST(ivol_capm_21d AS DOUBLE) AS ivol_capm_21d, \
                    CAST(iskew_capm_21d AS DOUBLE) AS iskew_capm_21d, \
                    CAST(coskew_21d AS DOUBLE) AS coskew_21d, \
                    CAST(beta_dimson_21d AS DOUBLE) AS beta_dimson_21d, \
                    CAST(ivol_ff3_21d AS DOUBLE) AS ivol_ff3_21d, \
                    CAST(iskew_ff3_21d AS DOUBLE) AS iskew_ff3_21d, \
                    CAST(ivol_hxz4_21d AS DOUBLE) AS ivol_hxz4_21d, \
                    CAST(iskew_hxz4_21d AS DOUBLE) AS iskew_hxz4_21d, \
                    CAST(rmax5_21d AS DOUBLE) AS rmax5_21d, \
                    CAST(rmax1_21d AS DOUBLE) AS rmax1_21d, \
                    CAST(rvol_21d AS DOUBLE) AS rvol_21d, \
                    CAST(rskew_21d AS DOUBLE) AS rskew_21d, \
                    CAST(zero_trades_21d AS DOUBLE) AS zero_trades_21d, \
                    CAST(dolvol_126d AS DOUBLE) AS dolvol_126d, \
                    CAST(dolvol_var_126d AS DOUBLE) AS dolvol_var_126d, \
                    CAST(turnover_126d AS DOUBLE) AS turnover_126d, \
                    CAST(turnover_var_126d AS DOUBLE) AS turnover_var_126d, \
                    CAST(zero_trades_126d AS DOUBLE) AS zero_trades_126d, \
                    CAST(zero_trades_252d AS DOUBLE) AS zero_trades_252d, \
                    CAST(ami_126d AS DOUBLE) AS ami_126d, \
                    CAST(ivol_capm_252d AS DOUBLE) AS ivol_capm_252d, \
                    CAST(prc_highprc_252d AS DOUBLE) AS prc_highprc_252d, \
                    CAST(betadown_252d AS DOUBLE) AS betadown_252d, \
                    CAST(bidaskhl_21d AS DOUBLE) AS bidaskhl_21d, \
                    CAST(corr_1260d AS DOUBLE) AS corr_1260d, \
                    CAST(betabab_1260d AS DOUBLE) AS betabab_1260d, \
                    CAST(rmax5_rvol_21d AS DOUBLE) AS rmax5_rvol_21d, \
                    CAST(age AS DOUBLE) AS age, \
                    CAST(qmj AS DOUBLE) AS qmj, \
                    CAST(qmj_prof AS DOUBLE) AS qmj_prof, \
                    CAST(qmj_growth AS DOUBLE) AS qmj_growth, \
                    CAST(qmj_safety AS DOUBLE) AS qmj_safety, \
                    CAST(enterprise_value AS DOUBLE) AS enterprise_value, \
                    CAST(book_equity AS DOUBLE) AS book_equity, \
                    CAST(assets AS DOUBLE) AS assets, \
                    CAST(sales AS DOUBLE) AS sales, \
                    CAST(net_income AS DOUBLE) AS net_income, \
                    CAST(div1m_me AS DOUBLE) AS div1m_me, \
                    CAST(div3m_me AS DOUBLE) AS div3m_me, \
                    CAST(div6m_me AS DOUBLE) AS div6m_me, \
                    CAST(divspc1m_me AS DOUBLE) AS divspc1m_me, \
                    CAST(divspc12m_me AS DOUBLE) AS divspc12m_me, \
                    CAST(chcsho_1m AS DOUBLE) AS chcsho_1m, \
                    CAST(chcsho_3m AS DOUBLE) AS chcsho_3m, \
                    CAST(chcsho_6m AS DOUBLE) AS chcsho_6m, \
                    CAST(eqnpo_1m AS DOUBLE) AS eqnpo_1m, \
                    CAST(eqnpo_3m AS DOUBLE) AS eqnpo_3m, \
                    CAST(eqnpo_6m AS DOUBLE) AS eqnpo_6m, \
                    CAST(ret_2_0 AS DOUBLE) AS ret_2_0, \
                    CAST(ret_3_0 AS DOUBLE) AS ret_3_0, \
                    CAST(ret_6_0 AS DOUBLE) AS ret_6_0, \
                    CAST(ret_9_0 AS DOUBLE) AS ret_9_0, \
                    CAST(ret_12_0 AS DOUBLE) AS ret_12_0, \
                    CAST(ret_18_1 AS DOUBLE) AS ret_18_1, \
                    CAST(ret_24_1 AS DOUBLE) AS ret_24_1, \
                    CAST(ret_24_12 AS DOUBLE) AS ret_24_12, \
                    CAST(ret_36_1 AS DOUBLE) AS ret_36_1, \
                    CAST(ret_36_12 AS DOUBLE) AS ret_36_12, \
                    CAST(ret_48_12 AS DOUBLE) AS ret_48_12, \
                    CAST(ret_48_1 AS DOUBLE) AS ret_48_1, \
                    CAST(ret_60_1 AS DOUBLE) AS ret_60_1, \
                    CAST(ret_60_36 AS DOUBLE) AS ret_60_36, \
                    CAST(ca_gr1 AS DOUBLE) AS ca_gr1, \
                    CAST(nca_gr1 AS DOUBLE) AS nca_gr1, \
                    CAST(lt_gr1 AS DOUBLE) AS lt_gr1, \
                    CAST(cl_gr1 AS DOUBLE) AS cl_gr1, \
                    CAST(ncl_gr1 AS DOUBLE) AS ncl_gr1, \
                    CAST(be_gr1 AS DOUBLE) AS be_gr1, \
                    CAST(pstk_gr1 AS DOUBLE) AS pstk_gr1, \
                    CAST(debt_gr1 AS DOUBLE) AS debt_gr1, \
                    CAST(cogs_gr1 AS DOUBLE) AS cogs_gr1, \
                    CAST(sga_gr1 AS DOUBLE) AS sga_gr1, \
                    CAST(opex_gr1 AS DOUBLE) AS opex_gr1, \
                    CAST(at_gr3 AS DOUBLE) AS at_gr3, \
                    CAST(ca_gr3 AS DOUBLE) AS ca_gr3, \
                    CAST(nca_gr3 AS DOUBLE) AS nca_gr3, \
                    CAST(lt_gr3 AS DOUBLE) AS lt_gr3, \
                    CAST(cl_gr3 AS DOUBLE) AS cl_gr3, \
                    CAST(ncl_gr3 AS DOUBLE) AS ncl_gr3, \
                    CAST(be_gr3 AS DOUBLE) AS be_gr3, \
                    CAST(pstk_gr3 AS DOUBLE) AS pstk_gr3, \
                    CAST(cogs_gr3 AS DOUBLE) AS cogs_gr3, \
                    CAST(sga_gr3 AS DOUBLE) AS sga_gr3, \
                    CAST(opex_gr3 AS DOUBLE) AS opex_gr3, \
                    CAST(cash_gr1a AS DOUBLE) AS cash_gr1a, \
                    CAST(rec_gr1a AS DOUBLE) AS rec_gr1a, \
                    CAST(ppeg_gr1a AS DOUBLE) AS ppeg_gr1a, \
                    CAST(intan_gr1a AS DOUBLE) AS intan_gr1a, \
                    CAST(debtst_gr1a AS DOUBLE) AS debtst_gr1a, \
                    CAST(ap_gr1a AS DOUBLE) AS ap_gr1a, \
                    CAST(txp_gr1a AS DOUBLE) AS txp_gr1a, \
                    CAST(debtlt_gr1a AS DOUBLE) AS debtlt_gr1a, \
                    CAST(txditc_gr1a AS DOUBLE) AS txditc_gr1a, \
                    CAST(oa_gr1a AS DOUBLE) AS oa_gr1a, \
                    CAST(ol_gr1a AS DOUBLE) AS ol_gr1a, \
                    CAST(fna_gr1a AS DOUBLE) AS fna_gr1a, \
                    CAST(gp_gr1a AS DOUBLE) AS gp_gr1a, \
                    CAST(ebitda_gr1a AS DOUBLE) AS ebitda_gr1a, \
                    CAST(ebit_gr1a AS DOUBLE) AS ebit_gr1a, \
                    CAST(ope_gr1a AS DOUBLE) AS ope_gr1a, \
                    CAST(ni_gr1a AS DOUBLE) AS ni_gr1a, \
                    CAST(nix_gr1a AS DOUBLE) AS nix_gr1a, \
                    CAST(dp_gr1a AS DOUBLE) AS dp_gr1a, \
                    CAST(fincf_gr1a AS DOUBLE) AS fincf_gr1a, \
                    CAST(ocf_gr1a AS DOUBLE) AS ocf_gr1a, \
                    CAST(fcf_gr1a AS DOUBLE) AS fcf_gr1a, \
                    CAST(nwc_gr1a AS DOUBLE) AS nwc_gr1a, \
                    CAST(eqnetis_gr1a AS DOUBLE) AS eqnetis_gr1a, \
                    CAST(dltnetis_gr1a AS DOUBLE) AS dltnetis_gr1a, \
                    CAST(dstnetis_gr1a AS DOUBLE) AS dstnetis_gr1a, \
                    CAST(dbnetis_gr1a AS DOUBLE) AS dbnetis_gr1a, \
                    CAST(netis_gr1a AS DOUBLE) AS netis_gr1a, \
                    CAST(eqnpo_gr1a AS DOUBLE) AS eqnpo_gr1a, \
                    CAST(eqbb_gr1a AS DOUBLE) AS eqbb_gr1a, \
                    CAST(eqis_gr1a AS DOUBLE) AS eqis_gr1a, \
                    CAST(div_gr1a AS DOUBLE) AS div_gr1a, \
                    CAST(eqpo_gr1a AS DOUBLE) AS eqpo_gr1a, \
                    CAST(capx_gr1a AS DOUBLE) AS capx_gr1a, \
                    CAST(cash_gr3a AS DOUBLE) AS cash_gr3a, \
                    CAST(inv_gr3a AS DOUBLE) AS inv_gr3a, \
                    CAST(rec_gr3a AS DOUBLE) AS rec_gr3a, \
                    CAST(ppeg_gr3a AS DOUBLE) AS ppeg_gr3a, \
                    CAST(lti_gr3a AS DOUBLE) AS lti_gr3a, \
                    CAST(intan_gr3a AS DOUBLE) AS intan_gr3a, \
                    CAST(debtst_gr3a AS DOUBLE) AS debtst_gr3a, \
                    CAST(ap_gr3a AS DOUBLE) AS ap_gr3a, \
                    CAST(txp_gr3a AS DOUBLE) AS txp_gr3a, \
                    CAST(debtlt_gr3a AS DOUBLE) AS debtlt_gr3a, \
                    CAST(txditc_gr3a AS DOUBLE) AS txditc_gr3a, \
                    CAST(coa_gr3a AS DOUBLE) AS coa_gr3a, \
                    CAST(col_gr3a AS DOUBLE) AS col_gr3a, \
                    CAST(cowc_gr3a AS DOUBLE) AS cowc_gr3a, \
                    CAST(ncoa_gr3a AS DOUBLE) AS ncoa_gr3a, \
                    CAST(ncol_gr3a AS DOUBLE) AS ncol_gr3a, \
                    CAST(nncoa_gr3a AS DOUBLE) AS nncoa_gr3a, \
                    CAST(oa_gr3a AS DOUBLE) AS oa_gr3a, \
                    CAST(ol_gr3a AS DOUBLE) AS ol_gr3a, \
                    CAST(fna_gr3a AS DOUBLE) AS fna_gr3a, \
                    CAST(fnl_gr3a AS DOUBLE) AS fnl_gr3a, \
                    CAST(nfna_gr3a AS DOUBLE) AS nfna_gr3a, \
                    CAST(gp_gr3a AS DOUBLE) AS gp_gr3a, \
                    CAST(ebitda_gr3a AS DOUBLE) AS ebitda_gr3a, \
                    CAST(ebit_gr3a AS DOUBLE) AS ebit_gr3a, \
                    CAST(ope_gr3a AS DOUBLE) AS ope_gr3a, \
                    CAST(ni_gr3a AS DOUBLE) AS ni_gr3a, \
                    CAST(nix_gr3a AS DOUBLE) AS nix_gr3a, \
                    CAST(dp_gr3a AS DOUBLE) AS dp_gr3a, \
                    CAST(fincf_gr3a AS DOUBLE) AS fincf_gr3a, \
                    CAST(ocf_gr3a AS DOUBLE) AS ocf_gr3a, \
                    CAST(fcf_gr3a AS DOUBLE) AS fcf_gr3a, \
                    CAST(nwc_gr3a AS DOUBLE) AS nwc_gr3a, \
                    CAST(eqnetis_gr3a AS DOUBLE) AS eqnetis_gr3a, \
                    CAST(dltnetis_gr3a AS DOUBLE) AS dltnetis_gr3a, \
                    CAST(dstnetis_gr3a AS DOUBLE) AS dstnetis_gr3a, \
                    CAST(dbnetis_gr3a AS DOUBLE) AS dbnetis_gr3a, \
                    CAST(netis_gr3a AS DOUBLE) AS netis_gr3a, \
                    CAST(eqnpo_gr3a AS DOUBLE) AS eqnpo_gr3a, \
                    CAST(tax_gr3a AS DOUBLE) AS tax_gr3a, \
                    CAST(eqbb_gr3a AS DOUBLE) AS eqbb_gr3a, \
                    CAST(eqis_gr3a AS DOUBLE) AS eqis_gr3a, \
                    CAST(div_gr3a AS DOUBLE) AS div_gr3a, \
                    CAST(eqpo_gr3a AS DOUBLE) AS eqpo_gr3a, \
                    CAST(capx_gr3a AS DOUBLE) AS capx_gr3a, \
                    CAST(capx_at AS DOUBLE) AS capx_at, \
                    CAST(rd_at AS DOUBLE) AS rd_at, \
                    CAST(spi_at AS DOUBLE) AS spi_at, \
                    CAST(xido_at AS DOUBLE) AS xido_at, \
                    CAST(nri_at AS DOUBLE) AS nri_at, \
                    CAST(gp_sale AS DOUBLE) AS gp_sale, \
                    CAST(ebitda_sale AS DOUBLE) AS ebitda_sale, \
                    CAST(pi_sale AS DOUBLE) AS pi_sale, \
                    CAST(ni_sale AS DOUBLE) AS ni_sale, \
                    CAST(nix_sale AS DOUBLE) AS nix_sale, \
                    CAST(ocf_sale AS DOUBLE) AS ocf_sale, \
                    CAST(fcf_sale AS DOUBLE) AS fcf_sale, \
                    CAST(ebitda_at AS DOUBLE) AS ebitda_at, \
                    CAST(ebit_at AS DOUBLE) AS ebit_at, \
                    CAST(fi_at AS DOUBLE) AS fi_at, \
                    CAST(ni_at AS DOUBLE) AS ni_at, \
                    CAST(nix_be AS DOUBLE) AS nix_be, \
                    CAST(ocf_be AS DOUBLE) AS ocf_be, \
                    CAST(fcf_be AS DOUBLE) AS fcf_be, \
                    CAST(gp_bev AS DOUBLE) AS gp_bev, \
                    CAST(ebitda_bev AS DOUBLE) AS ebitda_bev, \
                    CAST(fi_bev AS DOUBLE) AS fi_bev, \
                    CAST(cop_bev AS DOUBLE) AS cop_bev, \
                    CAST(gp_ppen AS DOUBLE) AS gp_ppen, \
                    CAST(ebitda_ppen AS DOUBLE) AS ebitda_ppen, \
                    CAST(fcf_ppen AS DOUBLE) AS fcf_ppen, \
                    CAST(fincf_at AS DOUBLE) AS fincf_at, \
                    CAST(eqis_at AS DOUBLE) AS eqis_at, \
                    CAST(dltnetis_at AS DOUBLE) AS dltnetis_at, \
                    CAST(dstnetis_at AS DOUBLE) AS dstnetis_at, \
                    CAST(eqnpo_at AS DOUBLE) AS eqnpo_at, \
                    CAST(eqbb_at AS DOUBLE) AS eqbb_at, \
                    CAST(div_at AS DOUBLE) AS div_at, \
                    CAST(be_bev AS DOUBLE) AS be_bev, \
                    CAST(debt_bev AS DOUBLE) AS debt_bev, \
                    CAST(cash_bev AS DOUBLE) AS cash_bev, \
                    CAST(pstk_bev AS DOUBLE) AS pstk_bev, \
                    CAST(debtlt_bev AS DOUBLE) AS debtlt_bev, \
                    CAST(debtst_bev AS DOUBLE) AS debtst_bev, \
                    CAST(int_debt AS DOUBLE) AS int_debt, \
                    CAST(int_debtlt AS DOUBLE) AS int_debtlt, \
                    CAST(ebitda_debt AS DOUBLE) AS ebitda_debt, \
                    CAST(profit_cl AS DOUBLE) AS profit_cl, \
                    CAST(ocf_cl AS DOUBLE) AS ocf_cl, \
                    CAST(ocf_debt AS DOUBLE) AS ocf_debt, \
                    CAST(cash_lt AS DOUBLE) AS cash_lt, \
                    CAST(inv_act AS DOUBLE) AS inv_act, \
                    CAST(rec_act AS DOUBLE) AS rec_act, \
                    CAST(debtst_debt AS DOUBLE) AS debtst_debt, \
                    CAST(cl_lt AS DOUBLE) AS cl_lt, \
                    CAST(debtlt_debt AS DOUBLE) AS debtlt_debt, \
                    CAST(lt_ppen AS DOUBLE) AS lt_ppen, \
                    CAST(debtlt_be AS DOUBLE) AS debtlt_be, \
                    CAST(nwc_at AS DOUBLE) AS nwc_at, \
                    CAST(fcf_ocf AS DOUBLE) AS fcf_ocf, \
                    CAST(debt_at AS DOUBLE) AS debt_at, \
                    CAST(debt_be AS DOUBLE) AS debt_be, \
                    CAST(ebit_int AS DOUBLE) AS ebit_int, \
                    CAST(inv_days AS DOUBLE) AS inv_days, \
                    CAST(rec_days AS DOUBLE) AS rec_days, \
                    CAST(ap_days AS DOUBLE) AS ap_days, \
                    CAST(cash_conversion AS DOUBLE) AS cash_conversion, \
                    CAST(cash_cl AS DOUBLE) AS cash_cl, \
                    CAST(caliq_cl AS DOUBLE) AS caliq_cl, \
                    CAST(ca_cl AS DOUBLE) AS ca_cl, \
                    CAST(inv_turnover AS DOUBLE) AS inv_turnover, \
                    CAST(rec_turnover AS DOUBLE) AS rec_turnover, \
                    CAST(ap_turnover AS DOUBLE) AS ap_turnover, \
                    CAST(adv_sale AS DOUBLE) AS adv_sale, \
                    CAST(staff_sale AS DOUBLE) AS staff_sale, \
                    CAST(sale_be AS DOUBLE) AS sale_be, \
                    CAST(div_ni AS DOUBLE) AS div_ni, \
                    CAST(sale_nwc AS DOUBLE) AS sale_nwc, \
                    CAST(tax_pi AS DOUBLE) AS tax_pi, \
                    CAST(ni_emp AS DOUBLE) AS ni_emp, \
                    CAST(sale_emp AS DOUBLE) AS sale_emp, \
                    CAST(niq_saleq_std AS DOUBLE) AS niq_saleq_std, \
                    CAST(roeq_be_std AS DOUBLE) AS roeq_be_std, \
                    CAST(roe_be_std AS DOUBLE) AS roe_be_std, \
                    CAST(intrinsic_value AS DOUBLE) AS intrinsic_value, \
                    CAST(gpoa_ch5 AS DOUBLE) AS gpoa_ch5, \
                    CAST(roe_ch5 AS DOUBLE) AS roe_ch5, \
                    CAST(roa_ch5 AS DOUBLE) AS roa_ch5, \
                    CAST(cfoa_ch5 AS DOUBLE) AS cfoa_ch5, \
                    CAST(gmar_ch5 AS DOUBLE) AS gmar_ch5, \
                    CAST(cash_me AS DOUBLE) AS cash_me, \
                    CAST(gp_me AS DOUBLE) AS gp_me, \
                    CAST(ebitda_me AS DOUBLE) AS ebitda_me, \
                    CAST(ebit_me AS DOUBLE) AS ebit_me, \
                    CAST(ope_me AS DOUBLE) AS ope_me, \
                    CAST(nix_me AS DOUBLE) AS nix_me, \
                    CAST(cop_me AS DOUBLE) AS cop_me, \
                    CAST(div_me AS DOUBLE) AS div_me, \
                    CAST(eqbb_me AS DOUBLE) AS eqbb_me, \
                    CAST(eqis_me AS DOUBLE) AS eqis_me, \
                    CAST(eqnetis_me AS DOUBLE) AS eqnetis_me, \
                    CAST(at_mev AS DOUBLE) AS at_mev, \
                    CAST(ppen_mev AS DOUBLE) AS ppen_mev, \
                    CAST(be_mev AS DOUBLE) AS be_mev, \
                    CAST(cash_mev AS DOUBLE) AS cash_mev, \
                    CAST(sale_mev AS DOUBLE) AS sale_mev, \
                    CAST(gp_mev AS DOUBLE) AS gp_mev, \
                    CAST(ebit_mev AS DOUBLE) AS ebit_mev, \
                    CAST(cop_mev AS DOUBLE) AS cop_mev, \
                    CAST(ocf_mev AS DOUBLE) AS ocf_mev, \
                    CAST(fcf_mev AS DOUBLE) AS fcf_mev, \
                    CAST(debt_mev AS DOUBLE) AS debt_mev, \
                    CAST(pstk_mev AS DOUBLE) AS pstk_mev, \
                    CAST(debtlt_mev AS DOUBLE) AS debtlt_mev, \
                    CAST(debtst_mev AS DOUBLE) AS debtst_mev, \
                    CAST(dltnetis_mev AS DOUBLE) AS dltnetis_mev, \
                    CAST(dstnetis_mev AS DOUBLE) AS dstnetis_mev, \
                    CAST(dbnetis_mev AS DOUBLE) AS dbnetis_mev, \
                    CAST(netis_mev AS DOUBLE) AS netis_mev, \
                    CAST(fincf_mev AS DOUBLE) AS fincf_mev, \
                    CAST(ivol_capm_60m AS DOUBLE) AS ivol_capm_60m, \
                    CAST(beta_21d AS DOUBLE) AS beta_21d, \
                    CAST(beta_252d AS DOUBLE) AS beta_252d, \
                    CAST(rvol_252d AS DOUBLE) AS rvol_252d, \
                    CAST(rvolhl_21d AS DOUBLE) AS rvolhl_21d \
                 FROM {table} AS t \
                 WHERE CAST(t.eom AS DATE) BETWEEN DATE '{start}' AND DATE '{end}' \
                 ORDER BY t.eom",
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
                let permno = i("permno");
                let permco = i("permco");
                let gvkey = i("gvkey");
                let iid = s("iid");
                let id = i("id");
                let date = d("date");
                let eom = d("eom");
                let excntry = s("excntry");
                let size_grp = s("size_grp");
                let me = f("me");
                let ret_exc = f("ret_exc");
                let ret = f("ret");
                let prc = f("prc");
                // Additional columns
                let obs_main = f("obs_main");
                let exch_main = f("exch_main");
                let common = f("common");
                let primary_sec = f("primary_sec");
                let me_company = f("me_company");
                let ret_exc_lead1m = f("ret_exc_lead1m");
                let ret_local = f("ret_local");
                let ret_lag_dif = f("ret_lag_dif");
                let prc_local = f("prc_local");
                let prc_high = f("prc_high");
                let prc_low = f("prc_low");
                let bidask = f("bidask");
                let curcd = s("curcd");
                let fx = f("fx");
                let gics = f("gics");
                let naics = f("naics");
                let sic = f("sic");
                let ff49 = f("ff49");
                let dolvol = f("dolvol");
                let shares = f("shares");
                let tvol = f("tvol");
                let adjfct = f("adjfct");
                let comp_tpci = s("comp_tpci");
                let crsp_shrcd = f("crsp_shrcd");
                let comp_exchg = f("comp_exchg");
                let crsp_exchcd = f("crsp_exchcd");
                let source_crsp = f("source_crsp");
                let market_equity = f("market_equity");
                let div12m_me = f("div12m_me");
                let chcsho_12m = f("chcsho_12m");
                let eqnpo_12m = f("eqnpo_12m");
                let ret_1_0 = f("ret_1_0");
                let ret_3_1 = f("ret_3_1");
                let ret_6_1 = f("ret_6_1");
                let ret_9_1 = f("ret_9_1");
                let ret_12_1 = f("ret_12_1");
                let ret_12_7 = f("ret_12_7");
                let ret_60_12 = f("ret_60_12");
                let seas_1_1an = f("seas_1_1an");
                let seas_1_1na = f("seas_1_1na");
                let seas_2_5an = f("seas_2_5an");
                let seas_2_5na = f("seas_2_5na");
                let seas_6_10an = f("seas_6_10an");
                let seas_6_10na = f("seas_6_10na");
                let seas_11_15an = f("seas_11_15an");
                let seas_11_15na = f("seas_11_15na");
                let seas_16_20an = f("seas_16_20an");
                let seas_16_20na = f("seas_16_20na");
                let at_gr1 = f("at_gr1");
                let sale_gr1 = f("sale_gr1");
                let capx_gr1 = f("capx_gr1");
                let inv_gr1 = f("inv_gr1");
                let debt_gr3 = f("debt_gr3");
                let sale_gr3 = f("sale_gr3");
                let capx_gr3 = f("capx_gr3");
                let inv_gr1a = f("inv_gr1a");
                let lti_gr1a = f("lti_gr1a");
                let sti_gr1a = f("sti_gr1a");
                let coa_gr1a = f("coa_gr1a");
                let col_gr1a = f("col_gr1a");
                let cowc_gr1a = f("cowc_gr1a");
                let ncoa_gr1a = f("ncoa_gr1a");
                let ncol_gr1a = f("ncol_gr1a");
                let nncoa_gr1a = f("nncoa_gr1a");
                let fnl_gr1a = f("fnl_gr1a");
                let nfna_gr1a = f("nfna_gr1a");
                let tax_gr1a = f("tax_gr1a");
                let be_gr1a = f("be_gr1a");
                let ebit_sale = f("ebit_sale");
                let gp_at = f("gp_at");
                let cop_at = f("cop_at");
                let ope_be = f("ope_be");
                let ni_be = f("ni_be");
                let ebit_bev = f("ebit_bev");
                let netis_at = f("netis_at");
                let eqnetis_at = f("eqnetis_at");
                let dbnetis_at = f("dbnetis_at");
                let oaccruals_at = f("oaccruals_at");
                let oaccruals_ni = f("oaccruals_ni");
                let taccruals_at = f("taccruals_at");
                let taccruals_ni = f("taccruals_ni");
                let noa_at = f("noa_at");
                let opex_at = f("opex_at");
                let at_turnover = f("at_turnover");
                let sale_bev = f("sale_bev");
                let rd_sale = f("rd_sale");
                let cash_at = f("cash_at");
                let sale_emp_gr1 = f("sale_emp_gr1");
                let emp_gr1 = f("emp_gr1");
                let ni_inc8q = f("ni_inc8q");
                let noa_gr1a = f("noa_gr1a");
                let ppeinv_gr1a = f("ppeinv_gr1a");
                let lnoa_gr1a = f("lnoa_gr1a");
                let capx_gr2 = f("capx_gr2");
                let saleq_gr1 = f("saleq_gr1");
                let niq_be = f("niq_be");
                let niq_at = f("niq_at");
                let niq_be_chg1 = f("niq_be_chg1");
                let niq_at_chg1 = f("niq_at_chg1");
                let rd5_at = f("rd5_at");
                let dsale_dinv = f("dsale_dinv");
                let dsale_drec = f("dsale_drec");
                let dgp_dsale = f("dgp_dsale");
                let dsale_dsga = f("dsale_dsga");
                let saleq_su = f("saleq_su");
                let niq_su = f("niq_su");
                let capex_abn = f("capex_abn");
                let op_atl1 = f("op_atl1");
                let gp_atl1 = f("gp_atl1");
                let ope_bel1 = f("ope_bel1");
                let cop_atl1 = f("cop_atl1");
                let pi_nix = f("pi_nix");
                let ocf_at = f("ocf_at");
                let op_at = f("op_at");
                let ocf_at_chg1 = f("ocf_at_chg1");
                let at_be = f("at_be");
                let ocfq_saleq_std = f("ocfq_saleq_std");
                let tangibility = f("tangibility");
                let earnings_variability = f("earnings_variability");
                let aliq_at = f("aliq_at");
                let f_score = f("f_score");
                let o_score = f("o_score");
                let z_score = f("z_score");
                let kz_index = f("kz_index");
                let ni_ar1 = f("ni_ar1");
                let ni_ivol = f("ni_ivol");
                let at_me = f("at_me");
                let be_me = f("be_me");
                let debt_me = f("debt_me");
                let netdebt_me = f("netdebt_me");
                let sale_me = f("sale_me");
                let ni_me = f("ni_me");
                let ocf_me = f("ocf_me");
                let fcf_me = f("fcf_me");
                let eqpo_me = f("eqpo_me");
                let eqnpo_me = f("eqnpo_me");
                let rd_me = f("rd_me");
                let ival_me = f("ival_me");
                let bev_mev = f("bev_mev");
                let ebitda_mev = f("ebitda_mev");
                let aliq_mat = f("aliq_mat");
                let eq_dur = f("eq_dur");
                let beta_60m = f("beta_60m");
                let resff3_12_1 = f("resff3_12_1");
                let resff3_6_1 = f("resff3_6_1");
                let mispricing_mgmt = f("mispricing_mgmt");
                let mispricing_perf = f("mispricing_perf");
                let ivol_capm_21d = f("ivol_capm_21d");
                let iskew_capm_21d = f("iskew_capm_21d");
                let coskew_21d = f("coskew_21d");
                let beta_dimson_21d = f("beta_dimson_21d");
                let ivol_ff3_21d = f("ivol_ff3_21d");
                let iskew_ff3_21d = f("iskew_ff3_21d");
                let ivol_hxz4_21d = f("ivol_hxz4_21d");
                let iskew_hxz4_21d = f("iskew_hxz4_21d");
                let rmax5_21d = f("rmax5_21d");
                let rmax1_21d = f("rmax1_21d");
                let rvol_21d = f("rvol_21d");
                let rskew_21d = f("rskew_21d");
                let zero_trades_21d = f("zero_trades_21d");
                let dolvol_126d = f("dolvol_126d");
                let dolvol_var_126d = f("dolvol_var_126d");
                let turnover_126d = f("turnover_126d");
                let turnover_var_126d = f("turnover_var_126d");
                let zero_trades_126d = f("zero_trades_126d");
                let zero_trades_252d = f("zero_trades_252d");
                let ami_126d = f("ami_126d");
                let ivol_capm_252d = f("ivol_capm_252d");
                let prc_highprc_252d = f("prc_highprc_252d");
                let betadown_252d = f("betadown_252d");
                let bidaskhl_21d = f("bidaskhl_21d");
                let corr_1260d = f("corr_1260d");
                let betabab_1260d = f("betabab_1260d");
                let rmax5_rvol_21d = f("rmax5_rvol_21d");
                let age = f("age");
                let qmj = f("qmj");
                let qmj_prof = f("qmj_prof");
                let qmj_growth = f("qmj_growth");
                let qmj_safety = f("qmj_safety");
                let enterprise_value = f("enterprise_value");
                let book_equity = f("book_equity");
                let assets = f("assets");
                let sales = f("sales");
                let net_income = f("net_income");
                let div1m_me = f("div1m_me");
                let div3m_me = f("div3m_me");
                let div6m_me = f("div6m_me");
                let divspc1m_me = f("divspc1m_me");
                let divspc12m_me = f("divspc12m_me");
                let chcsho_1m = f("chcsho_1m");
                let chcsho_3m = f("chcsho_3m");
                let chcsho_6m = f("chcsho_6m");
                let eqnpo_1m = f("eqnpo_1m");
                let eqnpo_3m = f("eqnpo_3m");
                let eqnpo_6m = f("eqnpo_6m");
                let ret_2_0 = f("ret_2_0");
                let ret_3_0 = f("ret_3_0");
                let ret_6_0 = f("ret_6_0");
                let ret_9_0 = f("ret_9_0");
                let ret_12_0 = f("ret_12_0");
                let ret_18_1 = f("ret_18_1");
                let ret_24_1 = f("ret_24_1");
                let ret_24_12 = f("ret_24_12");
                let ret_36_1 = f("ret_36_1");
                let ret_36_12 = f("ret_36_12");
                let ret_48_12 = f("ret_48_12");
                let ret_48_1 = f("ret_48_1");
                let ret_60_1 = f("ret_60_1");
                let ret_60_36 = f("ret_60_36");
                let ca_gr1 = f("ca_gr1");
                let nca_gr1 = f("nca_gr1");
                let lt_gr1 = f("lt_gr1");
                let cl_gr1 = f("cl_gr1");
                let ncl_gr1 = f("ncl_gr1");
                let be_gr1 = f("be_gr1");
                let pstk_gr1 = f("pstk_gr1");
                let debt_gr1 = f("debt_gr1");
                let cogs_gr1 = f("cogs_gr1");
                let sga_gr1 = f("sga_gr1");
                let opex_gr1 = f("opex_gr1");
                let at_gr3 = f("at_gr3");
                let ca_gr3 = f("ca_gr3");
                let nca_gr3 = f("nca_gr3");
                let lt_gr3 = f("lt_gr3");
                let cl_gr3 = f("cl_gr3");
                let ncl_gr3 = f("ncl_gr3");
                let be_gr3 = f("be_gr3");
                let pstk_gr3 = f("pstk_gr3");
                let cogs_gr3 = f("cogs_gr3");
                let sga_gr3 = f("sga_gr3");
                let opex_gr3 = f("opex_gr3");
                let cash_gr1a = f("cash_gr1a");
                let rec_gr1a = f("rec_gr1a");
                let ppeg_gr1a = f("ppeg_gr1a");
                let intan_gr1a = f("intan_gr1a");
                let debtst_gr1a = f("debtst_gr1a");
                let ap_gr1a = f("ap_gr1a");
                let txp_gr1a = f("txp_gr1a");
                let debtlt_gr1a = f("debtlt_gr1a");
                let txditc_gr1a = f("txditc_gr1a");
                let oa_gr1a = f("oa_gr1a");
                let ol_gr1a = f("ol_gr1a");
                let fna_gr1a = f("fna_gr1a");
                let gp_gr1a = f("gp_gr1a");
                let ebitda_gr1a = f("ebitda_gr1a");
                let ebit_gr1a = f("ebit_gr1a");
                let ope_gr1a = f("ope_gr1a");
                let ni_gr1a = f("ni_gr1a");
                let nix_gr1a = f("nix_gr1a");
                let dp_gr1a = f("dp_gr1a");
                let fincf_gr1a = f("fincf_gr1a");
                let ocf_gr1a = f("ocf_gr1a");
                let fcf_gr1a = f("fcf_gr1a");
                let nwc_gr1a = f("nwc_gr1a");
                let eqnetis_gr1a = f("eqnetis_gr1a");
                let dltnetis_gr1a = f("dltnetis_gr1a");
                let dstnetis_gr1a = f("dstnetis_gr1a");
                let dbnetis_gr1a = f("dbnetis_gr1a");
                let netis_gr1a = f("netis_gr1a");
                let eqnpo_gr1a = f("eqnpo_gr1a");
                let eqbb_gr1a = f("eqbb_gr1a");
                let eqis_gr1a = f("eqis_gr1a");
                let div_gr1a = f("div_gr1a");
                let eqpo_gr1a = f("eqpo_gr1a");
                let capx_gr1a = f("capx_gr1a");
                let cash_gr3a = f("cash_gr3a");
                let inv_gr3a = f("inv_gr3a");
                let rec_gr3a = f("rec_gr3a");
                let ppeg_gr3a = f("ppeg_gr3a");
                let lti_gr3a = f("lti_gr3a");
                let intan_gr3a = f("intan_gr3a");
                let debtst_gr3a = f("debtst_gr3a");
                let ap_gr3a = f("ap_gr3a");
                let txp_gr3a = f("txp_gr3a");
                let debtlt_gr3a = f("debtlt_gr3a");
                let txditc_gr3a = f("txditc_gr3a");
                let coa_gr3a = f("coa_gr3a");
                let col_gr3a = f("col_gr3a");
                let cowc_gr3a = f("cowc_gr3a");
                let ncoa_gr3a = f("ncoa_gr3a");
                let ncol_gr3a = f("ncol_gr3a");
                let nncoa_gr3a = f("nncoa_gr3a");
                let oa_gr3a = f("oa_gr3a");
                let ol_gr3a = f("ol_gr3a");
                let fna_gr3a = f("fna_gr3a");
                let fnl_gr3a = f("fnl_gr3a");
                let nfna_gr3a = f("nfna_gr3a");
                let gp_gr3a = f("gp_gr3a");
                let ebitda_gr3a = f("ebitda_gr3a");
                let ebit_gr3a = f("ebit_gr3a");
                let ope_gr3a = f("ope_gr3a");
                let ni_gr3a = f("ni_gr3a");
                let nix_gr3a = f("nix_gr3a");
                let dp_gr3a = f("dp_gr3a");
                let fincf_gr3a = f("fincf_gr3a");
                let ocf_gr3a = f("ocf_gr3a");
                let fcf_gr3a = f("fcf_gr3a");
                let nwc_gr3a = f("nwc_gr3a");
                let eqnetis_gr3a = f("eqnetis_gr3a");
                let dltnetis_gr3a = f("dltnetis_gr3a");
                let dstnetis_gr3a = f("dstnetis_gr3a");
                let dbnetis_gr3a = f("dbnetis_gr3a");
                let netis_gr3a = f("netis_gr3a");
                let eqnpo_gr3a = f("eqnpo_gr3a");
                let tax_gr3a = f("tax_gr3a");
                let eqbb_gr3a = f("eqbb_gr3a");
                let eqis_gr3a = f("eqis_gr3a");
                let div_gr3a = f("div_gr3a");
                let eqpo_gr3a = f("eqpo_gr3a");
                let capx_gr3a = f("capx_gr3a");
                let capx_at = f("capx_at");
                let rd_at = f("rd_at");
                let spi_at = f("spi_at");
                let xido_at = f("xido_at");
                let nri_at = f("nri_at");
                let gp_sale = f("gp_sale");
                let ebitda_sale = f("ebitda_sale");
                let pi_sale = f("pi_sale");
                let ni_sale = f("ni_sale");
                let nix_sale = f("nix_sale");
                let ocf_sale = f("ocf_sale");
                let fcf_sale = f("fcf_sale");
                let ebitda_at = f("ebitda_at");
                let ebit_at = f("ebit_at");
                let fi_at = f("fi_at");
                let ni_at = f("ni_at");
                let nix_be = f("nix_be");
                let ocf_be = f("ocf_be");
                let fcf_be = f("fcf_be");
                let gp_bev = f("gp_bev");
                let ebitda_bev = f("ebitda_bev");
                let fi_bev = f("fi_bev");
                let cop_bev = f("cop_bev");
                let gp_ppen = f("gp_ppen");
                let ebitda_ppen = f("ebitda_ppen");
                let fcf_ppen = f("fcf_ppen");
                let fincf_at = f("fincf_at");
                let eqis_at = f("eqis_at");
                let dltnetis_at = f("dltnetis_at");
                let dstnetis_at = f("dstnetis_at");
                let eqnpo_at = f("eqnpo_at");
                let eqbb_at = f("eqbb_at");
                let div_at = f("div_at");
                let be_bev = f("be_bev");
                let debt_bev = f("debt_bev");
                let cash_bev = f("cash_bev");
                let pstk_bev = f("pstk_bev");
                let debtlt_bev = f("debtlt_bev");
                let debtst_bev = f("debtst_bev");
                let int_debt = f("int_debt");
                let int_debtlt = f("int_debtlt");
                let ebitda_debt = f("ebitda_debt");
                let profit_cl = f("profit_cl");
                let ocf_cl = f("ocf_cl");
                let ocf_debt = f("ocf_debt");
                let cash_lt = f("cash_lt");
                let inv_act = f("inv_act");
                let rec_act = f("rec_act");
                let debtst_debt = f("debtst_debt");
                let cl_lt = f("cl_lt");
                let debtlt_debt = f("debtlt_debt");
                let lt_ppen = f("lt_ppen");
                let debtlt_be = f("debtlt_be");
                let nwc_at = f("nwc_at");
                let fcf_ocf = f("fcf_ocf");
                let debt_at = f("debt_at");
                let debt_be = f("debt_be");
                let ebit_int = f("ebit_int");
                let inv_days = f("inv_days");
                let rec_days = f("rec_days");
                let ap_days = f("ap_days");
                let cash_conversion = f("cash_conversion");
                let cash_cl = f("cash_cl");
                let caliq_cl = f("caliq_cl");
                let ca_cl = f("ca_cl");
                let inv_turnover = f("inv_turnover");
                let rec_turnover = f("rec_turnover");
                let ap_turnover = f("ap_turnover");
                let adv_sale = f("adv_sale");
                let staff_sale = f("staff_sale");
                let sale_be = f("sale_be");
                let div_ni = f("div_ni");
                let sale_nwc = f("sale_nwc");
                let tax_pi = f("tax_pi");
                let ni_emp = f("ni_emp");
                let sale_emp = f("sale_emp");
                let niq_saleq_std = f("niq_saleq_std");
                let roeq_be_std = f("roeq_be_std");
                let roe_be_std = f("roe_be_std");
                let intrinsic_value = f("intrinsic_value");
                let gpoa_ch5 = f("gpoa_ch5");
                let roe_ch5 = f("roe_ch5");
                let roa_ch5 = f("roa_ch5");
                let cfoa_ch5 = f("cfoa_ch5");
                let gmar_ch5 = f("gmar_ch5");
                let cash_me = f("cash_me");
                let gp_me = f("gp_me");
                let ebitda_me = f("ebitda_me");
                let ebit_me = f("ebit_me");
                let ope_me = f("ope_me");
                let nix_me = f("nix_me");
                let cop_me = f("cop_me");
                let div_me = f("div_me");
                let eqbb_me = f("eqbb_me");
                let eqis_me = f("eqis_me");
                let eqnetis_me = f("eqnetis_me");
                let at_mev = f("at_mev");
                let ppen_mev = f("ppen_mev");
                let be_mev = f("be_mev");
                let cash_mev = f("cash_mev");
                let sale_mev = f("sale_mev");
                let gp_mev = f("gp_mev");
                let ebit_mev = f("ebit_mev");
                let cop_mev = f("cop_mev");
                let ocf_mev = f("ocf_mev");
                let fcf_mev = f("fcf_mev");
                let debt_mev = f("debt_mev");
                let pstk_mev = f("pstk_mev");
                let debtlt_mev = f("debtlt_mev");
                let debtst_mev = f("debtst_mev");
                let dltnetis_mev = f("dltnetis_mev");
                let dstnetis_mev = f("dstnetis_mev");
                let dbnetis_mev = f("dbnetis_mev");
                let netis_mev = f("netis_mev");
                let fincf_mev = f("fincf_mev");
                let ivol_capm_60m = f("ivol_capm_60m");
                let beta_21d = f("beta_21d");
                let beta_252d = f("beta_252d");
                let rvol_252d = f("rvol_252d");
                let rvolhl_21d = f("rvolhl_21d");
                let rows: Vec<Row<'static>> = (0..batch.num_rows())
                    .into_par_iter()
                    .map(|row_i| {
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
                        gvkey: gi32(gvkey),
                        iid: gs(iid),
                        permno: gi32(permno),
                        permco: gi32(permco),
                        id: gi64(id),
                        date: gd(date),
                        eom: gd(eom),
                        excntry: gs(excntry),
                        size_grp: gs(size_grp),
                        me: gf(me),
                        ret_exc: gf(ret_exc),
                        ret: gf(ret),
                        prc: gf(prc),
                        obs_main: gf(obs_main),
                        exch_main: gf(exch_main),
                        common: gf(common),
                        primary_sec: gf(primary_sec),
                        me_company: gf(me_company),
                        ret_exc_lead1m: gf(ret_exc_lead1m),
                        ret_local: gf(ret_local),
                        ret_lag_dif: gf(ret_lag_dif),
                        prc_local: gf(prc_local),
                        prc_high: gf(prc_high),
                        prc_low: gf(prc_low),
                        bidask: gf(bidask),
                        curcd: gs(curcd),
                        fx: gf(fx),
                        gics: gf(gics),
                        naics: gf(naics),
                        sic: gf(sic),
                        ff49: gf(ff49),
                        dolvol: gf(dolvol),
                        shares: gf(shares),
                        tvol: gf(tvol),
                        adjfct: gf(adjfct),
                        comp_tpci: gs(comp_tpci),
                        crsp_shrcd: gf(crsp_shrcd),
                        comp_exchg: gf(comp_exchg),
                        crsp_exchcd: gf(crsp_exchcd),
                        source_crsp: gf(source_crsp),
                        market_equity: gf(market_equity),
                        div12m_me: gf(div12m_me),
                        chcsho_12m: gf(chcsho_12m),
                        eqnpo_12m: gf(eqnpo_12m),
                        ret_1_0: gf(ret_1_0),
                        ret_3_1: gf(ret_3_1),
                        ret_6_1: gf(ret_6_1),
                        ret_9_1: gf(ret_9_1),
                        ret_12_1: gf(ret_12_1),
                        ret_12_7: gf(ret_12_7),
                        ret_60_12: gf(ret_60_12),
                        seas_1_1an: gf(seas_1_1an),
                        seas_1_1na: gf(seas_1_1na),
                        seas_2_5an: gf(seas_2_5an),
                        seas_2_5na: gf(seas_2_5na),
                        seas_6_10an: gf(seas_6_10an),
                        seas_6_10na: gf(seas_6_10na),
                        seas_11_15an: gf(seas_11_15an),
                        seas_11_15na: gf(seas_11_15na),
                        seas_16_20an: gf(seas_16_20an),
                        seas_16_20na: gf(seas_16_20na),
                        at_gr1: gf(at_gr1),
                        sale_gr1: gf(sale_gr1),
                        capx_gr1: gf(capx_gr1),
                        inv_gr1: gf(inv_gr1),
                        debt_gr3: gf(debt_gr3),
                        sale_gr3: gf(sale_gr3),
                        capx_gr3: gf(capx_gr3),
                        inv_gr1a: gf(inv_gr1a),
                        lti_gr1a: gf(lti_gr1a),
                        sti_gr1a: gf(sti_gr1a),
                        coa_gr1a: gf(coa_gr1a),
                        col_gr1a: gf(col_gr1a),
                        cowc_gr1a: gf(cowc_gr1a),
                        ncoa_gr1a: gf(ncoa_gr1a),
                        ncol_gr1a: gf(ncol_gr1a),
                        nncoa_gr1a: gf(nncoa_gr1a),
                        fnl_gr1a: gf(fnl_gr1a),
                        nfna_gr1a: gf(nfna_gr1a),
                        tax_gr1a: gf(tax_gr1a),
                        be_gr1a: gf(be_gr1a),
                        ebit_sale: gf(ebit_sale),
                        gp_at: gf(gp_at),
                        cop_at: gf(cop_at),
                        ope_be: gf(ope_be),
                        ni_be: gf(ni_be),
                        ebit_bev: gf(ebit_bev),
                        netis_at: gf(netis_at),
                        eqnetis_at: gf(eqnetis_at),
                        dbnetis_at: gf(dbnetis_at),
                        oaccruals_at: gf(oaccruals_at),
                        oaccruals_ni: gf(oaccruals_ni),
                        taccruals_at: gf(taccruals_at),
                        taccruals_ni: gf(taccruals_ni),
                        noa_at: gf(noa_at),
                        opex_at: gf(opex_at),
                        at_turnover: gf(at_turnover),
                        sale_bev: gf(sale_bev),
                        rd_sale: gf(rd_sale),
                        cash_at: gf(cash_at),
                        sale_emp_gr1: gf(sale_emp_gr1),
                        emp_gr1: gf(emp_gr1),
                        ni_inc8q: gf(ni_inc8q),
                        noa_gr1a: gf(noa_gr1a),
                        ppeinv_gr1a: gf(ppeinv_gr1a),
                        lnoa_gr1a: gf(lnoa_gr1a),
                        capx_gr2: gf(capx_gr2),
                        saleq_gr1: gf(saleq_gr1),
                        niq_be: gf(niq_be),
                        niq_at: gf(niq_at),
                        niq_be_chg1: gf(niq_be_chg1),
                        niq_at_chg1: gf(niq_at_chg1),
                        rd5_at: gf(rd5_at),
                        dsale_dinv: gf(dsale_dinv),
                        dsale_drec: gf(dsale_drec),
                        dgp_dsale: gf(dgp_dsale),
                        dsale_dsga: gf(dsale_dsga),
                        saleq_su: gf(saleq_su),
                        niq_su: gf(niq_su),
                        capex_abn: gf(capex_abn),
                        op_atl1: gf(op_atl1),
                        gp_atl1: gf(gp_atl1),
                        ope_bel1: gf(ope_bel1),
                        cop_atl1: gf(cop_atl1),
                        pi_nix: gf(pi_nix),
                        ocf_at: gf(ocf_at),
                        op_at: gf(op_at),
                        ocf_at_chg1: gf(ocf_at_chg1),
                        at_be: gf(at_be),
                        ocfq_saleq_std: gf(ocfq_saleq_std),
                        tangibility: gf(tangibility),
                        earnings_variability: gf(earnings_variability),
                        aliq_at: gf(aliq_at),
                        f_score: gf(f_score),
                        o_score: gf(o_score),
                        z_score: gf(z_score),
                        kz_index: gf(kz_index),
                        ni_ar1: gf(ni_ar1),
                        ni_ivol: gf(ni_ivol),
                        at_me: gf(at_me),
                        be_me: gf(be_me),
                        debt_me: gf(debt_me),
                        netdebt_me: gf(netdebt_me),
                        sale_me: gf(sale_me),
                        ni_me: gf(ni_me),
                        ocf_me: gf(ocf_me),
                        fcf_me: gf(fcf_me),
                        eqpo_me: gf(eqpo_me),
                        eqnpo_me: gf(eqnpo_me),
                        rd_me: gf(rd_me),
                        ival_me: gf(ival_me),
                        bev_mev: gf(bev_mev),
                        ebitda_mev: gf(ebitda_mev),
                        aliq_mat: gf(aliq_mat),
                        eq_dur: gf(eq_dur),
                        beta_60m: gf(beta_60m),
                        resff3_12_1: gf(resff3_12_1),
                        resff3_6_1: gf(resff3_6_1),
                        mispricing_mgmt: gf(mispricing_mgmt),
                        mispricing_perf: gf(mispricing_perf),
                        ivol_capm_21d: gf(ivol_capm_21d),
                        iskew_capm_21d: gf(iskew_capm_21d),
                        coskew_21d: gf(coskew_21d),
                        beta_dimson_21d: gf(beta_dimson_21d),
                        ivol_ff3_21d: gf(ivol_ff3_21d),
                        iskew_ff3_21d: gf(iskew_ff3_21d),
                        ivol_hxz4_21d: gf(ivol_hxz4_21d),
                        iskew_hxz4_21d: gf(iskew_hxz4_21d),
                        rmax5_21d: gf(rmax5_21d),
                        rmax1_21d: gf(rmax1_21d),
                        rvol_21d: gf(rvol_21d),
                        rskew_21d: gf(rskew_21d),
                        zero_trades_21d: gf(zero_trades_21d),
                        dolvol_126d: gf(dolvol_126d),
                        dolvol_var_126d: gf(dolvol_var_126d),
                        turnover_126d: gf(turnover_126d),
                        turnover_var_126d: gf(turnover_var_126d),
                        zero_trades_126d: gf(zero_trades_126d),
                        zero_trades_252d: gf(zero_trades_252d),
                        ami_126d: gf(ami_126d),
                        ivol_capm_252d: gf(ivol_capm_252d),
                        prc_highprc_252d: gf(prc_highprc_252d),
                        betadown_252d: gf(betadown_252d),
                        bidaskhl_21d: gf(bidaskhl_21d),
                        corr_1260d: gf(corr_1260d),
                        betabab_1260d: gf(betabab_1260d),
                        rmax5_rvol_21d: gf(rmax5_rvol_21d),
                        age: gf(age),
                        qmj: gf(qmj),
                        qmj_prof: gf(qmj_prof),
                        qmj_growth: gf(qmj_growth),
                        qmj_safety: gf(qmj_safety),
                        enterprise_value: gf(enterprise_value),
                        book_equity: gf(book_equity),
                        assets: gf(assets),
                        sales: gf(sales),
                        net_income: gf(net_income),
                        div1m_me: gf(div1m_me),
                        div3m_me: gf(div3m_me),
                        div6m_me: gf(div6m_me),
                        divspc1m_me: gf(divspc1m_me),
                        divspc12m_me: gf(divspc12m_me),
                        chcsho_1m: gf(chcsho_1m),
                        chcsho_3m: gf(chcsho_3m),
                        chcsho_6m: gf(chcsho_6m),
                        eqnpo_1m: gf(eqnpo_1m),
                        eqnpo_3m: gf(eqnpo_3m),
                        eqnpo_6m: gf(eqnpo_6m),
                        ret_2_0: gf(ret_2_0),
                        ret_3_0: gf(ret_3_0),
                        ret_6_0: gf(ret_6_0),
                        ret_9_0: gf(ret_9_0),
                        ret_12_0: gf(ret_12_0),
                        ret_18_1: gf(ret_18_1),
                        ret_24_1: gf(ret_24_1),
                        ret_24_12: gf(ret_24_12),
                        ret_36_1: gf(ret_36_1),
                        ret_36_12: gf(ret_36_12),
                        ret_48_12: gf(ret_48_12),
                        ret_48_1: gf(ret_48_1),
                        ret_60_1: gf(ret_60_1),
                        ret_60_36: gf(ret_60_36),
                        ca_gr1: gf(ca_gr1),
                        nca_gr1: gf(nca_gr1),
                        lt_gr1: gf(lt_gr1),
                        cl_gr1: gf(cl_gr1),
                        ncl_gr1: gf(ncl_gr1),
                        be_gr1: gf(be_gr1),
                        pstk_gr1: gf(pstk_gr1),
                        debt_gr1: gf(debt_gr1),
                        cogs_gr1: gf(cogs_gr1),
                        sga_gr1: gf(sga_gr1),
                        opex_gr1: gf(opex_gr1),
                        at_gr3: gf(at_gr3),
                        ca_gr3: gf(ca_gr3),
                        nca_gr3: gf(nca_gr3),
                        lt_gr3: gf(lt_gr3),
                        cl_gr3: gf(cl_gr3),
                        ncl_gr3: gf(ncl_gr3),
                        be_gr3: gf(be_gr3),
                        pstk_gr3: gf(pstk_gr3),
                        cogs_gr3: gf(cogs_gr3),
                        sga_gr3: gf(sga_gr3),
                        opex_gr3: gf(opex_gr3),
                        cash_gr1a: gf(cash_gr1a),
                        rec_gr1a: gf(rec_gr1a),
                        ppeg_gr1a: gf(ppeg_gr1a),
                        intan_gr1a: gf(intan_gr1a),
                        debtst_gr1a: gf(debtst_gr1a),
                        ap_gr1a: gf(ap_gr1a),
                        txp_gr1a: gf(txp_gr1a),
                        debtlt_gr1a: gf(debtlt_gr1a),
                        txditc_gr1a: gf(txditc_gr1a),
                        oa_gr1a: gf(oa_gr1a),
                        ol_gr1a: gf(ol_gr1a),
                        fna_gr1a: gf(fna_gr1a),
                        gp_gr1a: gf(gp_gr1a),
                        ebitda_gr1a: gf(ebitda_gr1a),
                        ebit_gr1a: gf(ebit_gr1a),
                        ope_gr1a: gf(ope_gr1a),
                        ni_gr1a: gf(ni_gr1a),
                        nix_gr1a: gf(nix_gr1a),
                        dp_gr1a: gf(dp_gr1a),
                        fincf_gr1a: gf(fincf_gr1a),
                        ocf_gr1a: gf(ocf_gr1a),
                        fcf_gr1a: gf(fcf_gr1a),
                        nwc_gr1a: gf(nwc_gr1a),
                        eqnetis_gr1a: gf(eqnetis_gr1a),
                        dltnetis_gr1a: gf(dltnetis_gr1a),
                        dstnetis_gr1a: gf(dstnetis_gr1a),
                        dbnetis_gr1a: gf(dbnetis_gr1a),
                        netis_gr1a: gf(netis_gr1a),
                        eqnpo_gr1a: gf(eqnpo_gr1a),
                        eqbb_gr1a: gf(eqbb_gr1a),
                        eqis_gr1a: gf(eqis_gr1a),
                        div_gr1a: gf(div_gr1a),
                        eqpo_gr1a: gf(eqpo_gr1a),
                        capx_gr1a: gf(capx_gr1a),
                        cash_gr3a: gf(cash_gr3a),
                        inv_gr3a: gf(inv_gr3a),
                        rec_gr3a: gf(rec_gr3a),
                        ppeg_gr3a: gf(ppeg_gr3a),
                        lti_gr3a: gf(lti_gr3a),
                        intan_gr3a: gf(intan_gr3a),
                        debtst_gr3a: gf(debtst_gr3a),
                        ap_gr3a: gf(ap_gr3a),
                        txp_gr3a: gf(txp_gr3a),
                        debtlt_gr3a: gf(debtlt_gr3a),
                        txditc_gr3a: gf(txditc_gr3a),
                        coa_gr3a: gf(coa_gr3a),
                        col_gr3a: gf(col_gr3a),
                        cowc_gr3a: gf(cowc_gr3a),
                        ncoa_gr3a: gf(ncoa_gr3a),
                        ncol_gr3a: gf(ncol_gr3a),
                        nncoa_gr3a: gf(nncoa_gr3a),
                        oa_gr3a: gf(oa_gr3a),
                        ol_gr3a: gf(ol_gr3a),
                        fna_gr3a: gf(fna_gr3a),
                        fnl_gr3a: gf(fnl_gr3a),
                        nfna_gr3a: gf(nfna_gr3a),
                        gp_gr3a: gf(gp_gr3a),
                        ebitda_gr3a: gf(ebitda_gr3a),
                        ebit_gr3a: gf(ebit_gr3a),
                        ope_gr3a: gf(ope_gr3a),
                        ni_gr3a: gf(ni_gr3a),
                        nix_gr3a: gf(nix_gr3a),
                        dp_gr3a: gf(dp_gr3a),
                        fincf_gr3a: gf(fincf_gr3a),
                        ocf_gr3a: gf(ocf_gr3a),
                        fcf_gr3a: gf(fcf_gr3a),
                        nwc_gr3a: gf(nwc_gr3a),
                        eqnetis_gr3a: gf(eqnetis_gr3a),
                        dltnetis_gr3a: gf(dltnetis_gr3a),
                        dstnetis_gr3a: gf(dstnetis_gr3a),
                        dbnetis_gr3a: gf(dbnetis_gr3a),
                        netis_gr3a: gf(netis_gr3a),
                        eqnpo_gr3a: gf(eqnpo_gr3a),
                        tax_gr3a: gf(tax_gr3a),
                        eqbb_gr3a: gf(eqbb_gr3a),
                        eqis_gr3a: gf(eqis_gr3a),
                        div_gr3a: gf(div_gr3a),
                        eqpo_gr3a: gf(eqpo_gr3a),
                        capx_gr3a: gf(capx_gr3a),
                        capx_at: gf(capx_at),
                        rd_at: gf(rd_at),
                        spi_at: gf(spi_at),
                        xido_at: gf(xido_at),
                        nri_at: gf(nri_at),
                        gp_sale: gf(gp_sale),
                        ebitda_sale: gf(ebitda_sale),
                        pi_sale: gf(pi_sale),
                        ni_sale: gf(ni_sale),
                        nix_sale: gf(nix_sale),
                        ocf_sale: gf(ocf_sale),
                        fcf_sale: gf(fcf_sale),
                        ebitda_at: gf(ebitda_at),
                        ebit_at: gf(ebit_at),
                        fi_at: gf(fi_at),
                        ni_at: gf(ni_at),
                        nix_be: gf(nix_be),
                        ocf_be: gf(ocf_be),
                        fcf_be: gf(fcf_be),
                        gp_bev: gf(gp_bev),
                        ebitda_bev: gf(ebitda_bev),
                        fi_bev: gf(fi_bev),
                        cop_bev: gf(cop_bev),
                        gp_ppen: gf(gp_ppen),
                        ebitda_ppen: gf(ebitda_ppen),
                        fcf_ppen: gf(fcf_ppen),
                        fincf_at: gf(fincf_at),
                        eqis_at: gf(eqis_at),
                        dltnetis_at: gf(dltnetis_at),
                        dstnetis_at: gf(dstnetis_at),
                        eqnpo_at: gf(eqnpo_at),
                        eqbb_at: gf(eqbb_at),
                        div_at: gf(div_at),
                        be_bev: gf(be_bev),
                        debt_bev: gf(debt_bev),
                        cash_bev: gf(cash_bev),
                        pstk_bev: gf(pstk_bev),
                        debtlt_bev: gf(debtlt_bev),
                        debtst_bev: gf(debtst_bev),
                        int_debt: gf(int_debt),
                        int_debtlt: gf(int_debtlt),
                        ebitda_debt: gf(ebitda_debt),
                        profit_cl: gf(profit_cl),
                        ocf_cl: gf(ocf_cl),
                        ocf_debt: gf(ocf_debt),
                        cash_lt: gf(cash_lt),
                        inv_act: gf(inv_act),
                        rec_act: gf(rec_act),
                        debtst_debt: gf(debtst_debt),
                        cl_lt: gf(cl_lt),
                        debtlt_debt: gf(debtlt_debt),
                        lt_ppen: gf(lt_ppen),
                        debtlt_be: gf(debtlt_be),
                        nwc_at: gf(nwc_at),
                        fcf_ocf: gf(fcf_ocf),
                        debt_at: gf(debt_at),
                        debt_be: gf(debt_be),
                        ebit_int: gf(ebit_int),
                        inv_days: gf(inv_days),
                        rec_days: gf(rec_days),
                        ap_days: gf(ap_days),
                        cash_conversion: gf(cash_conversion),
                        cash_cl: gf(cash_cl),
                        caliq_cl: gf(caliq_cl),
                        ca_cl: gf(ca_cl),
                        inv_turnover: gf(inv_turnover),
                        rec_turnover: gf(rec_turnover),
                        ap_turnover: gf(ap_turnover),
                        adv_sale: gf(adv_sale),
                        staff_sale: gf(staff_sale),
                        sale_be: gf(sale_be),
                        div_ni: gf(div_ni),
                        sale_nwc: gf(sale_nwc),
                        tax_pi: gf(tax_pi),
                        ni_emp: gf(ni_emp),
                        sale_emp: gf(sale_emp),
                        niq_saleq_std: gf(niq_saleq_std),
                        roeq_be_std: gf(roeq_be_std),
                        roe_be_std: gf(roe_be_std),
                        intrinsic_value: gf(intrinsic_value),
                        gpoa_ch5: gf(gpoa_ch5),
                        roe_ch5: gf(roe_ch5),
                        roa_ch5: gf(roa_ch5),
                        cfoa_ch5: gf(cfoa_ch5),
                        gmar_ch5: gf(gmar_ch5),
                        cash_me: gf(cash_me),
                        gp_me: gf(gp_me),
                        ebitda_me: gf(ebitda_me),
                        ebit_me: gf(ebit_me),
                        ope_me: gf(ope_me),
                        nix_me: gf(nix_me),
                        cop_me: gf(cop_me),
                        div_me: gf(div_me),
                        eqbb_me: gf(eqbb_me),
                        eqis_me: gf(eqis_me),
                        eqnetis_me: gf(eqnetis_me),
                        at_mev: gf(at_mev),
                        ppen_mev: gf(ppen_mev),
                        be_mev: gf(be_mev),
                        cash_mev: gf(cash_mev),
                        sale_mev: gf(sale_mev),
                        gp_mev: gf(gp_mev),
                        ebit_mev: gf(ebit_mev),
                        cop_mev: gf(cop_mev),
                        ocf_mev: gf(ocf_mev),
                        fcf_mev: gf(fcf_mev),
                        debt_mev: gf(debt_mev),
                        pstk_mev: gf(pstk_mev),
                        debtlt_mev: gf(debtlt_mev),
                        debtst_mev: gf(debtst_mev),
                        dltnetis_mev: gf(dltnetis_mev),
                        dstnetis_mev: gf(dstnetis_mev),
                        dbnetis_mev: gf(dbnetis_mev),
                        netis_mev: gf(netis_mev),
                        fincf_mev: gf(fincf_mev),
                        ivol_capm_60m: gf(ivol_capm_60m),
                        beta_21d: gf(beta_21d),
                        beta_252d: gf(beta_252d),
                        rvol_252d: gf(rvol_252d),
                        rvolhl_21d: gf(rvolhl_21d),
                    };
                        let row: Row<'static> = temp.to_row();
                        row
                    })
                    .collect();
                out.extend(rows);
            }
            Ok::<Vec<Row>, AppError>(out)
        })
        .await?
    }

    fn date_to_any(d: Option<NaiveDate>) -> AnyValue<'static> {
        match d {
            Some(nd) => {
                let days: i32 = (nd.num_days_from_ce() - 719_163) as i32;
                AnyValue::Date(days)
            }
            None => AnyValue::Null,
        }
    }

    pub fn to_row<'a>(self) -> Row<'a> {
        Row::new(vec![
            // Core identifiers / dates
            self.gvkey.map_or(AnyValue::Null, |v| AnyValue::Int32(v)),
            self.iid
                .map(|v| AnyValue::StringOwned(v.into()))
                .unwrap_or(AnyValue::Null),
            self.permno.map_or(AnyValue::Null, |v| AnyValue::Int32(v)),
            self.permco.map_or(AnyValue::Null, |v| AnyValue::Int32(v)),
            self.id.map_or(AnyValue::Null, |v| AnyValue::Int64(v)),
            Self::date_to_any(self.date),
            Self::date_to_any(self.eom),
            self.excntry
                .map(|v| AnyValue::StringOwned(v.into()))
                .unwrap_or(AnyValue::Null),
            self.size_grp
                .map(|v| AnyValue::StringOwned(v.into()))
                .unwrap_or(AnyValue::Null),
            // Common measures
            self.me.map_or(AnyValue::Null, AnyValue::Float64),
            self.ret_exc.map_or(AnyValue::Null, AnyValue::Float64),
            self.ret.map_or(AnyValue::Null, AnyValue::Float64),
            self.prc.map_or(AnyValue::Null, AnyValue::Float64),
            // Added fields (Float64 unless noted)
            self.obs_main.map_or(AnyValue::Null, AnyValue::Float64),
            self.exch_main.map_or(AnyValue::Null, AnyValue::Float64),
            self.common.map_or(AnyValue::Null, AnyValue::Float64),
            self.primary_sec.map_or(AnyValue::Null, AnyValue::Float64),
            self.me_company.map_or(AnyValue::Null, AnyValue::Float64),
            self.ret_exc_lead1m
                .map_or(AnyValue::Null, AnyValue::Float64),
            self.ret_local.map_or(AnyValue::Null, AnyValue::Float64),
            self.ret_lag_dif.map_or(AnyValue::Null, AnyValue::Float64),
            self.prc_local.map_or(AnyValue::Null, AnyValue::Float64),
            self.prc_high.map_or(AnyValue::Null, AnyValue::Float64),
            self.prc_low.map_or(AnyValue::Null, AnyValue::Float64),
            self.bidask.map_or(AnyValue::Null, AnyValue::Float64),
            self.curcd
                .map(|v| AnyValue::StringOwned(v.into()))
                .unwrap_or(AnyValue::Null),
            self.fx.map_or(AnyValue::Null, AnyValue::Float64),
            self.gics.map_or(AnyValue::Null, AnyValue::Float64),
            self.naics.map_or(AnyValue::Null, AnyValue::Float64),
            self.sic.map_or(AnyValue::Null, AnyValue::Float64),
            self.ff49.map_or(AnyValue::Null, AnyValue::Float64),
            self.dolvol.map_or(AnyValue::Null, AnyValue::Float64),
            self.shares.map_or(AnyValue::Null, AnyValue::Float64),
            self.tvol.map_or(AnyValue::Null, AnyValue::Float64),
            self.adjfct.map_or(AnyValue::Null, AnyValue::Float64),
            self.comp_tpci
                .map(|v| AnyValue::StringOwned(v.into()))
                .unwrap_or(AnyValue::Null),
            self.crsp_shrcd.map_or(AnyValue::Null, AnyValue::Float64),
            self.comp_exchg.map_or(AnyValue::Null, AnyValue::Float64),
            self.crsp_exchcd.map_or(AnyValue::Null, AnyValue::Float64),
            self.source_crsp.map_or(AnyValue::Null, AnyValue::Float64),
            self.market_equity.map_or(AnyValue::Null, AnyValue::Float64),
            self.div12m_me.map_or(AnyValue::Null, AnyValue::Float64),
            self.chcsho_12m.map_or(AnyValue::Null, AnyValue::Float64),
            self.eqnpo_12m.map_or(AnyValue::Null, AnyValue::Float64),
            self.ret_1_0.map_or(AnyValue::Null, AnyValue::Float64),
            self.ret_3_1.map_or(AnyValue::Null, AnyValue::Float64),
            self.ret_6_1.map_or(AnyValue::Null, AnyValue::Float64),
            self.ret_9_1.map_or(AnyValue::Null, AnyValue::Float64),
            self.ret_12_1.map_or(AnyValue::Null, AnyValue::Float64),
            self.ret_12_7.map_or(AnyValue::Null, AnyValue::Float64),
            self.ret_60_12.map_or(AnyValue::Null, AnyValue::Float64),
            self.seas_1_1an.map_or(AnyValue::Null, AnyValue::Float64),
            self.seas_1_1na.map_or(AnyValue::Null, AnyValue::Float64),
            self.seas_2_5an.map_or(AnyValue::Null, AnyValue::Float64),
            self.seas_2_5na.map_or(AnyValue::Null, AnyValue::Float64),
            self.seas_6_10an.map_or(AnyValue::Null, AnyValue::Float64),
            self.seas_6_10na.map_or(AnyValue::Null, AnyValue::Float64),
            self.seas_11_15an.map_or(AnyValue::Null, AnyValue::Float64),
            self.seas_11_15na.map_or(AnyValue::Null, AnyValue::Float64),
            self.seas_16_20an.map_or(AnyValue::Null, AnyValue::Float64),
            self.seas_16_20na.map_or(AnyValue::Null, AnyValue::Float64),
            self.at_gr1.map_or(AnyValue::Null, AnyValue::Float64),
            self.sale_gr1.map_or(AnyValue::Null, AnyValue::Float64),
            self.capx_gr1.map_or(AnyValue::Null, AnyValue::Float64),
            self.inv_gr1.map_or(AnyValue::Null, AnyValue::Float64),
            self.debt_gr3.map_or(AnyValue::Null, AnyValue::Float64),
            self.sale_gr3.map_or(AnyValue::Null, AnyValue::Float64),
            self.capx_gr3.map_or(AnyValue::Null, AnyValue::Float64),
            self.inv_gr1a.map_or(AnyValue::Null, AnyValue::Float64),
            self.lti_gr1a.map_or(AnyValue::Null, AnyValue::Float64),
            self.sti_gr1a.map_or(AnyValue::Null, AnyValue::Float64),
            self.coa_gr1a.map_or(AnyValue::Null, AnyValue::Float64),
            self.col_gr1a.map_or(AnyValue::Null, AnyValue::Float64),
            self.cowc_gr1a.map_or(AnyValue::Null, AnyValue::Float64),
            self.ncoa_gr1a.map_or(AnyValue::Null, AnyValue::Float64),
            self.ncol_gr1a.map_or(AnyValue::Null, AnyValue::Float64),
            self.nncoa_gr1a.map_or(AnyValue::Null, AnyValue::Float64),
            self.fnl_gr1a.map_or(AnyValue::Null, AnyValue::Float64),
            self.nfna_gr1a.map_or(AnyValue::Null, AnyValue::Float64),
            self.tax_gr1a.map_or(AnyValue::Null, AnyValue::Float64),
            self.be_gr1a.map_or(AnyValue::Null, AnyValue::Float64),
            self.ebit_sale.map_or(AnyValue::Null, AnyValue::Float64),
            self.gp_at.map_or(AnyValue::Null, AnyValue::Float64),
            self.cop_at.map_or(AnyValue::Null, AnyValue::Float64),
            self.ope_be.map_or(AnyValue::Null, AnyValue::Float64),
            self.ni_be.map_or(AnyValue::Null, AnyValue::Float64),
            self.ebit_bev.map_or(AnyValue::Null, AnyValue::Float64),
            self.netis_at.map_or(AnyValue::Null, AnyValue::Float64),
            self.eqnetis_at.map_or(AnyValue::Null, AnyValue::Float64),
            self.dbnetis_at.map_or(AnyValue::Null, AnyValue::Float64),
            self.oaccruals_at.map_or(AnyValue::Null, AnyValue::Float64),
            self.oaccruals_ni.map_or(AnyValue::Null, AnyValue::Float64),
            self.taccruals_at.map_or(AnyValue::Null, AnyValue::Float64),
            self.taccruals_ni.map_or(AnyValue::Null, AnyValue::Float64),
            self.noa_at.map_or(AnyValue::Null, AnyValue::Float64),
            self.opex_at.map_or(AnyValue::Null, AnyValue::Float64),
            self.at_turnover.map_or(AnyValue::Null, AnyValue::Float64),
            self.sale_bev.map_or(AnyValue::Null, AnyValue::Float64),
            self.rd_sale.map_or(AnyValue::Null, AnyValue::Float64),
            self.cash_at.map_or(AnyValue::Null, AnyValue::Float64),
            self.sale_emp_gr1.map_or(AnyValue::Null, AnyValue::Float64),
            self.emp_gr1.map_or(AnyValue::Null, AnyValue::Float64),
            self.ni_inc8q.map_or(AnyValue::Null, AnyValue::Float64),
            self.noa_gr1a.map_or(AnyValue::Null, AnyValue::Float64),
            self.ppeinv_gr1a.map_or(AnyValue::Null, AnyValue::Float64),
            self.lnoa_gr1a.map_or(AnyValue::Null, AnyValue::Float64),
            self.capx_gr2.map_or(AnyValue::Null, AnyValue::Float64),
            self.saleq_gr1.map_or(AnyValue::Null, AnyValue::Float64),
            self.niq_be.map_or(AnyValue::Null, AnyValue::Float64),
            self.niq_at.map_or(AnyValue::Null, AnyValue::Float64),
            self.niq_be_chg1.map_or(AnyValue::Null, AnyValue::Float64),
            self.niq_at_chg1.map_or(AnyValue::Null, AnyValue::Float64),
            self.rd5_at.map_or(AnyValue::Null, AnyValue::Float64),
            self.dsale_dinv.map_or(AnyValue::Null, AnyValue::Float64),
            self.dsale_drec.map_or(AnyValue::Null, AnyValue::Float64),
            self.dgp_dsale.map_or(AnyValue::Null, AnyValue::Float64),
            self.dsale_dsga.map_or(AnyValue::Null, AnyValue::Float64),
            self.saleq_su.map_or(AnyValue::Null, AnyValue::Float64),
            self.niq_su.map_or(AnyValue::Null, AnyValue::Float64),
            self.capex_abn.map_or(AnyValue::Null, AnyValue::Float64),
            self.op_atl1.map_or(AnyValue::Null, AnyValue::Float64),
            self.gp_atl1.map_or(AnyValue::Null, AnyValue::Float64),
            self.ope_bel1.map_or(AnyValue::Null, AnyValue::Float64),
            self.cop_atl1.map_or(AnyValue::Null, AnyValue::Float64),
            self.pi_nix.map_or(AnyValue::Null, AnyValue::Float64),
            self.ocf_at.map_or(AnyValue::Null, AnyValue::Float64),
            self.op_at.map_or(AnyValue::Null, AnyValue::Float64),
            self.ocf_at_chg1.map_or(AnyValue::Null, AnyValue::Float64),
            self.at_be.map_or(AnyValue::Null, AnyValue::Float64),
            self.ocfq_saleq_std
                .map_or(AnyValue::Null, AnyValue::Float64),
            self.tangibility.map_or(AnyValue::Null, AnyValue::Float64),
            self.earnings_variability
                .map_or(AnyValue::Null, AnyValue::Float64),
            self.aliq_at.map_or(AnyValue::Null, AnyValue::Float64),
            self.f_score.map_or(AnyValue::Null, AnyValue::Float64),
            self.o_score.map_or(AnyValue::Null, AnyValue::Float64),
            self.z_score.map_or(AnyValue::Null, AnyValue::Float64),
            self.kz_index.map_or(AnyValue::Null, AnyValue::Float64),
            self.ni_ar1.map_or(AnyValue::Null, AnyValue::Float64),
            self.ni_ivol.map_or(AnyValue::Null, AnyValue::Float64),
            self.at_me.map_or(AnyValue::Null, AnyValue::Float64),
            self.be_me.map_or(AnyValue::Null, AnyValue::Float64),
            self.debt_me.map_or(AnyValue::Null, AnyValue::Float64),
            self.netdebt_me.map_or(AnyValue::Null, AnyValue::Float64),
            self.sale_me.map_or(AnyValue::Null, AnyValue::Float64),
            self.ni_me.map_or(AnyValue::Null, AnyValue::Float64),
            self.ocf_me.map_or(AnyValue::Null, AnyValue::Float64),
            self.fcf_me.map_or(AnyValue::Null, AnyValue::Float64),
            self.eqpo_me.map_or(AnyValue::Null, AnyValue::Float64),
            self.eqnpo_me.map_or(AnyValue::Null, AnyValue::Float64),
            self.rd_me.map_or(AnyValue::Null, AnyValue::Float64),
            self.ival_me.map_or(AnyValue::Null, AnyValue::Float64),
            self.bev_mev.map_or(AnyValue::Null, AnyValue::Float64),
            self.ebitda_mev.map_or(AnyValue::Null, AnyValue::Float64),
            self.aliq_mat.map_or(AnyValue::Null, AnyValue::Float64),
            self.eq_dur.map_or(AnyValue::Null, AnyValue::Float64),
            self.beta_60m.map_or(AnyValue::Null, AnyValue::Float64),
            self.resff3_12_1.map_or(AnyValue::Null, AnyValue::Float64),
            self.resff3_6_1.map_or(AnyValue::Null, AnyValue::Float64),
            self.mispricing_mgmt
                .map_or(AnyValue::Null, AnyValue::Float64),
            self.mispricing_perf
                .map_or(AnyValue::Null, AnyValue::Float64),
            self.ivol_capm_21d.map_or(AnyValue::Null, AnyValue::Float64),
            self.iskew_capm_21d
                .map_or(AnyValue::Null, AnyValue::Float64),
            self.coskew_21d.map_or(AnyValue::Null, AnyValue::Float64),
            self.beta_dimson_21d
                .map_or(AnyValue::Null, AnyValue::Float64),
            self.ivol_ff3_21d.map_or(AnyValue::Null, AnyValue::Float64),
            self.iskew_ff3_21d.map_or(AnyValue::Null, AnyValue::Float64),
            self.ivol_hxz4_21d.map_or(AnyValue::Null, AnyValue::Float64),
            self.iskew_hxz4_21d
                .map_or(AnyValue::Null, AnyValue::Float64),
            self.rmax5_21d.map_or(AnyValue::Null, AnyValue::Float64),
            self.rmax1_21d.map_or(AnyValue::Null, AnyValue::Float64),
            self.rvol_21d.map_or(AnyValue::Null, AnyValue::Float64),
            self.rskew_21d.map_or(AnyValue::Null, AnyValue::Float64),
            self.zero_trades_21d
                .map_or(AnyValue::Null, AnyValue::Float64),
            self.dolvol_126d.map_or(AnyValue::Null, AnyValue::Float64),
            self.dolvol_var_126d
                .map_or(AnyValue::Null, AnyValue::Float64),
            self.turnover_126d.map_or(AnyValue::Null, AnyValue::Float64),
            self.turnover_var_126d
                .map_or(AnyValue::Null, AnyValue::Float64),
            self.zero_trades_126d
                .map_or(AnyValue::Null, AnyValue::Float64),
            self.zero_trades_252d
                .map_or(AnyValue::Null, AnyValue::Float64),
            self.ami_126d.map_or(AnyValue::Null, AnyValue::Float64),
            self.ivol_capm_252d
                .map_or(AnyValue::Null, AnyValue::Float64),
            self.prc_highprc_252d
                .map_or(AnyValue::Null, AnyValue::Float64),
            self.betadown_252d.map_or(AnyValue::Null, AnyValue::Float64),
            self.bidaskhl_21d.map_or(AnyValue::Null, AnyValue::Float64),
            self.corr_1260d.map_or(AnyValue::Null, AnyValue::Float64),
            self.betabab_1260d.map_or(AnyValue::Null, AnyValue::Float64),
            self.rmax5_rvol_21d
                .map_or(AnyValue::Null, AnyValue::Float64),
            self.age.map_or(AnyValue::Null, AnyValue::Float64),
            self.qmj.map_or(AnyValue::Null, AnyValue::Float64),
            self.qmj_prof.map_or(AnyValue::Null, AnyValue::Float64),
            self.qmj_growth.map_or(AnyValue::Null, AnyValue::Float64),
            self.qmj_safety.map_or(AnyValue::Null, AnyValue::Float64),
            self.enterprise_value
                .map_or(AnyValue::Null, AnyValue::Float64),
            self.book_equity.map_or(AnyValue::Null, AnyValue::Float64),
            self.assets.map_or(AnyValue::Null, AnyValue::Float64),
            self.sales.map_or(AnyValue::Null, AnyValue::Float64),
            self.net_income.map_or(AnyValue::Null, AnyValue::Float64),
            self.div1m_me.map_or(AnyValue::Null, AnyValue::Float64),
            self.div3m_me.map_or(AnyValue::Null, AnyValue::Float64),
            self.div6m_me.map_or(AnyValue::Null, AnyValue::Float64),
            self.divspc1m_me.map_or(AnyValue::Null, AnyValue::Float64),
            self.divspc12m_me.map_or(AnyValue::Null, AnyValue::Float64),
            self.chcsho_1m.map_or(AnyValue::Null, AnyValue::Float64),
            self.chcsho_3m.map_or(AnyValue::Null, AnyValue::Float64),
            self.chcsho_6m.map_or(AnyValue::Null, AnyValue::Float64),
            self.eqnpo_1m.map_or(AnyValue::Null, AnyValue::Float64),
            self.eqnpo_3m.map_or(AnyValue::Null, AnyValue::Float64),
            self.eqnpo_6m.map_or(AnyValue::Null, AnyValue::Float64),
            self.ret_2_0.map_or(AnyValue::Null, AnyValue::Float64),
            self.ret_3_0.map_or(AnyValue::Null, AnyValue::Float64),
            self.ret_6_0.map_or(AnyValue::Null, AnyValue::Float64),
            self.ret_9_0.map_or(AnyValue::Null, AnyValue::Float64),
            self.ret_12_0.map_or(AnyValue::Null, AnyValue::Float64),
            self.ret_18_1.map_or(AnyValue::Null, AnyValue::Float64),
            self.ret_24_1.map_or(AnyValue::Null, AnyValue::Float64),
            self.ret_24_12.map_or(AnyValue::Null, AnyValue::Float64),
            self.ret_36_1.map_or(AnyValue::Null, AnyValue::Float64),
            self.ret_36_12.map_or(AnyValue::Null, AnyValue::Float64),
            self.ret_48_12.map_or(AnyValue::Null, AnyValue::Float64),
            self.ret_48_1.map_or(AnyValue::Null, AnyValue::Float64),
            self.ret_60_1.map_or(AnyValue::Null, AnyValue::Float64),
            self.ret_60_36.map_or(AnyValue::Null, AnyValue::Float64),
            self.ca_gr1.map_or(AnyValue::Null, AnyValue::Float64),
            self.nca_gr1.map_or(AnyValue::Null, AnyValue::Float64),
            self.lt_gr1.map_or(AnyValue::Null, AnyValue::Float64),
            self.cl_gr1.map_or(AnyValue::Null, AnyValue::Float64),
            self.ncl_gr1.map_or(AnyValue::Null, AnyValue::Float64),
            self.be_gr1.map_or(AnyValue::Null, AnyValue::Float64),
            self.pstk_gr1.map_or(AnyValue::Null, AnyValue::Float64),
            self.debt_gr1.map_or(AnyValue::Null, AnyValue::Float64),
            self.cogs_gr1.map_or(AnyValue::Null, AnyValue::Float64),
            self.sga_gr1.map_or(AnyValue::Null, AnyValue::Float64),
            self.opex_gr1.map_or(AnyValue::Null, AnyValue::Float64),
            self.at_gr3.map_or(AnyValue::Null, AnyValue::Float64),
            self.ca_gr3.map_or(AnyValue::Null, AnyValue::Float64),
            self.nca_gr3.map_or(AnyValue::Null, AnyValue::Float64),
            self.lt_gr3.map_or(AnyValue::Null, AnyValue::Float64),
            self.cl_gr3.map_or(AnyValue::Null, AnyValue::Float64),
            self.ncl_gr3.map_or(AnyValue::Null, AnyValue::Float64),
            self.be_gr3.map_or(AnyValue::Null, AnyValue::Float64),
            self.pstk_gr3.map_or(AnyValue::Null, AnyValue::Float64),
            self.cogs_gr3.map_or(AnyValue::Null, AnyValue::Float64),
            self.sga_gr3.map_or(AnyValue::Null, AnyValue::Float64),
            self.opex_gr3.map_or(AnyValue::Null, AnyValue::Float64),
            self.cash_gr1a.map_or(AnyValue::Null, AnyValue::Float64),
            self.rec_gr1a.map_or(AnyValue::Null, AnyValue::Float64),
            self.ppeg_gr1a.map_or(AnyValue::Null, AnyValue::Float64),
            self.intan_gr1a.map_or(AnyValue::Null, AnyValue::Float64),
            self.debtst_gr1a.map_or(AnyValue::Null, AnyValue::Float64),
            self.ap_gr1a.map_or(AnyValue::Null, AnyValue::Float64),
            self.txp_gr1a.map_or(AnyValue::Null, AnyValue::Float64),
            self.debtlt_gr1a.map_or(AnyValue::Null, AnyValue::Float64),
            self.txditc_gr1a.map_or(AnyValue::Null, AnyValue::Float64),
            self.oa_gr1a.map_or(AnyValue::Null, AnyValue::Float64),
            self.ol_gr1a.map_or(AnyValue::Null, AnyValue::Float64),
            self.fna_gr1a.map_or(AnyValue::Null, AnyValue::Float64),
            self.gp_gr1a.map_or(AnyValue::Null, AnyValue::Float64),
            self.ebitda_gr1a.map_or(AnyValue::Null, AnyValue::Float64),
            self.ebit_gr1a.map_or(AnyValue::Null, AnyValue::Float64),
            self.ope_gr1a.map_or(AnyValue::Null, AnyValue::Float64),
            self.ni_gr1a.map_or(AnyValue::Null, AnyValue::Float64),
            self.nix_gr1a.map_or(AnyValue::Null, AnyValue::Float64),
            self.dp_gr1a.map_or(AnyValue::Null, AnyValue::Float64),
            self.fincf_gr1a.map_or(AnyValue::Null, AnyValue::Float64),
            self.ocf_gr1a.map_or(AnyValue::Null, AnyValue::Float64),
            self.fcf_gr1a.map_or(AnyValue::Null, AnyValue::Float64),
            self.nwc_gr1a.map_or(AnyValue::Null, AnyValue::Float64),
            self.eqnetis_gr1a.map_or(AnyValue::Null, AnyValue::Float64),
            self.dltnetis_gr1a.map_or(AnyValue::Null, AnyValue::Float64),
            self.dstnetis_gr1a.map_or(AnyValue::Null, AnyValue::Float64),
            self.dbnetis_gr1a.map_or(AnyValue::Null, AnyValue::Float64),
            self.netis_gr1a.map_or(AnyValue::Null, AnyValue::Float64),
            self.eqnpo_gr1a.map_or(AnyValue::Null, AnyValue::Float64),
            self.eqbb_gr1a.map_or(AnyValue::Null, AnyValue::Float64),
            self.eqis_gr1a.map_or(AnyValue::Null, AnyValue::Float64),
            self.div_gr1a.map_or(AnyValue::Null, AnyValue::Float64),
            self.eqpo_gr1a.map_or(AnyValue::Null, AnyValue::Float64),
            self.capx_gr1a.map_or(AnyValue::Null, AnyValue::Float64),
            self.cash_gr3a.map_or(AnyValue::Null, AnyValue::Float64),
            self.inv_gr3a.map_or(AnyValue::Null, AnyValue::Float64),
            self.rec_gr3a.map_or(AnyValue::Null, AnyValue::Float64),
            self.ppeg_gr3a.map_or(AnyValue::Null, AnyValue::Float64),
            self.lti_gr3a.map_or(AnyValue::Null, AnyValue::Float64),
            self.intan_gr3a.map_or(AnyValue::Null, AnyValue::Float64),
            self.debtst_gr3a.map_or(AnyValue::Null, AnyValue::Float64),
            self.ap_gr3a.map_or(AnyValue::Null, AnyValue::Float64),
            self.txp_gr3a.map_or(AnyValue::Null, AnyValue::Float64),
            self.debtlt_gr3a.map_or(AnyValue::Null, AnyValue::Float64),
            self.txditc_gr3a.map_or(AnyValue::Null, AnyValue::Float64),
            self.coa_gr3a.map_or(AnyValue::Null, AnyValue::Float64),
            self.col_gr3a.map_or(AnyValue::Null, AnyValue::Float64),
            self.cowc_gr3a.map_or(AnyValue::Null, AnyValue::Float64),
            self.ncoa_gr3a.map_or(AnyValue::Null, AnyValue::Float64),
            self.ncol_gr3a.map_or(AnyValue::Null, AnyValue::Float64),
            self.nncoa_gr3a.map_or(AnyValue::Null, AnyValue::Float64),
            self.oa_gr3a.map_or(AnyValue::Null, AnyValue::Float64),
            self.ol_gr3a.map_or(AnyValue::Null, AnyValue::Float64),
            self.fna_gr3a.map_or(AnyValue::Null, AnyValue::Float64),
            self.fnl_gr3a.map_or(AnyValue::Null, AnyValue::Float64),
            self.nfna_gr3a.map_or(AnyValue::Null, AnyValue::Float64),
            self.gp_gr3a.map_or(AnyValue::Null, AnyValue::Float64),
            self.ebitda_gr3a.map_or(AnyValue::Null, AnyValue::Float64),
            self.ebit_gr3a.map_or(AnyValue::Null, AnyValue::Float64),
            self.ope_gr3a.map_or(AnyValue::Null, AnyValue::Float64),
            self.ni_gr3a.map_or(AnyValue::Null, AnyValue::Float64),
            self.nix_gr3a.map_or(AnyValue::Null, AnyValue::Float64),
            self.dp_gr3a.map_or(AnyValue::Null, AnyValue::Float64),
            self.fincf_gr3a.map_or(AnyValue::Null, AnyValue::Float64),
            self.ocf_gr3a.map_or(AnyValue::Null, AnyValue::Float64),
            self.fcf_gr3a.map_or(AnyValue::Null, AnyValue::Float64),
            self.nwc_gr3a.map_or(AnyValue::Null, AnyValue::Float64),
            self.eqnetis_gr3a.map_or(AnyValue::Null, AnyValue::Float64),
            self.dltnetis_gr3a.map_or(AnyValue::Null, AnyValue::Float64),
            self.dstnetis_gr3a.map_or(AnyValue::Null, AnyValue::Float64),
            self.dbnetis_gr3a.map_or(AnyValue::Null, AnyValue::Float64),
            self.netis_gr3a.map_or(AnyValue::Null, AnyValue::Float64),
            self.eqnpo_gr3a.map_or(AnyValue::Null, AnyValue::Float64),
            self.tax_gr3a.map_or(AnyValue::Null, AnyValue::Float64),
            self.eqbb_gr3a.map_or(AnyValue::Null, AnyValue::Float64),
            self.eqis_gr3a.map_or(AnyValue::Null, AnyValue::Float64),
            self.div_gr3a.map_or(AnyValue::Null, AnyValue::Float64),
            self.eqpo_gr3a.map_or(AnyValue::Null, AnyValue::Float64),
            self.capx_gr3a.map_or(AnyValue::Null, AnyValue::Float64),
            self.capx_at.map_or(AnyValue::Null, AnyValue::Float64),
            self.rd_at.map_or(AnyValue::Null, AnyValue::Float64),
            self.spi_at.map_or(AnyValue::Null, AnyValue::Float64),
            self.xido_at.map_or(AnyValue::Null, AnyValue::Float64),
            self.nri_at.map_or(AnyValue::Null, AnyValue::Float64),
            self.gp_sale.map_or(AnyValue::Null, AnyValue::Float64),
            self.ebitda_sale.map_or(AnyValue::Null, AnyValue::Float64),
            self.pi_sale.map_or(AnyValue::Null, AnyValue::Float64),
            self.ni_sale.map_or(AnyValue::Null, AnyValue::Float64),
            self.nix_sale.map_or(AnyValue::Null, AnyValue::Float64),
            self.ocf_sale.map_or(AnyValue::Null, AnyValue::Float64),
            self.fcf_sale.map_or(AnyValue::Null, AnyValue::Float64),
            self.ebitda_at.map_or(AnyValue::Null, AnyValue::Float64),
            self.ebit_at.map_or(AnyValue::Null, AnyValue::Float64),
            self.fi_at.map_or(AnyValue::Null, AnyValue::Float64),
            self.ni_at.map_or(AnyValue::Null, AnyValue::Float64),
            self.nix_be.map_or(AnyValue::Null, AnyValue::Float64),
            self.ocf_be.map_or(AnyValue::Null, AnyValue::Float64),
            self.fcf_be.map_or(AnyValue::Null, AnyValue::Float64),
            self.gp_bev.map_or(AnyValue::Null, AnyValue::Float64),
            self.ebitda_bev.map_or(AnyValue::Null, AnyValue::Float64),
            self.fi_bev.map_or(AnyValue::Null, AnyValue::Float64),
            self.cop_bev.map_or(AnyValue::Null, AnyValue::Float64),
            self.gp_ppen.map_or(AnyValue::Null, AnyValue::Float64),
            self.ebitda_ppen.map_or(AnyValue::Null, AnyValue::Float64),
            self.fcf_ppen.map_or(AnyValue::Null, AnyValue::Float64),
            self.fincf_at.map_or(AnyValue::Null, AnyValue::Float64),
            self.eqis_at.map_or(AnyValue::Null, AnyValue::Float64),
            self.dltnetis_at.map_or(AnyValue::Null, AnyValue::Float64),
            self.dstnetis_at.map_or(AnyValue::Null, AnyValue::Float64),
            self.eqnpo_at.map_or(AnyValue::Null, AnyValue::Float64),
            self.eqbb_at.map_or(AnyValue::Null, AnyValue::Float64),
            self.div_at.map_or(AnyValue::Null, AnyValue::Float64),
            self.be_bev.map_or(AnyValue::Null, AnyValue::Float64),
            self.debt_bev.map_or(AnyValue::Null, AnyValue::Float64),
            self.cash_bev.map_or(AnyValue::Null, AnyValue::Float64),
            self.pstk_bev.map_or(AnyValue::Null, AnyValue::Float64),
            self.debtlt_bev.map_or(AnyValue::Null, AnyValue::Float64),
            self.debtst_bev.map_or(AnyValue::Null, AnyValue::Float64),
            self.int_debt.map_or(AnyValue::Null, AnyValue::Float64),
            self.int_debtlt.map_or(AnyValue::Null, AnyValue::Float64),
            self.ebitda_debt.map_or(AnyValue::Null, AnyValue::Float64),
            self.profit_cl.map_or(AnyValue::Null, AnyValue::Float64),
            self.ocf_cl.map_or(AnyValue::Null, AnyValue::Float64),
            self.ocf_debt.map_or(AnyValue::Null, AnyValue::Float64),
            self.cash_lt.map_or(AnyValue::Null, AnyValue::Float64),
            self.inv_act.map_or(AnyValue::Null, AnyValue::Float64),
            self.rec_act.map_or(AnyValue::Null, AnyValue::Float64),
            self.debtst_debt.map_or(AnyValue::Null, AnyValue::Float64),
            self.cl_lt.map_or(AnyValue::Null, AnyValue::Float64),
            self.debtlt_debt.map_or(AnyValue::Null, AnyValue::Float64),
            self.lt_ppen.map_or(AnyValue::Null, AnyValue::Float64),
            self.debtlt_be.map_or(AnyValue::Null, AnyValue::Float64),
            self.nwc_at.map_or(AnyValue::Null, AnyValue::Float64),
            self.fcf_ocf.map_or(AnyValue::Null, AnyValue::Float64),
            self.debt_at.map_or(AnyValue::Null, AnyValue::Float64),
            self.debt_be.map_or(AnyValue::Null, AnyValue::Float64),
            self.ebit_int.map_or(AnyValue::Null, AnyValue::Float64),
            self.inv_days.map_or(AnyValue::Null, AnyValue::Float64),
            self.rec_days.map_or(AnyValue::Null, AnyValue::Float64),
            self.ap_days.map_or(AnyValue::Null, AnyValue::Float64),
            self.cash_conversion
                .map_or(AnyValue::Null, AnyValue::Float64),
            self.cash_cl.map_or(AnyValue::Null, AnyValue::Float64),
            self.caliq_cl.map_or(AnyValue::Null, AnyValue::Float64),
            self.ca_cl.map_or(AnyValue::Null, AnyValue::Float64),
            self.inv_turnover.map_or(AnyValue::Null, AnyValue::Float64),
            self.rec_turnover.map_or(AnyValue::Null, AnyValue::Float64),
            self.ap_turnover.map_or(AnyValue::Null, AnyValue::Float64),
            self.adv_sale.map_or(AnyValue::Null, AnyValue::Float64),
            self.staff_sale.map_or(AnyValue::Null, AnyValue::Float64),
            self.sale_be.map_or(AnyValue::Null, AnyValue::Float64),
            self.div_ni.map_or(AnyValue::Null, AnyValue::Float64),
            self.sale_nwc.map_or(AnyValue::Null, AnyValue::Float64),
            self.tax_pi.map_or(AnyValue::Null, AnyValue::Float64),
            self.ni_emp.map_or(AnyValue::Null, AnyValue::Float64),
            self.sale_emp.map_or(AnyValue::Null, AnyValue::Float64),
            self.niq_saleq_std.map_or(AnyValue::Null, AnyValue::Float64),
            self.roeq_be_std.map_or(AnyValue::Null, AnyValue::Float64),
            self.roe_be_std.map_or(AnyValue::Null, AnyValue::Float64),
            self.intrinsic_value
                .map_or(AnyValue::Null, AnyValue::Float64),
            self.gpoa_ch5.map_or(AnyValue::Null, AnyValue::Float64),
            self.roe_ch5.map_or(AnyValue::Null, AnyValue::Float64),
            self.roa_ch5.map_or(AnyValue::Null, AnyValue::Float64),
            self.cfoa_ch5.map_or(AnyValue::Null, AnyValue::Float64),
            self.gmar_ch5.map_or(AnyValue::Null, AnyValue::Float64),
            self.cash_me.map_or(AnyValue::Null, AnyValue::Float64),
            self.gp_me.map_or(AnyValue::Null, AnyValue::Float64),
            self.ebitda_me.map_or(AnyValue::Null, AnyValue::Float64),
            self.ebit_me.map_or(AnyValue::Null, AnyValue::Float64),
            self.ope_me.map_or(AnyValue::Null, AnyValue::Float64),
            self.nix_me.map_or(AnyValue::Null, AnyValue::Float64),
            self.cop_me.map_or(AnyValue::Null, AnyValue::Float64),
            self.div_me.map_or(AnyValue::Null, AnyValue::Float64),
            self.eqbb_me.map_or(AnyValue::Null, AnyValue::Float64),
            self.eqis_me.map_or(AnyValue::Null, AnyValue::Float64),
            self.eqnetis_me.map_or(AnyValue::Null, AnyValue::Float64),
            self.at_mev.map_or(AnyValue::Null, AnyValue::Float64),
            self.ppen_mev.map_or(AnyValue::Null, AnyValue::Float64),
            self.be_mev.map_or(AnyValue::Null, AnyValue::Float64),
            self.cash_mev.map_or(AnyValue::Null, AnyValue::Float64),
            self.sale_mev.map_or(AnyValue::Null, AnyValue::Float64),
            self.gp_mev.map_or(AnyValue::Null, AnyValue::Float64),
            self.ebit_mev.map_or(AnyValue::Null, AnyValue::Float64),
            self.cop_mev.map_or(AnyValue::Null, AnyValue::Float64),
            self.ocf_mev.map_or(AnyValue::Null, AnyValue::Float64),
            self.fcf_mev.map_or(AnyValue::Null, AnyValue::Float64),
            self.debt_mev.map_or(AnyValue::Null, AnyValue::Float64),
            self.pstk_mev.map_or(AnyValue::Null, AnyValue::Float64),
            self.debtlt_mev.map_or(AnyValue::Null, AnyValue::Float64),
            self.debtst_mev.map_or(AnyValue::Null, AnyValue::Float64),
            self.dltnetis_mev.map_or(AnyValue::Null, AnyValue::Float64),
            self.dstnetis_mev.map_or(AnyValue::Null, AnyValue::Float64),
            self.dbnetis_mev.map_or(AnyValue::Null, AnyValue::Float64),
            self.netis_mev.map_or(AnyValue::Null, AnyValue::Float64),
            self.fincf_mev.map_or(AnyValue::Null, AnyValue::Float64),
            self.ivol_capm_60m.map_or(AnyValue::Null, AnyValue::Float64),
            self.beta_21d.map_or(AnyValue::Null, AnyValue::Float64),
            self.beta_252d.map_or(AnyValue::Null, AnyValue::Float64),
            self.rvol_252d.map_or(AnyValue::Null, AnyValue::Float64),
            self.rvolhl_21d.map_or(AnyValue::Null, AnyValue::Float64),
        ])
    }

    pub fn polars_schema() -> Schema {
        Schema::from_iter([
            // Core identifiers / dates
            Field::new("gvkey".into(), DataType::Int32),
            Field::new("iid".into(), DataType::String),
            Field::new("permno".into(), DataType::Int32),
            Field::new("permco".into(), DataType::Int32),
            Field::new("id".into(), DataType::Int64),
            Field::new("date".into(), DataType::Date),
            Field::new("eom".into(), DataType::Date),
            Field::new("excntry".into(), DataType::String),
            Field::new("size_grp".into(), DataType::String),
            // Common measures
            Field::new("me".into(), DataType::Float64),
            Field::new("ret_exc".into(), DataType::Float64),
            Field::new("ret".into(), DataType::Float64),
            Field::new("prc".into(), DataType::Float64),
            // Added fields
            Field::new("obs_main".into(), DataType::Float64),
            Field::new("exch_main".into(), DataType::Float64),
            Field::new("common".into(), DataType::Float64),
            Field::new("primary_sec".into(), DataType::Float64),
            Field::new("me_company".into(), DataType::Float64),
            Field::new("ret_exc_lead1m".into(), DataType::Float64),
            Field::new("ret_local".into(), DataType::Float64),
            Field::new("ret_lag_dif".into(), DataType::Float64),
            Field::new("prc_local".into(), DataType::Float64),
            Field::new("prc_high".into(), DataType::Float64),
            Field::new("prc_low".into(), DataType::Float64),
            Field::new("bidask".into(), DataType::Float64),
            Field::new("curcd".into(), DataType::String),
            Field::new("fx".into(), DataType::Float64),
            Field::new("gics".into(), DataType::Float64),
            Field::new("naics".into(), DataType::Float64),
            Field::new("sic".into(), DataType::Float64),
            Field::new("ff49".into(), DataType::Float64),
            Field::new("dolvol".into(), DataType::Float64),
            Field::new("shares".into(), DataType::Float64),
            Field::new("tvol".into(), DataType::Float64),
            Field::new("adjfct".into(), DataType::Float64),
            Field::new("comp_tpci".into(), DataType::String),
            Field::new("crsp_shrcd".into(), DataType::Float64),
            Field::new("comp_exchg".into(), DataType::Float64),
            Field::new("crsp_exchcd".into(), DataType::Float64),
            Field::new("source_crsp".into(), DataType::Float64),
            Field::new("market_equity".into(), DataType::Float64),
            Field::new("div12m_me".into(), DataType::Float64),
            Field::new("chcsho_12m".into(), DataType::Float64),
            Field::new("eqnpo_12m".into(), DataType::Float64),
            Field::new("ret_1_0".into(), DataType::Float64),
            Field::new("ret_3_1".into(), DataType::Float64),
            Field::new("ret_6_1".into(), DataType::Float64),
            Field::new("ret_9_1".into(), DataType::Float64),
            Field::new("ret_12_1".into(), DataType::Float64),
            Field::new("ret_12_7".into(), DataType::Float64),
            Field::new("ret_60_12".into(), DataType::Float64),
            Field::new("seas_1_1an".into(), DataType::Float64),
            Field::new("seas_1_1na".into(), DataType::Float64),
            Field::new("seas_2_5an".into(), DataType::Float64),
            Field::new("seas_2_5na".into(), DataType::Float64),
            Field::new("seas_6_10an".into(), DataType::Float64),
            Field::new("seas_6_10na".into(), DataType::Float64),
            Field::new("seas_11_15an".into(), DataType::Float64),
            Field::new("seas_11_15na".into(), DataType::Float64),
            Field::new("seas_16_20an".into(), DataType::Float64),
            Field::new("seas_16_20na".into(), DataType::Float64),
            Field::new("at_gr1".into(), DataType::Float64),
            Field::new("sale_gr1".into(), DataType::Float64),
            Field::new("capx_gr1".into(), DataType::Float64),
            Field::new("inv_gr1".into(), DataType::Float64),
            Field::new("debt_gr3".into(), DataType::Float64),
            Field::new("sale_gr3".into(), DataType::Float64),
            Field::new("capx_gr3".into(), DataType::Float64),
            Field::new("inv_gr1a".into(), DataType::Float64),
            Field::new("lti_gr1a".into(), DataType::Float64),
            Field::new("sti_gr1a".into(), DataType::Float64),
            Field::new("coa_gr1a".into(), DataType::Float64),
            Field::new("col_gr1a".into(), DataType::Float64),
            Field::new("cowc_gr1a".into(), DataType::Float64),
            Field::new("ncoa_gr1a".into(), DataType::Float64),
            Field::new("ncol_gr1a".into(), DataType::Float64),
            Field::new("nncoa_gr1a".into(), DataType::Float64),
            Field::new("fnl_gr1a".into(), DataType::Float64),
            Field::new("nfna_gr1a".into(), DataType::Float64),
            Field::new("tax_gr1a".into(), DataType::Float64),
            Field::new("be_gr1a".into(), DataType::Float64),
            Field::new("ebit_sale".into(), DataType::Float64),
            Field::new("gp_at".into(), DataType::Float64),
            Field::new("cop_at".into(), DataType::Float64),
            Field::new("ope_be".into(), DataType::Float64),
            Field::new("ni_be".into(), DataType::Float64),
            Field::new("ebit_bev".into(), DataType::Float64),
            Field::new("netis_at".into(), DataType::Float64),
            Field::new("eqnetis_at".into(), DataType::Float64),
            Field::new("dbnetis_at".into(), DataType::Float64),
            Field::new("oaccruals_at".into(), DataType::Float64),
            Field::new("oaccruals_ni".into(), DataType::Float64),
            Field::new("taccruals_at".into(), DataType::Float64),
            Field::new("taccruals_ni".into(), DataType::Float64),
            Field::new("noa_at".into(), DataType::Float64),
            Field::new("opex_at".into(), DataType::Float64),
            Field::new("at_turnover".into(), DataType::Float64),
            Field::new("sale_bev".into(), DataType::Float64),
            Field::new("rd_sale".into(), DataType::Float64),
            Field::new("cash_at".into(), DataType::Float64),
            Field::new("sale_emp_gr1".into(), DataType::Float64),
            Field::new("emp_gr1".into(), DataType::Float64),
            Field::new("ni_inc8q".into(), DataType::Float64),
            Field::new("noa_gr1a".into(), DataType::Float64),
            Field::new("ppeinv_gr1a".into(), DataType::Float64),
            Field::new("lnoa_gr1a".into(), DataType::Float64),
            Field::new("capx_gr2".into(), DataType::Float64),
            Field::new("saleq_gr1".into(), DataType::Float64),
            Field::new("niq_be".into(), DataType::Float64),
            Field::new("niq_at".into(), DataType::Float64),
            Field::new("niq_be_chg1".into(), DataType::Float64),
            Field::new("niq_at_chg1".into(), DataType::Float64),
            Field::new("rd5_at".into(), DataType::Float64),
            Field::new("dsale_dinv".into(), DataType::Float64),
            Field::new("dsale_drec".into(), DataType::Float64),
            Field::new("dgp_dsale".into(), DataType::Float64),
            Field::new("dsale_dsga".into(), DataType::Float64),
            Field::new("saleq_su".into(), DataType::Float64),
            Field::new("niq_su".into(), DataType::Float64),
            Field::new("capex_abn".into(), DataType::Float64),
            Field::new("op_atl1".into(), DataType::Float64),
            Field::new("gp_atl1".into(), DataType::Float64),
            Field::new("ope_bel1".into(), DataType::Float64),
            Field::new("cop_atl1".into(), DataType::Float64),
            Field::new("pi_nix".into(), DataType::Float64),
            Field::new("ocf_at".into(), DataType::Float64),
            Field::new("op_at".into(), DataType::Float64),
            Field::new("ocf_at_chg1".into(), DataType::Float64),
            Field::new("at_be".into(), DataType::Float64),
            Field::new("ocfq_saleq_std".into(), DataType::Float64),
            Field::new("tangibility".into(), DataType::Float64),
            Field::new("earnings_variability".into(), DataType::Float64),
            Field::new("aliq_at".into(), DataType::Float64),
            Field::new("f_score".into(), DataType::Float64),
            Field::new("o_score".into(), DataType::Float64),
            Field::new("z_score".into(), DataType::Float64),
            Field::new("kz_index".into(), DataType::Float64),
            Field::new("ni_ar1".into(), DataType::Float64),
            Field::new("ni_ivol".into(), DataType::Float64),
            Field::new("at_me".into(), DataType::Float64),
            Field::new("be_me".into(), DataType::Float64),
            Field::new("debt_me".into(), DataType::Float64),
            Field::new("netdebt_me".into(), DataType::Float64),
            Field::new("sale_me".into(), DataType::Float64),
            Field::new("ni_me".into(), DataType::Float64),
            Field::new("ocf_me".into(), DataType::Float64),
            Field::new("fcf_me".into(), DataType::Float64),
            Field::new("eqpo_me".into(), DataType::Float64),
            Field::new("eqnpo_me".into(), DataType::Float64),
            Field::new("rd_me".into(), DataType::Float64),
            Field::new("ival_me".into(), DataType::Float64),
            Field::new("bev_mev".into(), DataType::Float64),
            Field::new("ebitda_mev".into(), DataType::Float64),
            Field::new("aliq_mat".into(), DataType::Float64),
            Field::new("eq_dur".into(), DataType::Float64),
            Field::new("beta_60m".into(), DataType::Float64),
            Field::new("resff3_12_1".into(), DataType::Float64),
            Field::new("resff3_6_1".into(), DataType::Float64),
            Field::new("mispricing_mgmt".into(), DataType::Float64),
            Field::new("mispricing_perf".into(), DataType::Float64),
            Field::new("ivol_capm_21d".into(), DataType::Float64),
            Field::new("iskew_capm_21d".into(), DataType::Float64),
            Field::new("coskew_21d".into(), DataType::Float64),
            Field::new("beta_dimson_21d".into(), DataType::Float64),
            Field::new("ivol_ff3_21d".into(), DataType::Float64),
            Field::new("iskew_ff3_21d".into(), DataType::Float64),
            Field::new("ivol_hxz4_21d".into(), DataType::Float64),
            Field::new("iskew_hxz4_21d".into(), DataType::Float64),
            Field::new("rmax5_21d".into(), DataType::Float64),
            Field::new("rmax1_21d".into(), DataType::Float64),
            Field::new("rvol_21d".into(), DataType::Float64),
            Field::new("rskew_21d".into(), DataType::Float64),
            Field::new("zero_trades_21d".into(), DataType::Float64),
            Field::new("dolvol_126d".into(), DataType::Float64),
            Field::new("dolvol_var_126d".into(), DataType::Float64),
            Field::new("turnover_126d".into(), DataType::Float64),
            Field::new("turnover_var_126d".into(), DataType::Float64),
            Field::new("zero_trades_126d".into(), DataType::Float64),
            Field::new("zero_trades_252d".into(), DataType::Float64),
            Field::new("ami_126d".into(), DataType::Float64),
            Field::new("ivol_capm_252d".into(), DataType::Float64),
            Field::new("prc_highprc_252d".into(), DataType::Float64),
            Field::new("betadown_252d".into(), DataType::Float64),
            Field::new("bidaskhl_21d".into(), DataType::Float64),
            Field::new("corr_1260d".into(), DataType::Float64),
            Field::new("betabab_1260d".into(), DataType::Float64),
            Field::new("rmax5_rvol_21d".into(), DataType::Float64),
            Field::new("age".into(), DataType::Float64),
            Field::new("qmj".into(), DataType::Float64),
            Field::new("qmj_prof".into(), DataType::Float64),
            Field::new("qmj_growth".into(), DataType::Float64),
            Field::new("qmj_safety".into(), DataType::Float64),
            Field::new("enterprise_value".into(), DataType::Float64),
            Field::new("book_equity".into(), DataType::Float64),
            Field::new("assets".into(), DataType::Float64),
            Field::new("sales".into(), DataType::Float64),
            Field::new("net_income".into(), DataType::Float64),
            Field::new("div1m_me".into(), DataType::Float64),
            Field::new("div3m_me".into(), DataType::Float64),
            Field::new("div6m_me".into(), DataType::Float64),
            Field::new("divspc1m_me".into(), DataType::Float64),
            Field::new("divspc12m_me".into(), DataType::Float64),
            Field::new("chcsho_1m".into(), DataType::Float64),
            Field::new("chcsho_3m".into(), DataType::Float64),
            Field::new("chcsho_6m".into(), DataType::Float64),
            Field::new("eqnpo_1m".into(), DataType::Float64),
            Field::new("eqnpo_3m".into(), DataType::Float64),
            Field::new("eqnpo_6m".into(), DataType::Float64),
            Field::new("ret_2_0".into(), DataType::Float64),
            Field::new("ret_3_0".into(), DataType::Float64),
            Field::new("ret_6_0".into(), DataType::Float64),
            Field::new("ret_9_0".into(), DataType::Float64),
            Field::new("ret_12_0".into(), DataType::Float64),
            Field::new("ret_18_1".into(), DataType::Float64),
            Field::new("ret_24_1".into(), DataType::Float64),
            Field::new("ret_24_12".into(), DataType::Float64),
            Field::new("ret_36_1".into(), DataType::Float64),
            Field::new("ret_36_12".into(), DataType::Float64),
            Field::new("ret_48_12".into(), DataType::Float64),
            Field::new("ret_48_1".into(), DataType::Float64),
            Field::new("ret_60_1".into(), DataType::Float64),
            Field::new("ret_60_36".into(), DataType::Float64),
            Field::new("ca_gr1".into(), DataType::Float64),
            Field::new("nca_gr1".into(), DataType::Float64),
            Field::new("lt_gr1".into(), DataType::Float64),
            Field::new("cl_gr1".into(), DataType::Float64),
            Field::new("ncl_gr1".into(), DataType::Float64),
            Field::new("be_gr1".into(), DataType::Float64),
            Field::new("pstk_gr1".into(), DataType::Float64),
            Field::new("debt_gr1".into(), DataType::Float64),
            Field::new("cogs_gr1".into(), DataType::Float64),
            Field::new("sga_gr1".into(), DataType::Float64),
            Field::new("opex_gr1".into(), DataType::Float64),
            Field::new("at_gr3".into(), DataType::Float64),
            Field::new("ca_gr3".into(), DataType::Float64),
            Field::new("nca_gr3".into(), DataType::Float64),
            Field::new("lt_gr3".into(), DataType::Float64),
            Field::new("cl_gr3".into(), DataType::Float64),
            Field::new("ncl_gr3".into(), DataType::Float64),
            Field::new("be_gr3".into(), DataType::Float64),
            Field::new("pstk_gr3".into(), DataType::Float64),
            Field::new("cogs_gr3".into(), DataType::Float64),
            Field::new("sga_gr3".into(), DataType::Float64),
            Field::new("opex_gr3".into(), DataType::Float64),
            Field::new("cash_gr1a".into(), DataType::Float64),
            Field::new("rec_gr1a".into(), DataType::Float64),
            Field::new("ppeg_gr1a".into(), DataType::Float64),
            Field::new("intan_gr1a".into(), DataType::Float64),
            Field::new("debtst_gr1a".into(), DataType::Float64),
            Field::new("ap_gr1a".into(), DataType::Float64),
            Field::new("txp_gr1a".into(), DataType::Float64),
            Field::new("debtlt_gr1a".into(), DataType::Float64),
            Field::new("txditc_gr1a".into(), DataType::Float64),
            Field::new("oa_gr1a".into(), DataType::Float64),
            Field::new("ol_gr1a".into(), DataType::Float64),
            Field::new("fna_gr1a".into(), DataType::Float64),
            Field::new("gp_gr1a".into(), DataType::Float64),
            Field::new("ebitda_gr1a".into(), DataType::Float64),
            Field::new("ebit_gr1a".into(), DataType::Float64),
            Field::new("ope_gr1a".into(), DataType::Float64),
            Field::new("ni_gr1a".into(), DataType::Float64),
            Field::new("nix_gr1a".into(), DataType::Float64),
            Field::new("dp_gr1a".into(), DataType::Float64),
            Field::new("fincf_gr1a".into(), DataType::Float64),
            Field::new("ocf_gr1a".into(), DataType::Float64),
            Field::new("fcf_gr1a".into(), DataType::Float64),
            Field::new("nwc_gr1a".into(), DataType::Float64),
            Field::new("eqnetis_gr1a".into(), DataType::Float64),
            Field::new("dltnetis_gr1a".into(), DataType::Float64),
            Field::new("dstnetis_gr1a".into(), DataType::Float64),
            Field::new("dbnetis_gr1a".into(), DataType::Float64),
            Field::new("netis_gr1a".into(), DataType::Float64),
            Field::new("eqnpo_gr1a".into(), DataType::Float64),
            Field::new("eqbb_gr1a".into(), DataType::Float64),
            Field::new("eqis_gr1a".into(), DataType::Float64),
            Field::new("div_gr1a".into(), DataType::Float64),
            Field::new("eqpo_gr1a".into(), DataType::Float64),
            Field::new("capx_gr1a".into(), DataType::Float64),
            Field::new("cash_gr3a".into(), DataType::Float64),
            Field::new("inv_gr3a".into(), DataType::Float64),
            Field::new("rec_gr3a".into(), DataType::Float64),
            Field::new("ppeg_gr3a".into(), DataType::Float64),
            Field::new("lti_gr3a".into(), DataType::Float64),
            Field::new("intan_gr3a".into(), DataType::Float64),
            Field::new("debtst_gr3a".into(), DataType::Float64),
            Field::new("ap_gr3a".into(), DataType::Float64),
            Field::new("txp_gr3a".into(), DataType::Float64),
            Field::new("debtlt_gr3a".into(), DataType::Float64),
            Field::new("txditc_gr3a".into(), DataType::Float64),
            Field::new("coa_gr3a".into(), DataType::Float64),
            Field::new("col_gr3a".into(), DataType::Float64),
            Field::new("cowc_gr3a".into(), DataType::Float64),
            Field::new("ncoa_gr3a".into(), DataType::Float64),
            Field::new("ncol_gr3a".into(), DataType::Float64),
            Field::new("nncoa_gr3a".into(), DataType::Float64),
            Field::new("oa_gr3a".into(), DataType::Float64),
            Field::new("ol_gr3a".into(), DataType::Float64),
            Field::new("fna_gr3a".into(), DataType::Float64),
            Field::new("fnl_gr3a".into(), DataType::Float64),
            Field::new("nfna_gr3a".into(), DataType::Float64),
            Field::new("gp_gr3a".into(), DataType::Float64),
            Field::new("ebitda_gr3a".into(), DataType::Float64),
            Field::new("ebit_gr3a".into(), DataType::Float64),
            Field::new("ope_gr3a".into(), DataType::Float64),
            Field::new("ni_gr3a".into(), DataType::Float64),
            Field::new("nix_gr3a".into(), DataType::Float64),
            Field::new("dp_gr3a".into(), DataType::Float64),
            Field::new("fincf_gr3a".into(), DataType::Float64),
            Field::new("ocf_gr3a".into(), DataType::Float64),
            Field::new("fcf_gr3a".into(), DataType::Float64),
            Field::new("nwc_gr3a".into(), DataType::Float64),
            Field::new("eqnetis_gr3a".into(), DataType::Float64),
            Field::new("dltnetis_gr3a".into(), DataType::Float64),
            Field::new("dstnetis_gr3a".into(), DataType::Float64),
            Field::new("dbnetis_gr3a".into(), DataType::Float64),
            Field::new("netis_gr3a".into(), DataType::Float64),
            Field::new("eqnpo_gr3a".into(), DataType::Float64),
            Field::new("tax_gr3a".into(), DataType::Float64),
            Field::new("eqbb_gr3a".into(), DataType::Float64),
            Field::new("eqis_gr3a".into(), DataType::Float64),
            Field::new("div_gr3a".into(), DataType::Float64),
            Field::new("eqpo_gr3a".into(), DataType::Float64),
            Field::new("capx_gr3a".into(), DataType::Float64),
            Field::new("capx_at".into(), DataType::Float64),
            Field::new("rd_at".into(), DataType::Float64),
            Field::new("spi_at".into(), DataType::Float64),
            Field::new("xido_at".into(), DataType::Float64),
            Field::new("nri_at".into(), DataType::Float64),
            Field::new("gp_sale".into(), DataType::Float64),
            Field::new("ebitda_sale".into(), DataType::Float64),
            Field::new("pi_sale".into(), DataType::Float64),
            Field::new("ni_sale".into(), DataType::Float64),
            Field::new("nix_sale".into(), DataType::Float64),
            Field::new("ocf_sale".into(), DataType::Float64),
            Field::new("fcf_sale".into(), DataType::Float64),
            Field::new("ebitda_at".into(), DataType::Float64),
            Field::new("ebit_at".into(), DataType::Float64),
            Field::new("fi_at".into(), DataType::Float64),
            Field::new("ni_at".into(), DataType::Float64),
            Field::new("nix_be".into(), DataType::Float64),
            Field::new("ocf_be".into(), DataType::Float64),
            Field::new("fcf_be".into(), DataType::Float64),
            Field::new("gp_bev".into(), DataType::Float64),
            Field::new("ebitda_bev".into(), DataType::Float64),
            Field::new("fi_bev".into(), DataType::Float64),
            Field::new("cop_bev".into(), DataType::Float64),
            Field::new("gp_ppen".into(), DataType::Float64),
            Field::new("ebitda_ppen".into(), DataType::Float64),
            Field::new("fcf_ppen".into(), DataType::Float64),
            Field::new("fincf_at".into(), DataType::Float64),
            Field::new("eqis_at".into(), DataType::Float64),
            Field::new("dltnetis_at".into(), DataType::Float64),
            Field::new("dstnetis_at".into(), DataType::Float64),
            Field::new("eqnpo_at".into(), DataType::Float64),
            Field::new("eqbb_at".into(), DataType::Float64),
            Field::new("div_at".into(), DataType::Float64),
            Field::new("be_bev".into(), DataType::Float64),
            Field::new("debt_bev".into(), DataType::Float64),
            Field::new("cash_bev".into(), DataType::Float64),
            Field::new("pstk_bev".into(), DataType::Float64),
            Field::new("debtlt_bev".into(), DataType::Float64),
            Field::new("debtst_bev".into(), DataType::Float64),
            Field::new("int_debt".into(), DataType::Float64),
            Field::new("int_debtlt".into(), DataType::Float64),
            Field::new("ebitda_debt".into(), DataType::Float64),
            Field::new("profit_cl".into(), DataType::Float64),
            Field::new("ocf_cl".into(), DataType::Float64),
            Field::new("ocf_debt".into(), DataType::Float64),
            Field::new("cash_lt".into(), DataType::Float64),
            Field::new("inv_act".into(), DataType::Float64),
            Field::new("rec_act".into(), DataType::Float64),
            Field::new("debtst_debt".into(), DataType::Float64),
            Field::new("cl_lt".into(), DataType::Float64),
            Field::new("debtlt_debt".into(), DataType::Float64),
            Field::new("lt_ppen".into(), DataType::Float64),
            Field::new("debtlt_be".into(), DataType::Float64),
            Field::new("nwc_at".into(), DataType::Float64),
            Field::new("fcf_ocf".into(), DataType::Float64),
            Field::new("debt_at".into(), DataType::Float64),
            Field::new("debt_be".into(), DataType::Float64),
            Field::new("ebit_int".into(), DataType::Float64),
            Field::new("inv_days".into(), DataType::Float64),
            Field::new("rec_days".into(), DataType::Float64),
            Field::new("ap_days".into(), DataType::Float64),
            Field::new("cash_conversion".into(), DataType::Float64),
            Field::new("cash_cl".into(), DataType::Float64),
            Field::new("caliq_cl".into(), DataType::Float64),
            Field::new("ca_cl".into(), DataType::Float64),
            Field::new("inv_turnover".into(), DataType::Float64),
            Field::new("rec_turnover".into(), DataType::Float64),
            Field::new("ap_turnover".into(), DataType::Float64),
            Field::new("adv_sale".into(), DataType::Float64),
            Field::new("staff_sale".into(), DataType::Float64),
            Field::new("sale_be".into(), DataType::Float64),
            Field::new("div_ni".into(), DataType::Float64),
            Field::new("sale_nwc".into(), DataType::Float64),
            Field::new("tax_pi".into(), DataType::Float64),
            Field::new("ni_emp".into(), DataType::Float64),
            Field::new("sale_emp".into(), DataType::Float64),
            Field::new("niq_saleq_std".into(), DataType::Float64),
            Field::new("roeq_be_std".into(), DataType::Float64),
            Field::new("roe_be_std".into(), DataType::Float64),
            Field::new("intrinsic_value".into(), DataType::Float64),
            Field::new("gpoa_ch5".into(), DataType::Float64),
            Field::new("roe_ch5".into(), DataType::Float64),
            Field::new("roa_ch5".into(), DataType::Float64),
            Field::new("cfoa_ch5".into(), DataType::Float64),
            Field::new("gmar_ch5".into(), DataType::Float64),
            Field::new("cash_me".into(), DataType::Float64),
            Field::new("gp_me".into(), DataType::Float64),
            Field::new("ebitda_me".into(), DataType::Float64),
            Field::new("ebit_me".into(), DataType::Float64),
            Field::new("ope_me".into(), DataType::Float64),
            Field::new("nix_me".into(), DataType::Float64),
            Field::new("cop_me".into(), DataType::Float64),
            Field::new("div_me".into(), DataType::Float64),
            Field::new("eqbb_me".into(), DataType::Float64),
            Field::new("eqis_me".into(), DataType::Float64),
            Field::new("eqnetis_me".into(), DataType::Float64),
            Field::new("at_mev".into(), DataType::Float64),
            Field::new("ppen_mev".into(), DataType::Float64),
            Field::new("be_mev".into(), DataType::Float64),
            Field::new("cash_mev".into(), DataType::Float64),
            Field::new("sale_mev".into(), DataType::Float64),
            Field::new("gp_mev".into(), DataType::Float64),
            Field::new("ebit_mev".into(), DataType::Float64),
            Field::new("cop_mev".into(), DataType::Float64),
            Field::new("ocf_mev".into(), DataType::Float64),
            Field::new("fcf_mev".into(), DataType::Float64),
            Field::new("debt_mev".into(), DataType::Float64),
            Field::new("pstk_mev".into(), DataType::Float64),
            Field::new("debtlt_mev".into(), DataType::Float64),
            Field::new("debtst_mev".into(), DataType::Float64),
            Field::new("dltnetis_mev".into(), DataType::Float64),
            Field::new("dstnetis_mev".into(), DataType::Float64),
            Field::new("dbnetis_mev".into(), DataType::Float64),
            Field::new("netis_mev".into(), DataType::Float64),
            Field::new("fincf_mev".into(), DataType::Float64),
            Field::new("ivol_capm_60m".into(), DataType::Float64),
            Field::new("beta_21d".into(), DataType::Float64),
            Field::new("beta_252d".into(), DataType::Float64),
            Field::new("rvol_252d".into(), DataType::Float64),
            Field::new("rvolhl_21d".into(), DataType::Float64),
        ])
    }
}

impl ToPolars for EquityFactorsMonthly {
    fn schema() -> Schema {
        Self::polars_schema()
    }
}
