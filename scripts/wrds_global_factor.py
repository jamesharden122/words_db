import os
import io
import zipfile
from requests import post as requests_post, get as requests_get
from datetime import date 
import csv
WRDS_BASE = "https://wrds-api.wharton.upenn.edu/data/"
WRDS_TOKEN = "275be14d2410414f1de31176d40e7da3352ab114" # or set env var
HEADERS = {
    "Authorization": f"Token {WRDS_TOKEN}",
    "Accept": "application/json",
    "Accept-Encoding": "gzip",
}

OUT_ROOT   = "data/zip/crsp_ciz_sample/"
COMP_ROOT   = "data/zip/compustat/"
YEAR_FROM  = 1925
YEAR_TO    = date.today().year


def ymd(d): return d.strftime("%Y-%m-%d")

def build_filters(**kv):
    parts=[]
    for k,v in kv.items():
        if v is None: 
            continue
        if k.endswith("__in") and not isinstance(v, str):
            v=",".join(str(x) for x in v)
        parts.append(f"{k}={v}")
    return "(" + "&".join(parts) + ")"

def write_all_pages_to_zip(url, headers, params, zip_path, zip_member_csv_name, fields):
    os.makedirs(os.path.dirname(zip_path), exist_ok=True)
    with zipfile.ZipFile(zip_path, mode="w", compression=zipfile.ZIP_DEFLATED) as zf:
        with zf.open(zip_member_csv_name, mode="w") as zipped_fp:
            with io.TextIOWrapper(zipped_fp, encoding="utf-8", newline="") as f:
                w = csv.DictWriter(f, fieldnames=fields)
                w.writeheader()
                next_url, first = url, True
                while next_url:
                    resp = requests_get(next_url, headers=headers, params=params if first else None, timeout=1500)
                    first = False
                    data = resp.json()
                    for row in data.get("results", []):
                        w.writerow({k: row.get(k) for k in fields})
                    next_url = data.get("next")



def compustat_global_factors(): 
    fields_global_factors_daily = [
    # --- Identifiers / labels ---
    "id", "gvkey", "iid", "permno", "permco", "comp_tpci", "conm",
    "size_grp", "primary_sec",

    # --- Dates ---
    "date", "eom", "datadate", "anncdate", "recorddate", "paydate",

    # --- Country / currency / exchange / industry ---
    "fic", "loc", "curcd", "excntry", "exchg", "exch_main", "crsp_exchcd", "crsp_shrcd",
    "hexcd", "sic", "naics", "gics", "gind", "gsubind", "ff49",

    # --- Prices (levels) ---
    "prc", "prc_local", "prc_high", "prc_low", "prc_highprc_252d",

    # --- Returns (various horizons) ---
    "ret", "ret_exc", "ret_exc_lead1m", "ret_local",
    "ret_1_0", "ret_2_0", "ret_3_0", "ret_3_1",
    "ret_6_0", "ret_6_1", "ret_9_0", "ret_9_1",
    "ret_12_0", "ret_12_1", "ret_12_7",
    "ret_18_1", "ret_24_1", "ret_24_12",
    "ret_36_1", "ret_36_12", "ret_48_1", "ret_48_12",
    "ret_60_1", "ret_60_12", "ret_60_36", "ret_lag_dif",
    "resff3_6_1", "resff3_12_1",

    # --- Volume / liquidity / trading activity ---
    "vol", "tvol", "dolvol", "dolvol_126d", "dolvol_var_126d",
    "turnover_126d", "turnover_var_126d",
    "zero_trades_21d", "zero_trades_126d", "zero_trades_252d",
    "bidask", "bidaskhl_21d",

    # --- Market cap / enterprise / shares ---
    "me", "me_company", "market_equity", "enterprise_value", "shares",
    "common", "primary_sec",

    # --- Cash equivalents / dividends / splits / adjustments ---
    "adjfct",
    "cheqv", "cheqvgross", "cheqvnet", "cheqvpaydate", "cheqvtm",
    "div", "divd", "divdgross", "divdnet", "divdpaydate", "divdtm",
    "divgross", "divnet",
    "divrc", "divrcgross", "divrcnet", "divrcpaydate",
    "divsp", "divspgross", "divspnet", "divsppaydate", "divsptm",
    "div12m_me", "div6m_me", "div3m_me", "div1m_me", "div_at", "div_me", "div_ni",
    "divspc12m_me", "divspc6m_me", "divspc3m_me", "divspc1m_me",  # some may be absent; safe to include
    "split", "splitf",

    # --- Balance sheet / income statement core levels ---
    "assets", "sales", "book_equity", "net_income", "enterprise_value",
    "market_equity", "shares",

    # --- Ratios and scaled fundamentals (tons of factors) ---
    "age",
    "aliq_at", "aliq_mat",
    "ami_126d",
    "ap_days", "ap_gr1a", "ap_gr3a", "ap_turnover",
    "at_be", "at_gr1", "at_gr3", "at_me", "at_mev", "at_turnover",
    "be_bev", "be_gr1", "be_gr1a", "be_gr3", "be_me", "be_mev",
    "bev_mev",
    "book_equity",
    "ca_cl", "ca_gr1", "ca_gr3",
    "caliq_cl",
    "capex_abn", "capx_at", "capx_gr1", "capx_gr1a", "capx_gr2", "capx_gr3", "capx_gr3a",
    "cash_at", "cash_bev", "cash_cl", "cash_conversion", "cash_gr1a", "cash_gr3a",
    "cash_lt", "cash_me", "cash_mev",
    "cfoa_ch5",
    "chcsho_12m", "chcsho_6m", "chcsho_3m", "chcsho_1m",
    "cl_gr1", "cl_gr3", "cl_lt",
    "coa_gr1a", "coa_gr3a",
    "cogs_gr1", "cogs_gr3",
    "col_gr1a", "col_gr3a",
    "comp_exchg", "comp_tpci",
    "cop_at", "cop_atl1", "cop_bev", "cop_me", "cop_mev",
    "corr_1260d",
    "coskew_21d",
    "cowc_gr1a", "cowc_gr3a",
    "dbnetis_at", "dbnetis_gr1a", "dbnetis_gr3a", "dbnetis_mev",
    "debt_at", "debt_be", "debt_bev", "debt_gr1", "debt_gr3",
    "debt_me", "debt_mev",
    "debtlt_be", "debtlt_bev", "debtlt_debt", "debtlt_gr1a", "debtlt_gr3a", "debtlt_mev",
    "debtst_bev", "debtst_debt", "debtst_gr1a", "debtst_gr3a", "debtst_mev",
    "dgp_dsale",
    "dltnetis_at", "dltnetis_gr1a", "dltnetis_gr3a", "dltnetis_mev",
    "dp_gr1a", "dp_gr3a",
    "dsale_dinv", "dsale_drec", "dsale_dsga",
    "dstnetis_at", "dstnetis_gr1a", "dstnetis_gr3a", "dstnetis_mev",
    "earnings_variability",
    "ebit_at", "ebit_bev", "ebit_gr1a", "ebit_gr3a", "ebit_int", "ebit_me", "ebit_mev",
    "ebit_sale",
    "ebitda_at", "ebitda_bev", "ebitda_debt", "ebitda_gr1a", "ebitda_gr3a", "ebitda_me",
    "ebitda_mev", "ebitda_ppen", "ebitda_sale",
    "emp_gr1",
    "eq_dur",
    "eqbb_at", "eqbb_gr1a", "eqbb_gr3a", "eqbb_me",
    "eqis_at", "eqis_gr1a", "eqis_gr3a", "eqis_me",
    "eqnetis_at", "eqnetis_gr1a", "eqnetis_gr3a", "eqnetis_me",
    "eqnpo_at", "eqnpo_gr1a", "eqnpo_gr3a", "eqnpo_me",
    "eqpo_gr1a", "eqpo_gr3a", "eqpo_me",
    "f_score",
    "fcf_be", "fcf_gr1a", "fcf_gr3a", "fcf_me", "fcf_mev", "fcf_ocf", "fcf_ppen", "fcf_sale",
    "fi_at", "fi_bev",
    "fincf_at", "fincf_gr1a", "fincf_gr3a", "fincf_mev",
    "fna_gr1a", "fna_gr3a",
    "fnl_gr1a", "fnl_gr3a",
    "fx",
    "gmar_ch5",
    "gp_at", "gp_atl1", "gp_bev", "gp_gr1a", "gp_gr3a", "gp_me", "gp_mev", "gp_ppen", "gp_sale",
    "gpoa_ch5",
    "id",
    "int_debt", "int_debtlt",
    "intan_gr1a", "intan_gr3a",
    "intrinsic_value",
    "inv_act", "inv_days", "inv_gr1", "inv_gr1a", "inv_gr3a", "inv_turnover",
    "iskew_capm_21d", "iskew_ff3_21d", "iskew_hxz4_21d",
    "ival_me",
    "ivol_capm_21d", "ivol_capm_252d", "ivol_capm_60m", "ivol_ff3_21d", "ivol_hxz4_21d",
    "kz_index",
    "lnoa_gr1a",
    "lt_gr1", "lt_gr3", "lt_ppen",
    "lti_gr1a", "lti_gr3a",
    "mispricing_mgmt", "mispricing_perf",
    "nca_gr1", "nca_gr3",
    "ncl_gr1", "ncl_gr3",
    "ncoa_gr1a", "ncoa_gr3a",
    "ncol_gr1a", "ncol_gr3a",
    "netdebt_me",
    "netis_at", "netis_gr1a", "netis_gr3a", "netis_mev",
    "nfna_gr1a", "nfna_gr3a",
    "ni_ar1", "ni_at", "ni_be", "ni_emp", "ni_gr1a", "ni_gr3a",
    "ni_inc8q", "ni_ivol", "ni_me", "ni_sale",
    "niq_at", "niq_at_chg1", "niq_be", "niq_be_chg1", "niq_saleq_std", "niq_su",
    "nix_be", "nix_gr1a", "nix_gr3a", "nix_me", "nix_sale",
    "nncoa_gr1a", "nncoa_gr3a",
    "noa_at", "noa_gr1a",
    "nri_at",
    "nwc_at", "nwc_gr1a", "nwc_gr3a",
    "o_score",
    "oa_gr1a", "oa_gr3a",
    "oaccruals_at", "oaccruals_ni",
    "obs_main",
    "ocf_at", "ocf_at_chg1", "ocf_be", "ocf_cl", "ocf_debt", "ocf_gr1a", "ocf_gr3a",
    "ocf_me", "ocf_mev", "ocf_sale",
    "ocfq_saleq_std",
    "ol_gr1a", "ol_gr3a",
    "op_at", "op_atl1", "ope_be", "ope_bel1", "ope_gr1a", "ope_gr3a", "ope_me",
    "opex_at", "opex_gr1", "opex_gr3",
    "pi_nix", "pi_sale",
    "ppeg_gr1a", "ppeg_gr3a",
    "ppeinv_gr1a",
    "ppen_mev",
    "profit_cl",
    "pstk_bev", "pstk_gr1", "pstk_gr3", "pstk_mev",
    "qmj", "qmj_growth", "qmj_prof", "qmj_safety",
    "rd5_at", "rd_at", "rd_me", "rd_sale",
    "rec_act", "rec_days", "rec_gr1a", "rec_gr3a", "rec_turnover",
    "rmax1_21d", "rmax5_21d", "rmax5_rvol_21d",
    "roa_ch5", "roe_be_std", "roe_ch5", "roeq_be_std",
    "rskew_21d", "rvol_21d", "rvol_252d", "rvolhl_21d",
    "sale_be", "sale_bev", "sale_emp", "sale_emp_gr1", "sale_gr1", "sale_gr3",
    "sale_me", "sale_mev", "sale_nwc",
    "saleq_gr1", "saleq_su",
    "seas_1_1an", "seas_1_1na",
    "seas_2_5an", "seas_2_5na",
    "seas_6_10an", "seas_6_10na",
    "seas_11_15an", "seas_11_15na",
    "seas_16_20an", "seas_16_20na",
    "sga_gr1", "sga_gr3",
    "source_crsp",
    "spi_at",
    "staff_sale",
    "sti_gr1a",
    "taccruals_at", "taccruals_ni",
    "tangibility",
    "tax_gr1a", "tax_gr3a", "tax_pi",
    "txditc_gr1a", "txditc_gr3a",
    "txp_gr1a", "txp_gr3a",
    "xido_at",
    "z_score",
]
 
    time_frame = [date(y, 1, 1).strftime("%Y-%m-%d") for y in range(1996, 2024)]
    filters = []
    #filters.append(build_filters(datadate__gte=date(1984, 1, 2).strftime("%Y-%m-%d"), datadate__lte=date(1985, 1, 1).strftime("%Y-%m-%d")))
    for i in range(1,len(time_frame)):
        filters.append(build_filters(datadate__gte=time_frame[i-1], datadate__lte=time_frame[i]))
    #filters.append(build_filters(datadate__gte=date(2024, 1, 2).strftime("%Y-%m-%d"), datadate__lte=date(2025, 11, 2).strftime("%Y-%m-%d")))
    for flt in filters:
        params = {'filters': flt, 'limit': 99999999}
        print(params)
        write_all_pages_to_zip(
            WRDS_BASE+"comp.g_secd/",
            headers=HEADERS,
            params=params,
            zip_path="../../"+COMP_ROOT+"global_securities/"+flt+"_comp_global_daily.zip",
            zip_member_csv_name = flt+"_comp_global_daily.csv",
            fields=fields_global_factors_daily 
            )
 



if __name__ == "__main__":
    #_ = crsp_daily_securities()
    #_ = csrp_indexes()
    _ = compustat_global_indexes()
    
