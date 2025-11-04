import os
import io
import zipfile
from requests import post as requests_post, get as requests_get
from datetime import date 
import csv
WRDS_BASE = "https://wrds-api.wharton.upenn.edu/data/"
WRDS_TOKEN = "TOKEN" # or set env var
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
                    resp = requests_get(next_url, headers=headers, params=params if first else None, timeout=1000)
                    first = False
                    data = resp.json()
                    for row in data.get("results", []):
                        w.writerow({k: row.get(k) for k in fields})
                    next_url = data.get("next")


def crsp_daily_securities():
    fields_dsf = [
        "permno", "permco", "cusip", "issuno", "date", "hexcd", "hsiccd",
        "prc", "openprc", "bid", "ask", "bidlo", "askhi", "ret", "retx",
        "vol", "shrout", "numtrd", "cfacpr", "cfacshr",
    ]    
    time_frame = [date(y, 1, 1).strftime("%Y-%m-%d") for y in range(1981, 2025)]
    filters = []
    for i in range(1,len(time_frame)):
        filters.append(build_filters(date__gte=time_frame[i-1], date__lte=time_frame[i]))
    #filters.append(build_filters(date__gte="2024-01-01", date__lte="2024-12-31"))
    for flt in filters:
        params = {'filters': flt, 'limit': 99999999}
        print(params)
        write_all_pages_to_zip(
            WRDS_BASE+"crsp.dsf/",
            headers=HEADERS,
            params=params,
            zip_path="../../"+OUT_ROOT+"stkdlysecuritydata/"+flt+"_crsp_daily.zip",
            zip_member_csv_name = flt+"_crsp_daily.csv",
            fields=fields_dsf
        )

def csrp_indexes(): 
        field_indexes = [
            "date", "ewretd", "ewretx", "spindx", "sprtrn", "totcnt",
            "totval", "usdcnt", "usdval",  "vwretd", "vwretx"
        ]
        params = {'filters': build_filters(date__gte="1925-12-31", date__lte="2024-12-31"), 'limit': 99999999}
        print(params)
        write_all_pages_to_zip(
            WRDS_BASE+"crsp.dsi/",
            headers=HEADERS,
            params=params,
            zip_path="../../"+OUT_ROOT+"market_index/"+"market_indexes_daily.zip",
            zip_member_csv_name = "market_indexes_daily.csv",
            fields=field_indexes
        )

def compustat_global_indexes(): 
    fields_g_secd = [
        # Identifiers / security metadata
        "permno", "permco", "gvkey", "iid", "cusip", "isin", "sedol", "issuno","conm", "tpci", "secstat", "epf",
        # Country / currency / exchange / industry
        "fic", "loc", "curcdd", "curcddv", "exchg", "hexcd", "gind", "gsubind",
        # Dates / period flags
        "date", "datadate", "anncdate", "recorddate", "paydate", "cheqvpaydate", "divdpaydate", "divrcpaydate", "divsppaydate", "divdtm", "cheqvtm", "divsptm", "monthend",
        # Prices / factors
        "prc", "openprc", "bid", "ask", "bidlo", "askhi", "prccd", "prchd", "prcld", "prcod", "prcstd", "cfacpr", "cfacshr", "ajexdi",
        # Returns
        "ret", "retx", "trfd",
        # Volume / shares / trading
        "vol", "shrout", "numtrd", "cshoc", "cshtrd", "qunit",
        # Cash equivalents (cheqv*) and dividends / splits
        "cheqv", "cheqvgross", "cheqvnet","div", "divd", "divdgross", "divdnet", "divgross", "divnet", "divrc", "divrcgross", "divrcnet", "divsp", "divspgross", "divspnet", "split", "splitf",
    ]
 
    time_frame = [date(y, 1, 1).strftime("%Y-%m-%d") for y in range(1985, 2024)]
    filters = []
    filters.append(build_filters(datadate__gte=date(1984, 1, 2).strftime("%Y-%m-%d"), datadate__lte=date(1985, 1, 1).strftime("%Y-%m-%d")))
    for i in range(1,len(time_frame)):
        filters.append(build_filters(datadate__gte=time_frame[i-1], datadate__lte=time_frame[i]))
    filters.append(build_filters(datadate__gte=date(2024, 1, 2).strftime("%Y-%m-%d"), datadate__lte=date(2025, 11, 2).strftime("%Y-%m-%d")))
    for flt in filters:
        params = {'filters': flt, 'limit': 99999999}
        print(params)
        write_all_pages_to_zip(
            WRDS_BASE+"comp.g_secd/",
            headers=HEADERS,
            params=params,
            zip_path="../../"+COMP_ROOT+"global_securities/"+flt+"_comp_global_daily.zip",
            zip_member_csv_name = flt+"_comp_global_daily.csv",
            fields=fields_g_secd
        )
 



if __name__ == "__main__":
    #_ = crsp_daily_securities()
    #_ = csrp_indexes()
    _ = compustat_global_indexes()
    
