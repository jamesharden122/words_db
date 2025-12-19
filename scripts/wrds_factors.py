import os
import io
import zipfile
from requests import post as requests_post, get as requests_get
from datetime import date 
import csv
import itertools
WRDS_BASE = "https://wrds-api.wharton.upenn.edu/data/"
WRDS_TOKEN = "275be14d2410414f1de31176d40e7da3352ab114" # or set env var
HEADERS = {
    "Authorization": f"Token {WRDS_TOKEN}",
    "Accept": "application/json",
    "Accept-Encoding": "gzip",
}

OUT_ROOT   = "data/zip/factors/us/"
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
                    resp = requests_get(next_url, headers=headers, params=params if first else None, timeout=3000)
                    first = False
                    data = resp.json()
                    for row in data.get("results", []):
                        w.writerow({k: row.get(k) for k in fields})
                    next_url = data.get("next")


def daily_factors():
    fields_dsf = [
        "cma","date","hml","mktrf","month","rf","rmw","smb","umd"
    ]    
    time_frame = [date(y, 1, 1).strftime("%Y-%m-%d") for y in range(1963, 2025, 10)]
    filters = []
    for i in range(1,len(time_frame)):
        filters.append(build_filters(date__gte=time_frame[i-1], date__lte=time_frame[i]))
    filters.append(build_filters(date__gte="2023-01-01", date__lte="2025-10-31"))
    for flt in filters:
        params = {'filters': flt, 'limit': 100000}
        print(params)
        write_all_pages_to_zip(
            WRDS_BASE+"ff.fivefactors_daily/",
            headers=HEADERS,
            params=params,
            zip_path="../../"+OUT_ROOT+"daily/"+flt+"_ff_factors_daily.zip",
            zip_member_csv_name = flt+"_ff_factors_daily.csv",
            fields=fields_dsf
        )

def monthly_factors():
    fields_dsf = [
        "cma","date","dateff","hml","mktrf","month","rf","rmw","smb","umd","year"
    ]    
    time_frame = [date(y, 1, 1).strftime("%Y-%m-%d") for y in range(1963, 2025, 10)]
    filters = []
    for i in range(1,len(time_frame)):
        filters.append(build_filters(date__gte=time_frame[i-1], date__lte=time_frame[i]))
    filters.append(build_filters(date__gte="2023-01-01", date__lte="2025-10-31"))
    for flt in filters:
        params = {'filters': flt, 'limit': 100000}
        print(params)
        write_all_pages_to_zip(
            WRDS_BASE+"ff.fivefactors_monthly/",
            headers=HEADERS,
            params=params,
            zip_path="../../"+OUT_ROOT+"monthly/"+flt+"_ff_factors_monthly.zip",
            zip_member_csv_name = flt+"_ff_factors_monthly.csv",
            fields=fields_dsf
        )
 



if __name__ == "__main__":
    #_ = daily_factors()
    _ = monthly_factors()

