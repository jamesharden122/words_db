import os
import io
import zipfile
from requests import post as requests_post, get as requests_get
from datetime import date 
import csv
import json
import calendar
WRDS_BASE = "https://wrds-api.wharton.upenn.edu/data/"
WRDS_TOKEN = "275be14d2410414f1de31176d40e7da3352ab114" # keep user token
HEADERS = {
    "Authorization": f"Token {WRDS_TOKEN}",
    "Accept": "application/json",
    "Accept-Encoding": "gzip",
}

OUT_ROOT   = "data/zip/crsp_ciz_sample/"
COMP_ROOT   = "data/zip/compustat/"
# Absolute output directory for g_fundq zips as requested
G_FUNDQ_OUT_DIR = "/home/yakaman/Dropbox/Desktop/tesero-sol/software_development/trading/data/zip/compustat/global_fundamentals"
# Output directory for comp.g_names
G_NAMES_OUT_DIR = "/home/yakaman/Dropbox/Desktop/tesero-sol/software_development/trading/data/zip/compustat/global_names"
EXECUCOMP_OUT_DIR = "/home/yakaman/Dropbox/Desktop/tesero-sol/software_development/trading/data/zip/compustat/execucomp"
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


def discover_fields(url, headers, params=None):
    """Discover field names by querying a single row from the endpoint."""
    probe_params = dict(params or {})
    probe_params["limit"] = 1
    resp = requests_get(url, headers=headers, params=probe_params, timeout=1500)
    resp.raise_for_status()
    data = resp.json()
    results = data.get("results", [])
    if not results:
        raise RuntimeError("No results returned for field discovery; adjust filters or timeframe.")
    # Use keys from the first result
    return list(results[0].keys())


def load_fields_g_fundq(preferred_path: str = None):
    """Load g_fundq fields from a user-provided file, else return None.

    Supported formats:
    - JSON file containing a list of strings
    - Text file with one field per line
    """
    candidates = []
    if preferred_path:
        candidates.append(preferred_path)
    env_path = os.environ.get("G_FUNDQ_FIELDS_PATH")
    if env_path:
        candidates.append(env_path)
    here = os.path.dirname(__file__)
    candidates.append(os.path.join(here, "fields_g_fund_q.json"))
    candidates.append(os.path.join(here, "fields_g_fund_q.txt"))

    for p in candidates:
        if not p:
            continue
        if os.path.exists(p):
            try:
                if p.endswith(".json"):
                    with open(p, "r", encoding="utf-8") as f:
                        data = json.load(f)
                        if isinstance(data, list) and all(isinstance(x, str) for x in data):
                            return data
                else:
                    with open(p, "r", encoding="utf-8") as f:
                        fields = [line.strip() for line in f if line.strip()]
                        if fields:
                            return fields
            except Exception:
                pass
    return None





def compustat_global_fundq():
    """Fetch Compustat Global quarterly fundamentals (comp.g_fundq) with annual datadate filters.

    Writes one ZIP per annual window, each containing a CSV of all pages for that window.
    """
    url = WRDS_BASE + "comp.g_fundq/"

    # Build annual windows from 1987-06 to 2025-11 using the same inclusive style
    start = date(1987, 6, 1)
    # inclusive end-of-month for 2025-11
    end   = date(2025, 11, calendar.monthrange(2025, 11)[1])

    # Create boundary list: start, start+1y, ..., end
    bounds = []
    cur = start
    while cur < end:
        bounds.append(cur.strftime("%Y-%m-%d"))
        # advance 1 year keeping month/day
        cur = cur.replace(year=cur.year + 1)
    bounds.append(end.strftime("%Y-%m-%d"))

    filters = []
    for i in range(1, len(bounds)):
        filters.append(build_filters(datadate__gte=bounds[i-1], datadate__lte=bounds[i]))

    # Load user-provided fields list; if missing, discover once for a stable header
    fields_g_fund_q = load_fields_g_fundq()
    if not fields_g_fund_q and filters:
        probe_params = {'filters': filters[0], 'limit': 1}
        fields_g_fund_q = discover_fields(url, HEADERS, probe_params)

    for flt in filters:
        params = {'filters': flt, 'limit': 99999999}
        print(params)
        # Ensure target dir exists and write
        target_dir = G_FUNDQ_OUT_DIR
        os.makedirs(target_dir, exist_ok=True)
        write_all_pages_to_zip(
            url,
            headers=HEADERS,
            params=params,
            zip_path=os.path.join(target_dir, f"{flt}_comp_g_fundq.zip"),
            zip_member_csv_name=f"{flt}_comp_g_fundq.csv",
            fields=fields_g_fund_q
        )


def compustat_global_names(start_year: int = 1987, end_year: int = 2025, window_years: int = 5):
    """Fetch Compustat Global names (comp.g_names) in 5-year windows by `year1`.

    - Splits queries into non-overlapping windows on `year1` (first active year):
      [start, start+5), [start+5, start+10), ..., [.., end].
    - Writes one ZIP per window under G_NAMES_OUT_DIR named with the filter string.
    """
    url = WRDS_BASE + "comp.g_names/"

    # Discover fields once for a stable header
    fields = discover_fields(url, HEADERS, {'limit': 1})

    # Build 5-year windows using closed-open ranges on year1
    bounds = list(range(int(start_year), int(end_year), int(window_years)))
    if bounds[-1] != end_year:
        bounds.append(end_year)

    os.makedirs(G_NAMES_OUT_DIR, exist_ok=True)
    for i in range(1, len(bounds)):
        start = bounds[i-1]
        stop = bounds[i]
        if i < len(bounds) - 1:
            flt = build_filters(year1__gte=start, year1__lt=stop)
        else:
            # Last window: include end_year inclusively
            flt = build_filters(year1__gte=start, year1__lte=stop)

        params = {'filters': flt, 'limit': 99999999}
        write_all_pages_to_zip(
            url,
            headers=HEADERS,
            params=params,
            zip_path=os.path.join(G_NAMES_OUT_DIR, f"{flt}_comp_g_names.zip"),
            zip_member_csv_name=f"{flt}_comp_g_names.csv",
            fields=fields,
        )


def compustat_execucomp_anncomp(start_year: int = 1992, end_year: int = 2025, window_years: int = 5):
    url = WRDS_BASE + "execcomp.anncomp/"
    # Discover fields once for a stable header
    fields = discover_fields(url, HEADERS, {'limit': 1})

    # Build 5-year windows using closed-open ranges on year1
    bounds = list(range(int(start_year), int(end_year), int(window_years)))
    if bounds[-1] != end_year:
        bounds.append(end_year)
    print(bounds)
    os.makedirs(G_NAMES_OUT_DIR, exist_ok=True)
    for i in range(1, len(bounds)):
        start = bounds[i-1]
        stop = bounds[i]
        if i < len(bounds) - 1:
            flt = build_filters(year1__gte=start, year1__lt=stop)
        else:
            # Last window: include end_year inclusively
            flt = build_filters(year1__gte=start, year1__lte=stop)
        print(flt)
        params = {'filters': flt, 'limit': 99999999}
        write_all_pages_to_zip(
            url,
            headers=HEADERS,
            params=params,
            zip_path=os.path.join(EXECUCOMP_OUT_DIR, "anncomp/",f"{flt}_anncomp.zip"),
            zip_member_csv_name=f"{flt}_anncomp.csv",
            fields=fields,
        )

def compustat_execucomp_defferedcomp(start_year: int = 1992, end_year: int = 2025, window_years: int = 5):
    url = WRDS_BASE + "execcomp.deferredcomp/"
    # Discover fields once for a stable header
    fields = discover_fields(url, HEADERS, {'limit': 1})

    # Build 5-year windows using closed-open ranges on year1
    bounds = list(range(int(start_year), int(end_year), int(window_years)))
    if bounds[-1] != end_year:
        bounds.append(end_year)
    print(bounds)
    os.makedirs(G_NAMES_OUT_DIR, exist_ok=True)
    for i in range(1, len(bounds)):
        start = bounds[i-1]
        stop = bounds[i]
        if i < len(bounds) - 1:
            flt = build_filters(year__gte=start, year__lt=stop)
        else:
            # Last window: include end_year inclusively
            flt = build_filters(year__gte=start, year__lte=stop)
        print(flt)
        params = {'filters': flt, 'limit': 99999999}
        write_all_pages_to_zip(
            url,
            headers=HEADERS,
            params=params,
            zip_path=os.path.join(EXECUCOMP_OUT_DIR, "defferedcomp/",f"{flt}_defferedcomp.zip"),
            zip_member_csv_name=f"{flt}_defferedcomp.csv",
            fields=fields,
        )
def compustat_execucomp_ltawdtab(start_year: int = 1992, end_year: int = 2025, window_years: int = 5):
    url = WRDS_BASE + "execcomp.ltawdtab/"
    # Discover fields once for a stable header
    fields = discover_fields(url, HEADERS, {'limit': 1})

    # Build 5-year windows using closed-open ranges on year1
    bounds = list(range(int(start_year), int(end_year), int(window_years)))
    if bounds[-1] != end_year:
        bounds.append(end_year)
    print(bounds)
    os.makedirs(G_NAMES_OUT_DIR, exist_ok=True)
    for i in range(1, len(bounds)):
        start = bounds[i-1]
        stop = bounds[i]
        if i < len(bounds) - 1:
            flt = build_filters(year__gte=start, year__lt=stop)
        else:
            # Last window: include end_year inclusively
            flt = build_filters(year__gte=start, year__lte=stop)
        print(flt)
        params = {'filters': flt, 'limit': 99999999}
        write_all_pages_to_zip(
            url,
            headers=HEADERS,
            params=params,
            zip_path=os.path.join(EXECUCOMP_OUT_DIR, "ltawdtab/",f"{flt}_ltawdtab.zip"),
            zip_member_csv_name=f"{flt}_ltawdtab.csv",
            fields=fields,
        )

def compustat_execucomp_outstandingawards(start_year: int = 1992, end_year: int = 2025, window_years: int = 5):
    url = WRDS_BASE + "execcomp.outstandingawards/"
    # Discover fields once for a stable header
    fields = discover_fields(url, HEADERS, {'limit': 1})

    # Build 5-year windows using closed-open ranges on year1
    bounds = list(range(int(start_year), int(end_year), int(window_years)))
    if bounds[-1] != end_year:
        bounds.append(end_year)
    print(bounds)
    os.makedirs(G_NAMES_OUT_DIR, exist_ok=True)
    for i in range(1, len(bounds)):
        start = bounds[i-1]
        stop = bounds[i]
        if i < len(bounds) - 1:
            flt = build_filters(year__gte=start, year__lt=stop)
        else:
            # Last window: include end_year inclusively
            flt = build_filters(year__gte=start, year__lte=stop)
        print(flt)
        params = {'filters': flt, 'limit': 99999999}
        write_all_pages_to_zip(
            url,
            headers=HEADERS,
            params=params,
            zip_path=os.path.join(EXECUCOMP_OUT_DIR, "outstandingawards/",f"{flt}_outstandingawards.zip"),
            zip_member_csv_name=f"{flt}_outstandingawards.csv",
            fields=fields,
        )

def compustat_execucomp_stgrttab(start_year: int = 1992, end_year: int = 2025, window_years: int = 5):
    url = WRDS_BASE + "execcomp.stgrttab/"
    # Discover fields once for a stable header
    fields = discover_fields(url, HEADERS, {'limit': 1})

    # Build 5-year windows using closed-open ranges on year1
    bounds = list(range(int(start_year), int(end_year), int(window_years)))
    if bounds[-1] != end_year:
        bounds.append(end_year)
    print(bounds)
    os.makedirs(G_NAMES_OUT_DIR, exist_ok=True)
    for i in range(1, len(bounds)):
        start = bounds[i-1]
        stop = bounds[i]
        if i < len(bounds) - 1:
            flt = build_filters(year__gte=start, year__lt=stop)
        else:
            # Last window: include end_year inclusively
            flt = build_filters(year__gte=start, year__lte=stop)
        print(flt)
        params = {'filters': flt, 'limit': 99999999}
        write_all_pages_to_zip(
            url,
            headers=HEADERS,
            params=params,
            zip_path=os.path.join(EXECUCOMP_OUT_DIR, "stgrttab/",f"{flt}_stgrttab.zip"),
            zip_member_csv_name=f"{flt}_stgrttab.csv",
            fields=fields,
        )

def compustat_execucomp_pension(start_year: int = 1992, end_year: int = 2025, window_years: int = 5):
    url = WRDS_BASE + "execcomp.pension/"
    # Discover fields once for a stable header
    fields = discover_fields(url, HEADERS, {'limit': 1})

    # Build 5-year windows using closed-open ranges on year1
    bounds = list(range(int(start_year), int(end_year), int(window_years)))
    if bounds[-1] != end_year:
        bounds.append(end_year)
    print(bounds)
    os.makedirs(G_NAMES_OUT_DIR, exist_ok=True)
    for i in range(1, len(bounds)):
        start = bounds[i-1]
        stop = bounds[i]
        if i < len(bounds) - 1:
            flt = build_filters(year__gte=start, year__lt=stop)
        else:
            # Last window: include end_year inclusively
            flt = build_filters(year__gte=start, year__lte=stop)
        print(flt)
        params = {'filters': flt, 'limit': 99999999}
        write_all_pages_to_zip(
            url,
            headers=HEADERS,
            params=params,
            zip_path=os.path.join(EXECUCOMP_OUT_DIR, "pension/",f"{flt}_pension.zip"),
            zip_member_csv_name=f"{flt}_pension.csv",
            fields=fields,
        )

def compustat_execucomp_planbasedawards(start_year: int = 1992, end_year: int = 2025, window_years: int = 5):
    url = WRDS_BASE + "execcomp.planbasedawards/"
    # Discover fields once for a stable header
    fields = discover_fields(url, HEADERS, {'limit': 1})

    # Build 5-year windows using closed-open ranges on year1
    bounds = list(range(int(start_year), int(end_year), int(window_years)))
    if bounds[-1] != end_year:
        bounds.append(end_year)
    print(bounds)
    os.makedirs(G_NAMES_OUT_DIR, exist_ok=True)
    for i in range(1, len(bounds)):
        start = bounds[i-1]
        stop = bounds[i]
        if i < len(bounds) - 1:
            flt = build_filters(year__gte=start, year__lt=stop)
        else:
            # Last window: include end_year inclusively
            flt = build_filters(year__gte=start, year__lte=stop)
        print(flt)
        params = {'filters': flt, 'limit': 99999999}
        write_all_pages_to_zip(
            url,
            headers=HEADERS,
            params=params,
            zip_path=os.path.join(EXECUCOMP_OUT_DIR, "planbasedawards/",f"{flt}_planbasedawards.zip"),
            zip_member_csv_name=f"{flt}_planbasedawards.csv",
            fields=fields,
        )






if __name__ == "__main__":
    #_ = crsp_daily_securities()
    #_ = csrp_indexes()
    #_ = compustat_global_fundq()
    #_ = compustat_execucomp_anncomp()
    _ = compustat_execucomp_pension(start_year = 2006, end_year = 2025)
    _ = compustat_execucomp_stgrttab(start_year = 1992, end_year = 2025)
    _ = compustat_execucomp_ltawdtab(start_year = 1992, end_year = 2006)
    _ = compustat_execucomp_defferedcomp(start_year = 2006, end_year = 2025)
    _ = compustat_execucomp_planbasedawards(start_year = 2006, end_year = 2025)
    _ = compustat_execucomp_outstandingawards(start_year = 1997, end_year = 2025)

    
