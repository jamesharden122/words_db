from requests import post as requests_post, get as requests_get
import csv

headers = {
    'Authorization': 'Token 275be14d2410414f1de31176d40e7da3352ab114',
    "Accept": "application/json",
}
countries = [
    "Australia", "Austria", "Belgium", "Brazil", "Chile",
    "China", "Colombia", "Denmark", "Egypt", "Finland",
    "France", "Germany", "Greece", "Hong Kong", "Hungary",
    "India", "Indonesia", "Ireland", "Italy", "Japan",
    "Malaysia", "Mexico", "Netherlands", "New Zealand", "Norway",
    "Philippines", "Poland", "Portugal", "Singapore", "South Africa",
    "South Korea", "Spain", "Sweden", "Switzerland", "Taiwan",
    "Thailand", "Turkey", "United Kingdom",
]

fics_iso3 = [
    "AUS","AUT","BEL","BRA","CHL","CHN","COL","DNK","EGY","FIN",
    "FRA","DEU","GRC","HKG","HUN","IND","IDN","IRL","ITA","JPN",
    "MYS","MEX","NLD","NZL","NOR","PHL","POL","PRT","SGP","ZAF",
    "KOR","ESP","SWE","CHE","TWN","THA","TUR","GBR",
]

def write_all_pages_to_csv(url, headers, params, out_path,fields):
    with open(out_path, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=fields)
        w.writeheader()
        next_url, first = url, True
        while next_url:
            resp = requests_get(next_url, headers=headers, params=params if first else None, timeout=60)
            first = False
            data = resp.json()
            for row in data.get("results", []):
                w.writerow({k: row.get(k) for k in fields})
            next_url = data.get("next")


returns = False
constituents = True

if returns:
    fields_returns = ["fic", "date", "portret", "portretx", "n", "country", "currency"]
    for ctry in fics_iso3:
        filters = "(date__gte=1980-01-01&date__lte=2025-11-01&"+"fic="+ctry+")"
        params = {'filters': filters, 'limit': 99999999}
        write_all_pages_to_csv(
            "https://wrds-api.wharton.upenn.edu/data/wrdsapps.dwcountryreturns/",
            headers=headers,
            params=params,
            out_path="data/raw_files/country_indexes/"+ctry+"_country_returns.csv",
            fields=fields_returns
        )

if constituents:
    fields_constituents = ["com", "curcdd", "gvkey", "ibtic", "iid", "juedate", "n"]
    for ctry in fics_iso3:
        filters = "(date__gte=1980-01-01&date__lte=2025-11-01&"+"fic="+ctry+")"
        params = {'filters': filters, 'limit': 99999999}
        write_all_pages_to_csv(
            "https://wrds-api.wharton.upenn.edu/data/wrdsapps.wcountryconstituents/",
            headers=headers,
            params=params,
            out_path="data/raw_files/country_indexes/constituents/"+ctry+"_country_returns.csv",
            fields=fields_constituents
        )

