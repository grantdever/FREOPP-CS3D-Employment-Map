"""
Pull state-level employment data from BLS QCEW Open Data API.
Fetches exact 2024 annual average employment for CS3D high-risk sectors.
"""

import csv
import io
import json
import urllib.request

YEAR = 2024
QUARTER = "a"

INDUSTRIES = {
    "11": "Agriculture",
    "21": "Mining",
    "1013": "Manufacturing",
}
TOTAL_INDUSTRY = "10"

FIPS_TO_ABBR = {
    "01": "AL", "02": "AK", "04": "AZ", "05": "AR", "06": "CA",
    "08": "CO", "09": "CT", "10": "DE", "12": "FL", "13": "GA",
    "15": "HI", "16": "ID", "17": "IL", "18": "IN", "19": "IA",
    "20": "KS", "21": "KY", "22": "LA", "23": "ME", "24": "MD",
    "25": "MA", "26": "MI", "27": "MN", "28": "MS", "29": "MO",
    "30": "MT", "31": "NE", "32": "NV", "33": "NH", "34": "NJ",
    "35": "NM", "36": "NY", "37": "NC", "38": "ND", "39": "OH",
    "40": "OK", "41": "OR", "42": "PA", "44": "RI", "45": "SC",
    "46": "SD", "47": "TN", "48": "TX", "49": "UT", "50": "VT",
    "51": "VA", "53": "WA", "54": "WV", "55": "WI", "56": "WY",
}

STATE_NAMES = {
    "AL": "Alabama", "AK": "Alaska", "AZ": "Arizona", "AR": "Arkansas",
    "CA": "California", "CO": "Colorado", "CT": "Connecticut", "DE": "Delaware",
    "FL": "Florida", "GA": "Georgia", "HI": "Hawaii", "ID": "Idaho",
    "IL": "Illinois", "IN": "Indiana", "IA": "Iowa", "KS": "Kansas",
    "KY": "Kentucky", "LA": "Louisiana", "ME": "Maine", "MD": "Maryland",
    "MA": "Massachusetts", "MI": "Michigan", "MN": "Minnesota", "MS": "Mississippi",
    "MO": "Missouri", "MT": "Montana", "NE": "Nebraska", "NV": "Nevada",
    "NH": "New Hampshire", "NJ": "New Jersey", "NM": "New Mexico", "NY": "New York",
    "NC": "North Carolina", "ND": "North Dakota", "OH": "Ohio", "OK": "Oklahoma",
    "OR": "Oregon", "PA": "Pennsylvania", "RI": "Rhode Island", "SC": "South Carolina",
    "SD": "South Dakota", "TN": "Tennessee", "TX": "Texas", "UT": "Utah",
    "VT": "Vermont", "VA": "Virginia", "WA": "Washington", "WV": "West Virginia",
    "WI": "Wisconsin", "WY": "Wyoming",
}

def fetch_industry_csv(industry_code, year=YEAR, quarter=QUARTER):
    url = f"https://data.bls.gov/cew/data/api/{year}/{quarter}/industry/{industry_code}.csv"
    print(f"  Fetching: {url}")
    req = urllib.request.Request(url, headers={"User-Agent": "Mozilla/5.0 (QCEW research script)"})
    try:
        resp = urllib.request.urlopen(req, timeout=30)
        text = resp.read().decode("utf-8")
        reader = csv.DictReader(io.StringIO(text))
        return list(reader)
    except Exception as e:
        print(f"  ERROR: {e}")
        return []

def extract_state_employment(rows, target_own="5"):
    state_emp = {}
    for row in rows:
        area_fips = row.get("area_fips", "")
        if not area_fips.endswith("000") or len(area_fips) != 5:
            continue
        
        state_fips = area_fips[:2]
        abbr = FIPS_TO_ABBR.get(state_fips)
        if not abbr:
            continue
        
        own_code = row.get("own_code", "")
        if own_code != target_own:
            continue
        
        emp_str = row.get("annual_avg_emplvl", "0")
        try:
            emp = int(emp_str.replace(",", ""))
        except ValueError:
            continue
        
        if abbr not in state_emp or emp > state_emp[abbr]:
            state_emp[abbr] = emp
    
    return state_emp

def main():
    print(f"Pulling QCEW {YEAR} annual average data for 50 states...\n")
    
    print("Fetching total employment (NAICS 10 - all industries)...")
    total_rows = fetch_industry_csv(TOTAL_INDUSTRY)
    total_emp = extract_state_employment(total_rows, target_own="0")
    print(f"  Got data for {len(total_emp)} states\n")
    
    sector_data = {}
    for code, label in INDUSTRIES.items():
        print(f"Fetching {label} (NAICS {code})...")
        rows = fetch_industry_csv(code)
        emp = extract_state_employment(rows, target_own="5")
        sector_data[label] = emp
        print(f"  Got data for {len(emp)} states\n")
    
    print("=" * 90)
    print(f"{'State':<22} {'Total':>10} {'Mfg':>10} {'Mining':>10} {'Ag':>10} {'Combined':>10} {'Pct':>7}")
    print("-" * 90)
    
    results = {}
    for abbr in sorted(FIPS_TO_ABBR.values()):
        total = total_emp.get(abbr, 0)
        mfg = sector_data.get("Manufacturing", {}).get(abbr, 0)
        mining = sector_data.get("Mining", {}).get(abbr, 0)
        ag = sector_data.get("Agriculture", {}).get(abbr, 0)
        combined = mfg + mining + ag
        pct = round(combined / total * 100, 1) if total > 0 else 0
        
        results[abbr] = {
            "name": STATE_NAMES[abbr],
            "total": total,
            "mfg": mfg,
            "mining": mining,
            "ag": ag,
            "combined": combined,
            "combined_pct": pct,
            "mfg_pct": round(mfg / total * 100, 1) if total > 0 else 0,
            "mining_pct": round(mining / total * 100, 1) if total > 0 else 0,
            "ag_pct": round(ag / total * 100, 1) if total > 0 else 0,
        }
        
        name = STATE_NAMES[abbr]
        print(f"  {abbr} {name:<18} {total:>10,} {mfg:>10,} {mining:>10,} {ag:>10,} {combined:>10,} {pct:>6.1f}%")
    
    csv_path = "qcew_state_employment_exact.csv"
    with open(csv_path, "w", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(["state_abbr", "state_name", "total_employment", "manufacturing", 
                         "mining", "agriculture", "combined", "combined_pct",
                         "mfg_pct", "mining_pct", "ag_pct"])
        for abbr in sorted(results.keys()):
            r = results[abbr]
            writer.writerow([abbr, r["name"], r["total"], r["mfg"], r["mining"], 
                           r["ag"], r["combined"], r["combined_pct"],
                           r["mfg_pct"], r["mining_pct"], r["ag_pct"]])
    print(f"\nSaved CSV: {csv_path}")
    
    print(f"\n{'=' * 90}")
    print("COMPACT JS (paste into HTML as STATE_DATA):")
    print(f"{'=' * 90}")
    print(f"const SD = {json.dumps(results, separators=(',', ':'))};")

if __name__ == "__main__":
    main()