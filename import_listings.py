"""
NestMatch — import_listings.py (v3)
Reads C:\\dev\\nestmatch-data\\nestmatch_listings_template.xlsx
and loads all listings into the Neon PostgreSQL database.

Usage:
    python import_listings.py
"""

import os
import uuid
import openpyxl
import psycopg2
from dotenv import load_dotenv

EXCEL_PATH     = r"C:\dev\nestmatch-data\nestmatch_listings_template.xlsx"
SHEET_NAME     = "Listings"
DATA_START_ROW = 3

load_dotenv(r"C:\dev\nestmatch\.env")
DATABASE_URL = os.getenv("DATABASE_URL")

COLUMNS = [
    "id", "property_type", "suburb", "title",
    "land_size_sqm", "bedrooms", "bathrooms", "parking_spaces",
    "price_min", "price_max", "renovation_status", "internal_size_sqm",
    "development_zone", "commute_cbd_mins", "distance_to_station_m",
    "distance_to_bus_stop_m", "distance_to_hospital_m",
    "transport_score", "school_score", "noise_score", "suburb_lifestyle_score",
]

VALID_RENOVATION = {"original", "partially_renovated", "fully_renovated", "new_build"}

def parse_int(val):
    if val is None or str(val).strip() in ("", "NULL"):
        return None
    return int(float(str(val).replace(",", "").replace(" ", "")))

def parse_float(val):
    if val is None or str(val).strip() in ("", "NULL"):
        return None
    return float(str(val).replace(",", "."))

def parse_str(val):
    if val is None:
        return None
    s = str(val).strip()
    return s if s else None

def load_excel(path):
    wb = openpyxl.load_workbook(path, data_only=True)
    ws = wb[SHEET_NAME]
    rows = []
    for row in ws.iter_rows(min_row=DATA_START_ROW, values_only=True):
        if not any(row):
            continue
        padded = list(row) + [None] * (len(COLUMNS) - len(row))
        data = dict(zip(COLUMNS, padded))
        rows.append(data)
    return rows

def normalise_renovation(val):
    """Accept common variations and map to valid values."""
    if val is None:
        return "original"
    mapping = {
        "original":              "original",
        "partially_renovated":   "partially_renovated",
        "partically_renovated":  "partially_renovated",   # typo tolerance
        "partial renovated":     "partially_renovated",
        "partially renovated":   "partially_renovated",
        "partically renovated":  "partially_renovated",
        "fully_renovated":       "fully_renovated",
        "fully renovated":       "fully_renovated",
        "new_build":             "new_build",
        "new build":             "new_build",
    }
    normalised = mapping.get(val.strip().lower())
    if normalised is None:
        raise ValueError(f"Invalid renovation_status: '{val}'. Must be one of {VALID_RENOVATION}")
    return normalised

def insert_row(cur, data):
    prop_id     = str(uuid.uuid4())
    renovation  = normalise_renovation(parse_str(data["renovation_status"]))

    cur.execute("""
        INSERT INTO properties
            (id, title, suburb, price_min, price_max, bedrooms, bathrooms,
             internal_size_sqm, property_type, parking_spaces,
             land_size_sqm, development_zone, renovation_status)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT (id) DO NOTHING
    """, (
        prop_id,
        str(data["title"]).strip(),
        str(data["suburb"]).strip(),
        parse_int(data["price_min"]),
        parse_int(data["price_max"]),
        parse_int(data["bedrooms"]),
        parse_int(data["bathrooms"]) or 1,
        parse_int(data["internal_size_sqm"]),
        str(data["property_type"]).strip().lower(),
        parse_int(data["parking_spaces"]) or 0,
        parse_int(data["land_size_sqm"]),
        parse_str(data["development_zone"]),
        renovation,
    ))

    cur.execute("""
        INSERT INTO property_features
            (property_id, commute_cbd_mins, distance_to_station_m,
             transport_score, school_score, noise_score, suburb_lifestyle_score,
             distance_to_bus_stop_m, distance_to_hospital_m)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT (property_id) DO NOTHING
    """, (
        prop_id,
        parse_int(data["commute_cbd_mins"]),
        parse_int(data["distance_to_station_m"]),   # now nullable
        parse_float(data["transport_score"]),
        parse_float(data["school_score"]),
        parse_float(data["noise_score"]),
        parse_float(data["suburb_lifestyle_score"]),
        parse_int(data["distance_to_bus_stop_m"]),
        parse_int(data["distance_to_hospital_m"]),
    ))

    return prop_id

def main():
    print(f"Reading: {EXCEL_PATH}")
    rows = load_excel(EXCEL_PATH)
    print(f"Found {len(rows)} listing(s) to import\n")

    if not rows:
        print("No data found — check the sheet has entries from row 3 onwards.")
        return

    conn = psycopg2.connect(DATABASE_URL)
    cur  = conn.cursor()

    imported = 0
    errors   = 0

    for i, data in enumerate(rows, start=1):
        try:
            prop_id = insert_row(cur, data)
            print(f"  ✓ [{i:02d}] {data['title']} ({data['suburb']}) → {prop_id}")
            imported += 1
        except Exception as e:
            print(f"  ✗ [{i:02d}] {data.get('title', '?')} — ERROR: {e}")
            conn.rollback()
            errors += 1
            continue
        conn.commit()

    cur.close()
    conn.close()
    print(f"\n{'─'*50}")
    print(f"Done — {imported} imported, {errors} failed.")

if __name__ == "__main__":
    main()