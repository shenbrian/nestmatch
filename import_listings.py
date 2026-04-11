"""
import_listings.py — Session 8 final
Schema: real_estate_agency added, agent_name renamed to sales_agent, agent_email removed.

Run: python import_listings.py C:\dev\nestmatch-data\nestmatch_listings_template.xlsx
"""

import sys
import os
import uuid
import hashlib
from datetime import date
import pandas as pd
import psycopg2
from psycopg2.extras import execute_values


DATABASE_URL = os.environ["DATABASE_URL"]


def deterministic_uuid(suburb: str, address: str, price: int) -> str:
    key = f"{suburb.lower().strip()}|{address.lower().strip()}|{price}"
    return str(uuid.UUID(hashlib.md5(key.encode()).hexdigest()))


def clean_phone(val) -> str | None:
    if pd.isna(val) or not str(val).strip():
        return None
    return str(val).strip()


def clean_url(val) -> str | None:
    if pd.isna(val) or not str(val).strip():
        return None
    url = str(val).strip()
    return url if url.startswith("http") else None


def clean_date(val) -> date | None:
    if pd.isna(val):
        return None
    if isinstance(val, date):
        return val
    try:
        return pd.to_datetime(str(val)).date()
    except Exception:
        return None


def clean_int(val) -> int | None:
    try:
        return int(val) if not pd.isna(val) else None
    except Exception:
        return None


def clean_float(val) -> float | None:
    try:
        return float(val) if not pd.isna(val) else None
    except Exception:
        return None


def clean_str(val) -> str | None:
    if pd.isna(val):
        return None
    s = str(val).strip()
    return s if s else None


def import_listings(filepath: str):
    df = pd.read_excel(filepath, sheet_name="Listings")
    print(f"Loaded {len(df)} rows from {filepath}")

    conn = psycopg2.connect(DATABASE_URL)
    cur = conn.cursor()

    rows = []
    skipped = 0

    for _, row in df.iterrows():
        suburb = clean_str(row.get("suburb"))
        address = clean_str(row.get("street_address")) or ""
        price_max_raw = row.get("price_max")
        price_min_raw = row.get("price_min")

        if not suburb or pd.isna(price_max_raw):
            skipped += 1
            continue

        price_max = int(price_max_raw)
        price_min = int(price_min_raw) if not pd.isna(price_min_raw) else price_max
        prop_id = deterministic_uuid(suburb, address, price_max)

        rows.append((
            prop_id,
            clean_str(row.get("title")),
            suburb,
            price_min,
            price_max,
            clean_int(row.get("bedrooms")) or 3,
            clean_int(row.get("internal_size_sqm")),
            clean_str(row.get("property_type")) or "house",
            clean_int(row.get("parking_spaces")),
            clean_int(row.get("land_size_sqm")),
            clean_str(row.get("development_zone")),
            clean_int(row.get("bathrooms")),
            clean_str(row.get("renovation_status")),
            # D29 — actionable details
            address or None,
            clean_str(row.get("real_estate_agency")),
            clean_str(row.get("sales_agent")),
            clean_phone(row.get("agent_phone")),
            clean_url(row.get("listing_url_rea")),
            clean_url(row.get("listing_url_domain")),
            clean_date(row.get("inspection_date")),
            clean_int(row.get("days_on_market")),
        ))

    execute_values(cur, """
        INSERT INTO properties (
            id, title, suburb, price_min, price_max,
            bedrooms, internal_size_sqm, property_type, parking_spaces,
            land_size_sqm, development_zone, bathrooms, renovation_status,
            street_address, real_estate_agency, sales_agent, agent_phone,
            listing_url_rea, listing_url_domain, inspection_date, days_on_market
        )
        VALUES %s
        ON CONFLICT (id) DO UPDATE SET
            title                = EXCLUDED.title,
            suburb               = EXCLUDED.suburb,
            price_min            = EXCLUDED.price_min,
            price_max            = EXCLUDED.price_max,
            bedrooms             = EXCLUDED.bedrooms,
            internal_size_sqm    = EXCLUDED.internal_size_sqm,
            property_type        = EXCLUDED.property_type,
            parking_spaces       = EXCLUDED.parking_spaces,
            land_size_sqm        = EXCLUDED.land_size_sqm,
            development_zone     = EXCLUDED.development_zone,
            bathrooms            = EXCLUDED.bathrooms,
            renovation_status    = EXCLUDED.renovation_status,
            street_address       = EXCLUDED.street_address,
            real_estate_agency   = EXCLUDED.real_estate_agency,
            sales_agent          = EXCLUDED.sales_agent,
            agent_phone          = EXCLUDED.agent_phone,
            listing_url_rea      = EXCLUDED.listing_url_rea,
            listing_url_domain   = EXCLUDED.listing_url_domain,
            inspection_date      = EXCLUDED.inspection_date,
            days_on_market       = EXCLUDED.days_on_market
    """, rows)

    conn.commit()
    cur.close()
    conn.close()

    print(f"✓ Upserted {len(rows)} properties ({skipped} skipped — missing suburb or price)")
    print(f"  D29 actionable fields included in upsert.")


if __name__ == "__main__":
    filepath = sys.argv[1] if len(sys.argv) > 1 else "listings.xlsx"
    import_listings(filepath)