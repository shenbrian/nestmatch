"""
NestMatch — Session 12 Schema Migration
Adds agentic audit trail columns and land_values staging table.
Idempotent — safe to re-run.
"""

import os
import sys
import psycopg2
from psycopg2.extras import RealDictCursor

DATABASE_URL = os.environ.get("DATABASE_URL")
if not DATABASE_URL:
    print("ERROR: DATABASE_URL not set.")
    sys.exit(1)

MIGRATIONS = [
    # GTFS audit trail columns on properties
    """ALTER TABLE properties ADD COLUMN IF NOT EXISTS
       nearest_stop_name TEXT""",

    """ALTER TABLE properties ADD COLUMN IF NOT EXISTS
       nearest_stop_distance_m INTEGER""",

    """ALTER TABLE properties ADD COLUMN IF NOT EXISTS
       commute_source TEXT DEFAULT 'manual'""",

    # VG / investment columns on properties
    """ALTER TABLE properties ADD COLUMN IF NOT EXISTS
       land_to_asset_ratio NUMERIC(5,3)""",

    """ALTER TABLE properties ADD COLUMN IF NOT EXISTS
       land_value_source TEXT""",

    """ALTER TABLE properties ADD COLUMN IF NOT EXISTS
       median_weekly_rent INTEGER""",

    """ALTER TABLE properties ADD COLUMN IF NOT EXISTS
       capital_gain_pct NUMERIC(5,2)""",

    # Land values staging table
    """CREATE TABLE IF NOT EXISTS land_values (
        id               SERIAL PRIMARY KEY,
        address          TEXT,
        suburb           TEXT NOT NULL,
        land_value       INTEGER NOT NULL,
        assessment_year  INTEGER NOT NULL,
        source           TEXT NOT NULL DEFAULT 'valuer_general',
        match_type       TEXT,          -- 'address' | 'suburb_median'
        loaded_at        TIMESTAMPTZ NOT NULL DEFAULT NOW()
    )""",

    """CREATE INDEX IF NOT EXISTS idx_land_values_suburb
       ON land_values (suburb)""",

    # Data readiness audit table — agent trigger gate
    """CREATE TABLE IF NOT EXISTS data_pipeline_runs (
        id            SERIAL PRIMARY KEY,
        pipeline      TEXT NOT NULL,     -- 'gtfs' | 'valuer_general' | 'reconcile'
        status        TEXT NOT NULL,     -- 'started' | 'completed' | 'failed'
        rows_affected INTEGER,
        notes         TEXT,
        run_at        TIMESTAMPTZ NOT NULL DEFAULT NOW()
    )""",
]

def run():
    conn = psycopg2.connect(DATABASE_URL)
    conn.autocommit = False
    cur = conn.cursor(cursor_factory=RealDictCursor)

    print("Running Session 12 schema migration...")
    for i, sql in enumerate(MIGRATIONS, 1):
        label = sql.strip().split("\n")[0][:80]
        try:
            cur.execute(sql)
            print(f"  [{i:02d}] OK  — {label}")
        except Exception as e:
            conn.rollback()
            print(f"  [{i:02d}] FAIL — {label}")
            print(f"        {e}")
            sys.exit(1)

    conn.commit()
    cur.close()
    conn.close()
    print("\nMigration complete. All columns and tables confirmed.")

if __name__ == "__main__":
    run()
