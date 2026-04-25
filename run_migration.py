"""
run_migration.py — create agent_outbound table in Neon
Run from C:\dev\nestmatch with DATABASE_URL set in environment.
"""

import asyncio
import asyncpg
import os

SQL = """
CREATE TABLE IF NOT EXISTS agent_outbound (

    id                  SERIAL PRIMARY KEY,
    message_id          TEXT UNIQUE NOT NULL,
    received_at         TIMESTAMPTZ NOT NULL,

    nester_id           TEXT,
    nester_email        TEXT,

    agency_name         TEXT,
    agent_name          TEXT,
    agent_email         TEXT,

    email_type          TEXT,
    is_off_corridor     BOOLEAN DEFAULT FALSE,
    is_pre_portal       BOOLEAN DEFAULT FALSE,

    street_address      TEXT,
    suburb              TEXT,
    property_type       TEXT,
    bedrooms            INTEGER,
    bathrooms           INTEGER,
    parking             INTEGER,
    land_size_sqm       NUMERIC,
    price_guide_low     NUMERIC,
    price_guide_high    NUMERIC,
    auction_date        TEXT,
    inspection_times    JSONB,

    listing_count       INTEGER DEFAULT 1,
    listings_raw        JSONB,

    anomaly_flag        BOOLEAN DEFAULT FALSE,
    anomaly_note        TEXT,
    raw_subject         TEXT,
    raw_body            TEXT
);

CREATE INDEX IF NOT EXISTS idx_agent_outbound_nester_id
    ON agent_outbound (nester_id);

CREATE INDEX IF NOT EXISTS idx_agent_outbound_email_type
    ON agent_outbound (email_type);

CREATE INDEX IF NOT EXISTS idx_agent_outbound_received_at
    ON agent_outbound (received_at DESC);

CREATE INDEX IF NOT EXISTS idx_agent_outbound_suburb
    ON agent_outbound (suburb);
"""

async def main():
    db_url = os.environ.get("DATABASE_URL", "")
    if not db_url:
        print("ERROR: DATABASE_URL not set.")
        return

    print("Connecting to Neon...")
    conn = await asyncpg.connect(db_url)
    print("Running migration...")
    await conn.execute(SQL)
    await conn.close()
    print("Done. agent_outbound table created.")

asyncio.run(main())
