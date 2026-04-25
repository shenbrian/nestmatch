"""
create_agent_replies.py
Run once to create the agent_replies table in Neon.

Usage (PowerShell):
    $env:DATABASE_URL = 'postgresql://...'
    python create_agent_replies.py
"""

import asyncio
import asyncpg
import os


async def main():
    url = os.environ["DATABASE_URL"]
    conn = await asyncpg.connect(url)

    await conn.execute("""
        CREATE TABLE IF NOT EXISTS agent_replies (
            id                  SERIAL PRIMARY KEY,
            received_at         TIMESTAMPTZ,
            message_id          TEXT UNIQUE,          -- prevents duplicate ingestion
            nester_email        TEXT,                 -- which nester received this
            nester_id           TEXT,                 -- e.g. N03
            agent_name          TEXT,
            agent_email         TEXT,
            agency              TEXT,
            agent_phone         TEXT,
            property_address    TEXT,
            suburb              TEXT,
            price_guide         TEXT,
            auction_date        TEXT,
            auction_venue       TEXT,
            open_home_times     TEXT,
            internal_size_sqm   NUMERIC,
            total_size_sqm      NUMERIC,
            parking             TEXT,
            rental_estimate_pw  NUMERIC,
            outgoings           JSONB,
            property_type       TEXT,
            email_type          TEXT,                 -- A | B | C | D | E
            has_attachment      BOOLEAN DEFAULT FALSE,
            document_links      TEXT[],
            raw_subject         TEXT,
            raw_body            TEXT,
            processed           BOOLEAN DEFAULT FALSE,
            anomaly_flag        BOOLEAN DEFAULT FALSE,
            anomaly_note        TEXT,
            created_at          TIMESTAMPTZ DEFAULT NOW()
        )
    """)

    # Index for quick lookups by suburb and nester
    await conn.execute("""
        CREATE INDEX IF NOT EXISTS idx_agent_replies_suburb
        ON agent_replies (suburb)
    """)
    await conn.execute("""
        CREATE INDEX IF NOT EXISTS idx_agent_replies_nester
        ON agent_replies (nester_id)
    """)
    await conn.execute("""
        CREATE INDEX IF NOT EXISTS idx_agent_replies_processed
        ON agent_replies (processed)
    """)

    await conn.close()
    print("agent_replies table created successfully.")


if __name__ == "__main__":
    asyncio.run(main())
