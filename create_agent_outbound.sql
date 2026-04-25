-- NestMatch — Session 21
-- Migration: create agent_outbound table
-- Captures agent-initiated outbound emails (Types F–L)
-- Separate from agent_replies (Types A–E) which captures inquiry responses

CREATE TABLE IF NOT EXISTS agent_outbound (

    -- Identity / dedup
    id                  SERIAL PRIMARY KEY,
    message_id          TEXT UNIQUE NOT NULL,
    received_at         TIMESTAMPTZ NOT NULL,

    -- Source — nester who received it
    nester_id           TEXT,
    nester_email        TEXT,

    -- Sender
    agency_name         TEXT,
    agent_name          TEXT,
    agent_email         TEXT,

    -- Classification
    email_type          TEXT,        -- F / G / H / I / J / K / L
    is_off_corridor     BOOLEAN DEFAULT FALSE,   -- TRUE for Type J
    is_pre_portal       BOOLEAN DEFAULT FALSE,   -- TRUE for Type H

    -- Property data (nullable — single listing emails)
    street_address      TEXT,
    suburb              TEXT,
    property_type       TEXT,        -- house / apartment / townhouse / unit
    bedrooms            INTEGER,
    bathrooms           INTEGER,
    parking             INTEGER,
    land_size_sqm       NUMERIC,
    price_guide_low     NUMERIC,
    price_guide_high    NUMERIC,
    auction_date        TEXT,
    inspection_times    JSONB,       -- array of time strings

    -- Batch listings (for newsletters / digests)
    listing_count       INTEGER DEFAULT 1,
    listings_raw        JSONB,       -- full extracted listings array for F / K / L

    -- Flags
    anomaly_flag        BOOLEAN DEFAULT FALSE,
    anomaly_note        TEXT,
    raw_subject         TEXT,
    raw_body            TEXT

);

-- Indexes
CREATE INDEX IF NOT EXISTS idx_agent_outbound_nester_id
    ON agent_outbound (nester_id);

CREATE INDEX IF NOT EXISTS idx_agent_outbound_email_type
    ON agent_outbound (email_type);

CREATE INDEX IF NOT EXISTS idx_agent_outbound_received_at
    ON agent_outbound (received_at DESC);

CREATE INDEX IF NOT EXISTS idx_agent_outbound_suburb
    ON agent_outbound (suburb);
