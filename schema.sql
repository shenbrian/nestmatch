-- NestMatch — PostgreSQL schema
-- Compatible with Neon / Supabase / standard Postgres 15+
-- Run this once to initialise the database.

-- ── Extensions ────────────────────────────────────────────────────────────────

CREATE EXTENSION IF NOT EXISTS "pgcrypto";

-- ── Properties ────────────────────────────────────────────────────────────────

CREATE TABLE IF NOT EXISTS properties (
    id              UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    title           TEXT        NOT NULL,
    suburb          TEXT        NOT NULL,
    price_min       INTEGER     NOT NULL CHECK (price_min >= 0),
    price_max       INTEGER     NOT NULL CHECK (price_max >= price_min),
    bedrooms        SMALLINT    NOT NULL CHECK (bedrooms >= 0),
    internal_size_sqm INTEGER   NOT NULL CHECK (internal_size_sqm >= 0),
    property_type   TEXT        NOT NULL CHECK (property_type IN ('apartment', 'house', 'townhouse')),
    parking_spaces  SMALLINT    NOT NULL DEFAULT 0,
    is_new_build    BOOLEAN     NOT NULL DEFAULT FALSE,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at      TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- ── Property features (pre-computed derived attributes) ────────────────────────

CREATE TABLE IF NOT EXISTS property_features (
    property_id             UUID PRIMARY KEY REFERENCES properties(id) ON DELETE CASCADE,
    commute_cbd_mins        SMALLINT        NOT NULL,
    distance_to_station_m   INTEGER         NOT NULL,
    transport_score         NUMERIC(4,3)    NOT NULL CHECK (transport_score BETWEEN 0 AND 1),
    school_score            NUMERIC(4,3)    NOT NULL CHECK (school_score BETWEEN 0 AND 1),
    noise_score             NUMERIC(4,3)    NOT NULL CHECK (noise_score BETWEEN 0 AND 1),
    lifestyle_family_score  NUMERIC(4,3)    NOT NULL CHECK (lifestyle_family_score BETWEEN 0 AND 1),
    updated_at              TIMESTAMPTZ     NOT NULL DEFAULT now()
);

-- ── Indexes ────────────────────────────────────────────────────────────────────

CREATE INDEX IF NOT EXISTS idx_properties_suburb       ON properties(suburb);
CREATE INDEX IF NOT EXISTS idx_properties_type         ON properties(property_type);
CREATE INDEX IF NOT EXISTS idx_properties_price_max    ON properties(price_max);
CREATE INDEX IF NOT EXISTS idx_properties_bedrooms     ON properties(bedrooms);

-- ── Seed data ──────────────────────────────────────────────────────────────────
-- Mirrors app/seed_data.py; safe to re-run (DO NOTHING on conflict).

INSERT INTO properties (id, title, suburb, price_min, price_max, bedrooms, internal_size_sqm, property_type, parking_spaces, is_new_build)
VALUES
    ('11111111-0001-0000-0000-000000000001', 'Spacious apartment in Chatswood',  'Chatswood',  1180000, 1280000, 2, 105, 'apartment', 1, FALSE),
    ('11111111-0002-0000-0000-000000000002', 'Modern 2-bed in Epping',            'Epping',      950000, 1050000, 2,  98, 'apartment', 1, FALSE),
    ('11111111-0003-0000-0000-000000000003', 'Large apartment in Burwood',        'Burwood',     920000, 1020000, 3, 118, 'apartment', 1, FALSE),
    ('11111111-0004-0000-0000-000000000004', 'New build apartment in Parramatta', 'Parramatta',  780000,  880000, 2,  82, 'apartment', 1, TRUE),
    ('11111111-0005-0000-0000-000000000005', 'Quiet apartment in Mascot',         'Mascot',     1050000, 1150000, 2,  92, 'apartment', 1, FALSE),
    ('11111111-0006-0000-0000-000000000006', 'Family apartment in Lane Cove',     'Lane Cove',  1200000, 1350000, 3, 125, 'apartment', 2, FALSE),
    ('11111111-0007-0000-0000-000000000007', 'Compact apartment in Ashfield',     'Ashfield',    890000,  980000, 2,  80, 'apartment', 1, FALSE),
    ('11111111-0008-0000-0000-000000000008', 'Affordable house in Penrith',       'Penrith',     820000,  920000, 3, 155,     'house', 2, FALSE),
    ('11111111-0009-0000-0000-000000000009', 'Premium apartment in Chatswood',    'Chatswood',  1350000, 1450000, 3, 138, 'apartment', 2, TRUE),
    ('11111111-0010-0000-0000-000000000010', 'Mid-size apartment in Epping',      'Epping',     1020000, 1120000, 3, 112, 'apartment', 1, FALSE)
ON CONFLICT (id) DO NOTHING;

INSERT INTO property_features (property_id, commute_cbd_mins, distance_to_station_m, transport_score, school_score, noise_score, lifestyle_family_score)
VALUES
    ('11111111-0001-0000-0000-000000000001', 31,  420, 0.880, 0.820, 0.600, 0.780),
    ('11111111-0002-0000-0000-000000000002', 42,  280, 0.820, 0.850, 0.780, 0.800),
    ('11111111-0003-0000-0000-000000000003', 28,  350, 0.840, 0.720, 0.550, 0.700),
    ('11111111-0004-0000-0000-000000000004', 45,  180, 0.910, 0.650, 0.420, 0.600),
    ('11111111-0005-0000-0000-000000000005', 20,  450, 0.800, 0.580, 0.450, 0.550),
    ('11111111-0006-0000-0000-000000000006', 33,  950, 0.620, 0.900, 0.880, 0.920),
    ('11111111-0007-0000-0000-000000000007', 22,  310, 0.860, 0.680, 0.520, 0.620),
    ('11111111-0008-0000-0000-000000000008', 65,  600, 0.650, 0.700, 0.820, 0.750),
    ('11111111-0009-0000-0000-000000000009', 29,  380, 0.900, 0.850, 0.620, 0.800),
    ('11111111-0010-0000-0000-000000000010', 40,  290, 0.830, 0.880, 0.800, 0.830)
ON CONFLICT (property_id) DO NOTHING;
