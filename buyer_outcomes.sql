-- buyer_outcomes table — D23 moat layer
-- Session 31

CREATE TABLE IF NOT EXISTS buyer_outcomes (
    id               SERIAL PRIMARY KEY,
    buyer_id         TEXT NOT NULL,                          -- nb_ prefixed session ID from localStorage
    listing_id       UUID NOT NULL,                         -- references properties.id
    outcome_type     TEXT NOT NULL                          -- shortlisted | this_is_the_one | passed | purchased
                         CHECK (outcome_type IN ('shortlisted', 'this_is_the_one', 'passed', 'purchased')),
    search_params    JSONB,                                  -- full search context at moment of signal
    match_score      NUMERIC(5,3),                          -- score NestMatch assigned this property
    suburb           TEXT,                                  -- denormalised for analytics without join
    property_type    TEXT,                                  -- denormalised
    price            INTEGER,                               -- denormalised
    created_at       TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- Index for per-buyer journey reconstruction
CREATE INDEX IF NOT EXISTS idx_buyer_outcomes_buyer_id ON buyer_outcomes(buyer_id);

-- Index for per-listing outcome aggregation
CREATE INDEX IF NOT EXISTS idx_buyer_outcomes_listing_id ON buyer_outcomes(listing_id);

-- Index for outcome type analytics
CREATE INDEX IF NOT EXISTS idx_buyer_outcomes_type ON buyer_outcomes(outcome_type);
