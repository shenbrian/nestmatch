-- NestMatch — Investment Fields SQL UPDATE
-- Session 15 · Run in Neon SQL editor
-- capital_gain_pct stored as decimal (0.063 = 6.3%)
-- median_weekly_rent: apartments only (integer AUD); NULL for houses

-- ── EXISTING SUBURBS (from Session 14) ────────────────────────────────────────

UPDATE properties SET capital_gain_pct = 0.040, median_weekly_rent = 660  WHERE suburb = 'Ashfield'       AND property_type = 'apartment';
UPDATE properties SET capital_gain_pct = 0.263                            WHERE suburb = 'Bankstown'      AND property_type = 'house';
UPDATE properties SET capital_gain_pct = 0.050                            WHERE suburb = 'Bayview'        AND property_type = 'house';
UPDATE properties SET capital_gain_pct = 0.056                            WHERE suburb = 'Blacktown'      AND property_type = 'house';
UPDATE properties SET capital_gain_pct = 0.068                            WHERE suburb = 'Canterbury'     AND property_type = 'townhouse';
UPDATE properties SET capital_gain_pct = 0.039                            WHERE suburb = 'Chatswood'      AND property_type = 'house';
UPDATE properties SET capital_gain_pct = 0.050, median_weekly_rent = 950  WHERE suburb = 'Coogee'         AND property_type = 'apartment';
UPDATE properties SET capital_gain_pct = 0.055, median_weekly_rent = 750  WHERE suburb = 'Cronulla'       AND property_type = 'apartment';
UPDATE properties SET capital_gain_pct = 0.090, median_weekly_rent = 780  WHERE suburb = 'Dee Why'        AND property_type = 'apartment';
UPDATE properties SET capital_gain_pct = 0.055                            WHERE suburb = 'Epping'         AND property_type = 'house';
UPDATE properties SET capital_gain_pct = 0.063                            WHERE suburb = 'Five Dock'      AND property_type = 'house';
UPDATE properties SET capital_gain_pct = 0.055                            WHERE suburb = 'Frenchs Forest' AND property_type = 'house';
UPDATE properties SET capital_gain_pct = 0.045, median_weekly_rent = 620  WHERE suburb = 'Hornsby'        AND property_type = 'apartment';
UPDATE properties SET capital_gain_pct = 0.050                            WHERE suburb = 'Hunters Hill'   AND property_type = 'house';
UPDATE properties SET capital_gain_pct = 0.055                            WHERE suburb = 'Hurstville'     AND property_type = 'house';
UPDATE properties SET capital_gain_pct = 0.063                            WHERE suburb = 'Leichhardt'     AND property_type = 'house';
UPDATE properties SET capital_gain_pct = 0.055, median_weekly_rent = 900  WHERE suburb = 'Manly'          AND property_type = 'apartment';
UPDATE properties SET capital_gain_pct = 0.060                            WHERE suburb = 'Maroubra'       AND property_type = 'house';
UPDATE properties SET capital_gain_pct = 0.063                            WHERE suburb = 'Marrickville'   AND property_type = 'house';
UPDATE properties SET capital_gain_pct = 0.055                            WHERE suburb = 'Marsfield'      AND property_type = 'townhouse';
UPDATE properties SET capital_gain_pct = 0.060, median_weekly_rent = 700  WHERE suburb = 'Meadowbank'     AND property_type = 'apartment';
UPDATE properties SET capital_gain_pct = 0.149, median_weekly_rent = 640  WHERE suburb = 'Merrylands'     AND property_type = 'apartment';
UPDATE properties SET capital_gain_pct = -0.032                           WHERE suburb = 'Mosman'         AND property_type = 'house';
UPDATE properties SET capital_gain_pct = -0.020                           WHERE suburb = 'Mosman'         AND property_type = 'townhouse';
UPDATE properties SET capital_gain_pct = 0.045, median_weekly_rent = 780  WHERE suburb = 'Neutral Bay'    AND property_type = 'apartment';
UPDATE properties SET capital_gain_pct = 0.063                            WHERE suburb = 'Newtown'        AND property_type = 'townhouse';
UPDATE properties SET capital_gain_pct = 0.055                            WHERE suburb = 'Paddington'     AND property_type = 'townhouse';
UPDATE properties SET capital_gain_pct = 0.068                            WHERE suburb = 'Parramatta'     AND property_type = 'house';
UPDATE properties SET capital_gain_pct = 0.062                            WHERE suburb = 'Randwick'       AND property_type = 'house';
UPDATE properties SET capital_gain_pct = 0.063                            WHERE suburb = 'Redfern'        AND property_type = 'townhouse';
UPDATE properties SET capital_gain_pct = 0.055                            WHERE suburb = 'Rose Bay'       AND property_type = 'house';
UPDATE properties SET capital_gain_pct = 0.058                            WHERE suburb = 'Ryde'           AND property_type = 'townhouse';
UPDATE properties SET capital_gain_pct = 0.055                            WHERE suburb = 'Strathfield'    AND property_type = 'house';
UPDATE properties SET capital_gain_pct = 0.050, median_weekly_rent = 650  WHERE suburb = 'Summer Hill'    AND property_type = 'apartment';
UPDATE properties SET capital_gain_pct = 0.055, median_weekly_rent = 650  WHERE suburb = 'Sutherland'     AND property_type = 'apartment';
UPDATE properties SET capital_gain_pct = 0.040, median_weekly_rent = 800  WHERE suburb = 'Sydney City'    AND property_type = 'apartment';
UPDATE properties SET capital_gain_pct = 0.055                            WHERE suburb = 'Terrey Hills'   AND property_type = 'house';
UPDATE properties SET capital_gain_pct = 0.045, median_weekly_rent = 760  WHERE suburb = 'Waverton'       AND property_type = 'apartment';
UPDATE properties SET capital_gain_pct = 0.050                            WHERE suburb = 'West Pymble'    AND property_type = 'house';

-- ── NEW PILOT SUBURBS (Session 15) ────────────────────────────────────────────
-- Sources: CoreLogic/Cotality, Domain suburb profiles, inner-west/eastern benchmarks

-- Camperdown: inner west apartment, adjacent to Newtown corridor. ~6.0% growth estimate.
UPDATE properties SET capital_gain_pct = 0.060, median_weekly_rent = 720  WHERE suburb = 'Camperdown'     AND property_type = 'apartment';

-- Newtown house: inner west, strong capital growth corridor. ~6.3% (same as Marrickville/Leichhardt).
UPDATE properties SET capital_gain_pct = 0.063                            WHERE suburb = 'Newtown'        AND property_type = 'house';

-- Mosman apartment: same negative trend as house. Applied -1.5% (less negative than house).
UPDATE properties SET capital_gain_pct = -0.015, median_weekly_rent = 1100 WHERE suburb = 'Mosman'        AND property_type = 'apartment';

-- Marrickville apartment: inner west gentrification. ~6.0% growth, $750/wk rent.
UPDATE properties SET capital_gain_pct = 0.060, median_weekly_rent = 750  WHERE suburb = 'Marrickville'   AND property_type = 'apartment';

-- Leichhardt apartment: inner west. ~5.5% growth, $680/wk rent.
UPDATE properties SET capital_gain_pct = 0.055, median_weekly_rent = 680  WHERE suburb = 'Leichhardt'     AND property_type = 'apartment';

-- Randwick apartment: eastern suburbs. ~5.5% growth, $850/wk rent.
UPDATE properties SET capital_gain_pct = 0.055, median_weekly_rent = 850  WHERE suburb = 'Randwick'       AND property_type = 'apartment';

-- Maroubra apartment: eastern suburbs coastal. ~5.5% growth, $750/wk rent.
UPDATE properties SET capital_gain_pct = 0.055, median_weekly_rent = 750  WHERE suburb = 'Maroubra'       AND property_type = 'apartment';

-- Paddington house: eastern suburbs prestige. ~5.5% estimate.
UPDATE properties SET capital_gain_pct = 0.055                            WHERE suburb = 'Paddington'     AND property_type = 'house';

-- Paddington apartment: eastern suburbs. ~5.0% growth, $950/wk rent.
UPDATE properties SET capital_gain_pct = 0.050, median_weekly_rent = 950  WHERE suburb = 'Paddington'     AND property_type = 'apartment';

-- Woollahra apartment: eastern suburbs prestige. ~4.5% growth, $1,000/wk rent.
UPDATE properties SET capital_gain_pct = 0.045, median_weekly_rent = 1000 WHERE suburb = 'Woollahra'      AND property_type = 'apartment';

-- Redfern house: inner city gentrification. ~6.3% growth.
UPDATE properties SET capital_gain_pct = 0.063                            WHERE suburb = 'Redfern'        AND property_type = 'house';

-- Redfern apartment: inner city. ~6.0% growth, $800/wk rent.
UPDATE properties SET capital_gain_pct = 0.060, median_weekly_rent = 800  WHERE suburb = 'Redfern'        AND property_type = 'apartment';

-- Rose Bay apartment: eastern suburbs prestige. ~4.5% growth, $1,200/wk rent.
UPDATE properties SET capital_gain_pct = 0.045, median_weekly_rent = 1200 WHERE suburb = 'Rose Bay'       AND property_type = 'apartment';

-- Bondi house: iconic eastern suburb. ~5.5% growth (high base, moderate growth).
UPDATE properties SET capital_gain_pct = 0.055                            WHERE suburb = 'Bondi'          AND property_type = 'house';

-- South Granville house: western Sydney, emerging corridor. ~5.0% growth estimate.
UPDATE properties SET capital_gain_pct = 0.050                            WHERE suburb = 'South Granville' AND property_type = 'house';

-- Blacktown apartment: western Sydney. ~5.0% growth, $550/wk rent.
UPDATE properties SET capital_gain_pct = 0.050, median_weekly_rent = 550  WHERE suburb = 'Blacktown'      AND property_type = 'apartment';

-- Epping apartment: north-west corridor. ~5.5% growth, $700/wk rent.
UPDATE properties SET capital_gain_pct = 0.055, median_weekly_rent = 700  WHERE suburb = 'Epping'         AND property_type = 'apartment';

-- Ryde house: mid-ring, employment hub. ~5.8% growth (same as townhouse).
UPDATE properties SET capital_gain_pct = 0.058                            WHERE suburb = 'Ryde'           AND property_type = 'house';

-- Ryde apartment: mid-ring. ~5.5% growth, $680/wk rent.
UPDATE properties SET capital_gain_pct = 0.055, median_weekly_rent = 680  WHERE suburb = 'Ryde'           AND property_type = 'apartment';

-- East Killara house: upper North Shore, tightly held. ~4.5% growth estimate.
UPDATE properties SET capital_gain_pct = 0.045                            WHERE suburb = 'East Killara'   AND property_type = 'house';

-- Hornsby house: upper North Shore. ~4.5% growth estimate.
UPDATE properties SET capital_gain_pct = 0.045                            WHERE suburb = 'Hornsby'        AND property_type = 'house';

-- Strathfield apartment: inner west. ~5.0% growth, $700/wk rent.
UPDATE properties SET capital_gain_pct = 0.050, median_weekly_rent = 700  WHERE suburb = 'Strathfield'    AND property_type = 'apartment';

-- Mortlake apartment: inner west/Strathfield corridor. ~5.0% growth, $720/wk rent.
UPDATE properties SET capital_gain_pct = 0.050, median_weekly_rent = 720  WHERE suburb = 'Mortlake'       AND property_type = 'apartment';
