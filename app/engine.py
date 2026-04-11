"""
engine.py — NestMatch matching engine
Session 8: scoring weight iteration based on tester feedback

Key insight from Session 6/7 testers:
- All 5 testers cited capital gain as a primary residential motive
- price_fit was over-weighted relative to trajectory_score
- D12: residential buyers ask TWO questions simultaneously —
  "Will I love living here?" AND "Will I be better off financially?"

Revised weights:
  BEFORE (Session 7):  price_fit=0.30, trajectory=0.12, location=0.20, school=0.12, land=0.10, lifestyle=0.08, renovation=0.08
  AFTER  (Session 8):  price_fit=0.25, trajectory=0.18, location=0.18, school=0.12, land=0.12, lifestyle=0.08, renovation=0.07
  
  Change: trajectory +0.06, land +0.02, price_fit -0.05, location -0.02, renovation -0.01
  Rationale: trajectory is the most differentiated signal NestMatch has vs REA/Domain.
  Elevating it makes results feel smarter, not just cheaper.
"""

import asyncpg
from app.models import SearchRequest, MatchResult, Property, TradeoffItem, TrajectoryInfo
from typing import Optional


# ── Weights (Session 8 — revised from tester feedback) ───────────────────────

RESIDENTIAL_WEIGHTS = {
    "price_fit":    0.25,   # Was 0.30 — still important but no longer dominant
    "trajectory":   0.18,   # Was 0.12 — elevated per all-5-testers capital gain signal
    "location":     0.18,   # Was 0.20 — slight reduction to make room for trajectory
    "school":       0.12,
    "land":         0.12,   # Was 0.10 — elevated: T5 land-primary use case validates this
    "lifestyle":    0.08,
    "renovation":   0.07,   # Was 0.08 — minor reduction
}
assert abs(sum(RESIDENTIAL_WEIGHTS.values()) - 1.0) < 0.001, "Weights must sum to 1.0"

INVESTMENT_WEIGHTS = {
    "trajectory":   0.28,   # Capital gain first (D14)
    "location":     0.22,   # Within 10-15km CBD (D15 factor 1)
    "land":         0.18,   # Land scarcity (D15 factor 2)
    "price_fit":    0.15,
    "rental_proxy": 0.10,   # Station distance as rental demand proxy
    "renovation":   0.07,
}
assert abs(sum(INVESTMENT_WEIGHTS.values()) - 1.0) < 0.001, "Weights must sum to 1.0"


# ── Hard filters ──────────────────────────────────────────────────────────────

def passes_hard_filters(row: dict, req: SearchRequest) -> tuple[bool, str]:
    """Returns (passes, reason_if_not)."""
    if row["price_max"] > req.budget_max:
        return False, f"Price ${row['price']:,} exceeds budget ${req.budget_max:,}"
    if row["bedrooms"] < req.bedrooms_min:
        return False, f"{row['bedrooms']} beds below minimum {req.bedrooms_min}"
    if req.property_type and row["property_type"] != req.property_type:
        return False, f"Property type mismatch"
    # D26: land size is a hard filter — exclude entirely, do not penalise
    if req.land_size_sqm_min and row.get("land_size_sqm"):
        if row["land_size_sqm"] < req.land_size_sqm_min:
            return False, f"Land {row['land_size_sqm']}sqm below {req.land_size_sqm_min}sqm minimum"
    # Investment mode: CBD distance hard filter
    if req.mode == "investment" and req.max_km_from_cbd:
        if row.get("cbd_distance_km") and row["cbd_distance_km"] > req.max_km_from_cbd:
            return False, f"CBD distance {row['cbd_distance_km']}km exceeds {req.max_km_from_cbd}km"
    return True, ""


# ── Score functions ───────────────────────────────────────────────────────────

def score_price_fit(price: int, budget_max: int) -> float:
    """Higher score = more budget remaining = better deal relative to budget."""
    ratio = price / budget_max
    if ratio <= 0.80: return 1.0
    if ratio <= 0.90: return 0.85
    if ratio <= 0.95: return 0.70
    if ratio <= 1.00: return 0.50
    return 0.0  # over budget — caught by hard filter but defensive


def score_location(station_km: Optional[float], commute_max: Optional[int]) -> float:
    if station_km is None:
        return 0.5  # neutral if unknown
    # Commute preference informs what "close" means
    threshold = 0.5 if (commute_max and commute_max <= 20) else 0.8
    if station_km <= threshold:     return 1.0
    if station_km <= threshold*1.5: return 0.75
    if station_km <= threshold*2.5: return 0.50
    if station_km <= threshold*4:   return 0.25
    return 0.10


def score_school(in_catchment: Optional[bool]) -> float:
    if in_catchment is None: return 0.5
    return 1.0 if in_catchment else 0.0


def score_land(land_sqm: Optional[int], req_min: Optional[int]) -> float:
    if land_sqm is None: return 0.5
    if not req_min:
        # No preference — score generously for larger land
        if land_sqm >= 700: return 1.0
        if land_sqm >= 500: return 0.80
        if land_sqm >= 300: return 0.60
        return 0.40
    # With an explicit minimum (already hard-filtered, score = how much over)
    ratio = land_sqm / req_min
    if ratio >= 1.5: return 1.0
    if ratio >= 1.2: return 0.85
    return 0.70


def score_renovation(status: Optional[str]) -> float:
    mapping = {"new": 1.0, "full": 0.85, "cosmetic": 0.60, "original": 0.35}
    return mapping.get(status, 0.50)


def score_lifestyle(score: Optional[float]) -> float:
    if score is None: return 0.5
    return min(max(score / 10.0, 0.0), 1.0)


def score_trajectory(trajectory_row: Optional[dict]) -> float:
    if not trajectory_row:
        return 0.5
    label = trajectory_row.get("trajectory_label", "stable")
    change = trajectory_row.get("median_price_change", 0.0) or 0.0
    if label == "rising":
        return min(0.75 + change * 2, 1.0)   # e.g. +13.9% → 0.75 + 0.278 = 1.0
    if label == "stable":
        return 0.55
    if label == "cooling":
        return max(0.30 + change * 2, 0.0)
    return 0.50


# ── Explanation generation ────────────────────────────────────────────────────

def generate_explanation(
    row: dict,
    trajectory_row: Optional[dict],
    req: SearchRequest,
) -> tuple[list[str], list[TradeoffItem]]:
    """
    Returns (highlights, tradeoffs).
    Tradeoffs carry severity 1-3, sorted worst-first per D28.
    """
    highlights = []
    tradeoffs = []

    # Price
    saving = req.budget_max - row["price"]
    if saving >= 100_000:
        highlights.append(f"${saving:,.0f} under budget")

    # Bedrooms
    if row["bedrooms"] > req.bedrooms_min:
        highlights.append(f"{row['bedrooms']} beds — {row['bedrooms'] - req.bedrooms_min} more than minimum")

    # Station
    if row.get("station_distance_km") is not None:
        km = row["station_distance_km"]
        walk_min = round(km * 12)  # ~12 min/km walking
        if km <= 0.5:
            highlights.append(f"{walk_min}-min walk to station")
        elif km <= 1.0:
            highlights.append(f"{walk_min}-min walk to station")
        elif req.commute_max_min:
            tradeoffs.append(TradeoffItem(
                message=f"{walk_min}-min walk to station may extend commute",
                severity=2 if km <= 2.0 else 3,
            ))

    # School
    if row.get("school_catchment") is True:
        highlights.append("In target school catchment")
    elif row.get("school_catchment") is False:
        tradeoffs.append(TradeoffItem(
            message="Outside target school catchment",
            severity=2,
        ))

    # Land
    if row.get("land_size_sqm"):
        sqm = row["land_size_sqm"]
        if sqm >= 700:
            highlights.append(f"{sqm}sqm land — strong capital gain potential")
        elif req.land_size_sqm_min and sqm < req.land_size_sqm_min * 1.1:
            tradeoffs.append(TradeoffItem(
                message=f"{sqm}sqm land is close to your {req.land_size_sqm_min}sqm minimum",
                severity=1,
            ))

    # Renovation
    reno = row.get("renovation_status")
    if reno in ("full", "new"):
        highlights.append(f"{'Newly built' if reno == 'new' else 'Fully renovated'} — move-in ready")
    elif reno == "original":
        tradeoffs.append(TradeoffItem(
            message="Original condition — renovation budget likely needed",
            severity=2,
        ))

    # Trajectory
    if trajectory_row:
        label = trajectory_row.get("trajectory_label")
        change = trajectory_row.get("median_price_change", 0) or 0
        if label == "rising":
            pct = f"+{change*100:.1f}% YoY" if change else ""
            highlights.append(f"Rising suburb {pct}".strip())
        elif label == "cooling":
            pct = f"{change*100:.1f}% YoY" if change else ""
            tradeoffs.append(TradeoffItem(
                message=f"Suburb trend cooling {pct} — verify before committing".strip(),
                severity=2,
            ))

    # Sort tradeoffs: worst severity first (D28)
    tradeoffs.sort(key=lambda t: t.severity, reverse=True)

    return highlights, tradeoffs


# ── Main search function ──────────────────────────────────────────────────────

async def run_search(conn: asyncpg.Connection, req: SearchRequest) -> list[MatchResult]:
    # Fetch properties — now includes D29 actionable fields
    rows = await conn.fetch("""
        SELECT
          p.*,
          st.trajectory AS trajectory_label,
          st.median_price_change,
          st.source          AS traj_source,
          st.reference_year  AS traj_year
        FROM properties p
        LEFT JOIN suburb_trajectories st
          ON st.suburb = p.suburb
          AND st.property_type = CASE
            WHEN p.property_type = 'townhouse' THEN 'house'
            ELSE p.property_type
          END
        WHERE p.price <= $1
          AND p.bedrooms >= $2
    """, req.budget_max, req.bedrooms_min)

    weights = INVESTMENT_WEIGHTS if req.mode == "investment" else RESIDENTIAL_WEIGHTS
    results = []

    for row in rows:
        row = dict(row)

        # Hard filters
        passes, _ = passes_hard_filters(row, req)
        if not passes:
            continue

        # Trajectory sub-dict
        traj = None
        if row.get("trajectory_label"):
            traj = {
                "trajectory_label": row["trajectory_label"],
                "median_price_change": row["median_price_change"],
                "source": row.get("traj_source"),
                "year": row.get("traj_year"),
            }

        # Component scores
        scores = {
            "price_fit":    score_price_fit(row["price_max"], req.budget_max),
            "trajectory":   score_trajectory(traj),
            "location":     score_location(row.get("station_distance_km"), req.commute_max_min),
            "school":       score_school(row.get("school_catchment")),
            "land":         score_land(row.get("land_size_sqm"), req.land_size_sqm_min),
            "lifestyle":    score_lifestyle(row.get("suburb_lifestyle_score")),
            "renovation":   score_renovation(row.get("renovation_status")),
            "rental_proxy": score_location(row.get("station_distance_km"), None),  # investment proxy
        }

        # Weighted score
        total = sum(weights.get(k, 0) * v for k, v in scores.items())
        total = round(total * 100, 1)  # 0–100

        # Explanations
        highlights, tradeoffs = generate_explanation(row, traj, req)

        # Build Property — D29 actionable fields included
        prop = Property(
            id=row["id"],
            suburb=row["suburb"],
            property_type=row["property_type"],
            bedrooms=row["bedrooms"],
            bathrooms=row.get("bathrooms"),
            price=row["price_max"],
            land_size_sqm=row.get("land_size_sqm"),
            station_distance_km=row.get("station_distance_km"),
            school_catchment=row.get("school_catchment"),
            renovation_status=row.get("renovation_status"),
            development_zone=row.get("development_zone"),
            # D29 actionable
            street_address=row.get("street_address"),
            agent_name=row.get("agent_name"),
            agent_phone=row.get("agent_phone"),
            agent_email=row.get("agent_email"),
            listing_url_rea=row.get("listing_url_rea"),
            listing_url_domain=row.get("listing_url_domain"),
            inspection_date=row.get("inspection_date"),
            days_on_market=row.get("days_on_market"),
        )

        traj_info = None
        if traj:
            traj_info = TrajectoryInfo(
                label=traj["trajectory_label"],
                median_price_change=traj["median_price_change"],
                source=traj.get("source"),
                year=str(traj["year"]) if traj.get("year") else None,
            )

        results.append(MatchResult(
            property=prop,
            score=total,
            highlights=highlights,
            tradeoffs=tradeoffs,
            trajectory=traj_info,
        ))

    # Sort descending by score
    results.sort(key=lambda r: r.score, reverse=True)

    # Apply threshold (D17 — 60% default, adjustable on results page)
    results = [r for r in results if r.score >= 60.0]

    return results[:20]  # cap at 20 results