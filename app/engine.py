"""
engine.py — NestMatch matching engine
Session 8: all column names corrected to match actual Neon schema

Actual Neon properties columns:
  id, title, suburb, price_min, price_max, bedrooms, internal_size_sqm,
  property_type, parking_spaces, created_at, updated_at, land_size_sqm,
  development_zone, bathrooms, renovation_status, street_address,
  real_estate_agency, sales_agent, agent_phone, listing_url_rea,
  listing_url_domain, inspection_date, days_on_market,
  commute_cbd_mins, distance_to_station_m, distance_to_bus_stop_m,
  distance_to_hospital_m, transport_score, school_score,
  noise_score, suburb_lifestyle_score

Actual suburb_trajectories columns:
  suburb, property_type, trajectory, median_price_change,
  transaction_volume, data_source, reference_period, updated_at
"""

import asyncpg
from app.models import SearchRequest, MatchResult, Property, TradeoffItem, TrajectoryInfo
from typing import Optional


RESIDENTIAL_WEIGHTS = {
    "price_fit":    0.25,
    "trajectory":   0.18,
    "location":     0.18,
    "school":       0.12,
    "land":         0.12,
    "lifestyle":    0.08,
    "renovation":   0.07,
}
assert abs(sum(RESIDENTIAL_WEIGHTS.values()) - 1.0) < 0.001

INVESTMENT_WEIGHTS = {
    "trajectory":   0.28,
    "location":     0.22,
    "land":         0.18,
    "price_fit":    0.15,
    "rental_proxy": 0.10,
    "renovation":   0.07,
}
assert abs(sum(INVESTMENT_WEIGHTS.values()) - 1.0) < 0.001


def passes_hard_filters(row: dict, req: SearchRequest) -> tuple[bool, str]:
    if row["price_max"] > req.budget_max:
        return False, f"Price ${row['price_max']:,} exceeds budget ${req.budget_max:,}"
    if row["bedrooms"] < req.bedrooms_min:
        return False, f"{row['bedrooms']} beds below minimum {req.bedrooms_min}"
    if req.property_type and row["property_type"] != req.property_type:
        return False, "Property type mismatch"
    if req.land_size_sqm_min and row.get("land_size_sqm"):
        if row["land_size_sqm"] < req.land_size_sqm_min:
            return False, f"Land {row['land_size_sqm']}sqm below {req.land_size_sqm_min}sqm minimum"
    return True, ""


def score_price_fit(price: int, budget_max: int) -> float:
    ratio = price / budget_max
    if ratio <= 0.80: return 1.0
    if ratio <= 0.90: return 0.85
    if ratio <= 0.95: return 0.70
    if ratio <= 1.00: return 0.50
    return 0.0


def score_location(transport_score: Optional[float], commute_max: Optional[int]) -> float:
    """Use transport_score (0-1) from schema as location proxy."""
    if transport_score is None:
        return 0.5
    # Higher transport score = better location
    if transport_score >= 0.8: return 1.0
    if transport_score >= 0.6: return 0.75
    if transport_score >= 0.4: return 0.55
    if transport_score >= 0.2: return 0.35
    return 0.15


def score_school(school_score: Optional[float]) -> float:
    """Use school_score (0-1) from schema."""
    if school_score is None: return 0.5
    return min(max(school_score, 0.0), 1.0)


def score_land(land_sqm: Optional[int], req_min: Optional[int]) -> float:
    if land_sqm is None: return 0.5
    if not req_min:
        if land_sqm >= 700: return 1.0
        if land_sqm >= 500: return 0.80
        if land_sqm >= 300: return 0.60
        return 0.40
    ratio = land_sqm / req_min
    if ratio >= 1.5: return 1.0
    if ratio >= 1.2: return 0.85
    return 0.70


def score_renovation(status: Optional[str]) -> float:
    mapping = {
        "new_build":           1.0,
        "fully_renovated":     0.85,
        "partially_renovated": 0.60,
        "original":            0.35,
    }
    return mapping.get(status, 0.50)


def score_lifestyle(score: Optional[float]) -> float:
    if score is None: return 0.5
    return min(max(score, 0.0), 1.0)


def score_trajectory(trajectory_row: Optional[dict]) -> float:
    if not trajectory_row:
        return 0.5
    label = trajectory_row.get("trajectory_label", "stable")
    change = trajectory_row.get("median_price_change", 0.0) or 0.0
    if label == "rising":
        return min(0.75 + change * 2, 1.0)
    if label == "stable":
        return 0.55
    if label == "cooling":
        return max(0.30 + change * 2, 0.0)
    return 0.50


def generate_explanation(
    row: dict,
    trajectory_row: Optional[dict],
    req: SearchRequest,
) -> tuple[list[str], list[TradeoffItem]]:
    highlights = []
    tradeoffs = []

    # Price
    saving = req.budget_max - row["price_max"]
    if saving >= 100_000:
        highlights.append(f"${saving:,.0f} under budget")

    # Bedrooms
    if row["bedrooms"] > req.bedrooms_min:
        highlights.append(f"{row['bedrooms']} beds — {row['bedrooms'] - req.bedrooms_min} more than minimum")

    # Transport
    transport = row.get("transport_score")
    if transport is not None:
        if transport >= 0.8:
            highlights.append("Excellent transport access")
        elif transport <= 0.3 and req.commute_max_min:
            tradeoffs.append(TradeoffItem(
                message="Limited public transport — commute may be longer than preferred",
                severity=2,
            ))

    # School
    school = row.get("school_score")
    if school is not None:
        if school >= 0.8:
            highlights.append("Strong school catchment score")
        elif school <= 0.3:
            tradeoffs.append(TradeoffItem(
                message="Below average school catchment score for this area",
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
    if reno in ("fully_renovated", "new_build"):
        highlights.append(f"{'Newly built' if reno == 'new_build' else 'Fully renovated'} — move-in ready")
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

    tradeoffs.sort(key=lambda t: t.severity, reverse=True)
    return highlights, tradeoffs


async def run_search(conn: asyncpg.Connection, req: SearchRequest) -> list[MatchResult]:
    rows = await conn.fetch("""
        SELECT
          p.*,
          st.trajectory        AS trajectory_label,
          st.median_price_change,
          st.data_source       AS traj_source,
          st.reference_period  AS traj_year
        FROM properties p
        LEFT JOIN suburb_trajectories st
          ON st.suburb = p.suburb
          AND st.property_type = CASE
            WHEN p.property_type = 'townhouse' THEN 'house'
            ELSE p.property_type
          END
        WHERE p.price_max <= $1
          AND p.bedrooms >= $2
    """, req.budget_max, req.bedrooms_min)

    weights = INVESTMENT_WEIGHTS if req.mode == "investment" else RESIDENTIAL_WEIGHTS
    results = []

    for row in rows:
        row = dict(row)

        passes, _ = passes_hard_filters(row, req)
        if not passes:
            continue

        traj = None
        if row.get("trajectory_label"):
            traj = {
                "trajectory_label":    row["trajectory_label"],
                "median_price_change": row["median_price_change"],
                "source":              row.get("traj_source"),
                "year":                row.get("traj_year"),
            }

        transport_score = row.get("transport_score")
        school_score = row.get("school_score")

        scores = {
            "price_fit":    score_price_fit(row["price_max"], req.budget_max),
            "trajectory":   score_trajectory(traj),
            "location":     score_location(transport_score, req.commute_max_min),
            "school":       score_school(school_score),
            "land":         score_land(row.get("land_size_sqm"), req.land_size_sqm_min),
            "lifestyle":    score_lifestyle(row.get("suburb_lifestyle_score")),
            "renovation":   score_renovation(row.get("renovation_status")),
            "rental_proxy": score_location(transport_score, None),
        }

        total = sum(weights.get(k, 0) * v for k, v in scores.items())
        total = round(total * 100, 1)

        highlights, tradeoffs = generate_explanation(row, traj, req)

        prop = Property(
            id=row["id"],
            suburb=row["suburb"],
            property_type=row["property_type"],
            bedrooms=row["bedrooms"],
            bathrooms=row.get("bathrooms"),
            price=row["price_max"],
            land_size_sqm=row.get("land_size_sqm"),
            renovation_status=row.get("renovation_status"),
            development_zone=row.get("development_zone"),
            street_address=row.get("street_address"),
            agent_name=row.get("sales_agent"),
            agent_phone=row.get("agent_phone"),
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

    results.sort(key=lambda r: r.score, reverse=True)
    results = [r for r in results if r.score >= 60.0]
    return results[:20]