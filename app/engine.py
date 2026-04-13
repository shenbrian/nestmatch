"""
engine.py — NestMatch matching engine
Session 8: matches actual 23-column Neon schema exactly.

Actual properties columns:
  id, title, suburb, price_min, price_max, bedrooms, internal_size_sqm,
  property_type, parking_spaces, created_at, updated_at, land_size_sqm,
  development_zone, bathrooms, renovation_status, street_address,
  sales_agent, agent_phone, listing_url_rea, listing_url_domain,
  inspection_date, days_on_market, real_estate_agency

Scoring uses: price_max, bedrooms, land_size_sqm, renovation_status,
              development_zone, trajectory (from suburb_trajectories join).
No transport_score / school_score / lifestyle_score in DB.
"""

import asyncpg
from app.models import SearchRequest, MatchResult, Property, TradeoffItem, TrajectoryInfo
from typing import Optional


RESIDENTIAL_WEIGHTS = {
    "price_fit":   0.35,
    "trajectory":  0.25,
    "land":        0.20,
    "renovation":  0.12,
    "zone":        0.08,
}
assert abs(sum(RESIDENTIAL_WEIGHTS.values()) - 1.0) < 0.001

INVESTMENT_WEIGHTS = {
    "trajectory":  0.35,
    "land":        0.25,
    "price_fit":   0.20,
    "zone":        0.12,
    "renovation":  0.08,
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
            return False, f"Land {row['land_size_sqm']}sqm below minimum"
    return True, ""


def score_price_fit(price: int, budget_max: int) -> float:
    ratio = price / budget_max
    if ratio <= 0.75: return 1.0
    if ratio <= 0.85: return 0.85
    if ratio <= 0.92: return 0.70
    if ratio <= 1.00: return 0.55
    return 0.0


def score_land(land_sqm: Optional[int], req_min: Optional[int]) -> float:
    if land_sqm is None:
        return 0.55  # neutral — don't penalise missing data heavily
    if not req_min:
        if land_sqm >= 700: return 1.0
        if land_sqm >= 500: return 0.85
        if land_sqm >= 300: return 0.70
        if land_sqm >= 100: return 0.60
        return 0.50
    ratio = land_sqm / req_min
    if ratio >= 1.5: return 1.0
    if ratio >= 1.2: return 0.85
    return 0.70


def score_renovation(status: Optional[str]) -> float:
    mapping = {
        "new_build":           1.0,
        "fully_renovated":     0.85,
        "partially_renovated": 0.65,
        "original":            0.45,
    }
    return mapping.get(status, 0.60)  # neutral default — don't penalise missing


def score_zone(zone: Optional[str]) -> float:
    """Development zone as capital gain proxy."""
    if zone is None: return 0.60
    zone_upper = zone.upper()
    if zone_upper in ("R1", "MU1", "B4", "B2"): return 1.0   # high density / mixed use
    if zone_upper in ("R2",):                    return 0.80   # low density residential
    if zone_upper in ("R3",):                    return 0.90   # medium density
    if zone_upper in ("R4",):                    return 0.95   # high density residential
    return 0.60


def score_trajectory(trajectory_row: Optional[dict]) -> float:
    if not trajectory_row:
        return 0.55  # neutral default
    label = trajectory_row.get("trajectory_label", "stable")
    change = float(trajectory_row.get("median_price_change") or 0.0)
    if label == "rising":
        return min(0.75 + change * 2, 1.0)
    if label == "stable":
        return 0.60
    if label == "cooling":
        return max(0.35 + change * 2, 0.0)
    return 0.55


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
        extra = row["bedrooms"] - req.bedrooms_min
        highlights.append(f"{row['bedrooms']} beds — {extra} more than minimum")

    # Land
    if row.get("land_size_sqm"):
        sqm = row["land_size_sqm"]
        if sqm >= 700:
            highlights.append(f"{sqm}sqm land — strong capital gain potential")
        elif sqm >= 400:
            highlights.append(f"{sqm}sqm land")
        elif req.land_size_sqm_min and sqm < req.land_size_sqm_min * 1.1:
            tradeoffs.append(TradeoffItem(
                message=f"{sqm}sqm is close to your {req.land_size_sqm_min}sqm minimum",
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

    # Development zone
    zone = row.get("development_zone", "")
    if zone and zone.upper() in ("R3", "R4", "MU1", "B4"):
        highlights.append(f"{zone} zoning — development upside")

    # Trajectory
    if trajectory_row:
        label = trajectory_row.get("trajectory_label")
        change = float(trajectory_row.get("median_price_change") or 0)
        if label == "rising":
            pct = f"+{change*100:.1f}% YoY" if change else ""
            highlights.append(f"Rising suburb {pct}".strip())
        elif label == "cooling":
            pct = f"{change*100:.1f}% YoY" if change else ""
            tradeoffs.append(TradeoffItem(
                message=f"Suburb cooling {pct} — verify before committing".strip(),
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

        scores = {
            "price_fit":  score_price_fit(row["price_max"], req.budget_max),
            "trajectory": score_trajectory(traj),
            "land":       score_land(row.get("land_size_sqm"), req.land_size_sqm_min),
            "renovation": score_renovation(row.get("renovation_status")),
            "zone":       score_zone(row.get("development_zone")),
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
            school_rating=row.get("school_rating"),
            hospital_rating=row.get("hospital_rating"),
            commute_rating=row.get("commute_rating"),
            commute_mode=row.get("commute_mode"),
            commute_drive_mins=row.get("commute_drive_mins"),
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
                median_price_change=float(traj["median_price_change"]) if traj["median_price_change"] else None,
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