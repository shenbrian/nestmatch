"""
NestMatch matching engine v2.1

Three-stage pipeline:
  1. Hard filters  — eliminates non-viable candidates
  2. Feature scores — normalised 0–1 scores per dimension
  3. Weighted score — single match_score (0–100) + explanation

D26: land_size_sqm_min is a hard filter — properties below minimum are excluded
     entirely, not penalised in soft scoring.
"""

from __future__ import annotations
from typing import Any
from app.models import SearchRequest, PropertyResult


# ── Weights (must sum to 1.0) ─────────────────────────────────────────────────

WEIGHTS = {
    "price_fit":        0.26,
    "commute_score":    0.20,
    "size_score":       0.13,
    "transport_score":  0.13,
    "school_score":     0.10,
    "bathroom_score":   0.06,
    "lifestyle_fit":    0.06,
    "trajectory_score": 0.06,
}

NOISE_QUIET_MIN    = 0.70
NOISE_MODERATE_MIN = 0.45

RENOVATION_BOOST = {
    "new_build":           0.10,
    "fully_renovated":     0.08,
    "partially_renovated": 0.03,
    "original":            0.00,
}

TRAJECTORY_SCORES = {
    "rising":  1.0,
    "stable":  0.6,
    "cooling": 0.2,
}


# ── Stage 1: Hard filters ─────────────────────────────────────────────────────

def passes_hard_filters(prop: dict[str, Any], req: SearchRequest) -> bool:
    # Budget — price_max must be within budget (mid-point check)
    asking_mid = (prop["price_min"] + prop["price_max"]) / 2
    if asking_mid > req.budget_max:
        return False

    # Property type — if specified, must match exactly
    if req.property_type and prop["property_type"] != req.property_type:
        return False

    # Bedrooms
    if req.bedrooms_min and prop["bedrooms"] < req.bedrooms_min:
        return False

    # Parking
    if req.parking_required and prop.get("parking_spaces", 0) < 1:
        return False

    # Renovation — if specified, must match exactly
    if req.renovation_preference and prop.get("renovation_status") != req.renovation_preference:
        return False

    # Land size — D26: hard filter, excluded entirely not penalised
    if req.land_size_sqm_min is not None:
        land = prop.get("land_size_sqm")
        if land is None or land < req.land_size_sqm_min:
            return False

    return True


# ── Stage 2: Feature scores ───────────────────────────────────────────────────

def score_price_fit(prop: dict, req: SearchRequest) -> float:
    asking = (prop["price_min"] + prop["price_max"]) / 2
    budget = req.budget_max
    if asking > budget:
        return 0.0
    # Best score when asking is ~80% of budget, score degrades toward 0% or 100%
    ratio = asking / budget
    if ratio <= 0.80:
        return round(0.85 + ratio * 0.15 / 0.80, 4)
    return round(1.0 - (ratio - 0.80) * 0.75, 4)


def score_commute(prop: dict, req: SearchRequest) -> float:
    actual = prop["commute_cbd_mins"]
    if not req.commute_max_min:
        return 0.7  # neutral when no preference given
    ceiling = req.commute_max_min
    if actual <= ceiling * 0.5:
        return 1.0
    if actual >= ceiling * 1.5:
        return 0.0
    return round(1.0 - (actual - ceiling * 0.5) / ceiling, 4)


def score_size(prop: dict, req: SearchRequest) -> float:
    actual = prop["internal_size_sqm"]
    # Without a minimum set, treat any reasonably sized property as neutral
    return min(1.0, round(actual / 100 * 0.6, 4)) if actual else 0.5


def score_transport(prop: dict, req: SearchRequest) -> float:
    base = prop.get("transport_score") or 0.5
    return round(float(base), 4)


def score_school(prop: dict, req: SearchRequest) -> float:
    if not req.school_priority:
        return 0.5
    school = prop.get("school_score")
    if school is None:
        return 0.5
    return round(float(school), 4)


def score_bathrooms(prop: dict, req: SearchRequest) -> float:
    actual = prop.get("bathrooms", 1)
    # No minimum specified — give full credit at 2+, decent credit at 1
    if actual >= 2:
        return 1.0
    return 0.7


def score_trajectory(prop: dict, req: SearchRequest) -> float:
    trajectory = prop.get("suburb_trajectory")
    return TRAJECTORY_SCORES.get(trajectory, 0.5)


def score_lifestyle(prop: dict, req: SearchRequest) -> float:
    noise_score  = prop.get("noise_score")
    family_score = prop.get("suburb_lifestyle_score")
    reno         = prop.get("renovation_status", "original")
    boost        = RENOVATION_BOOST.get(reno, 0.0)

    components = []

    if noise_score is not None:
        pref = req.noise_preference
        if pref == "quiet":
            noise_fit = float(noise_score)
        elif pref == "moderate":
            noise_fit = max(0.0, 1.0 - abs(float(noise_score) - 0.6) * 2)
        else:
            noise_fit = 0.7
        components.append(noise_fit)

    if family_score is not None:
        components.append(float(family_score))

    base = sum(components) / len(components) if components else 0.6
    return round(min(1.0, base + boost), 4)


# ── Stage 3: Weighted score ───────────────────────────────────────────────────

def compute_match_score(scores: dict[str, float]) -> int:
    total = sum(WEIGHTS[k] * scores[k] for k in WEIGHTS)
    return round(total * 100)


# ── Explanation layer ─────────────────────────────────────────────────────────

def generate_explanation(
    prop: dict,
    req: SearchRequest,
    scores: dict[str, float],
) -> tuple[list[str], list[str]]:
    highlights: list[str] = []
    tradeoffs_raw: list[tuple[int, str]] = []  # (severity 1–3, text)

    def hi(text: str) -> None:
        highlights.append(text)

    def td(severity: int, text: str) -> None:
        tradeoffs_raw.append((severity, text))

    # ── Price ──────────────────────────────────────────────────────────────
    asking_mid = (prop["price_min"] + prop["price_max"]) / 2
    budget     = req.budget_max
    if scores["price_fit"] >= 0.85:
        hi("Asking price well within your budget")
    elif scores["price_fit"] >= 0.65:
        hi("Price sits comfortably in your range")
    elif asking_mid > budget:
        td(3, f"Price guide above your ${budget // 1000:,}k budget")

    # ── Commute ────────────────────────────────────────────────────────────
    commute = prop["commute_cbd_mins"]
    if req.commute_max_min:
        ceiling = req.commute_max_min
        if commute <= ceiling * 0.6:
            hi(f"{commute}-min commute — well under your {ceiling}-min limit")
        elif commute <= ceiling:
            hi(f"{commute}-min commute to CBD")
        else:
            over = commute - ceiling
            td(3 if over >= 15 else 2 if over >= 8 else 1,
               f"{commute}-min commute exceeds your {ceiling}-min preference")
    else:
        if commute <= 30:
            hi(f"{commute}-min commute to CBD")

    # ── Transport ──────────────────────────────────────────────────────────
    dist = prop.get("distance_to_station_m")
    if dist is not None:
        if dist <= 400:
            hi(f"{dist}m to train station — excellent walkability")
        elif dist <= 750:
            hi(f"{dist}m walk to nearest station")
        else:
            td(1, f"{dist}m to station — likely bus-dependent for some trips")
    else:
        td(1, "No train station data — confirm transport access independently")

    # ── Bedrooms ───────────────────────────────────────────────────────────
    if req.bedrooms_min:
        actual_beds = prop["bedrooms"]
        if actual_beds > req.bedrooms_min:
            hi(f"{actual_beds} bedrooms — above your {req.bedrooms_min}+ requirement")
        elif actual_beds == req.bedrooms_min:
            hi(f"{actual_beds} bedrooms — meets your requirement")
        else:
            td(3, f"{actual_beds} bedrooms — below your {req.bedrooms_min}+ requirement")

    # ── Land size ─────────────────────────────────────────────────────────
    # (Only properties that passed the hard filter appear here — so if
    #  land_size_sqm_min was set, every surviving property already meets it.)
    land = prop.get("land_size_sqm")
    if land and req.land_size_sqm_min:
        surplus = land - req.land_size_sqm_min
        if surplus >= 200:
            hi(f"{land:,} sqm land — generously above your {req.land_size_sqm_min:,} sqm minimum")
        else:
            hi(f"{land:,} sqm land — meets your {req.land_size_sqm_min:,} sqm minimum")

    # ── Schools ────────────────────────────────────────────────────────────
    if req.school_priority:
        school = prop.get("school_score") or 0
        if school >= 0.80:
            hi("Strong school catchment area")
        elif school >= 0.65:
            hi("Reasonable school catchment")
        else:
            td(2, "School catchment weaker than ideal")

    # ── Renovation ─────────────────────────────────────────────────────────
    reno = prop.get("renovation_status", "original")
    if reno == "fully_renovated":
        hi("Fully renovated — move-in ready")
    elif reno == "new_build":
        hi("New build — everything fresh")
    elif reno == "partially_renovated":
        hi("Partially renovated — kitchen or bathrooms updated")
    elif reno == "original":
        td(1, "Original condition — may need updating")

    # ── Noise ──────────────────────────────────────────────────────────────
    noise = prop.get("noise_score")
    if noise is not None and req.noise_preference != "any":
        if req.noise_preference == "quiet":
            if float(noise) >= NOISE_QUIET_MIN:
                hi("Quiet residential street environment")
            elif float(noise) < NOISE_MODERATE_MIN:
                td(2, "Busier street environment — noise likely")
            else:
                td(1, "Moderate street noise — not fully quiet")
        elif req.noise_preference == "moderate":
            if NOISE_MODERATE_MIN <= float(noise) <= NOISE_QUIET_MIN:
                hi("Balanced street energy — not too quiet, not too busy")

    # ── Trajectory ─────────────────────────────────────────────────────────
    trajectory = prop.get("suburb_trajectory")
    if trajectory == "rising":
        hi(f"{prop['suburb']} prices trending upward")
    elif trajectory == "cooling":
        td(2, f"{prop['suburb']} suburb prices under downward pressure")
    elif trajectory == "stable":
        hi(f"{prop['suburb']} prices holding steady")

    # ── Development zone ────────────────────────────────────────────────────
    zone = prop.get("development_zone", "")
    if zone and any(x in zone for x in ["R4", "B4", "B6", "High Density", "Mixed Use"]):
        td(1, f"Development zone: {zone} — future high-density possible nearby")

    # Sort tradeoffs by severity (worst first) per D28
    tradeoffs_raw.sort(key=lambda x: x[0], reverse=True)
    tradeoffs = [t for _, t in tradeoffs_raw]

    return highlights, tradeoffs


# ── Public interface ──────────────────────────────────────────────────────────

def run_search(
    candidates: list[dict],
    req: SearchRequest,
) -> tuple[list[PropertyResult], int]:
    total_candidates = len(candidates)
    passed = [p for p in candidates if passes_hard_filters(p, req)]

    results: list[tuple[int, PropertyResult]] = []

    for prop in passed:
        scores = {
            "price_fit":        score_price_fit(prop, req),
            "commute_score":    score_commute(prop, req),
            "size_score":       score_size(prop, req),
            "transport_score":  score_transport(prop, req),
            "school_score":     score_school(prop, req),
            "bathroom_score":   score_bathrooms(prop, req),
            "lifestyle_fit":    score_lifestyle(prop, req),
            "trajectory_score": score_trajectory(prop, req),
        }

        match_score          = compute_match_score(scores)
        highlights, tradeoffs = generate_explanation(prop, req, scores)

        price_lo = prop["price_min"] // 1000
        price_hi = prop["price_max"] // 1000
        price_display = f"${price_lo:,}k – ${price_hi:,}k"

        result = PropertyResult(
            property_id=prop["id"],
            title=prop["title"],
            suburb=prop["suburb"],
            price_display=price_display,
            bedrooms=prop["bedrooms"],
            bathrooms=prop["bathrooms"],
            internal_size_sqm=prop.get("internal_size_sqm", 0),
            land_size_sqm=prop.get("land_size_sqm"),
            renovation_status=prop.get("renovation_status", "original"),
            match_score=match_score,
            highlights=highlights,
            tradeoffs=tradeoffs,
            suburb_trajectory=prop.get("suburb_trajectory"),
            suburb_median_price_change=prop.get("suburb_median_price_change"),
            suburb_reference_period=prop.get("suburb_reference_period"),
        )
        results.append((match_score, result))

    results.sort(key=lambda x: x[0], reverse=True)
    ranked = [r for _, r in results]
    return ranked, total_candidates
