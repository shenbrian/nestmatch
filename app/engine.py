"""
NestMatch matching engine v2.

Three-stage pipeline:
  1. Hard filters  — eliminates non-viable candidates
  2. Feature scores — normalised 0–1 scores per dimension
  3. Weighted score — single match_score (0–100) + explanation
"""

from __future__ import annotations
from typing import Any
from app.models import SearchRequest, PropertyResult


# ── Weights (must sum to 1.0) ─────────────────────────────────────────────────

WEIGHTS = {
    "price_fit":       0.28,
    "commute_score":   0.20,
    "size_score":      0.14,
    "transport_score": 0.14,
    "school_score":    0.10,
    "bathroom_score":  0.07,
    "lifestyle_fit":   0.07,
}

SIZE_TOLERANCE_SQM  = 10
NOISE_QUIET_MIN     = 0.70
NOISE_MODERATE_MIN  = 0.45

RENOVATION_BOOST = {
    "new_build":           0.10,
    "fully_renovated":     0.08,
    "partially_renovated": 0.03,
    "original":            0.00,
}


# ── Stage 1: Hard filters ─────────────────────────────────────────────────────

def passes_hard_filters(prop: dict[str, Any], req: SearchRequest) -> bool:
    asking_mid = (prop["price_min"] + prop["price_max"]) / 2
    if asking_mid > req.budget.max_price:
        return False
    if prop["property_type"] != req.property.property_type:
        return False
    if prop["bedrooms"] < req.property.min_bedrooms:
        return False
    if prop["bathrooms"] < req.property.min_bathrooms:
        return False
    effective_min = req.property.min_internal_size_sqm - SIZE_TOLERANCE_SQM
    if prop["internal_size_sqm"] < effective_min:
        return False
    if req.lifestyle.parking_required and prop["parking_spaces"] < 1:
        return False
    if req.property.renovation_preference is not None:
        if prop["renovation_status"] != req.property.renovation_preference:
            return False
    return True


# ── Stage 2: Feature scores ───────────────────────────────────────────────────

def score_price_fit(prop: dict, req: SearchRequest) -> float:
    asking       = (prop["price_min"] + prop["price_max"]) / 2
    budget_mid   = (req.budget.min_price + req.budget.max_price) / 2
    budget_range = req.budget.max_price - req.budget.min_price
    if asking > req.budget.max_price:
        return 0.0
    deviation  = abs(asking - budget_mid)
    half_range = budget_range / 2 if budget_range > 0 else 1
    fit = max(0.0, 1.0 - (deviation / half_range) * 0.4)
    return round(min(fit, 1.0), 4)


def score_commute(prop: dict, req: SearchRequest) -> float:
    actual  = prop["commute_cbd_mins"]
    ceiling = req.location.max_commute_mins
    if actual <= ceiling * 0.5:
        return 1.0
    if actual >= ceiling * 1.5:
        return 0.0
    return round(1.0 - (actual - ceiling * 0.5) / ceiling, 4)


def score_size(prop: dict, req: SearchRequest) -> float:
    actual  = prop["internal_size_sqm"]
    minimum = req.property.min_internal_size_sqm
    ideal   = minimum * 1.2
    if actual >= ideal:
        return 1.0
    if actual <= minimum - SIZE_TOLERANCE_SQM:
        return 0.0
    return round((actual - (minimum - SIZE_TOLERANCE_SQM)) / (ideal - (minimum - SIZE_TOLERANCE_SQM)), 4)


def score_transport(prop: dict, req: SearchRequest) -> float:
    base = prop["transport_score"]
    preference_map = {"high": 1.0, "medium": 0.7, "low": 0.4}
    weight = preference_map.get(req.location.transport_access, 0.7)
    return round(base * weight + base * (1 - weight) * 0.5, 4)


def score_school(prop: dict, req: SearchRequest) -> float:
    if not req.lifestyle.school_priority:
        return 0.5
    return round(prop["school_score"], 4)


def score_bathrooms(prop: dict, req: SearchRequest) -> float:
    """Small boost when property has more bathrooms than buyer's minimum."""
    actual  = prop["bathrooms"]
    minimum = req.property.min_bathrooms
    if actual < minimum:
        return 0.0
    if actual == minimum:
        return 0.7       # meets requirement — decent base score
    if actual == minimum + 1:
        return 0.9       # one extra — good
    return 1.0           # two or more extra


def score_lifestyle(prop: dict, req: SearchRequest) -> float:
    """
    Blends noise preference, family score, and renovation status.
    Noise and suburb_lifestyle_score are optional — falls back gracefully.
    """
    noise_score  = prop.get("noise_score")
    family_score = prop.get("suburb_lifestyle_score")

    # Renovation boost (always present)
    reno  = prop.get("renovation_status", "original")
    boost = RENOVATION_BOOST.get(reno, 0.0)

    components = []

    if noise_score is not None:
        pref = req.lifestyle.noise_preference
        if pref == "quiet":
            noise_fit = noise_score
        elif pref == "moderate":
            noise_fit = max(0.0, 1.0 - abs(noise_score - 0.6) * 2)
        else:
            noise_fit = 0.7
        components.append(noise_fit)

    if family_score is not None:
        components.append(family_score)

    if not components:
        base = 0.6      # neutral when no optional data provided
    else:
        base = sum(components) / len(components)

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
    tradeoffs:  list[str] = []

    # Price
    asking_mid = (prop["price_min"] + prop["price_max"]) / 2
    if scores["price_fit"] >= 0.85:
        highlights.append("Asking price well within your budget")
    elif scores["price_fit"] >= 0.65:
        highlights.append("Price sits comfortably in your range")
    elif asking_mid < req.budget.min_price:
        highlights.append("Below your minimum — room to negotiate upwards")
    else:
        tradeoffs.append("Asking price is towards the top of your budget")

    # Commute
    commute = prop["commute_cbd_mins"]
    ceiling = req.location.max_commute_mins
    if commute <= ceiling * 0.6:
        highlights.append(f"{commute} min commute — well under your {ceiling} min limit")
    elif commute <= ceiling:
        highlights.append(f"{commute} min commute to {req.location.commute_destination}")
    else:
        tradeoffs.append(f"{commute} min commute — over your {ceiling} min limit")

    # Size
    size     = prop["internal_size_sqm"]
    min_size = req.property.min_internal_size_sqm
    if size >= min_size * 1.2:
        highlights.append(f"{size} sqm internal — generously above your {min_size} sqm minimum")
    elif size >= min_size:
        highlights.append(f"{size} sqm internal — meets your size requirement")
    else:
        tradeoffs.append(f"{size} sqm internal — slightly under your {min_size} sqm target")

    # Bathrooms
    actual_bath = prop["bathrooms"]
    min_bath    = req.property.min_bathrooms
    if actual_bath > min_bath:
        highlights.append(f"{actual_bath} bathrooms — above your {min_bath} minimum")
    elif actual_bath == min_bath:
        highlights.append(f"{actual_bath} bathroom{'s' if actual_bath > 1 else ''} — meets your requirement")

    # Transport
    dist = prop.get("distance_to_station_m")
    if dist is None:
        tradeoffs.append("No nearby train station - bus or car dependent")
    elif dist <= 400:
        highlights.append(f"{dist}m to station — excellent walkability")
    elif dist <= 700:
        highlights.append(f"{dist}m to nearest station")
    else:
        tradeoffs.append(f"{dist}m to station — likely bus-dependent")

    if prop["transport_score"] >= 0.85 and req.location.transport_access == "high":
        highlights.append("Strong public transport connectivity")
    elif prop["transport_score"] < 0.65 and req.location.transport_access == "high":
        tradeoffs.append("Transport access below your preference")

    # Schools
    if req.lifestyle.school_priority:
        school = prop["school_score"]
        if school >= 0.80:
            highlights.append("Strong school catchment area")
        elif school >= 0.65:
            highlights.append("Reasonable school catchment")
        else:
            tradeoffs.append("School catchment weaker than ideal")

    # Renovation
    reno = prop.get("renovation_status", "original")
    if reno == "fully_renovated":
        highlights.append("Fully renovated — move-in ready")
    elif reno == "new_build":
        highlights.append("New build — everything fresh")
    elif reno == "partially_renovated":
        highlights.append("Partially renovated — kitchen or bathrooms updated")
    elif reno == "original":
        tradeoffs.append("Original condition — may need updating")

    # Noise (optional)
    noise = prop.get("noise_score")
    if noise is not None:
        pref = req.lifestyle.noise_preference
        if pref == "quiet":
            if noise >= NOISE_QUIET_MIN:
                highlights.append("Quiet residential street environment")
            elif noise < NOISE_MODERATE_MIN:
                tradeoffs.append("Busier street environment — likely some noise")
            else:
                tradeoffs.append("Moderate street noise — not fully quiet")
        elif pref == "moderate":
            if NOISE_MODERATE_MIN <= noise <= NOISE_QUIET_MIN:
                highlights.append("Balanced street energy — not too quiet, not too busy")

    # Development zone warning
    zone = prop.get("development_zone", "")
    if zone and any(x in zone for x in ["R4", "B4", "B6", "High Density", "Mixed Use"]):
        tradeoffs.append(f"Development zone: {zone} — future high-density possible nearby")

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
            "price_fit":       score_price_fit(prop, req),
            "commute_score":   score_commute(prop, req),
            "size_score":      score_size(prop, req),
            "transport_score": score_transport(prop, req),
            "school_score":    score_school(prop, req),
            "bathroom_score":  score_bathrooms(prop, req),
            "lifestyle_fit":   score_lifestyle(prop, req),
        }

        match_score = compute_match_score(scores)
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
            internal_size_sqm=prop["internal_size_sqm"],
            renovation_status=prop.get("renovation_status", "original"),
            match_score=match_score,
            highlights=highlights,
            tradeoffs=tradeoffs,
        )
        results.append((match_score, result))

    results.sort(key=lambda x: x[0], reverse=True)
    ranked = [r for _, r in results]
    return ranked, total_candidates
