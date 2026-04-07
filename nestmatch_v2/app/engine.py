"""
NestMatch matching engine.

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
    "price_fit":       0.30,
    "commute_score":   0.20,
    "size_score":      0.15,
    "transport_score": 0.15,
    "school_score":    0.10,
    "lifestyle_fit":   0.10,
}

# Size tolerance: don't hard-reject a property that's only slightly under
SIZE_TOLERANCE_SQM = 10

# Noise score thresholds (property noise_score is 0–1, higher = quieter)
NOISE_QUIET_MIN    = 0.70
NOISE_MODERATE_MIN = 0.45


# ── Stage 1: Hard filters ─────────────────────────────────────────────────────

def passes_hard_filters(prop: dict[str, Any], req: SearchRequest) -> bool:
    """
    Return False for any property that is unambiguously wrong for the buyer.
    Uses tolerance on size to avoid unfair rejections near the threshold.
    """
    # Budget — use the property's asking midpoint vs buyer's ceiling
    asking_mid = (prop["price_min"] + prop["price_max"]) / 2
    if asking_mid > req.budget.max_price:
        return False

    # Property type
    if prop["property_type"] != req.property.property_type:
        return False

    # Bedrooms
    if prop["bedrooms"] < req.property.min_bedrooms:
        return False

    # Size — allow a small tolerance so a 90 sqm property isn't binned for a
    # 95 sqm requirement; the size_score will penalise it appropriately.
    effective_min = req.property.min_internal_size_sqm - SIZE_TOLERANCE_SQM
    if prop["internal_size_sqm"] < effective_min:
        return False

    # Parking
    if req.lifestyle.parking_required and prop["parking_spaces"] < 1:
        return False

    # New build preference — only enforce when the buyer has an explicit view
    if req.property.new_build is not None:
        if prop["is_new_build"] != req.property.new_build:
            return False

    return True


# ── Stage 2: Feature scores ───────────────────────────────────────────────────

def score_price_fit(prop: dict, req: SearchRequest) -> float:
    """
    How well does the asking price sit inside the buyer's budget?
    Perfect fit = centre of the range.  Above max = 0, well below min = ~0.8.
    """
    asking = (prop["price_min"] + prop["price_max"]) / 2
    budget_mid   = (req.budget.min_price + req.budget.max_price) / 2
    budget_range = req.budget.max_price - req.budget.min_price

    if asking > req.budget.max_price:
        return 0.0

    # Distance from the ideal midpoint, normalised to half the range
    deviation = abs(asking - budget_mid)
    half_range = budget_range / 2 if budget_range > 0 else 1
    fit = max(0.0, 1.0 - (deviation / half_range) * 0.4)
    return round(min(fit, 1.0), 4)


def score_commute(prop: dict, req: SearchRequest) -> float:
    """
    Linear decay from perfect (at max/2) to 0 (at 1.5× max).
    Smooth — not a cliff at the exact threshold.
    """
    actual  = prop["commute_cbd_mins"]
    ceiling = req.location.max_commute_mins

    if actual <= ceiling * 0.5:
        return 1.0
    if actual >= ceiling * 1.5:
        return 0.0
    # Linear between 0.5× and 1.5× the ceiling
    return round(1.0 - (actual - ceiling * 0.5) / ceiling, 4)


def score_size(prop: dict, req: SearchRequest) -> float:
    """
    Perfect at 1.2× the minimum; diminishing returns above that;
    penalised below (including the tolerance band already admitted).
    """
    actual  = prop["internal_size_sqm"]
    minimum = req.property.min_internal_size_sqm
    ideal   = minimum * 1.2

    if actual >= ideal:
        return 1.0
    if actual <= minimum - SIZE_TOLERANCE_SQM:
        return 0.0
    return round((actual - (minimum - SIZE_TOLERANCE_SQM)) / (ideal - (minimum - SIZE_TOLERANCE_SQM)), 4)


def score_transport(prop: dict, req: SearchRequest) -> float:
    """
    Use the property's pre-computed transport_score, adjusted by how much
    the buyer values it.
    """
    base = prop["transport_score"]
    preference_map = {"high": 1.0, "medium": 0.7, "low": 0.4}
    weight = preference_map.get(req.location.transport_access, 0.7)
    # When the buyer values transport highly, don't inflate weak scores
    return round(base * weight + base * (1 - weight) * 0.5, 4)


def score_school(prop: dict, req: SearchRequest) -> float:
    """Pass through property score; if not a priority, neutral 0.5."""
    if not req.lifestyle.school_priority:
        return 0.5
    return round(prop["school_score"], 4)


def score_lifestyle(prop: dict, req: SearchRequest) -> float:
    """
    Blend noise preference alignment with the property's family score.
    """
    noise_score = prop["noise_score"]
    family_score = prop["lifestyle_family_score"]

    if req.lifestyle.noise_preference == "quiet":
        noise_fit = noise_score           # higher noise_score = quieter = good
    elif req.lifestyle.noise_preference == "moderate":
        # Sweet spot in the middle; penalise both extremes
        noise_fit = 1.0 - abs(noise_score - 0.6) * 2
        noise_fit = max(0.0, noise_fit)
    else:  # "any"
        noise_fit = 0.7                   # neutral

    return round((noise_fit + family_score) / 2, 4)


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
    """
    Deterministic rule-based explanation.
    Returns (highlights, tradeoffs) as plain English strings.
    """
    highlights: list[str] = []
    tradeoffs:  list[str] = []

    # ── Price ─────────────────────────────────────────────────────────────────
    asking_mid = (prop["price_min"] + prop["price_max"]) / 2
    budget_mid = (req.budget.min_price + req.budget.max_price) / 2

    if scores["price_fit"] >= 0.85:
        highlights.append(f"Asking price well within your budget")
    elif scores["price_fit"] >= 0.65:
        highlights.append(f"Price sits comfortably in your range")
    elif asking_mid < req.budget.min_price:
        highlights.append(f"Below your minimum — room to negotiate upwards")
    else:
        tradeoffs.append(f"Asking price is towards the top of your budget")

    # ── Commute ───────────────────────────────────────────────────────────────
    commute = prop["commute_cbd_mins"]
    ceiling = req.location.max_commute_mins

    if commute <= ceiling * 0.6:
        highlights.append(f"{commute} min commute — well under your {ceiling} min limit")
    elif commute <= ceiling:
        highlights.append(f"{commute} min commute to {req.location.commute_destination}")
    else:
        tradeoffs.append(f"{commute} min commute — over your {ceiling} min limit")

    # ── Size ──────────────────────────────────────────────────────────────────
    size = prop["internal_size_sqm"]
    min_size = req.property.min_internal_size_sqm

    if size >= min_size * 1.2:
        highlights.append(f"{size} sqm internal — generously above your {min_size} sqm minimum")
    elif size >= min_size:
        highlights.append(f"{size} sqm internal — meets your size requirement")
    else:
        tradeoffs.append(f"{size} sqm internal — slightly under your {min_size} sqm target")

    # ── Transport ─────────────────────────────────────────────────────────────
    dist = prop["distance_to_station_m"]
    transport_score = prop["transport_score"]

    if dist <= 400:
        highlights.append(f"{dist}m to station — excellent walkability")
    elif dist <= 700:
        highlights.append(f"{dist}m to nearest station")
    else:
        tradeoffs.append(f"{dist}m to station — likely bus-dependent")

    if transport_score >= 0.85 and req.location.transport_access == "high":
        highlights.append("Strong public transport connectivity")
    elif transport_score < 0.65 and req.location.transport_access == "high":
        tradeoffs.append("Transport access below your preference")

    # ── Schools ───────────────────────────────────────────────────────────────
    if req.lifestyle.school_priority:
        school = prop["school_score"]
        if school >= 0.80:
            highlights.append("Strong school catchment area")
        elif school >= 0.65:
            highlights.append("Reasonable school catchment")
        else:
            tradeoffs.append("School catchment weaker than ideal")

    # ── Noise / lifestyle ─────────────────────────────────────────────────────
    noise = prop["noise_score"]
    pref  = req.lifestyle.noise_preference

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

    # ── New build ─────────────────────────────────────────────────────────────
    if req.property.new_build is False and prop["is_new_build"]:
        tradeoffs.append("New build — may not suit buyers seeking established character")

    return highlights, tradeoffs


# ── Public interface ──────────────────────────────────────────────────────────

def run_search(
    candidates: list[dict],
    req: SearchRequest,
) -> tuple[list[PropertyResult], int]:
    """
    Full pipeline: filter → score → rank → explain.

    Returns (results, total_candidates_before_filter).
    """
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
            internal_size_sqm=prop["internal_size_sqm"],
            match_score=match_score,
            highlights=highlights,
            tradeoffs=tradeoffs,
        )
        results.append((match_score, result))

    # Sort descending by score
    results.sort(key=lambda x: x[0], reverse=True)
    ranked = [r for _, r in results]

    return ranked, total_candidates
