from pydantic import BaseModel, Field
from typing import Optional

# ── Request model ─────────────────────────────────────────────────────────────
#
# Flat structure — mirrors the URL params the frontend sends to /results/residential.
# All fields optional except budget_max (the only true hard requirement).
#
# D26: land_size_sqm_min added — hard filter applied before soft scoring.

class SearchRequest(BaseModel):
    # Budget
    budget_max: int = Field(..., ge=0, description="Max purchase price AUD")

    # Property
    bedrooms_min: Optional[int]  = Field(None, ge=1)
    property_type: Optional[str] = None   # "house" | "apartment" | "townhouse" | None = any
    renovation_preference: Optional[str] = None  # None = any

    # Location
    commute_max_min: Optional[int] = Field(None, ge=1, description="Max CBD commute in minutes")

    # Land — hard filter (D26)
    land_size_sqm_min: Optional[int] = Field(None, ge=0, description="Min land size sqm — hard filter")

    # Lifestyle (future signals — accepted but optional)
    school_priority: bool = False
    noise_preference: str = "any"   # "quiet" | "moderate" | "any"
    parking_required: bool = False


# ── Response models ───────────────────────────────────────────────────────────

class PropertyResult(BaseModel):
    property_id: str
    title: str
    suburb: str
    price_display: str
    bedrooms: int
    bathrooms: int
    internal_size_sqm: int
    land_size_sqm: Optional[int] = None
    renovation_status: str
    match_score: int                            # 0–100
    highlights: list[str]
    tradeoffs: list[str]
    # Suburb trajectory — sourced from suburb_trajectories lookup table (D19, D20)
    # Displayed with source facts on result card per D24 (labels are not enough)
    suburb_trajectory: Optional[str] = None             # "rising" | "stable" | "cooling" | None
    suburb_median_price_change: Optional[float] = None  # YoY decimal e.g. 0.139 = +13.9%
    suburb_reference_period: Optional[str] = None       # e.g. "CoreLogic Q1 2026"


class SearchResponse(BaseModel):
    search_id: str
    total_candidates: int
    results: list[PropertyResult]
