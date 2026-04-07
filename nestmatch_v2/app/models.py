from pydantic import BaseModel, Field
from typing import Optional
from uuid import UUID


# ── Request models ────────────────────────────────────────────────────────────

class BudgetInput(BaseModel):
    min_price: int = Field(..., ge=0)
    max_price: int = Field(..., ge=0)


class PropertyInput(BaseModel):
    property_type: str                          # "apartment" | "house" | "townhouse"
    min_bedrooms: int = Field(..., ge=1)
    min_internal_size_sqm: int = Field(..., ge=0)
    new_build: Optional[bool] = None            # None = no preference


class LocationInput(BaseModel):
    commute_destination: str = "Sydney CBD"
    max_commute_mins: int = Field(..., ge=1)
    transport_access: str = "medium"            # "low" | "medium" | "high"


class LifestyleInput(BaseModel):
    school_priority: bool = False
    noise_preference: str = "any"               # "quiet" | "moderate" | "any"
    parking_required: bool = False


class SearchRequest(BaseModel):
    budget: BudgetInput
    property: PropertyInput
    location: LocationInput
    lifestyle: LifestyleInput


# ── Response models ───────────────────────────────────────────────────────────

class PropertyResult(BaseModel):
    property_id: str
    title: str
    suburb: str
    price_display: str
    bedrooms: int
    internal_size_sqm: int
    match_score: int                            # 0–100
    highlights: list[str]
    tradeoffs: list[str]


class SearchResponse(BaseModel):
    search_id: str
    total_candidates: int
    results: list[PropertyResult]
