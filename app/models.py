"""
models.py — NestMatch data models
Session 8: added D29 actionable fields + outcome tracking
"""

from pydantic import BaseModel, Field
from typing import Optional, Literal
from datetime import date
from uuid import UUID


# ── Search request (flat model — established Session 7) ──────────────────────

class SearchRequest(BaseModel):
    budget_max: int
    bedrooms_min: int
    commute_max_min: Optional[int] = None
    property_type: Optional[str] = None       # house | apartment | townhouse | unit
    land_size_sqm_min: Optional[int] = None   # D26 hard filter

    # Investment-mode fields
    mode: Literal["residential", "investment"] = "residential"
    investment_priority: Optional[str] = None  # capital_gain | balanced | rental_income
    max_km_from_cbd: Optional[float] = None
    holding_period: Optional[str] = None       # short | medium | long


# ── Property (DB row → API response) ─────────────────────────────────────────

class Property(BaseModel):
    id: UUID
    suburb: str
    property_type: str
    bedrooms: int
    bathrooms: Optional[int] = None
    price: int
    land_size_sqm: Optional[int] = None
    station_distance_km: Optional[float] = None
    school_catchment: Optional[bool] = None
    renovation_status: Optional[str] = None    # original | cosmetic | full | new
    development_zone: Optional[str] = None
    noise_score: Optional[float] = None
    suburb_lifestyle_score: Optional[float] = None

    # D29 — actionable details (Session 8)
    street_address: Optional[str] = None
    agent_name: Optional[str] = None
    agent_phone: Optional[str] = None
    agent_email: Optional[str] = None
    listing_url_rea: Optional[str] = None
    listing_url_domain: Optional[str] = None
    inspection_date: Optional[date] = None
    days_on_market: Optional[int] = None


# ── Match result ──────────────────────────────────────────────────────────────

class TradeoffItem(BaseModel):
    message: str
    severity: int    # 1 (mild) | 2 (moderate) | 3 (significant)


class TrajectoryInfo(BaseModel):
    label: str       # rising | stable | cooling
    median_price_change: Optional[float] = None
    source: Optional[str] = None
    year: Optional[str] = None


class MatchResult(BaseModel):
    property: Property
    score: float
    highlights: list[str]
    tradeoffs: list[TradeoffItem]
    trajectory: Optional[TrajectoryInfo] = None


# ── Outcome tracking (D23 moat) ───────────────────────────────────────────────

class OutcomeReport(BaseModel):
    session_id: str
    property_id: UUID
    outcome_type: Literal["inspected", "purchased", "shortlisted", "passed"]
    search_criteria: Optional[dict] = None
    match_score: Optional[float] = None
    notes: Optional[str] = None


class OutcomeResponse(BaseModel):
    success: bool
    message: str