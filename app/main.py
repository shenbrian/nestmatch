"""
NestMatch API — entry point.

Run locally:
    uvicorn app.main:app --reload

Docs available at:
    http://localhost:8000/docs
"""

import uuid
import os
import psycopg2
from dotenv import load_dotenv
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from app.models import SearchRequest, SearchResponse
from app.engine import run_search

load_dotenv()

app = FastAPI(
    title="NestMatch API",
    version="1.0.0",
    description="Buyer-side real estate matching engine for Sydney.",
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)


def load_properties() -> list[dict]:
    """Load all properties from Neon, joined with features and suburb trajectories.

    Townhouses inherit the house trajectory for their suburb — townhouse
    transaction volumes are too thin in most suburbs for a reliable
    independent series (Decision D19).
    """
    conn = psycopg2.connect(os.getenv("DATABASE_URL"))
    cur  = conn.cursor()

    cur.execute("""
        SELECT
            p.id,
            p.title,
            p.suburb,
            p.price_min,
            p.price_max,
            p.bedrooms,
            p.bathrooms,
            p.internal_size_sqm,
            p.property_type,
            p.parking_spaces,
            p.land_size_sqm,
            p.development_zone,
            p.renovation_status,
            f.commute_cbd_mins,
            f.distance_to_station_m,
            f.distance_to_bus_stop_m,
            f.distance_to_hospital_m,
            f.transport_score,
            f.school_score,
            f.noise_score,
            f.suburb_lifestyle_score,
            st.trajectory          AS suburb_trajectory,
            st.median_price_change AS suburb_median_price_change,
            st.reference_period    AS suburb_reference_period
        FROM properties p
        JOIN property_features f ON f.property_id = p.id
        LEFT JOIN suburb_trajectories st
            ON st.suburb = p.suburb
            AND st.property_type = CASE
                WHEN p.property_type = 'townhouse' THEN 'house'
                ELSE p.property_type
            END
        ORDER BY p.suburb, p.title
    """)

    cols = [desc[0] for desc in cur.description]
    rows = [dict(zip(cols, row)) for row in cur.fetchall()]

    for row in rows:
        row["id"] = str(row["id"])
        for key in ("transport_score", "school_score", "noise_score", "suburb_lifestyle_score"):
            if row[key] is not None:
                row[key] = float(row[key])
        if row.get("suburb_median_price_change") is not None:
            row["suburb_median_price_change"] = float(row["suburb_median_price_change"])

    cur.close()
    conn.close()
    return rows


@app.get("/health")
def health():
    props = load_properties()
    return {"status": "ok", "properties_loaded": len(props)}


@app.post("/search", response_model=SearchResponse)
def search(req: SearchRequest) -> SearchResponse:
    properties = load_properties()
    ranked, total_candidates = run_search(properties, req)
    return SearchResponse(
        search_id=str(uuid.uuid4()),
        total_candidates=total_candidates,
        results=ranked,
    )