"""
NestMatch API — entry point.

Run locally:
    uvicorn app.main:app --reload

Docs available at:
    http://localhost:8000/docs
"""

import uuid
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from app.models import SearchRequest, SearchResponse
from app.engine import run_search
from app.seed_data import SEED_PROPERTIES

app = FastAPI(
    title="NestMatch API",
    version="1.0.0",
    description="Buyer-side real estate matching engine for Sydney.",
)

# Allow local Next.js dev server and production Vercel domain
app.add_middleware(
    CORSMiddleware,
    allow_origins=[
        "http://localhost:3000",
        "https://*.vercel.app",
    ],
    allow_methods=["POST", "GET"],
    allow_headers=["*"],
)


@app.get("/health")
def health():
    return {"status": "ok", "properties_loaded": len(SEED_PROPERTIES)}


@app.post("/search", response_model=SearchResponse)
def search(req: SearchRequest) -> SearchResponse:
    ranked, total_candidates = run_search(SEED_PROPERTIES, req)
    return SearchResponse(
        search_id=str(uuid.uuid4()),
        total_candidates=total_candidates,
        results=ranked,
    )
