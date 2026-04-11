"""
main.py — NestMatch FastAPI backend
Session 8: D29 actionable details in response + D23 outcome endpoint
"""

import os
import uuid
from contextlib import asynccontextmanager
from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
import asyncpg

from app.models import SearchRequest, MatchResult, OutcomeReport, OutcomeResponse
from app.engine import run_search


DATABASE_URL = os.environ["DATABASE_URL"]
pool: asyncpg.Pool | None = None


@asynccontextmanager
async def lifespan(app: FastAPI):
    global pool
    pool = await asyncpg.create_pool(DATABASE_URL, min_size=2, max_size=10)
    yield
    await pool.close()


app = FastAPI(
    title="NestMatch API",
    version="0.8.0",
    lifespan=lifespan,
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["https://nestmatch.com.au", "http://localhost:3000"],
    allow_methods=["GET", "POST"],
    allow_headers=["*"],
)


# ── Health ────────────────────────────────────────────────────────────────────

@app.get("/health")
async def health():
    async with pool.acquire() as conn:
        count = await conn.fetchval("SELECT COUNT(*) FROM properties")
    return {"status": "ok", "properties_loaded": count}


# ── Search ────────────────────────────────────────────────────────────────────

@app.post("/search", response_model=list[MatchResult])
async def search(req: SearchRequest):
    async with pool.acquire() as conn:
        results = await run_search(conn, req)
    return results


# ── Outcome reporting (D23 — begins the feedback loop) ───────────────────────

@app.post("/outcome", response_model=OutcomeResponse)
async def report_outcome(report: OutcomeReport):
    """
    Called when a buyer takes an action on a result card:
    'I inspected this', 'I bought this', 'Shortlist', 'Not for me'.

    Even at low volume (5% of users), this begins seeding the outcome
    data that becomes NestMatch's durable moat per D23.
    """
    try:
        async with pool.acquire() as conn:
            await conn.execute(
                """
                INSERT INTO buyer_outcomes
                  (session_id, property_id, search_criteria, match_score, outcome_type)
                VALUES ($1, $2, $3, $4, $5)
                """,
                report.session_id,
                report.property_id,
                str(report.search_criteria) if report.search_criteria else None,
                report.match_score,
                report.outcome_type,
            )
        return OutcomeResponse(success=True, message="Outcome recorded. Thank you.")
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


# ── Outcome stats (internal — not buyer-facing yet) ──────────────────────────

@app.get("/admin/outcomes/summary")
async def outcomes_summary():
    """Quick read on how the feedback loop is going."""
    async with pool.acquire() as conn:
        rows = await conn.fetch(
            """
            SELECT
              outcome_type,
              COUNT(*) as count,
              AVG(match_score) as avg_score,
              MIN(outcome_date) as first_seen
            FROM buyer_outcomes
            GROUP BY outcome_type
            ORDER BY count DESC
            """
        )
    return [dict(r) for r in rows]