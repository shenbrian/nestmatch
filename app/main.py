"""
main.py — NestMatch FastAPI backend
Session 8: D29 actionable details in response + D23 outcome endpoint
"""

import os
import json
import uuid
from contextlib import asynccontextmanager
from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
from typing import Optional, Any
import asyncpg

from app.models import SearchRequest, MatchResult, OutcomeReport, OutcomeResponse
from app.engine import run_search
from app.outcome_agent import run_outcome_agent

# — Session logging (D23 — outcome data flywheel) ————————————————

async def log_search_session(conn, req, results: list) -> None:
    """Logs every completed search to search_sessions table."""
    raw_summary = (
        f"{req.mode} | budget ${req.budget_max:,} | "
        f"{req.bedrooms_min}bd | {req.property_type or 'any type'}"
    )
    await conn.execute(
        """
        INSERT INTO search_sessions
            (mode, raw_input, extracted_params, results_returned, user_fingerprint)
        VALUES ($1, $2, $3, $4, $5)
        """,
        req.mode,
        raw_summary,
        json.dumps(req.dict()),
        json.dumps([r.dict() for r in results], default=str),
        "anon"
    )


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
        await log_search_session(conn, req, results)
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
                json.dumps(report.search_criteria) if report.search_criteria else None,
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

# — Outcome agent (D32 — weekly cron trigger) ————————————————————

@app.post("/internal/run-outcome-agent")
async def trigger_outcome_agent():
    """Called by Render cron every Monday. Not buyer-facing."""
    result = await run_outcome_agent(pool)
    return result


# — Outcome candidates review (admin) ————————————————————————————

@app.get("/internal/outcomes/candidates")
async def list_outcome_candidates():
    async with pool.acquire() as conn:
        rows = await conn.fetch(
            """
            SELECT o.*, s.raw_input, s.mode, s.created_at as search_date
            FROM outcome_records o
            JOIN search_sessions s ON s.id = o.session_id
            WHERE o.status = 'candidate'
            ORDER BY o.created_at DESC
            """
        )
    return [dict(r) for r in rows]


@app.post("/internal/outcomes/{outcome_id}/review")
async def review_outcome(outcome_id: str, action: str):
    """action = 'confirm' or 'discard'"""
    status = "confirmed" if action == "confirm" else "discarded"
    async with pool.acquire() as conn:
        await conn.execute(
            "UPDATE outcome_records SET status=$1, reviewed_at=now() WHERE id=$2",
            status, outcome_id
        )
    return {"ok": True, "status": status}
        
# ── Pre-portal listings (D80/D81) ─────────────────────────────────────────────

@app.get("/pre-portal")
async def pre_portal(suburbs: str = ""):
    """
    Returns agent_outbound records where is_pre_portal = true.
    Optionally filtered by comma-separated suburb list (D80).
    Unnests listings_raw array so each listing is a flat object.
    """
    suburb_list = [s.strip().lower() for s in suburbs.split(",") if s.strip()]

    async with pool.acquire() as conn:
        if suburb_list:
            rows = await conn.fetch(
                """
                SELECT
                    ao.id,
                    ao.agency_name,
                    ao.agent_name,
                    ao.received_at,
                    ao.is_pre_portal,
                    listing->>'street_address' AS street_address,
                    listing->>'suburb'         AS suburb,
                    listing->>'property_type'  AS property_type,
                    listing->>'price_guide'    AS price_guide,
                    listing->>'bedrooms'       AS bedrooms,
                    listing->>'bathrooms'      AS bathrooms
                FROM agent_outbound ao,
                     jsonb_array_elements(ao.listings_raw) AS listing
                WHERE ao.is_pre_portal = true
                  AND ao.listings_raw IS NOT NULL
                  AND LOWER(listing->>'suburb') = ANY($1::text[])
                ORDER BY ao.received_at DESC
                """,
                suburb_list,
            )
        else:
            rows = await conn.fetch(
                """
                SELECT
                    ao.id,
                    ao.agency_name,
                    ao.agent_name,
                    ao.received_at,
                    ao.is_pre_portal,
                    listing->>'street_address' AS street_address,
                    listing->>'suburb'         AS suburb,
                    listing->>'property_type'  AS property_type,
                    listing->>'price_guide'    AS price_guide,
                    listing->>'bedrooms'       AS bedrooms,
                    listing->>'bathrooms'      AS bathrooms
                FROM agent_outbound ao,
                     jsonb_array_elements(ao.listings_raw) AS listing
                WHERE ao.is_pre_portal = true
                  AND ao.listings_raw IS NOT NULL
                ORDER BY ao.received_at DESC
                """
            )

    return [dict(r) for r in rows]


# ── Feedback (Session 26 — D89) ───────────────────────────────────────────────

class FeedbackRequest(BaseModel):
    comment: str
    search_params: Optional[Any] = None
    page: Optional[str] = "residential"

@app.post("/feedback")
async def submit_feedback(body: FeedbackRequest):
    """
    Stores free-text pilot feedback from the results page.
    comment: buyer's raw text (capped at 2000 chars).
    search_params: JSONB snapshot of the search that produced the results.
    page: 'residential' (investment to follow post-pilot).
    """
    if not body.comment or not body.comment.strip():
        raise HTTPException(status_code=400, detail="Comment cannot be empty")

    comment = body.comment.strip()[:2000]

    async with pool.acquire() as conn:
        await conn.execute(
            """
            INSERT INTO user_feedback (comment, search_params, page)
            VALUES ($1, $2::jsonb, $3)
            """,
            comment,
            json.dumps(body.search_params) if body.search_params is not None else None,
            body.page,
        )

 # ── Card reactions (Session 29 — D93 outcome data) ────────────────────────────

class CardReactionRequest(BaseModel):
    property_id: str
    reaction: str   # 'looks_right' | 'not_for_me'
    search_params: Optional[Any] = None
    session_id: Optional[str] = None

@app.post("/card-reaction")
async def card_reaction(body: CardReactionRequest):
    if body.reaction not in ("looks_right", "not_for_me"):
        raise HTTPException(status_code=400, detail="Invalid reaction")
    async with pool.acquire() as conn:
        await conn.execute(
            """
            INSERT INTO card_reactions (property_id, reaction, search_params, session_id)
            VALUES ($1::uuid, $2, $3::jsonb, $4)
            """,
            body.property_id,
            body.reaction,
            json.dumps(body.search_params) if body.search_params is not None else None,
            body.session_id,
        )
    return {"status": "ok"}