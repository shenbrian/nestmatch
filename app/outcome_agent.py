"""
outcome_agent.py — NestMatch Session 9
Weekly agent: reads unresolved search sessions → calls Claude → writes candidate outcomes.
Uses asyncpg pool pattern to match main.py.
"""

import os, json, httpx
from datetime import datetime, timedelta

ANTHROPIC_API_KEY = os.environ.get("ANTHROPIC_API_KEY")

AGENT_PROMPT = """
You are the NestMatch outcome tracking agent.

A buyer used NestMatch on {search_date}. Here is their search:
Mode: {mode}
Search summary: {raw_input}
Full parameters: {extracted_params}
Properties shortlisted: {results_returned}

It has been {days_elapsed} days since their search.

Your job: assess whether this buyer has likely purchased or inspected a property.
At this early stage you have no sold data to compare against — use the search
parameters and shortlist to reason about likely outcome suburbs and price points.

Return ONLY valid JSON, no preamble, no markdown:

If a match seems plausible:
{{"match": true, "property_address": "...", "suburb": "...", "agent_confidence": "low|medium|high", "agent_reasoning": "..."}}

If too early or insufficient info:
{{"match": false, "agent_reasoning": "..."}}
"""

async def call_outcome_agent(session: dict) -> dict:
    days_elapsed = (datetime.utcnow() - session["created_at"]).days
    prompt = AGENT_PROMPT.format(
        search_date=session["created_at"].strftime("%Y-%m-%d"),
        mode=session["mode"],
        raw_input=session["raw_input"],
        extracted_params=session["extracted_params"],
        results_returned=session["results_returned"],
        days_elapsed=days_elapsed
    )
    async with httpx.AsyncClient() as client:
        resp = await client.post(
            "https://api.anthropic.com/v1/messages",
            headers={
                "x-api-key": ANTHROPIC_API_KEY,
                "anthropic-version": "2023-06-01",
                "content-type": "application/json"
            },
            json={
                "model": "claude-sonnet-4-20250514",
                "max_tokens": 500,
                "messages": [{"role": "user", "content": prompt}]
            },
            timeout=30.0
        )
        data = resp.json()
        text = data["content"][0]["text"].strip()
        return json.loads(text)

async def run_outcome_agent(pool) -> dict:
    """
    Call this from main.py endpoint.
    Finds sessions older than 90 days with no outcome record, runs agent on each.
    """
    cutoff = datetime.utcnow() - timedelta(days=90)
    processed, skipped = 0, 0

    async with pool.acquire() as conn:
        sessions = await conn.fetch(
            """
            SELECT s.id, s.created_at, s.mode, s.raw_input,
                   s.extracted_params, s.results_returned
            FROM search_sessions s
            LEFT JOIN outcome_records o ON o.session_id = s.id
            WHERE s.created_at < $1
              AND o.id IS NULL
            ORDER BY s.created_at ASC
            LIMIT 20
            """,
            cutoff
        )

        for session in sessions:
            try:
                result = await call_outcome_agent(dict(session))
                if result.get("match"):
                    await conn.execute(
                        """
                        INSERT INTO outcome_records
                            (session_id, property_address, suburb,
                             agent_confidence, agent_reasoning, status)
                        VALUES ($1, $2, $3, $4, $5, 'candidate')
                        """,
                        session["id"],
                        result.get("property_address", ""),
                        result.get("suburb", ""),
                        result.get("agent_confidence", "low"),
                        result.get("agent_reasoning", "")
                    )
                    processed += 1
                else:
                    skipped += 1
            except Exception as e:
                print(f"Agent error for session {session['id']}: {e}")
                skipped += 1

    return {"processed": processed, "skipped": skipped}