"""
deployment_trigger.py
---------------------
Controls WHEN nesters send enquiries and enforces pattern-safety rules.

Two trigger modes:
  1. DEMAND trigger  — fires after a buyer search returns a matched property
                       that has no listing_agent_email yet. Goal: open the door.
  2. SCHEDULED trigger — fires on a cron (e.g. daily), cycling through
                         properties that need agent coverage refreshed.

Rate + pattern safety rules (all enforced here):
  - MAX_SENDS_PER_NESTER_PER_DAY: hard ceiling per nester ID
  - MIN_GAP_BETWEEN_SENDS_SECONDS: minimum seconds between any two sends
    from the same nester (prevents burst patterns)
  - D74 rule: no two nesters in the same corridor may enquire at the same
    agency within a rolling AGENCY_LOCKOUT_DAYS window
  - Jitter: actual send time randomised ±JITTER_SECONDS around the
    scheduled moment (prevents clockwork patterns detectable by REA/Domain)

All state is persisted in the neon DB (send_log table). See schema below.

send_log table DDL (run once in Neon):
---------------------------------------
CREATE TABLE IF NOT EXISTS send_log (
    id              SERIAL PRIMARY KEY,
    nester_id       TEXT NOT NULL,
    property_id     TEXT,
    agent_email     TEXT,
    agency_name     TEXT,
    corridor        TEXT,
    suburb          TEXT,
    sent_at         TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    resend_id       TEXT,
    trigger_type    TEXT,   -- 'demand' | 'scheduled' | 'manual'
    status          TEXT    -- 'sent' | 'failed' | 'skipped'
);
CREATE INDEX IF NOT EXISTS idx_send_log_nester_sent
    ON send_log(nester_id, sent_at DESC);
CREATE INDEX IF NOT EXISTS idx_send_log_agency_corridor
    ON send_log(agency_name, corridor, sent_at DESC);
---------------------------------------
"""

import asyncio
import asyncpg
import os
import random
import logging
from datetime import datetime, timezone, timedelta
from app.nester_router import pick_nester

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Rate + safety constants — adjust as operational experience grows
# ---------------------------------------------------------------------------
MAX_SENDS_PER_NESTER_PER_DAY = 3        # absolute ceiling per nester
MIN_GAP_BETWEEN_SENDS_SECONDS = 1800    # 30 min minimum between sends from same nester
AGENCY_LOCKOUT_DAYS = 7                 # D74: same corridor, same agency lockout window
JITTER_SECONDS = 600                    # ±10 min randomisation on scheduled sends

DATABASE_URL = os.environ.get("DATABASE_URL", "")


# ---------------------------------------------------------------------------
# DB helpers
# ---------------------------------------------------------------------------

async def get_db() -> asyncpg.Connection:
    return await asyncpg.connect(DATABASE_URL)


async def count_sends_today(conn: asyncpg.Connection, nester_id: str) -> int:
    """How many sends has this nester made since midnight Sydney time today."""
    # Use UTC midnight as proxy — close enough at this stage
    today_utc = datetime.now(timezone.utc).replace(
        hour=0, minute=0, second=0, microsecond=0
    )
    row = await conn.fetchrow(
        """
        SELECT COUNT(*) AS n FROM send_log
        WHERE nester_id = $1
          AND sent_at >= $2
          AND status = 'sent'
        """,
        nester_id, today_utc
    )
    return row["n"] if row else 0


async def seconds_since_last_send(conn: asyncpg.Connection, nester_id: str) -> float:
    """Seconds elapsed since this nester's most recent send. 999999 if never."""
    row = await conn.fetchrow(
        """
        SELECT sent_at FROM send_log
        WHERE nester_id = $1 AND status = 'sent'
        ORDER BY sent_at DESC LIMIT 1
        """,
        nester_id
    )
    if not row:
        return 999999.0
    elapsed = (datetime.now(timezone.utc) - row["sent_at"]).total_seconds()
    return elapsed


async def agency_locked_out(
    conn: asyncpg.Connection,
    agency_name: str,
    corridor: str,
) -> bool:
    """
    D74 check: has any nester in this corridor already sent to this agency
    within the lockout window?
    """
    if not agency_name:
        return False
    cutoff = datetime.now(timezone.utc) - timedelta(days=AGENCY_LOCKOUT_DAYS)
    row = await conn.fetchrow(
        """
        SELECT 1 FROM send_log
        WHERE agency_name ILIKE $1
          AND corridor = $2
          AND sent_at >= $3
          AND status = 'sent'
        LIMIT 1
        """,
        f"%{agency_name}%", corridor, cutoff
    )
    return row is not None


async def log_send(
    conn: asyncpg.Connection,
    nester_id: str,
    property_id: str | None,
    agent_email: str | None,
    agency_name: str | None,
    corridor: str | None,
    suburb: str | None,
    resend_id: str | None,
    trigger_type: str,
    status: str,
) -> None:
    await conn.execute(
        """
        INSERT INTO send_log
          (nester_id, property_id, agent_email, agency_name,
           corridor, suburb, resend_id, trigger_type, status)
        VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9)
        """,
        nester_id, property_id, agent_email, agency_name,
        corridor, suburb, resend_id, trigger_type, status,
    )


# ---------------------------------------------------------------------------
# Safety gate — call before every send attempt
# ---------------------------------------------------------------------------

async def can_send(
    conn: asyncpg.Connection,
    nester_id: str,
    agency_name: str | None,
    corridor: str | None,
) -> tuple[bool, str]:
    """
    Returns (True, "") if send is allowed, or (False, reason) if blocked.
    Checks in order: daily cap → gap → D74 agency lockout.
    """
    daily = await count_sends_today(conn, nester_id)
    if daily >= MAX_SENDS_PER_NESTER_PER_DAY:
        return False, f"daily cap reached ({daily}/{MAX_SENDS_PER_NESTER_PER_DAY})"

    gap = await seconds_since_last_send(conn, nester_id)
    if gap < MIN_GAP_BETWEEN_SENDS_SECONDS:
        wait = int(MIN_GAP_BETWEEN_SENDS_SECONDS - gap)
        return False, f"gap too short — wait {wait}s"

    if agency_name and corridor:
        locked = await agency_locked_out(conn, agency_name, corridor)
        if locked:
            return False, f"D74 lockout: {agency_name} in {corridor} within {AGENCY_LOCKOUT_DAYS}d"

    return True, ""


# ---------------------------------------------------------------------------
# Core trigger functions
# ---------------------------------------------------------------------------

async def demand_trigger(
    property_id: str,
    suburb: str,
    property_type: str,
    agent_email: str,
    agency_name: str,
    listing_agent_name: str,
    property_address: str,
    price_guide: str | None = None,
) -> dict:
    """
    Called immediately after a buyer search returns a matched property
    that has no listing_agent_email yet (or email needs refreshing).

    Selects the best nester for the suburb, checks all safety gates,
    generates and sends the enquiry via question_engine.

    Returns a result dict with status and details.
    """
    from app.question_engine import generate_enquiry, send_enquiry

    conn = await get_db()
    try:
        # 1. Route to nester
        nester = pick_nester(suburb=suburb, property_type=property_type)
        if not nester:
            return {"status": "skipped", "reason": f"no nester for suburb: {suburb}"}

        nester_id = nester["nester_id"]
        corridor = nester.get("corridor")

        # 2. Safety gate
        allowed, reason = await can_send(conn, nester_id, agency_name, corridor)
        if not allowed:
            return {"status": "skipped", "reason": reason, "nester_id": nester_id}

        # 3. Generate enquiry
        property_data = {
            "address": property_address,
            "suburb": suburb,
            "property_type": property_type,
            "agent_name": listing_agent_name,
            "agency_name": agency_name,
            "price_guide": price_guide or "not listed",
        }
        enquiry = await generate_enquiry(property_data, nester_id)
        if not enquiry or "email_body" not in enquiry:
            await log_send(conn, nester_id, property_id, agent_email,
                           agency_name, corridor, suburb, None, "demand", "failed")
            return {"status": "failed", "reason": "enquiry generation failed"}

        # 4. Subject line (see subject_line.py)
        from app.subject_line import build_subject
        subject = build_subject(property_address, agency_name)

        # 5. Send
        result = await send_enquiry(enquiry, to_email=agent_email, subject=subject)
        status = "sent" if result.get("success") else "failed"

        # 6. Log
        await log_send(
            conn, nester_id, property_id, agent_email, agency_name,
            corridor, suburb, result.get("resend_id"), "demand", status
        )

        return {
            "status": status,
            "nester_id": nester_id,
            "nester_name": nester["full_name"],
            "resend_id": result.get("resend_id"),
            "reason": result.get("error") if status == "failed" else None,
        }

    finally:
        await conn.close()


async def scheduled_trigger(limit: int = 10) -> list[dict]:
    """
    Scheduled (cron) trigger. Processes up to `limit` properties per run.

    Selection criteria for scheduled sends:
      - Properties that have listing_agent_email populated (agent known)
      - No send logged in the past AGENCY_LOCKOUT_DAYS for that corridor+agency
      - Prioritise properties with oldest last_sent date (or never sent)

    This keeps existing agent relationships warm and harvests mailing list adds.
    """
    conn = await get_db()
    results = []
    try:
        # Fetch candidate properties: have agent email, haven't been sent recently
        cutoff = datetime.now(timezone.utc) - timedelta(days=AGENCY_LOCKOUT_DAYS)
        rows = await conn.fetch(
            """
            SELECT p.id, p.suburb, p.property_type,
                   p.listing_agent_email, p.listing_agent_name,
                   p.agency_name, p.address,
                   MAX(sl.sent_at) AS last_sent
            FROM properties p
            LEFT JOIN send_log sl ON sl.property_id = p.id::text AND sl.status = 'sent'
            WHERE p.listing_agent_email IS NOT NULL
              AND p.listing_agent_email NOT LIKE '%proton%'
              AND p.listing_agent_email NOT LIKE '%zoho%'
            GROUP BY p.id, p.suburb, p.property_type,
                     p.listing_agent_email, p.listing_agent_name,
                     p.agency_name, p.address
            HAVING MAX(sl.sent_at) IS NULL OR MAX(sl.sent_at) < $1
            ORDER BY MAX(sl.sent_at) ASC NULLS FIRST
            LIMIT $2
            """,
            cutoff, limit
        )

        for row in rows:
            # Add jitter to avoid clockwork send pattern
            jitter = random.randint(-JITTER_SECONDS, JITTER_SECONDS)
            if jitter > 0:
                await asyncio.sleep(jitter)

            result = await demand_trigger(
                property_id=str(row["id"]),
                suburb=row["suburb"],
                property_type=row["property_type"] or "apartment",
                agent_email=row["listing_agent_email"],
                agency_name=row["agency_name"] or "",
                listing_agent_name=row["listing_agent_name"] or "",
                property_address=row["address"],
            )
            results.append(result)
            logger.info(f"Scheduled send: {result}")

    finally:
        await conn.close()

    return results


# ---------------------------------------------------------------------------
# CLI smoke test (no real sends — prints routing only)
# ---------------------------------------------------------------------------
if __name__ == "__main__":
    import sys
    print("\n--- deployment_trigger routing check ---\n")
    test = [
        ("Cremorne", "apartment", "agent@mcgrath.com.au", "McGrath"),
        ("Bondi Beach", "house", "agent@laingandson.com.au", "Laing+Simmons"),
        ("Parramatta", "apartment", "agent@raywhite.com", "Ray White"),
    ]
    for suburb, pt, email, agency in test:
        nester = pick_nester(suburb, pt)
        if nester:
            print(f"  {suburb:20s} → {nester['nester_id']} {nester['full_name']}")
            print(f"             from: {nester['email']}")
            print(f"               to: {email} ({agency})")
        else:
            print(f"  {suburb:20s} → NO NESTER MATCHED")
        print()
