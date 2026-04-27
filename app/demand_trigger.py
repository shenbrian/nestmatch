"""
app/demand_trigger.py -- Component 3: demand-driven nester targeting
Session 36 — auto-send update

Called after every /search that returns results.
For each result property:
  1. Check if a nester enquiry was sent in the last 14 days
  2. If not, select the best-matched nester by corridor/suburb
  3. Call deployment_trigger.demand_trigger() — generates, sends, logs to send_log
  4. Write to enquiry_queue with status 'sent' as audit record (no pending/review step)

Session 31 note: manual review step (enquiry_queue pending → review_queue.py) retired.
Auto-send is live as of Session 36 per D115.
"""

import json
import logging
from datetime import datetime, timezone

logger = logging.getLogger(__name__)

# Suburb -> nester_id mapping derived from personas.json
# Order within each list = preference order (primary nester first)
SUBURB_TO_NESTERS: dict[str, list[str]] = {
    # Lower North Shore
    "neutral bay":       ["N01", "N02", "N03"],
    "cremorne":          ["N02", "N01", "N03"],
    "cammeray":          ["N01", "N03"],
    "crows nest":        ["N01"],
    "north sydney":      ["N01"],
    "kirribilli":        ["N01", "N03"],
    "mosman":            ["N02", "N03"],
    "balmoral":          ["N02"],
    "clifton gardens":   ["N02"],
    "lavender bay":      ["N03"],

    # Upper North Shore
    "gordon":            ["N04", "N05"],
    "pymble":            ["N04"],
    "killara":           ["N05", "N04"],
    "turramurra":        ["N04"],
    "st ives":           ["N04"],
    "lindfield":         ["N05"],
    "roseville":         ["N05"],
    "wahroonga":         ["N05"],
    "west pymble":       ["N04", "N05"],

    # Inner West
    "leichhardt":        ["N06", "N07"],
    "annandale":         ["N06", "N07"],
    "balmain":           ["N06"],
    "rozelle":           ["N06"],
    "lilyfield":         ["N06"],
    "petersham":         ["N07"],
    "stanmore":          ["N07"],
    "newtown":           ["N07"],
    "marrickville":      ["N07"],

    # Western Sydney
    "parramatta":        ["N08"],
    "westmead":          ["N08"],
    "harris park":       ["N08"],
    "merrylands":        ["N08"],
    "granville":         ["N08"],

    # SW Corridor
    "liverpool":         ["N09"],
    "campbelltown":      ["N09"],
    "ingleburn":         ["N09"],
    "leumeah":           ["N09"],
    "macquarie fields":  ["N09"],
}


def select_nester(suburb: str, property_type: str, bedrooms: int) -> str | None:
    """
    Select the best nester for a property.
    Returns nester_id or None if no match.

    Selection rules:
    - Match by suburb (case-insensitive)
    - For apartments, prefer nesters with apartment archetype (N01, N07)
    - For houses, prefer nesters with house archetype
    - Fall back to first available nester for the suburb
    """
    suburb_key = suburb.strip().lower()
    candidates = SUBURB_TO_NESTERS.get(suburb_key, [])
    if not candidates:
        return None

    is_apartment = property_type.lower() in ("apartment", "unit")
    is_house = property_type.lower() in ("house",)

    apartment_nesters = {"N01", "N07"}
    house_nesters = {"N02", "N03", "N04", "N05", "N06", "N08", "N09"}

    for nester_id in candidates:
        if is_apartment and nester_id in apartment_nesters:
            return nester_id
        if is_house and nester_id in house_nesters:
            return nester_id

    # No type match — return first candidate
    return candidates[0]


async def was_recently_contacted(conn, property_id: str, days: int = 14) -> bool:
    """
    Returns True if a nester enquiry was sent for this property in the last N days.
    Checks enquiry_queue for rows with status 'sent'.
    """
    row = await conn.fetchrow(
        """
        SELECT 1 FROM enquiry_queue
        WHERE property_id = $1::uuid
          AND status = 'sent'
          AND created_at > NOW() - ($2 || ' days')::interval
        LIMIT 1
        """,
        property_id,
        str(days),
    )
    return row is not None


async def trigger_enquiries(
    conn,
    results: list,
    search_params: dict,
    buyer_id: str = "anon",
) -> int:
    """
    Main entry point. Called from /search after results are returned.

    Args:
        conn: asyncpg connection from pool
        results: list of MatchResult objects from run_search()
        search_params: the SearchRequest dict for context
        buyer_id: nm_ buyer ID if available

    Returns:
        Number of enquiries sent
    """
    from app.deployment_trigger import demand_trigger

    sent = 0

    for result in results:
        p = result.property
        property_id = str(p.id)
        suburb = p.suburb
        property_type = p.property_type

        # Skip if no agent email or email is a NestMatch alias — can't send real enquiry
        if not p.listing_agent_email:
            continue
        if "proton.me" in p.listing_agent_email or "zoho.com" in p.listing_agent_email:
            continue

        # Skip if already contacted recently
        already_contacted = await was_recently_contacted(conn, property_id)
        if already_contacted:
            continue

        # Select best nester
        nester_id = select_nester(suburb, property_type, p.bedrooms)
        if not nester_id:
            logger.info(f"No nester mapped for suburb: {suburb}")
            continue

        # Fire demand_trigger — generates enquiry, applies safety gates, sends via Resend
        try:
            result_dict = await demand_trigger(
                property_id=property_id,
                suburb=suburb,
                property_type=property_type,
                agent_email=p.listing_agent_email,
                agency_name=getattr(p, "agency_name", "") or "",
                listing_agent_name=p.listing_agent_name or "Agent",
                property_address=p.street_address or f"{suburb} property",
                price_guide=str(p.price) if p.price else None,
            )
        except Exception as e:
            logger.error(f"demand_trigger failed for {property_id}: {e}")
            continue

        status = result_dict.get("status")  # 'sent' | 'skipped' | 'failed'

        if status not in ("sent", "skipped", "failed"):
            continue

        # Write audit record to enquiry_queue regardless of outcome
        try:
            await conn.execute(
                """
                INSERT INTO enquiry_queue
                    (property_id, nester_id, agent_email, agent_name,
                     street_address, suburb, property_type,
                     email_body, status, triggered_by, search_params)
                VALUES ($1::uuid, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11)
                ON CONFLICT DO NOTHING
                """,
                property_id,
                nester_id,
                p.listing_agent_email,
                p.listing_agent_name,
                p.street_address,
                suburb,
                property_type,
                result_dict.get("reason") or status,   # brief note in email_body slot
                status,
                buyer_id,
                json.dumps(search_params),
            )
        except Exception as e:
            logger.error(f"enquiry_queue audit write failed for {property_id}: {e}")

        if status == "sent":
            sent += 1
            logger.info(
                f"Auto-sent: {result_dict.get('nester_id')} "
                f"({result_dict.get('nester_name')}) -> {p.street_address or suburb}"
            )
        else:
            logger.info(
                f"Send {status}: {property_id} — {result_dict.get('reason')}"
            )

    return sent
