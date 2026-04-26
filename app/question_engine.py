"""
NestMatch Question Engine
app/question_engine.py

Generates persona-consistent, territory-targeted enquiry emails for each nester.
Takes property data + nester_id, returns a ready-to-send email body.

Territory logic (D93):
  A = Price         — ALWAYS included
  B = Competition   — days_on_market > 14
  C = Condition     — property_type is apartment OR building_age > 15 years
  D = Vendor        — days_on_market > 30
  E = Process       — auction within 21 days

Usage:
  from app.question_engine import generate_enquiry
  result = await generate_enquiry(property_data, nester_id)
"""

import json
import os
import httpx
from pathlib import Path
from datetime import datetime, date

RESEND_API_KEY = os.environ.get("RESEND_API_KEY", "")

# Load personas once at import time
PERSONAS_PATH = Path(__file__).parent.parent / "personas.json"
with open(PERSONAS_PATH, "r", encoding="utf-8") as f:
    PERSONAS = json.load(f)


def determine_territories(property_data: dict) -> list[str]:
    """
    Determine which intelligence territories apply to this property.
    Territory A (Price) is always included.
    """
    territories = ["A"]  # Price — always

    days_on_market = property_data.get("days_on_market", 0) or 0
    property_type = (property_data.get("property_type") or "").lower()
    building_age = property_data.get("building_age_years", 0) or 0
    auction_date_str = property_data.get("auction_date")

    # B: Competition — listed more than 14 days
    if days_on_market > 14:
        territories.append("B")

    # C: Condition — apartment type or older building
    if "apartment" in property_type or "unit" in property_type or "flat" in property_type:
        territories.append("C")
    elif building_age > 15:
        territories.append("C")

    # D: Vendor motivation — listed more than 30 days
    if days_on_market > 30:
        territories.append("D")

    # E: Process — auction within 21 days
    if auction_date_str:
        try:
            auction_date = date.fromisoformat(str(auction_date_str))
            days_to_auction = (auction_date - date.today()).days
            if 0 <= days_to_auction <= 21:
                territories.append("E")
        except (ValueError, TypeError):
            pass

    return territories


def build_territory_descriptions(territories: list[str], property_data: dict) -> str:
    """
    Convert territory codes into descriptive instructions for Claude.
    """
    descriptions = []
    days_on_market = property_data.get("days_on_market", 0) or 0

    for t in territories:
        if t == "A":
            descriptions.append(
                "Territory A — PRICE: Ask for the price guide or asking price. "
                "This is mandatory in every enquiry."
            )
        elif t == "B":
            descriptions.append(
                f"Territory B — COMPETITION: Property has been listed {days_on_market} days. "
                "Probe gently: how much interest have you had, how many groups through inspections, "
                "any offers received. Do not ask bluntly — frame it as natural buyer curiosity."
            )
        elif t == "C":
            descriptions.append(
                "Territory C — CONDITION: Ask about building condition, strata levies (if apartment), "
                "any known defects or upcoming works, age of key fixtures. "
                "Frame as a practical buyer doing due diligence."
            )
        elif t == "D":
            descriptions.append(
                f"Territory D — VENDOR MOTIVATION: Property has been listed {days_on_market} days — "
                "vendor may be motivated. Probe: is the vendor looking for a quick sale, "
                "have there been any price adjustments, is there flexibility. "
                "Keep tone neutral and respectful — do not imply distress."
            )
        elif t == "E":
            descriptions.append(
                "Territory E — PROCESS: Auction is imminent. Ask: what is the vendor's reserve expectation, "
                "would they consider a strong pre-auction offer, what happens if it passes in. "
                "Frame as a serious buyer clarifying their options."
            )

    return "\n".join(f"  - {d}" for d in descriptions)


async def generate_enquiry(property_data: dict, nester_id: str) -> dict:
    """
    Generate a ready-to-send enquiry email body for the given nester and property.

    Args:
        property_data: dict with keys: address, suburb, property_type, bedrooms,
                       price_guide, days_on_market, building_age_years, auction_date,
                       agent_name, agency_name, listing_description (optional)
        nester_id: one of N01-N09

    Returns:
        dict with keys:
          - nester_id
          - nester_name
          - nester_email
          - property_address
          - territories_triggered
          - email_body (ready to send)
          - generated_at
    """
    if nester_id not in PERSONAS:
        raise ValueError(f"Unknown nester_id: {nester_id}. Must be one of {list(PERSONAS.keys())}")

    persona = PERSONAS[nester_id]
    territories = determine_territories(property_data)
    territory_instructions = build_territory_descriptions(territories, property_data)

    # Build recent question history note
    history = persona.get("question_history", [])
    history_note = ""
    if history:
        recent = history[-5:]  # last 5 phrases used
        history_note = f"\nAvoid reusing these recently used phrasings: {', '.join(repr(p) for p in recent)}"

    # Property context string
    bedrooms = property_data.get("bedrooms", "")
    prop_type = property_data.get("property_type", "property")
    address = property_data.get("address", "this property")
    suburb = property_data.get("suburb", "")
    agent_name = property_data.get("agent_name", "")
    agency = property_data.get("agency_name", "")
    description_snippet = property_data.get("listing_description", "")[:300] if property_data.get("listing_description") else ""

    property_context = f"{bedrooms} bed {prop_type} at {address}, {suburb}".strip(", ")
    if agency:
        property_context += f". Listed by {agency}"
    if agent_name:
        property_context += f" (agent: {agent_name})"
    if description_snippet:
        property_context += f". Listing excerpt: \"{description_snippet}\""

    prompt = f"""You are generating an enquiry email on behalf of a property buyer contacting a real estate agent.

BUYER PERSONA:
- Name: {persona['full_name']}
- Life stage: {persona['life_stage']}
- Looking for: {persona['property_target']}
- Budget: ${persona['budget_min']:,} – ${persona['budget_max']:,}
- Writing style: {persona['style']}
- Sign-off to use: {persona['sign_off']}

PROPERTY:
- {property_context}

INTELLIGENCE TERRITORIES TO PROBE:
{territory_instructions}
{history_note}

RULES:
1. Write ONLY the email body — no subject line, no metadata.
2. Include 3–4 questions total covering the triggered territories.
3. Territory A (price) must always be present as one of the questions.
4. Match the buyer persona's voice exactly — a downsizer sounds different from a first home buyer.
5. Questions must feel natural and human. Never sound like a checklist.
6. Keep it concise — under 120 words total.
7. End with the exact sign-off specified above.
8. Do not mention NestMatch or any platform. This is a direct buyer enquiry.
9. Do not use the phrase "I hope this email finds you well" or any similar filler opener.

Write the email body now:"""

    api_key = os.environ.get("ANTHROPIC_API_KEY")
    if not api_key:
        raise EnvironmentError("ANTHROPIC_API_KEY environment variable not set")

    async with httpx.AsyncClient(timeout=30.0) as client:
        response = await client.post(
            "https://api.anthropic.com/v1/messages",
            headers={
                "x-api-key": api_key,
                "anthropic-version": "2023-06-01",
                "content-type": "application/json",
            },
            json={
                "model": "claude-sonnet-4-20250514",
                "max_tokens": 400,
                "messages": [{"role": "user", "content": prompt}],
            },
        )
        response.raise_for_status()
        data = response.json()
        email_body = data["content"][0]["text"].strip()

    return {
        "nester_id": nester_id,
        "nester_name": persona["full_name"],
        "nester_email": persona["email"],
        "property_address": address,
        "property_suburb": suburb,
        "territories_triggered": territories,
        "email_body": email_body,
        "generated_at": datetime.utcnow().isoformat(),
    }

async def send_enquiry(enquiry: dict, to_email: str, subject: str) -> dict:
    """
    Send a generated enquiry email via Resend API.

    Args:
        enquiry: dict returned by generate_enquiry()
        to_email: the listing agent's email address
        subject: subject line for the email

    Returns:
        dict with keys: success (bool), resend_id (str or None), error (str or None)
    """
    if not RESEND_API_KEY:
        raise EnvironmentError("RESEND_API_KEY environment variable not set")

    payload = {
        "from": f"{enquiry['nester_name']} <{enquiry['nester_email']}>",
        "to": [to_email],
        "subject": subject,
        "text": enquiry["email_body"],
    }

    async with httpx.AsyncClient(timeout=30.0) as client:
        response = await client.post(
            "https://api.resend.com/emails",
            headers={
                "Authorization": f"Bearer {RESEND_API_KEY}",
                "Content-Type": "application/json",
            },
            json=payload,
        )

        if response.status_code == 200 or response.status_code == 201:
            data = response.json()
            return {
                "success": True,
                "resend_id": data.get("id"),
                "error": None,
            }
        else:
            return {
                "success": False,
                "resend_id": None,
                "error": f"Resend API error {response.status_code}: {response.text}",
            }