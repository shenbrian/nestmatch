"""
email_poller.py — NestMatch agent reply ingestion pipeline
Session 21: Extended with Category 2 (agent-initiated outbound) detection

Runs as a Render Cron Job every 15 minutes.
Reads unseen emails from buyers@nestmatch.com.au via IMAP,
classifies as Cat 1 (inquiry reply) or Cat 2 (agent-initiated outbound),
extracts structured data using Claude API, stores to appropriate table.

Environment variables required:
    DATABASE_URL        — Neon connection string
    ZOHO_IMAP_PASSWORD  — buyers@nestmatch.com.au Zoho password
    ANTHROPIC_API_KEY   — Claude API key
"""

import asyncio
import asyncpg
import httpx
import imaplib
import email
import json
import os
import re
from datetime import datetime, timezone
from email.header import decode_header
from email.utils import parsedate_to_datetime


# ── Config ────────────────────────────────────────────────────────────────────

IMAP_HOST = "imap.zoho.com.au"
IMAP_PORT = 993
IMAP_USER = "buyers@nestmatch.com.au"
IMAP_PASS = os.environ.get("ZOHO_IMAP_PASSWORD", "")

DATABASE_URL = os.environ.get("DATABASE_URL", "")
ANTHROPIC_API_KEY = os.environ.get("ANTHROPIC_API_KEY", "")

# Nester email → nester ID mapping
NESTER_MAP = {
    "james.nguyen@homemailbox.com.au":        "N01",
    "priya.sharma@homemailbox.com.au":        "N02",
    "david.chen@homemailbox.com.au":          "N03",
    "michelle.park@homemailbox.com.au":       "N04",
    "andrew.whitfield@homemailbox.com.au":    "N05",
    "sophie.laurent@homemailbox.com.au":      "N06",
    "marcus.webb@homemailbox.com.au":         "N07",
    "linda.wu@homemailbox.com.au":            "N08",
    "kevin.zhang@homemailbox.com.au":         "N09",
    "emma.thompson@homemailbox.com.au":       "N10",
    "lachlan.reid@homemailbox.com.au":        "N11",
    "aisha.mohamed@homemailbox.com.au":       "N12",
    "tom.gallagher@homemailbox.com.au":       "N13",
    "mei.lin@homemailbox.com.au":             "N14",
    "james.fairfax@homemailbox.com.au":       "N15",
    "catherine.moore@homemailbox.com.au":     "N16",
    "robert.kim@homemailbox.com.au":          "N17",
    "helen.nguyen@homemailbox.com.au":        "N18",
    "paul.anderson@homemailbox.com.au":       "N19",
    "grace.liu@homemailbox.com.au":           "N20",
    "raj.patel@homemailbox.com.au":           "N21",
    "sarah.johnson@homemailbox.com.au":       "N22",
    "michael.tran@homemailbox.com.au":        "N23",
    "olivia.burns@homemailbox.com.au":        "N24",
    "daniel.park@homemailbox.com.au":         "N25",
    "amy.chen@homemailbox.com.au":            "N26",
    "george.papadopoulos@homemailbox.com.au": "N27",
    "lily.nguyen@homemailbox.com.au":         "N28",
    "steven.ho@homemailbox.com.au":           "N29",
    "natalie.cross@homemailbox.com.au":       "N30",
}

# Pilot corridors — used to detect off-corridor emails (Type J)
PILOT_SUBURBS = {
    "mosman", "cremorne", "neutral bay", "cammeray", "northbridge",
    "willoughby", "chatswood", "lane cove", "st leonards", "crows nest",
    "gordon", "killara", "lindfield", "turramurra", "pymble", "wahroonga",
    "castle hill", "baulkham hills", "kellyville", "norwest", "bella vista",
    "newtown", "glebe", "leichhardt", "annandale", "balmain",
    "bondi", "double bay", "edgecliff", "woollahra", "paddington",
    "parramatta", "westmead", "merrylands", "granville", "auburn",
}

# Subject line keywords that indicate Cat 2 (agent-initiated outbound)
CAT2_SUBJECT_KEYWORDS = [
    "weekly listings",
    "new to market",
    "exclusive market preview",
    "exclusive preview",
    "property alert",
    "just listed",
    "open for inspection",
    "open home alert",
    "market preview",
    "matched properties",
    "your matched",
    "for lease listings",
    "properties for sale & lease",
    "properties for sale and lease",
    "listing alert",
    "new listing",
    "off market",
    "off-market",
    "a versatile opportunity",
    "new development",
]

# Sender patterns that indicate broadcast/CRM (Cat 2)
CAT2_SENDER_PATTERNS = [
    "no-reply",
    "noreply",
    "admin@",
    "info@",
    "bounces-",
    "newsletter",
    "alerts@",
    "listings@",
    "properties@",
]


# Noise subjects — discard entirely (not Cat1 or Cat2)
NOISE_SUBJECT_KEYWORDS = [
    "security code",
    "verification code",
    "confirm your email",
    "forwarding confirmation",
    "new login activity",
    "sign-in activity",
    "welcome to zoho",
    "access from anywhere",
    "microsoft account",
]

# Noise sender domains — discard entirely
NOISE_SENDER_DOMAINS = [
    "microsoft.com",
    "live.com",
    "zoho.com",
    "accounts.google.com",
    "googlemail.com",
]


# ── Email classifier ──────────────────────────────────────────────────────────

def is_noise(subject: str, sender_email: str) -> bool:
    """Returns True if email is system/account noise — should be skipped entirely."""
    subject_lower = subject.lower()
    sender_lower = sender_email.lower()

    for kw in NOISE_SUBJECT_KEYWORDS:
        if kw in subject_lower:
            return True

    for domain in NOISE_SENDER_DOMAINS:
        if sender_lower.endswith(domain):
            return True

    return False


def classify_email(subject: str, sender_email: str, body: str) -> int:
    """
    Returns 1 for Category 1 (agent inquiry reply)
    Returns 2 for Category 2 (agent-initiated outbound)
    Returns 0 for noise (skip)
    """
    subject_lower = subject.lower()
    sender_lower = sender_email.lower()

    # Strip Fwd:/Fw: prefix for matching — nesters forward Cat1 replies manually
    clean_subject = subject_lower
    for prefix in ("fwd: ", "fw: ", "fwd:", "fw:"):
        if clean_subject.startswith(prefix):
            clean_subject = clean_subject[len(prefix):].strip()
            break

    # ── Cat 1 checks first (strongest signals) ────────────────────────────────
    cat1_patterns = [
        "thanks for your enqui",
        "thank you for your enqui",
        "thank you for your interest",
        "your enquiry",
        "your recent enquiry",
        "enquiry on",
        "enquiry for",
        "enquiry-",
        "enquiry about",
        "re: enquiry",
        "property documents",
        "re: re:",
    ]
    for pattern in cat1_patterns:
        if pattern in clean_subject:
            return 1

    if clean_subject.startswith("re:"):
        return 1

    # ── Cat 2 subject keywords ────────────────────────────────────────────────
    for kw in CAT2_SUBJECT_KEYWORDS:
        if kw in clean_subject:
            return 2

    # ── Cat 2 sender patterns ─────────────────────────────────────────────────
    for pattern in CAT2_SENDER_PATTERNS:
        if pattern in sender_lower:
            return 2

    # ── Body-level Cat 1 signal — agent replying to a named nester ───────────
    body_lower = body[:500].lower()
    if any(phrase in body_lower for phrase in [
        "thank you for your enquiry",
        "thanks for your enquiry",
        "thank you for your interest",
        "thank you for getting in touch",
        "thanks for getting in touch",
    ]):
        return 1

    # Default to Cat 1 (safer)
    return 1


def detect_outbound_type(subject: str, sender_email: str, body: str) -> str:
    """
    For Cat 2 emails, determine the specific type F–L.
    """
    subject_lower = subject.lower()
    body_lower = body.lower()

    # H — pre-portal exclusive (highest priority check)
    if "exclusive market preview" in subject_lower or "exclusive preview" in subject_lower:
        return "H"

    # K — OFI + auction results digest
    if ("open for inspection" in subject_lower or "open home alert" in subject_lower) and (
        "auction" in body_lower or "sold" in body_lower
    ):
        return "K"

    # F — agency newsletter (batch listings)
    if any(kw in subject_lower for kw in [
        "weekly listings", "properties for sale & lease",
        "properties for sale and lease", "current listings"
    ]):
        return "F"

    # L — rental listings digest
    if "for lease" in subject_lower or "rental" in subject_lower:
        return "L"

    # I — CRM matched alert
    if any(kw in subject_lower for kw in [
        "matched properties", "your matched", "property alert", "listing alert"
    ]):
        return "I"

    # G — new to market (on portal)
    if any(kw in subject_lower for kw in ["new to market", "just listed", "new listing"]):
        return "G"

    # Default to G for unclassified agent-initiated single property emails
    return "G"


def detect_off_corridor(suburb: str) -> bool:
    """Returns True if suburb is outside pilot corridors."""
    if not suburb:
        return False
    return suburb.lower().strip() not in PILOT_SUBURBS


# ── IMAP helpers ──────────────────────────────────────────────────────────────

def decode_str(value):
    """Decode encoded email header strings."""
    if value is None:
        return ""
    parts = decode_header(value)
    result = []
    for part, charset in parts:
        if isinstance(part, bytes):
            result.append(part.decode(charset or "utf-8", errors="replace"))
        else:
            result.append(part)
    return "".join(result)


def extract_body(msg):
    """Extract plain text body from email message."""
    body = ""
    if msg.is_multipart():
        for part in msg.walk():
            ct = part.get_content_type()
            cd = str(part.get("Content-Disposition", ""))
            if ct == "text/plain" and "attachment" not in cd:
                charset = part.get_content_charset() or "utf-8"
                body += part.get_payload(decode=True).decode(charset, errors="replace")
    else:
        charset = msg.get_content_charset() or "utf-8"
        body = msg.get_payload(decode=True).decode(charset, errors="replace")
    return body.strip()


def has_attachments(msg):
    """Check if email has file attachments."""
    for part in msg.walk():
        cd = str(part.get("Content-Disposition", ""))
        if "attachment" in cd:
            return True
    return False


def extract_links(body):
    """Extract URLs from email body."""
    urls = re.findall(r'https?://[^\s<>"]+', body)
    return list(set(urls))


def fetch_unseen_emails():
    """Connect to Zoho IMAP and fetch all unseen messages."""
    messages = []
    try:
        mail = imaplib.IMAP4_SSL(IMAP_HOST, IMAP_PORT)
        mail.login(IMAP_USER, IMAP_PASS)
        mail.select("INBOX")

        _, data = mail.search(None, "UNSEEN")
        uids = data[0].split()

        print(f"[poller] Found {len(uids)} unseen email(s).")

        for uid in uids:
            _, msg_data = mail.fetch(uid, "(RFC822)")
            raw = msg_data[0][1]
            msg = email.message_from_bytes(raw)

            message_id = msg.get("Message-ID", "").strip()
            subject    = decode_str(msg.get("Subject", ""))
            from_addr  = decode_str(msg.get("From", ""))
            to_addr    = decode_str(msg.get("To", ""))
            date_str   = msg.get("Date", "")

            # Parse received date
            try:
                received_at = parsedate_to_datetime(date_str)
                if received_at.tzinfo is None:
                    received_at = received_at.replace(tzinfo=timezone.utc)
            except Exception:
                received_at = datetime.now(timezone.utc)

            body = extract_body(msg)
            attachments = has_attachments(msg)
            links = extract_links(body)

            # Extract sender email address only
            sender_email = re.search(r'[\w.+-]+@[\w-]+\.[a-z.]+', from_addr)
            sender_email = sender_email.group(0) if sender_email else from_addr

            # ── Nester identification (three-pass) ───────────────────────────
            # Pass 1: subject line tag e.g. [N01] — works for forwarded emails
            nester_id = None
            nester_email = None
            tag_match = re.search(r'\[N(\d{2})\]', subject, re.IGNORECASE)
            if tag_match:
                nester_id = f"N{tag_match.group(1)}"
                # Reverse-lookup email from nester_id
                nester_email = next(
                    (e for e, n in NESTER_MAP.items() if n == nester_id), None
                )

            # Pass 2: To: header contains nester address (direct delivery)
            if not nester_id:
                for ne in NESTER_MAP:
                    if ne.lower() in to_addr.lower():
                        nester_email = ne
                        nester_id = NESTER_MAP[ne]
                        break

            # Pass 3: sender IS a nester (nester emailing us directly)
            if not nester_id:
                for ne in NESTER_MAP:
                    if ne.lower() in sender_email.lower():
                        nester_email = ne
                        nester_id = NESTER_MAP[ne]
                        break

            messages.append({
                "message_id":    message_id,
                "subject":       subject,
                "agent_email":   sender_email,
                "nester_email":  nester_email,
                "nester_id":     nester_id or "UNKNOWN",
                "received_at":   received_at,
                "body":          body,
                "has_attachment": attachments,
                "links":         links,
            })

            # Mark as seen
            mail.store(uid, "+FLAGS", "\\Seen")

        mail.logout()

    except Exception as e:
        print(f"[poller] IMAP error: {e}")

    return messages


# ── Claude extraction — Category 1 (inquiry replies) ─────────────────────────

EXTRACTION_PROMPT_CAT1 = """You are a data extraction assistant for a property platform.
Read the following real estate agent email and extract structured information.
Return ONLY a valid JSON object with these exact keys.
If a field is not mentioned, return null for that field.

Keys to extract:
- agent_name: full name of the agent (string or null)
- agency: agency/company name (string or null)
- agent_phone: agent phone number (string or null)
- property_address: full property address (string or null)
- suburb: suburb name only, no state or postcode (string or null)
- price_guide: price guide as stated e.g. "$2,300,000" or "Guide $1.8m" (string or null)
- auction_date: auction date as stated e.g. "Saturday 16th May" (string or null)
- auction_venue: auction venue or "onsite" (string or null)
- open_home_times: inspection/open home times as stated (string or null)
- internal_size_sqm: internal area in sqm as a number only e.g. 49 (number or null)
- total_size_sqm: total area on title in sqm as a number only e.g. 273 (number or null)
- parking: parking description e.g. "one secure parking", "street with permit" (string or null)
- rental_estimate_pw: estimated weekly rent as a number only e.g. 2000 (number or null)
- outgoings: object with council, water, strata as annual or quarterly numbers e.g. {{"council_pq": 381, "water_pq": 201, "strata_pq": 1722}} use null for any not mentioned
- property_type: one of "apartment", "house", "townhouse", "unit" or null
- email_type: classify as one of:
    "A" = rich structured reply with multiple data fields
    "B" = short conversational reply answering a specific question
    "C" = virtual assistant / automated acknowledgement only
    "D" = document link drop (contract, FAQ, property docs)
    "E" = reply references attachments but body has little data
- anomaly_flag: true if something seems unusual or inconsistent, false otherwise
- anomaly_note: brief note if anomaly_flag is true, otherwise null

Return only the JSON object. No preamble, no explanation, no markdown.

Email subject: {subject}

Email body:
{body}
"""

# ── Claude extraction — Category 2 (agent-initiated outbound) ─────────────────

EXTRACTION_PROMPT_CAT2 = """You are a data extraction assistant for a property platform.
Read the following agent-initiated marketing email and extract structured information.
This email was NOT sent in response to an inquiry — it was sent proactively by an agent or agency.
Return ONLY a valid JSON object with these exact keys.
If a field is not mentioned, return null for that field.

Keys to extract:
- agency_name: agency or company name (string or null)
- agent_name: individual agent name if present (string or null)
- agent_email: agent email if present (string or null)
- listing_count: number of properties mentioned in this email (integer, minimum 1)
- listings_raw: array of listing objects, one per property mentioned. Each object should have:
    - street_address (string or null)
    - suburb (string or null)
    - property_type: one of "apartment", "house", "townhouse", "unit" or null
    - bedrooms (integer or null)
    - bathrooms (integer or null)
    - parking (integer or null)
    - land_size_sqm (number or null)
    - price_guide (string or null, e.g. "$940,000" or "Contact Agent")
    - auction_date (string or null)
    - inspection_times (array of strings or null)
    - asking_rent_pw (number or null — for rental listings only)
- is_pre_portal: true if the email explicitly says "exclusive", "exclusive preview", "not yet listed", or similar pre-portal language. Otherwise false.
- anomaly_flag: true if something seems unusual, false otherwise
- anomaly_note: brief note if anomaly_flag is true, otherwise null

Return only the JSON object. No preamble, no explanation, no markdown.

Email subject: {subject}

Email body:
{body}
"""


async def extract_with_claude(subject: str, body: str, category: int) -> dict:
    """Send email to Claude API for structured extraction."""
    if category == 2:
        prompt = EXTRACTION_PROMPT_CAT2.format(subject=subject, body=body[:4000])
    else:
        prompt = EXTRACTION_PROMPT_CAT1.format(subject=subject, body=body[:3000])

    async with httpx.AsyncClient(timeout=30.0) as client:
        response = await client.post(
            "https://api.anthropic.com/v1/messages",
            headers={
                "x-api-key": ANTHROPIC_API_KEY,
                "anthropic-version": "2023-06-01",
                "content-type": "application/json",
            },
            json={
                "model": "claude-haiku-4-5-20251001",
                "max_tokens": 1500,
                "messages": [{"role": "user", "content": prompt}],
            },
        )
        response.raise_for_status()
        data = response.json()
        raw_text = data["content"][0]["text"].strip()

        # Strip markdown fences if present
        lines = raw_text.splitlines()
        if lines and lines[0].strip().startswith("```"):
            lines = lines[1:]
        if lines and lines[-1].strip() == "```":
            lines = lines[:-1]
        raw_text = "\n".join(lines).strip()

        try:
            parsed = json.loads(raw_text)
        except json.JSONDecodeError:
            print(f"[poller] JSON parse failed for '{subject}' — storing raw with anomaly flag")
            parsed = {
                "anomaly_flag": True,
                "anomaly_note": f"Claude returned unparseable JSON: {raw_text[:200]}",
            }

        # Cat 1 only: ensure outgoings is always a dict or None
        if category == 1:
            outgoings = parsed.get("outgoings")
            if isinstance(outgoings, str):
                parsed["outgoings"] = None

        return parsed


# ── Neon storage — Category 1 ─────────────────────────────────────────────────

async def store_reply(conn, msg: dict, extracted: dict) -> bool:
    """Insert one agent reply row. Returns True if inserted, False if duplicate."""
    try:
        await conn.execute(
            """
            INSERT INTO agent_replies (
                received_at, message_id, nester_email, nester_id,
                agent_name, agent_email, agency, agent_phone,
                property_address, suburb,
                price_guide, auction_date, auction_venue, open_home_times,
                internal_size_sqm, total_size_sqm, parking,
                rental_estimate_pw, outgoings, property_type,
                email_type, has_attachment, document_links,
                raw_subject, raw_body,
                anomaly_flag, anomaly_note
            ) VALUES (
                $1,$2,$3,$4,$5,$6,$7,$8,$9,$10,
                $11,$12,$13,$14,$15,$16,$17,$18,$19,$20,
                $21,$22,$23,$24,$25,$26,$27
            )
            ON CONFLICT (message_id) DO NOTHING
            """,
            msg["received_at"],
            msg["message_id"],
            msg["nester_email"],
            msg["nester_id"],
            extracted.get("agent_name"),
            msg["agent_email"],
            extracted.get("agency"),
            extracted.get("agent_phone"),
            extracted.get("property_address"),
            extracted.get("suburb"),
            extracted.get("price_guide"),
            extracted.get("auction_date"),
            extracted.get("auction_venue"),
            extracted.get("open_home_times"),
            extracted.get("internal_size_sqm"),
            extracted.get("total_size_sqm"),
            extracted.get("parking"),
            extracted.get("rental_estimate_pw"),
            json.dumps(extracted.get("outgoings")) if isinstance(extracted.get("outgoings"), dict) else None,
            extracted.get("property_type"),
            extracted.get("email_type"),
            msg["has_attachment"],
            msg["links"] or [],
            msg["subject"],
            msg["body"],
            extracted.get("anomaly_flag", False),
            extracted.get("anomaly_note"),
        )
        return True
    except asyncpg.UniqueViolationError:
        return False


# ── Neon storage — Category 2 ─────────────────────────────────────────────────

async def store_outbound(conn, msg: dict, extracted: dict, email_type: str) -> bool:
    """Insert one agent outbound row. Returns True if inserted, False if duplicate."""

    listings_raw = extracted.get("listings_raw")
    listing_count = extracted.get("listing_count", 1)

    # For single-listing emails, pull fields from the first listing
    first = listings_raw[0] if listings_raw and len(listings_raw) > 0 else {}

    suburb = first.get("suburb") if first else None
    is_off_corridor = detect_off_corridor(suburb)
    is_pre_portal = extracted.get("is_pre_portal", False) or (email_type == "H")

    try:
        await conn.execute(
            """
            INSERT INTO agent_outbound (
                message_id, received_at,
                nester_id, nester_email,
                agency_name, agent_name, agent_email,
                email_type, is_off_corridor, is_pre_portal,
                street_address, suburb, property_type,
                bedrooms, bathrooms, parking,
                land_size_sqm, price_guide_low, price_guide_high,
                auction_date, inspection_times,
                listing_count, listings_raw,
                anomaly_flag, anomaly_note,
                raw_subject, raw_body
            ) VALUES (
                $1,$2,$3,$4,$5,$6,$7,$8,$9,$10,
                $11,$12,$13,$14,$15,$16,$17,$18,$19,$20,
                $21,$22,$23,$24,$25,$26,$27
            )
            ON CONFLICT (message_id) DO NOTHING
            """,
            msg["message_id"],
            msg["received_at"],
            msg["nester_id"],
            msg["nester_email"],
            extracted.get("agency_name"),
            extracted.get("agent_name"),
            msg["agent_email"],
            email_type,
            is_off_corridor,
            is_pre_portal,
            first.get("street_address"),
            suburb,
            first.get("property_type"),
            first.get("bedrooms"),
            first.get("bathrooms"),
            first.get("parking"),
            first.get("land_size_sqm"),
            None,   # price_guide_low — parsed from string, left for future pass
            None,   # price_guide_high
            first.get("auction_date"),
            json.dumps(first.get("inspection_times")) if first.get("inspection_times") else None,
            listing_count,
            json.dumps(listings_raw) if listings_raw else None,
            extracted.get("anomaly_flag", False),
            extracted.get("anomaly_note"),
            msg["subject"],
            msg["body"],
        )
        return True
    except asyncpg.UniqueViolationError:
        return False

async def maybe_promote_agent_email(conn, property_address: str, suburb: str, agent_email: str):
    """
    If a property record exists with a matching address and has no listing_agent_email,
    update it with the real agent email captured from the reply.
    """
    if not property_address or not agent_email:
        return

    result = await conn.fetchrow(
        """
        SELECT id, listing_agent_email
        FROM properties
        WHERE LOWER(address) = LOWER($1)
          AND listing_agent_email IS NULL
        LIMIT 1
        """,
        property_address,
    )

    if result:
        await conn.execute(
            "UPDATE properties SET listing_agent_email = $1 WHERE id = $2",
            agent_email,
            result["id"],
        )
        print(f"[poller] Agent email promoted: {agent_email} → property id {result['id']}")

# ── Main pipeline ─────────────────────────────────────────────────────────────

async def run_poller():
    print(f"[poller] Starting at {datetime.now(timezone.utc).isoformat()}")

    messages = fetch_unseen_emails()
    if not messages:
        print("[poller] No new emails. Done.")
        return

    conn = await asyncpg.connect(DATABASE_URL)

    inserted_cat1 = 0
    inserted_cat2 = 0
    skipped       = 0
    errors        = 0

    for msg in messages:
        try:
            # Skip noise emails entirely
            if is_noise(msg["subject"], msg["agent_email"]):
                print(f"[poller] Noise skipped: {msg['subject']}")
                skipped += 1
                continue

            # Classify email
            category = classify_email(msg["subject"], msg["agent_email"], msg["body"])

            # Check dedup against the right table
            if category == 1:
                existing = await conn.fetchval(
                    "SELECT id FROM agent_replies WHERE message_id = $1",
                    msg["message_id"]
                )
            else:
                existing = await conn.fetchval(
                    "SELECT id FROM agent_outbound WHERE message_id = $1",
                    msg["message_id"]
                )

            if existing:
                print(f"[poller] Duplicate skipped: {msg['subject']}")
                skipped += 1
                continue

            print(f"[poller] Cat{category} extracting: {msg['subject']}")
            extracted = await extract_with_claude(msg["subject"], msg["body"], category)

            # Anomaly flag if nester unknown
            if not msg["nester_email"]:
                extracted["anomaly_flag"] = True
                extracted["anomaly_note"] = (extracted.get("anomaly_note") or "") + \
                    " | Could not identify nester from To: field"

            if category == 1:
                ok = await store_reply(conn, msg, extracted)
                if ok:
                    inserted_cat1 += 1
                    await maybe_promote_agent_email(
                        conn,
                        extracted.get("property_address"),
                        extracted.get("suburb"),
                        msg["agent_email"],
                    )
                    flag = " [ANOMALY]" if extracted.get("anomaly_flag") else ""
                    print(f"[poller] Cat1 stored ({extracted.get('email_type','?')}){flag}: {msg['subject']}")
                else:
                    skipped += 1

            else:
                email_type = detect_outbound_type(msg["subject"], msg["agent_email"], msg["body"])
                ok = await store_outbound(conn, msg, extracted, email_type)
                if ok:
                    inserted_cat2 += 1
                    off = " [OFF-CORRIDOR]" if detect_off_corridor(
                        (extracted.get("listings_raw") or [{}])[0].get("suburb")
                    ) else ""
                    pre = " [PRE-PORTAL]" if extracted.get("is_pre_portal") else ""
                    print(f"[poller] Cat2 stored ({email_type}){off}{pre}: {msg['subject']}")
                else:
                    skipped += 1

        except Exception as e:
            errors += 1
            import traceback
            print(f"[poller] Error on '{msg['subject']}': {e}")
            traceback.print_exc()

    await conn.close()

    print(f"[poller] Done. cat1={inserted_cat1} cat2={inserted_cat2} skipped={skipped} errors={errors}")


if __name__ == "__main__":
    asyncio.run(run_poller())
