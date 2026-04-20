"""
email_poller.py — NestMatch agent reply ingestion pipeline
Session 20: IMAP polling + Claude extraction + Neon storage

Runs as a Render Cron Job every 15 minutes.
Reads unseen emails from buyers@nestmatch.com.au via IMAP,
extracts structured data using Claude API, stores to agent_replies table.

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
    "jam.nguyen@gmail.com":       "N01",
    "clawson1503@outlook.com":    "N02",
    "r.haines52@outlook.com":     "N03",
    "mpark_home@proton.me":       "N04",
    "awhitfieldnsw@gmail.com":    "N05",
    "sbrennan93@proton.me":       "N06",
    "k.daniel@tutamail.com":      "N07",
    "ktrann75@proton.me":         "N08",
    "moconnor1403@tutamail.com":  "N09",
}


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

            # Identify which nester this was sent to
            nester_email = None
            for ne in NESTER_MAP:
                if ne.lower() in to_addr.lower():
                    nester_email = ne
                    break

            messages.append({
                "message_id":    message_id,
                "subject":       subject,
                "agent_email":   sender_email,
                "nester_email":  nester_email,
                "nester_id":     NESTER_MAP.get(nester_email, "UNKNOWN") if nester_email else "UNKNOWN",
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


# ── Claude extraction ─────────────────────────────────────────────────────────

EXTRACTION_PROMPT = """You are a data extraction assistant for a property platform.
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
- outgoings: object with council, water, strata as annual or quarterly numbers — e.g. {{"council_pq": 381, "water_pq": 201, "strata_pq": 1722}} — use null for any not mentioned
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


async def extract_with_claude(subject: str, body: str) -> dict:
    """Send email to Claude API for structured extraction."""
    prompt = EXTRACTION_PROMPT.format(subject=subject, body=body[:3000])

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
                "max_tokens": 1000,
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
            # If JSON is malformed, return a minimal safe dict with anomaly flag
            print(f"[poller] JSON parse failed for '{subject}' — storing raw with anomaly flag")
            parsed = {
                "anomaly_flag": True,
                "anomaly_note": f"Claude returned unparseable JSON: {raw_text[:200]}",
            }

        # Ensure outgoings is always a dict or None, never a string
        outgoings = parsed.get("outgoings")
        if isinstance(outgoings, str):
            parsed["outgoings"] = None

        return parsed


# ── Neon storage ──────────────────────────────────────────────────────────────

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


# ── Main pipeline ─────────────────────────────────────────────────────────────

async def run_poller():
    print(f"[poller] Starting at {datetime.now(timezone.utc).isoformat()}")

    # Fetch unseen emails from Zoho
    messages = fetch_unseen_emails()
    if not messages:
        print("[poller] No new emails. Done.")
        return

    # Connect to Neon
    conn = await asyncpg.connect(DATABASE_URL)

    inserted = 0
    skipped  = 0
    errors   = 0

    for msg in messages:
        try:
            # Skip if message_id already exists
            existing = await conn.fetchval(
                "SELECT id FROM agent_replies WHERE message_id = $1",
                msg["message_id"]
            )
            if existing:
                print(f"[poller] Duplicate skipped: {msg['subject']}")
                skipped += 1
                continue

            # Extract with Claude
            print(f"[poller] Extracting: {msg['subject']}")
            extracted = await extract_with_claude(msg["subject"], msg["body"])

            # Flag anomalies for manual review
            if not msg["nester_email"]:
                extracted["anomaly_flag"] = True
                extracted["anomaly_note"] = (extracted.get("anomaly_note") or "") + \
                    " | Could not identify nester from To: field"

            # Store to Neon
            ok = await store_reply(conn, msg, extracted)
            if ok:
                inserted += 1
                flag = " [ANOMALY]" if extracted.get("anomaly_flag") else ""
                print(f"[poller] Stored ({extracted.get('email_type','?')}){flag}: {msg['subject']}")
            else:
                skipped += 1

        except Exception as e:
            errors += 1
            import traceback
            print(f"[poller] Error on '{msg['subject']}': {e}")
            traceback.print_exc()

    await conn.close()

    print(f"[poller] Done. inserted={inserted} skipped={skipped} errors={errors}")


if __name__ == "__main__":
    asyncio.run(run_poller())
