"""
review_queue.py -- Daily enquiry review and send script
Session 31

Replaces review_enquiry.py as the daily workflow.
Pulls pending rows from enquiry_queue, shows each draft,
Brian approves (s=send) or skips (k=skip).

On send: fires email via smtplib from the nester's address,
         marks row as 'sent' in enquiry_queue.

Usage:
    cd C:\\dev\\nestmatch
    python review_queue.py

Environment variables required:
    DATABASE_URL          -- Neon connection string
    ZOHO_PASSWORD         -- Zoho App Password (nestmatch-poller)

Nester email credentials are loaded from a local .env or environment.
Proton nesters (N01, N04, N08 if applicable) require manual send --
script will show the draft and mark as 'manual_send_required'.
"""

import asyncio
import asyncpg
import os
import json
import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from datetime import datetime

# Nester SMTP config
# Gmail nesters: send via smtp.gmail.com with App Password
# Hotmail/Outlook: send via smtp-mail.outlook.com
# Proton: cannot auto-send — manual only

NESTER_SMTP: dict[str, dict] = {
    "N02": {"smtp": "smtp-mail.outlook.com", "port": 587, "user": "clawson@hotmail.com"},
    "N03": {"smtp": "smtp-mail.outlook.com", "port": 587, "user": "r.haines52@outlook.com"},
    "N05": {"smtp": "smtp.gmail.com",         "port": 587, "user": "awhitfield@gmail.com"},
    "N06": {"smtp": "smtp.gmail.com",         "port": 587, "user": "sbrennan93@gmail.com"},
    "N07": {"smtp": "smtp-mail.outlook.com", "port": 587, "user": "d.kim96@hotmail.com"},
    "N09": {"smtp": "smtp.gmail.com",         "port": 587, "user": "moconnor@gmail.com"},
}

# Nesters that require manual send (Proton Mail)
MANUAL_NESTERS = {"N01", "N04", "N08"}

# Nester display names
NESTER_NAMES: dict[str, str] = {
    "N01": "James Nguyen (Proton — MANUAL)",
    "N02": "Catherine Lawson",
    "N03": "Robert Haines",
    "N04": "Michelle Park (Proton — MANUAL)",
    "N05": "Andrew Whitfield",
    "N06": "Sophie Brennan",
    "N07": "Daniel Kim",
    "N08": "Karen Tran (Proton — MANUAL)",
    "N09": "Michael O'Connor",
}


def send_email(nester_id: str, to_email: str, to_name: str, body: str, password: str) -> bool:
    """Send enquiry email from nester SMTP account. Returns True on success."""
    if nester_id not in NESTER_SMTP:
        return False

    cfg = NESTER_SMTP[nester_id]
    from_email = cfg["user"]
    subject = f"Property enquiry"

    msg = MIMEMultipart("alternative")
    msg["Subject"] = subject
    msg["From"] = from_email
    msg["To"] = to_email
    msg.attach(MIMEText(body, "plain"))

    try:
        with smtplib.SMTP(cfg["smtp"], cfg["port"]) as server:
            server.ehlo()
            server.starttls()
            server.login(from_email, password)
            server.sendmail(from_email, to_email, msg.as_string())
        return True
    except Exception as e:
        print(f"  SMTP error: {e}")
        return False


async def main():
    database_url = os.environ.get("DATABASE_URL")
    if not database_url:
        print("ERROR: DATABASE_URL not set.")
        return

    conn = await asyncpg.connect(database_url)

    rows = await conn.fetch(
        """
        SELECT id, property_id, nester_id, agent_email, agent_name,
               street_address, suburb, property_type, email_body,
               triggered_by, created_at
        FROM enquiry_queue
        WHERE status = 'pending'
        ORDER BY created_at ASC
        """
    )

    if not rows:
        print("No pending enquiries in queue.")
        await conn.close()
        return

    print(f"\n{'='*60}")
    print(f"  NESTMATCH ENQUIRY QUEUE — {len(rows)} pending")
    print(f"{'='*60}\n")

    sent = 0
    skipped = 0

    for row in rows:
        nester_display = NESTER_NAMES.get(row["nester_id"], row["nester_id"])
        is_manual = row["nester_id"] in MANUAL_NESTERS

        print(f"--- Enquiry #{row['id']} ---")
        print(f"  Property : {row['street_address'] or 'Unknown'}, {row['suburb']}")
        print(f"  Type     : {row['property_type']}")
        print(f"  Agent    : {row['agent_name'] or 'Unknown'} <{row['agent_email']}>")
        print(f"  Nester   : {nester_display}")
        print(f"  Triggered: {row['triggered_by']} at {row['created_at'].strftime('%d %b %H:%M')}")
        print(f"\n  --- EMAIL BODY ---")
        print(f"{row['email_body']}")
        print(f"  --- END ---\n")

        if is_manual:
            print(f"  [MANUAL SEND REQUIRED — Proton Mail]")
            print(f"  Open {nester_display.split('(')[0].strip()}'s Proton inbox and send manually.")
            action = input("  Mark as sent after manual send? (s=sent / k=skip): ").strip().lower()
            if action == "s":
                await conn.execute(
                    "UPDATE enquiry_queue SET status='sent', actioned_at=NOW() WHERE id=$1",
                    row["id"]
                )
                print("  Marked as sent.\n")
                sent += 1
            else:
                await conn.execute(
                    "UPDATE enquiry_queue SET status='skipped', actioned_at=NOW() WHERE id=$1",
                    row["id"]
                )
                print("  Skipped.\n")
                skipped += 1
        else:
            action = input("  Action? (s=send / k=skip / q=quit): ").strip().lower()
            if action == "q":
                print("Exiting.")
                break
            elif action == "s":
                password = os.environ.get(f"SMTP_PASS_{row['nester_id']}", "")
                if not password:
                    password = input(f"  App password for {cfg_user}: ").strip()

                success = send_email(
                    row["nester_id"],
                    row["agent_email"],
                    row["agent_name"] or "Agent",
                    row["email_body"],
                    password,
                )
                if success:
                    await conn.execute(
                        "UPDATE enquiry_queue SET status='sent', actioned_at=NOW() WHERE id=$1",
                        row["id"]
                    )
                    print(f"  Sent. Marked as sent.\n")
                    sent += 1
                else:
                    print(f"  Send failed. Left as pending.\n")
            else:
                await conn.execute(
                    "UPDATE enquiry_queue SET status='skipped', actioned_at=NOW() WHERE id=$1",
                    row["id"]
                )
                print("  Skipped.\n")
                skipped += 1

    await conn.close()
    print(f"\n{'='*60}")
    print(f"  Done. Sent: {sent}  Skipped: {skipped}")
    print(f"{'='*60}\n")


if __name__ == "__main__":
    asyncio.run(main())
