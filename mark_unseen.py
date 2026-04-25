"""
mark_unseen.py — mark all emails in buyers@ inbox as UNSEEN
Run once to allow poller to reprocess emails already marked as read.
"""

import imaplib
import os

IMAP_HOST = "imap.zoho.com.au"
IMAP_PORT = 993
IMAP_USER = "buyers@nestmatch.com.au"
IMAP_PASS = os.environ.get("ZOHO_IMAP_PASSWORD", "")

mail = imaplib.IMAP4_SSL(IMAP_HOST, IMAP_PORT)
mail.login(IMAP_USER, IMAP_PASS)
mail.select("INBOX")

_, data = mail.search(None, "ALL")
uids = data[0].split()
print(f"Found {len(uids)} total email(s) in inbox.")

for uid in uids:
    mail.store(uid, "-FLAGS", "\\Seen")

print(f"Marked {len(uids)} email(s) as UNSEEN.")
mail.logout()
