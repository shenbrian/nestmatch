"""
fix_misclassified.py — delete Cat1 emails wrongly stored in agent_outbound
These will be reprocessed correctly on next poller run.
"""

import asyncio
import asyncpg
import os

# Subjects known to be Cat1 (agent inquiry replies) misclassified as Cat2
MISCLASSIFIED_SUBJECTS = [
    "Enquiry- 2001/79-81 Berry Street, North Sydney NSW 2060",
    "Fwd: Enquiry- 34 Nelson Street, Gordon NSW 2072",
    "Fwd: Your recent enquiry about 66 The Chase Road, Turramurra",
    "Fwd: Thanks for your enquiry on 35 Hamilton Corner, Lindfield NSW 2070",
]

async def main():
    db_url = os.environ.get("DATABASE_URL", "")
    conn = await asyncpg.connect(db_url)

    for subject in MISCLASSIFIED_SUBJECTS:
        result = await conn.execute(
            "DELETE FROM agent_outbound WHERE raw_subject = $1",
            subject
        )
        print(f"Deleted from agent_outbound: {subject} — {result}")

    await conn.close()
    print("Done. Run mark_unseen.py then email_poller.py to reprocess.")

asyncio.run(main())
