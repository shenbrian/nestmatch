"""
rerate_properties.py — Session 11
Re-scores all existing properties against the new Session 10 framework.
Run once from local (or Render shell) after Session 10 framework finialised.

Changes applied:
  • school_rating  → ICSEA catchment-anchored, private schools excluded
  • hospital_rating → major public ED driving distance only, GPs excluded
  • commute_rating  → best PT mode to CBD, train preferred, –2 mode penalty if no train within 2km
  • lifestyle_rating → column dropped permanently
  • suburb_trajectory → validated / backfilled to 'stable' if missing
  • commute_mode → new column surfacing 'train' | 'bus' | 'ferry' | 'no_train' label
"""

import os
import psycopg2
from psycopg2.extras import RealDictCursor

DATABASE_URL = os.environ["DATABASE_URL"]

# ---------------------------------------------------------------------------
# Calibrated scores — validated Session 10 calibration exercise
# Add any new suburbs here before running.
# Format: suburb_key (lowercase, no spaces) → dict of new scores
# ---------------------------------------------------------------------------
SUBURB_SCORES = {
    "epping": {
        "school_rating": 9, "hospital_rating": 6, "commute_rating": 9,
        "suburb_trajectory": "rising", "commute_mode": "train",
        "commute_drive_mins": 35
    },
    "hornsby": {
        "school_rating": 8, "hospital_rating": 8, "commute_rating": 6,
        "suburb_trajectory": "stable", "commute_mode": "train",
        "commute_drive_mins": 45
    },
    "terreyhills": {
        "school_rating": 3, "hospital_rating": 3, "commute_rating": 2,
        "suburb_trajectory": "stable", "commute_mode": "no_train",
        "commute_drive_mins": 50
    },
    "manly": {
        "school_rating": 5, "hospital_rating": 6, "commute_rating": 8,
        "suburb_trajectory": "stable", "commute_mode": "ferry",
        "commute_drive_mins": 30
    },
    "paddington": {
        "school_rating": 5, "hospital_rating": 9, "commute_rating": 9,
        "suburb_trajectory": "rising", "commute_mode": "bus",
        "commute_drive_mins": 15
    },
    "summerhillsydney": {
        "school_rating": 6, "hospital_rating": 7, "commute_rating": 9,
        "suburb_trajectory": "rising", "commute_mode": "train",
        "commute_drive_mins": 20
    },
    "neutralbay": {
        "school_rating": 5, "hospital_rating": 7, "commute_rating": 8,
        "suburb_trajectory": "stable", "commute_mode": "bus",
        "commute_drive_mins": 20
    },
    "fivedock": {
        "school_rating": 6, "hospital_rating": 7, "commute_rating": 5,
        "suburb_trajectory": "rising", "commute_mode": "bus",
        "commute_drive_mins": 25
    },
    "westpymble": {
        "school_rating": 8, "hospital_rating": 4, "commute_rating": 8,
        "suburb_trajectory": "stable", "commute_mode": "train",
        "commute_drive_mins": 40
    },
    "ashfield": {
        "school_rating": 6, "hospital_rating": 7, "commute_rating": 8,
        "suburb_trajectory": "rising", "commute_mode": "train",
        "commute_drive_mins": 20
    },
    "bankstown": {
        "school_rating": 5, "hospital_rating": 8, "commute_rating": 7,
        "suburb_trajectory": "rising", "commute_mode": "train",
        "commute_drive_mins": 30
    },
    "bayview": {
        "school_rating": 6, "hospital_rating": 4, "commute_rating": 2,
        "suburb_trajectory": "stable", "commute_mode": "no_train",
        "commute_drive_mins": 55
    },
    "blacktown": {
        "school_rating": 4, "hospital_rating": 8, "commute_rating": 6,
        "suburb_trajectory": "rising", "commute_mode": "train",
        "commute_drive_mins": 50
    },
    "canterbury": {
        "school_rating": 5, "hospital_rating": 6, "commute_rating": 8,
        "suburb_trajectory": "rising", "commute_mode": "train",
        "commute_drive_mins": 20
    },
    "chatswood": {
        "school_rating": 8, "hospital_rating": 6, "commute_rating": 9,
        "suburb_trajectory": "stable", "commute_mode": "train",
        "commute_drive_mins": 25
    },
    "coogee": {
        "school_rating": 6, "hospital_rating": 7, "commute_rating": 5,
        "suburb_trajectory": "stable", "commute_mode": "bus",
        "commute_drive_mins": 20
    },
    "cronulla": {
        "school_rating": 6, "hospital_rating": 5, "commute_rating": 5,
        "suburb_trajectory": "stable", "commute_mode": "train",
        "commute_drive_mins": 55
    },
    "deewhy": {
        "school_rating": 5, "hospital_rating": 5, "commute_rating": 4,
        "suburb_trajectory": "stable", "commute_mode": "bus",
        "commute_drive_mins": 45
    },
    "frenchsforest": {
        "school_rating": 6, "hospital_rating": 9, "commute_rating": 4,
        "suburb_trajectory": "rising", "commute_mode": "bus",
        "commute_drive_mins": 40
    },
    "huntershill": {
        "school_rating": 7, "hospital_rating": 5, "commute_rating": 6,
        "suburb_trajectory": "stable", "commute_mode": "bus",
        "commute_drive_mins": 30
    },
    "hurstville": {
        "school_rating": 6, "hospital_rating": 7, "commute_rating": 7,
        "suburb_trajectory": "rising", "commute_mode": "train",
        "commute_drive_mins": 35
    },
    "leichhardt": {
        "school_rating": 6, "hospital_rating": 8, "commute_rating": 7,
        "suburb_trajectory": "rising", "commute_mode": "bus",
        "commute_drive_mins": 15
    },
    "maroubra": {
        "school_rating": 5, "hospital_rating": 7, "commute_rating": 5,
        "suburb_trajectory": "rising", "commute_mode": "bus",
        "commute_drive_mins": 25
    },
    "marrickville": {
        "school_rating": 6, "hospital_rating": 7, "commute_rating": 8,
        "suburb_trajectory": "rising", "commute_mode": "train",
        "commute_drive_mins": 18
    },
    "marsfield": {
        "school_rating": 7, "hospital_rating": 6, "commute_rating": 6,
        "suburb_trajectory": "stable", "commute_mode": "bus",
        "commute_drive_mins": 35
    },
    "meadowbank": {
        "school_rating": 6, "hospital_rating": 6, "commute_rating": 8,
        "suburb_trajectory": "rising", "commute_mode": "train",
        "commute_drive_mins": 25
    },
    "merrylands": {
        "school_rating": 4, "hospital_rating": 7, "commute_rating": 6,
        "suburb_trajectory": "rising", "commute_mode": "train",
        "commute_drive_mins": 40
    },
    "mosman": {
        "school_rating": 8, "hospital_rating": 6, "commute_rating": 6,
        "suburb_trajectory": "stable", "commute_mode": "bus",
        "commute_drive_mins": 25
    },
    "newtown": {
        "school_rating": 5, "hospital_rating": 8, "commute_rating": 9,
        "suburb_trajectory": "stable", "commute_mode": "train",
        "commute_drive_mins": 12
    },
    "parramatta": {
        "school_rating": 5, "hospital_rating": 9, "commute_rating": 8,
        "suburb_trajectory": "rising", "commute_mode": "train",
        "commute_drive_mins": 35
    },
    "randwick": {
        "school_rating": 7, "hospital_rating": 10, "commute_rating": 6,
        "suburb_trajectory": "stable", "commute_mode": "bus",
        "commute_drive_mins": 20
    },
    "redfern": {
        "school_rating": 5, "hospital_rating": 9, "commute_rating": 9,
        "suburb_trajectory": "rising", "commute_mode": "train",
        "commute_drive_mins": 10
    },
    "rosebay": {
        "school_rating": 7, "hospital_rating": 6, "commute_rating": 5,
        "suburb_trajectory": "stable", "commute_mode": "bus",
        "commute_drive_mins": 25
    },
    "ryde": {
        "school_rating": 7, "hospital_rating": 6, "commute_rating": 7,
        "suburb_trajectory": "rising", "commute_mode": "train",
        "commute_drive_mins": 30
    },
    "strathfield": {
        "school_rating": 7, "hospital_rating": 6, "commute_rating": 8,
        "suburb_trajectory": "rising", "commute_mode": "train",
        "commute_drive_mins": 25
    },
    "summerhill": {
        "school_rating": 6, "hospital_rating": 7, "commute_rating": 9,
        "suburb_trajectory": "rising", "commute_mode": "train",
        "commute_drive_mins": 20
    },
    "sutherland": {
        "school_rating": 6, "hospital_rating": 6, "commute_rating": 6,
        "suburb_trajectory": "stable", "commute_mode": "train",
        "commute_drive_mins": 50
    },
    "sydneycity": {
        "school_rating": 4, "hospital_rating": 10, "commute_rating": 10,
        "suburb_trajectory": "stable", "commute_mode": "train",
        "commute_drive_mins": 0
    },
    "waverton": {
        "school_rating": 7, "hospital_rating": 6, "commute_rating": 8,
        "suburb_trajectory": "stable", "commute_mode": "train",
        "commute_drive_mins": 15
    },

}

def suburb_key(raw: str) -> str:
    return raw.lower().replace(" ", "").replace("-", "")

def main():
    conn = psycopg2.connect(DATABASE_URL)
    cur = conn.cursor(cursor_factory=RealDictCursor)

    # ── 1. Add new columns if they don't exist ──────────────────────────────
    print("Migrating schema...")
    migrations = [
        "ALTER TABLE properties ADD COLUMN IF NOT EXISTS commute_mode TEXT DEFAULT 'unknown'",
        "ALTER TABLE properties ADD COLUMN IF NOT EXISTS commute_drive_mins INT",
        "ALTER TABLE properties ADD COLUMN IF NOT EXISTS suburb_trajectory TEXT DEFAULT 'stable'",
    ]
    for sql in migrations:
        cur.execute(sql)

    # Drop lifestyle_rating if it still exists
    cur.execute("""
        SELECT column_name FROM information_schema.columns
        WHERE table_name = 'properties' AND column_name = 'lifestyle_rating'
    """)
    if cur.fetchone():
        cur.execute("ALTER TABLE properties DROP COLUMN lifestyle_rating")
        print("  ✓ lifestyle_rating column dropped")
    else:
        print("  ✓ lifestyle_rating already absent")

    conn.commit()

    # ── 2. Fetch all properties ────────────────────────────────────────────────
    cur.execute("SELECT id, suburb FROM properties")
    properties = cur.fetchall()
    print(f"\nFound {len(properties)} properties to re-score.")

    updated = 0
    skipped = 0

    for listing in properties:
        key = suburb_key(listing["suburb"])
        scores = SUBURB_SCORES.get(key)

        if not scores:
            # Fallback: leave existing values, set trajectory to 'stable' if null
            cur.execute("""
                UPDATE properties SET
                    suburb_trajectory = COALESCE(suburb_trajectory, 'stable'),
                    commute_mode = COALESCE(commute_mode, 'unknown')
                WHERE id = %s
            """, (listing["id"],))
            skipped += 1
            continue

        cur.execute("""
            UPDATE properties SET
                school_rating    = %(school_rating)s,
                hospital_rating  = %(hospital_rating)s,
                commute_rating   = %(commute_rating)s,
                suburb_trajectory = %(suburb_trajectory)s,
                commute_mode     = %(commute_mode)s,
                commute_drive_mins = %(commute_drive_mins)s
            WHERE id = %(id)s
        """, {**scores, "id": listing["id"]})
        updated += 1

    conn.commit()
    cur.close()
    conn.close()

    print(f"\n  ✓ Re-scored:  {updated} properties")
    print(f"  ⚠ Skipped (suburb not in map): {skipped} properties")
    if skipped > 0:
        print("    → Add missing suburbs to SUBURB_SCORES dict and re-run.")
    print("\nDone. Database is on Session 10 framework.")

if __name__ == "__main__":
    main()