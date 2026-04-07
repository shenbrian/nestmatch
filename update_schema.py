"""
NestMatch — update_schema.py (v3)
- Makes distance_to_station_m optional (allow NULL)
- Renames lifestyle_family_score to suburb_lifestyle_score

Usage:
    python update_schema.py
"""

import os
import psycopg2
from dotenv import load_dotenv

load_dotenv(r"C:\dev\nestmatch\.env")
DATABASE_URL = os.getenv("DATABASE_URL")

STEPS = [
    ("Allow NULL on distance_to_station_m",
     "ALTER TABLE property_features ALTER COLUMN distance_to_station_m DROP NOT NULL;"),
    ("Rename lifestyle_family_score to suburb_lifestyle_score",
     "ALTER TABLE property_features RENAME COLUMN lifestyle_family_score TO suburb_lifestyle_score;"),
]

def main():
    print("Connecting to Neon...")
    conn = psycopg2.connect(DATABASE_URL)
    conn.autocommit = True
    cur = conn.cursor()

    print("Applying schema changes...\n")
    for label, sql in STEPS:
        cur.execute(sql)
        print(f"  ✓ {label}")

    print("\nCurrent columns in property_features:")
    cur.execute("""
        SELECT column_name, is_nullable, data_type
        FROM information_schema.columns
        WHERE table_name = 'property_features' AND table_schema = 'public'
        ORDER BY ordinal_position;
    """)
    for row in cur.fetchall():
        nullable = "nullable" if row[1] == "YES" else "required"
        print(f"  • {row[0]:30s} {row[2]:20s} ({nullable})")

    cur.close()
    conn.close()
    print("\nSchema updated successfully.")

if __name__ == "__main__":
    main()