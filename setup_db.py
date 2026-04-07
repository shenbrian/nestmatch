"""
NestMatch — setup_db.py
Runs schema.sql against the Neon PostgreSQL database to create all tables.

Usage:
    python setup_db.py
"""

import os
import psycopg2
from dotenv import load_dotenv

load_dotenv(r"C:\dev\nestmatch\.env")
DATABASE_URL = os.getenv("DATABASE_URL")

SCHEMA_PATH = r"C:\dev\nestmatch\schema.sql"

def main():
    print(f"Reading schema: {SCHEMA_PATH}")
    with open(SCHEMA_PATH, "r", encoding="utf-8") as f:
        schema_sql = f.read()

    print("Connecting to Neon...")
    conn = psycopg2.connect(DATABASE_URL)
    conn.autocommit = True
    cur = conn.cursor()

    print("Running schema.sql...")
    cur.execute(schema_sql)

    print("\n✓ Tables created successfully:")
    cur.execute("""
        SELECT table_name FROM information_schema.tables
        WHERE table_schema = 'public'
        ORDER BY table_name;
    """)
    for row in cur.fetchall():
        print(f"  • {row[0]}")

    cur.close()
    conn.close()
    print("\nDatabase is ready.")

if __name__ == "__main__":
    main()
