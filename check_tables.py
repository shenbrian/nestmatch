import os
import psycopg2

conn = psycopg2.connect(os.environ["DATABASE_URL"])
cur = conn.cursor()
cur.execute("SELECT suburb, school_rating, hospital_rating, commute_rating, commute_mode FROM properties ORDER BY suburb LIMIT 10")
for row in cur.fetchall():
    print(row)