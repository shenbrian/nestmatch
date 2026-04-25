import os
import psycopg2

conn = psycopg2.connect(os.environ["DATABASE_URL"])
cur = conn.cursor()
cur.execute("SELECT suburb, school_rating, hospital_rating, commute_rating FROM properties WHERE suburb IN ('West Pymble', 'Maroubra')")
print(cur.fetchall())