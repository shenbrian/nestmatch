import psycopg2, os
conn = psycopg2.connect(os.environ['DATABASE_URL'])
cur = conn.cursor()
cur.execute("UPDATE properties SET suburb = 'Hunters Hill' WHERE suburb = 'Hunters hill'")
print(cur.rowcount, 'rows updated')
conn.commit()
conn.close()