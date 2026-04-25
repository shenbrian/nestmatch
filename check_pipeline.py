import asyncio
import asyncpg
import os

async def check():
    conn = await asyncpg.connect(os.environ['DATABASE_URL'])
    rows = await conn.fetch("""
        SELECT nester_id, property_address, suburb, price_guide, received_at
        FROM agent_replies
        ORDER BY received_at DESC
        LIMIT 20
    """)
    print(f"{'NESTER':<6} {'PRICE GUIDE':<20} PROPERTY")
    print("-" * 70)
    for r in rows:
        guide = r['price_guide'] or 'no guide'
        print(f"{r['nester_id']:<6} {guide:<20} {r['property_address']}, {r['suburb']}")
    print(f"\nTotal rows shown: {len(rows)}")
    await conn.close()

asyncio.run(check())
