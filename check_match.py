import asyncio, asyncpg, os

async def main():
    conn = await asyncpg.connect(os.environ['DATABASE_URL'])
    
    ar_rows = await conn.fetch("SELECT DISTINCT property_address FROM agent_replies WHERE property_address IS NOT NULL")
    ar_streets = {r['property_address'].split(',')[0].strip().lower() for r in ar_rows}
    print("agent_replies street keys:")
    for s in sorted(ar_streets):
        print(" ", s)
    
    print()
    
    p_rows = await conn.fetch("SELECT street_address, suburb, listing_status FROM properties WHERE street_address IS NOT NULL")
    p_streets = {r['street_address'].split(',')[0].strip().lower() for r in p_rows}
    print("properties street keys (for_sale):")
    for r in p_rows:
        if r['listing_status'] == 'for_sale':
            print(" ", r['street_address'].split(',')[0].strip().lower(), '|', r['suburb'])
    
    print()
    print("MATCHES:")
    for s in ar_streets:
        if s in p_streets:
            print(" HIT:", s)

    await conn.close()

asyncio.run(main())
