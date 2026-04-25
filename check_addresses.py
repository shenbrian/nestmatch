import asyncio, asyncpg, os
async def main():
    conn = await asyncpg.connect(os.environ['DATABASE_URL'])
    rows = await conn.fetch("SELECT street_address, suburb FROM properties WHERE street_address IS NOT NULL LIMIT 5")
    for r in rows:
        print(dict(r))
    await conn.close()
asyncio.run(main())
