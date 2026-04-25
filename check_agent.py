import asyncio, asyncpg, os
async def main():
    conn = await asyncpg.connect(os.environ['DATABASE_URL'])
    rows = await conn.fetch("SELECT property_address, agent_email, price_guide FROM agent_replies WHERE property_address IS NOT NULL LIMIT 5")
    for r in rows:
        print(dict(r))
    await conn.close()
asyncio.run(main())
