import asyncio, asyncpg, os

async def main():
    conn = await asyncpg.connect(os.environ['DATABASE_URL'])
    result = await conn.execute("""
        UPDATE properties
        SET price_min = 500000, price_max = 3000000
        WHERE listing_status = 'agent_sourced'
          AND price_max = 0
    """)
    print(result)
    await conn.close()

asyncio.run(main())
