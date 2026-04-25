import asyncio, asyncpg, os

async def main():
    conn = await asyncpg.connect(os.environ['DATABASE_URL'])
    rows = await conn.fetch("""
        SELECT column_name, data_type
        FROM information_schema.columns
        WHERE table_name = 'agent_replies'
        ORDER BY ordinal_position
    """)
    for r in rows:
        print(dict(r))
    await conn.close()

asyncio.run(main())
