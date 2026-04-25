import asyncio, asyncpg, os

async def main():
    conn = await asyncpg.connect(os.environ['DATABASE_URL'])
    rows = await conn.fetch("""
        SELECT column_name, data_type, is_nullable, column_default
        FROM information_schema.columns
        WHERE table_name = 'properties'
        ORDER BY ordinal_position
    """)
    for r in rows:
        if r['is_nullable'] == 'NO':
            print('REQUIRED:', dict(r))
        else:
            print('optional:', r['column_name'])
    await conn.close()

asyncio.run(main())
