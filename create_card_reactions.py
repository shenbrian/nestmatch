import asyncio, asyncpg, os

async def main():
    conn = await asyncpg.connect(os.environ['DATABASE_URL'])
    await conn.execute("""
        CREATE TABLE IF NOT EXISTS card_reactions (
            id           SERIAL PRIMARY KEY,
            created_at   TIMESTAMPTZ DEFAULT now(),
            property_id  UUID NOT NULL,
            reaction     TEXT NOT NULL,
            search_params JSONB,
            session_id   TEXT
        )
    """)
    print('card_reactions table ready')
    await conn.close()

asyncio.run(main())
