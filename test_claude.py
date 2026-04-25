import asyncio, httpx, re, json, os

ANTHROPIC_API_KEY = os.environ.get('ANTHROPIC_API_KEY', '')

async def test():
    async with httpx.AsyncClient(timeout=30.0) as client:
        response = await client.post(
            'https://api.anthropic.com/v1/messages',
            headers={
                'x-api-key': ANTHROPIC_API_KEY,
                'anthropic-version': '2023-06-01',
                'content-type': 'application/json',
            },
            json={
                'model': 'claude-haiku-4-5-20251001',
                'max_tokens': 1000,
                'messages': [{'role': 'user', 'content': 'Return only this JSON with no other text: {\"outgoings\": {\"council_pq\": 381, \"water_pq\": 201, \"strata_pq\": 1722}}'}],
            },
        )
        data = response.json()
        print(repr(data['content'][0]['text']))

asyncio.run(test())
