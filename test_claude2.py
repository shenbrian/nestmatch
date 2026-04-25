import asyncio, httpx, re, json, os

ANTHROPIC_API_KEY = os.environ.get('ANTHROPIC_API_KEY', '')

async def test():
    prompt = 'Return only this exact JSON with no other text: {"agent_name": null, "outgoings": {"council_pq": 381}}'
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
                'messages': [{'role': 'user', 'content': prompt}],
            },
        )
        data = response.json()
        raw_text = data['content'][0]['text'].strip()
        print('RAW:', repr(raw_text))
        raw_text = re.sub(r'^\\\(?:json)?\s*\n?', '', raw_text, flags=re.MULTILINE)
        raw_text = re.sub(r'\n?\\\\s*$', '', raw_text, flags=re.MULTILINE)
        raw_text = raw_text.strip()
        print('CLEAN:', repr(raw_text))
        parsed = json.loads(raw_text)
        print('PARSED:', parsed)
        print('outgoings type:', type(parsed.get('outgoings')))

asyncio.run(test())
