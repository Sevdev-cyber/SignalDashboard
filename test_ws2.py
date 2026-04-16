import asyncio
import websockets
import json

async def run():
    async with websockets.connect('wss://web-production-3ff3f.up.railway.app/ws') as ws:
        res = await ws.recv()
        j = json.loads(res)
        print(json.dumps(j.get('state', {}).get('trader_guide', {}), indent=2))

asyncio.run(run())
