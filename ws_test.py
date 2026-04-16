import asyncio
import websockets
import json

async def test_ws():
    uri = "ws://localhost:8765"
    async with websockets.connect(uri) as ws:
        await ws.send("refresh")
        response = await ws.recv()
        data = json.loads(response)
        bars = data.get("bars", [])
        if not bars:
            print("No bars in response!")
            print(data.keys())
        else:
            print(f"Got {len(bars)} bars.")
            print("Last 3 bars CVD:", [b.get("cum_delta") for b in bars[-3:]])
        state = data.get("state", {})
        print("State cum_delta:", state.get("cum_delta"))

asyncio.run(test_ws())
