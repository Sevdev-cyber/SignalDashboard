"""Railway Relay Server — receives signal data via HTTP POST, broadcasts to browsers via WebSocket.

Architecture:
  Local signal_server.py  --POST /push-->  Railway relay  --WebSocket-->  Browser dashboard

Environment variables:
  PORT          — Railway sets this automatically
  PUSH_SECRET   — shared secret to authenticate local pushes (optional but recommended)
"""

import asyncio
import json
import logging
import os
from pathlib import Path

LOG_FMT = "%(asctime)s [%(name)-12s] %(levelname)-5s  %(message)s"
logging.basicConfig(level=logging.INFO, format=LOG_FMT)
log = logging.getLogger("relay")

try:
    from aiohttp import web
    import aiohttp
except ImportError:
    log.error("aiohttp required: pip install aiohttp")
    raise

# ── State ──
latest_data = {
    "type": "full_update",
    "state": {},
    "signals": [],
    "zones": {},
    "history": [],
}
ws_clients: set = set()
PUSH_SECRET = os.environ.get("PUSH_SECRET", "")


# ── WebSocket handler ──
async def ws_handler(request):
    ws = web.WebSocketResponse()
    await ws.prepare(request)
    ws_clients.add(ws)
    peer = request.remote
    log.info("🌐 Browser connected: %s (total: %d)", peer, len(ws_clients))

    # Send current state immediately
    try:
        if latest_data["state"]:
            await ws.send_str(json.dumps(latest_data))
    except Exception:
        pass

    try:
        async for msg in ws:
            if msg.type == aiohttp.WSMsgType.TEXT:
                if msg.data == "refresh" and latest_data["state"]:
                    await ws.send_str(json.dumps(latest_data))
            elif msg.type in (aiohttp.WSMsgType.ERROR, aiohttp.WSMsgType.CLOSE):
                break
    except Exception:
        pass
    finally:
        ws_clients.discard(ws)
        log.info("🌐 Browser disconnected: %s (total: %d)", peer, len(ws_clients))
    return ws


# ── Push endpoint (receives data from local signal_server) ──
async def push_handler(request):
    # Auth check: accept either what's in Railway env OR the fallback
    token = request.headers.get("X-Push-Secret", "").strip().strip("'").strip('"')
    accepted = ["SacredForestSignal1234"]
    if PUSH_SECRET:
        accepted.append(PUSH_SECRET.strip().strip("'").strip('"'))
        
    if token not in accepted:
        return web.Response(status=403, text="Forbidden")

    try:
        data = await request.json()
    except Exception as e:
        return web.Response(status=400, text=f"Bad JSON: {e}")

    # Update global state
    if "state" in data:
        latest_data["state"] = data["state"]
    if "signals" in data:
        latest_data["signals"] = data["signals"]
    if "zones" in data:
        latest_data["zones"] = data["zones"]
    if "history" in data:
        latest_data["history"] = data["history"]
    latest_data["type"] = data.get("type", "full_update")

    # Broadcast to all connected browsers
    msg = json.dumps(latest_data)
    dead = set()
    for ws in ws_clients:
        try:
            await ws.send_str(msg)
        except Exception:
            dead.add(ws)
    ws_clients -= dead

    n_signals = len(latest_data.get("signals", []))
    price = latest_data.get("state", {}).get("price", "?")
    log.info("📡 Push received: %d signals | price=%s | clients=%d",
             n_signals, price, len(ws_clients))

    return web.Response(text=f"OK: {n_signals} signals → {len(ws_clients)} clients")


# ── Health check ──
async def health_handler(request):
    return web.Response(text="OK")


# ── Dashboard HTML ──
async def dashboard_handler(request):
    html_path = Path(__file__).parent / "index_railway.html"
    if not html_path.exists():
        html_path = Path(__file__).parent / "index.html"
    content = html_path.read_text(encoding="utf-8")
    return web.Response(text=content, content_type="text/html")


# ── Main ──
def create_app():
    app = web.Application()
    app.router.add_get("/", dashboard_handler)
    app.router.add_get("/ws", ws_handler)
    app.router.add_post("/push", push_handler)
    app.router.add_get("/health", health_handler)
    return app


if __name__ == "__main__":
    port = int(os.environ.get("PORT", 8080))
    log.info("=" * 50)
    log.info("  📡 SIGNAL DASHBOARD RELAY")
    log.info("  Port: %d", port)
    log.info("  Push secret: %s", "SET" if PUSH_SECRET else "NONE")
    log.info("=" * 50)
    app = create_app()
    web.run_app(app, host="0.0.0.0", port=port)
