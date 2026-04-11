"""Signal Dashboard Server — TCP reader + WebSocket broadcaster.

Connects to NinjaTrader TickStreamerMirror (read-only) and broadcasts
enriched signal data to the web dashboard via WebSocket.

Usage:
    python signal_server.py --port 5557 --ws-port 8080
    python signal_server.py --demo  # demo mode with simulated data
"""

from __future__ import annotations

import argparse
import asyncio
import json
import logging
import sys
import threading
import time
import urllib.request
import urllib.error
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime
from pathlib import Path

import pandas as pd

sys.path.insert(0, str(Path(__file__).parent))

import compat  # noqa: F401 — patches dataclass for Python 3.9 (before hsb)
from signal_engine import SignalEngine

LOG_FMT = "%(asctime)s [%(name)-14s] %(levelname)-5s  %(message)s"
log = logging.getLogger("signal_dash")

# Try import websockets (async WebSocket server)
try:
    import websockets
    import websockets.server
    HAS_WS = True
except ImportError:
    HAS_WS = False
    log.warning("websockets not installed — run: pip install websockets")


class SignalDashboardServer:
    """Main server: reads TCP feed, computes signals, broadcasts via WS."""

    def __init__(self, *, tcp_host: str, tcp_port: int, ws_port: int, demo: bool = False, relay_url: str = None, relay_secret: str = None):
        self.tcp_host = tcp_host
        self.tcp_port = tcp_port
        self.ws_port = ws_port
        self.demo = demo
        self.relay_url = relay_url
        self.relay_secret = relay_secret
        self.executor = ThreadPoolExecutor(max_workers=2)

        self.engine = SignalEngine()
        self.bars_df = pd.DataFrame()
        self.current_price = 0.0
        self.bar_count = 0

        # Tick delta tracking
        self._bar_buy_vol = 0
        self._bar_sell_vol = 0
        self._bar_delta_pct = 0.0

        # WebSocket clients
        self._ws_clients: set = set()
        self.active_signals: dict = {}
        self.resolved_signals: list[dict] = []  # signals that hit TP/SL (kept for chart markers)
        self._latest_state: dict = {}
        self._latest_signals: list[dict] = []
        self._latest_zones: dict = {}
        self.loop = None

    # ── WebSocket Server ──

    def _safe_json(self, data: dict) -> str:
        """Serialize to JSON, replacing NaN/Infinity with null."""
        return json.dumps(data, allow_nan=True).replace('NaN', 'null').replace('Infinity', 'null').replace('-Infinity', 'null')

    async def _ws_handler(self, ws):
        """Handle a new WebSocket connection."""
        self._ws_clients.add(ws)
        peer = ws.remote_address
        log.info("🌐 WS client connected: %s", peer)
        try:
            # Send current state immediately
            if self._latest_state:
                await ws.send(self._safe_json({
                    "type": "full_update",
                    "state": self._latest_state,
                    "signals": self._latest_signals,
                    "resolved": self.resolved_signals,
                    "zones": self._latest_zones,
                    "history": self.engine.get_history(),
                }))
            async for msg in ws:
                # Client can request full refresh
                if msg == "refresh":
                    await ws.send(self._safe_json({
                        "type": "full_update",
                        "state": self._latest_state,
                        "signals": self._latest_signals,
                        "resolved": self.resolved_signals,
                        "zones": self._latest_zones,
                        "history": self.engine.get_history(),
                    }))
        except websockets.exceptions.ConnectionClosed:
            pass
        finally:
            self._ws_clients.discard(ws)
            log.info("🌐 WS client disconnected: %s", peer)

    async def _broadcast(self, data: dict):
        """Send data to all connected WebSocket clients AND to the relay."""
        msg = self._safe_json(data)
        
        # ALWAYS push to relay HTTP server (regardless of local WS clients)
        if self.relay_url:
            try:
                loop = self.loop or asyncio.get_event_loop()
                loop.run_in_executor(self.executor, self._push_to_relay, msg)
            except Exception as e:
                log.error("Relay dispatch error: %s", e)

        # Then broadcast to local WS clients (if any)
        dead = set()
        for ws in list(self._ws_clients):
            try:
                await ws.send(msg)
            except Exception:
                dead.add(ws)
        self._ws_clients -= dead

    def _push_to_relay(self, payload: str):
        """Push state payload to the remote relay server."""
        try:
            req = urllib.request.Request(self.relay_url, data=payload.encode('utf-8'))
            req.add_header('Content-Type', 'application/json')
            if self.relay_secret:
                req.add_header('X-Push-Secret', self.relay_secret)
            
            with urllib.request.urlopen(req, timeout=3) as response:
                if response.status in (200, 201):
                    log.debug("✅ Relay push OK (%d bytes)", len(payload))
                else:
                    log.warning("Relay push failed: %s", response.status)
        except urllib.error.URLError as e:
            log.error("Relay push failed (URLError): %s", e.reason)
        except Exception as e:
            log.error("Relay push failed: %s", e)

    # ── TCP Feed (reuses tcp_adapter patterns) ──

    def _start_tcp_reader(self):
        """Start TCP reader thread (same protocol as existing bots)."""
        if self.demo:
            threading.Thread(target=self._demo_loop, daemon=True).start()
            return

        from tcp_adapter import TickStreamerAdapter
        from bar_builder import warmup_bars_to_df, append_bar

        adapter = TickStreamerAdapter(host=self.tcp_host, port=self.tcp_port, dry_run=True)

        def on_warmup():
            self.bars_df = warmup_bars_to_df(adapter.warmup_bars)
            self.bar_count = len(self.bars_df)
            if not self.bars_df.empty:
                self.current_price = float(self.bars_df.iloc[-1]["close"])
            log.info("✅ Warmup: %d bars loaded | price=%.2f", self.bar_count, self.current_price)
            self._evaluate_and_broadcast()

        def on_bar_close(bar):
            from bar_builder import append_bar
            if self.bars_df.empty:
                return

            # Compute actual delta from tick data before appending
            total_vol = self._bar_buy_vol + self._bar_sell_vol
            true_delta = self._bar_buy_vol - self._bar_sell_vol
            
            if total_vol > 0:
                self._bar_delta_pct = (true_delta) / total_vol * 100
            
            self.bars_df = append_bar(self.bars_df, bar, true_delta=true_delta)
            self.bar_count += 1
            last = self.bars_df.iloc[-1]
            
            self._bar_buy_vol = 0
            self._bar_sell_vol = 0

            self.current_price = float(last["close"])
            log.info("BAR %d | C=%.2f | ATR=%.1f | delta=%.0f%%",
                     self.bar_count, self.current_price,
                     float(last.get("atr", 0)), self._bar_delta_pct)

            now = time.time()
            if not hasattr(self, '_last_full_broadcast'):
                self._last_full_broadcast = 0
            
            if now - self._last_full_broadcast >= 3:
                self._last_full_broadcast = now
                self._evaluate_and_broadcast()

        self._last_tick_broadcast = time.time()

        def on_tick(tick):
            self.current_price = tick.price
            if tick.aggressor == 1:
                self._bar_buy_vol += tick.size
            elif tick.aggressor == 2:
                self._bar_sell_vol += tick.size

            # Validate internal memory - resolve signals that hit TP or SL
            to_remove = []
            for sid, s in list(self.active_signals.items()):
                buffer = 1.0  # 1 point (4 MNQ ticks) margin
                hit = None
                if s["direction"] == "long":
                    if self.current_price <= s["sl"] + buffer:
                        hit = "SL"
                    elif self.current_price >= s["tp1"] - buffer:
                        hit = "TP"
                else: # short
                    if self.current_price >= s["sl"] - buffer:
                        hit = "SL"
                    elif self.current_price <= s["tp1"] + buffer:
                        hit = "TP"
                if hit:
                    log.info("🗑️ Signal RESOLVED [%s] %s %s @ %.2f → hit %s (price=%.2f)",
                             s["name"], s["direction"], s["id"][:8], s["entry"], hit, self.current_price)
                    resolved = dict(s)
                    resolved["hit"] = hit
                    resolved["hit_price"] = round(self.current_price, 2)
                    resolved["hit_time"] = int(time.time())
                    self.resolved_signals.append(resolved)
                    to_remove.append(sid)

            # Keep last 100 resolved signals for chart history
            if len(self.resolved_signals) > 100:
                self.resolved_signals = self.resolved_signals[-100:]

            for r in to_remove:
                del self.active_signals[r]

            # Broadcast tick update every 4 seconds
            now = time.time()
            if now - self._last_tick_broadcast >= 4:
                self._last_tick_broadcast = now
                self._tick_update()

        def on_heartbeat(meta):
            adapter.ping()  # keep connection alive

        adapter.on_warmup_complete = on_warmup
        adapter.on_bar_close = on_bar_close
        adapter.on_tick = on_tick
        adapter.on_heartbeat = on_heartbeat

        def tcp_loop():
            while True:
                try:
                    if not adapter.connect():
                        log.error("TCP connect failed, retry in 10s...")
                        time.sleep(10)
                        continue
                    adapter.ping()
                    adapter.read_loop()  # blocking
                except Exception as e:
                    log.error("TCP error: %s — reconnecting in 10s", e)
                    time.sleep(10)

        threading.Thread(target=tcp_loop, daemon=True).start()

    def _evaluate_and_broadcast(self):
        """Run signal engine and broadcast results."""
        new_signals = self.engine.evaluate(
            self.bars_df,
            bar_delta_pct=self._bar_delta_pct,
            current_price=self.current_price,
        )
        
        # Memory tracking
        now_ts = int(time.time() * 1000)
        # 1. Update existing or add new
        for s in new_signals:
            matched = False
            for sid, acts in self.active_signals.items():
                # Matching identical setups by their stable logical ID from the generator
                if acts["id"] == s["id"]:
                    acts["entry"] = s["entry"]
                    acts["sl"] = s["sl"]
                    acts["tp1"] = s["tp1"]
                    if s["confidence_pct"] > acts["confidence_pct"]:
                        acts["confidence_pct"] = s["confidence_pct"]
                    matched = True
                    break
                    
            if not matched:
                s["creation_time"] = now_ts
                # Remember what bar it originated on
                s["origin_bar"] = int(self.bar_count)
                # Keep the true historical origin_time provided by the engine
                if "origin_time" not in s:
                    s["origin_time"] = int(time.time())
                self.active_signals[s["id"]] = s

        state = self.engine.get_market_state(
            self.bars_df,
            current_price=self.current_price,
            bar_delta_pct=self._bar_delta_pct,
        )
        
        # Convert dictionary to descending sorted list for broadcast
        signals_payload = list(self.active_signals.values())
        signals_payload.sort(key=lambda x: x["confidence_pct"], reverse=True)
        
        zones = self.engine.compute_weighted_zones(signals_payload)

        self._latest_state = state
        self._latest_signals = signals_payload
        self._latest_zones = zones

        # Extract recent candlestick history for TradingView chart (last 800 bars for FVG context)
        bars_for_chart = []
        if not self.bars_df.empty:
            recent = self.bars_df.tail(800)
            for _, row in recent.iterrows():
                # Support both 'datetime' (live) and 'timestamp' (demo) column names
                dt = row.get("datetime") or row.get("timestamp")
                if dt is None or (hasattr(dt, '__class__') and str(dt) == 'NaT'):
                    continue
                try:
                    ts = int(dt.timestamp())
                except AttributeError:
                    ts = int(pd.to_datetime(dt).timestamp())
                bars_for_chart.append({
                    "time": ts,
                    "open": float(row["open"]),
                    "high": float(row["high"]),
                    "low": float(row["low"]),
                    "close": float(row["close"]),
                    "cum_delta": float(row.get("cum_delta", 0.0)),
                })

        # Schedule broadcast on the event loop safely from the TCP thread
        if self.loop:
            asyncio.run_coroutine_threadsafe(self._broadcast({
                "type": "full_update",
                "state": state,
                "signals": signals_payload,
                "resolved": self.resolved_signals,
                "zones": zones,
                "history": self.engine.get_history(),
                "bars": bars_for_chart,
            }), self.loop)

    def _tick_update(self):
        """Broadcast lightweight tick update every 4 seconds."""
        if not self._latest_state:
            return

        # Recompute state with current tick price
        state = self.engine.get_market_state(
            self.bars_df,
            current_price=self.current_price,
            bar_delta_pct=self._bar_delta_pct,
        )
        self._latest_state = state

        # Always send current active signals (may have been pruned by TP/SL)
        signals_payload = list(self.active_signals.values())
        signals_payload.sort(key=lambda x: x["confidence_pct"], reverse=True)
        self._latest_signals = signals_payload

        if self.loop:
            asyncio.run_coroutine_threadsafe(self._broadcast({
                "type": "tick_update",
                "state": state,
                "signals": signals_payload,
                "resolved": self.resolved_signals,
                "zones": self._latest_zones,
            }), self.loop)

    # ── Demo Mode ──

    def _demo_loop(self):
        """Generate fake signals for testing the dashboard without NT8."""
        import random
        from bar_builder import enrich_bars

        # Wait up to 3s for the asyncio loop to be set
        for _ in range(30):
            if self.loop:
                break
            time.sleep(0.1)

        log.info("🎭 DEMO MODE — generating simulated signals")
        CENTER_PRICE = 21500.0
        price = CENTER_PRICE
        bars = []

        # Prefill 800 historical bars (5-min bars going back ~2.7 days)
        BAR_SECS = 300  # 5 minutes per bar
        now_ts = int(pd.Timestamp.now(tz="UTC").timestamp())
        for i in range(800, 0, -1):
            atr = random.uniform(15, 35)
            # Mean-revert price to center
            price += (CENTER_PRICE - price) * 0.05 + random.uniform(-8, 8)
            bar_open = price + random.uniform(-3, 3)
            bar_high = bar_open + random.uniform(2, atr * 0.5)
            bar_low  = bar_open - random.uniform(2, atr * 0.5)
            bar_close = random.uniform(bar_low + 0.5, bar_high - 0.5)
            volume = random.randint(800, 4000)
            delta = random.uniform(-volume * 0.35, volume * 0.35)
            bar_dt = pd.Timestamp((now_ts - i * BAR_SECS), unit="s", tz="UTC")
            bars.append({
                "timestamp": bar_dt,
                "datetime":  bar_dt,
                "open": bar_open, "high": bar_high,
                "low": bar_low,   "close": bar_close,
                "volume": volume, "delta": delta,
            })
            price = bar_close

        self.bars_df = pd.DataFrame(bars)
        try:
            self.bars_df = enrich_bars(self.bars_df)
        except Exception as e:
            log.warning("enrich_bars warmup error: %s", e)
        self.bars_df["cum_delta"] = self.bars_df["delta"].cumsum()
        self.bar_count = len(self.bars_df)
        self.current_price = price
        self._evaluate_and_broadcast()

        # Maintain a simulated clock that advances by 5 minutes every loop
        sim_time = now_ts

        while True:
            # Advance simulated time
            sim_time += BAR_SECS
            bar_dt = pd.Timestamp(sim_time, unit="s", tz="UTC")

            # Mean-revert price to center, small random walk
            price += (CENTER_PRICE - price) * 0.05 + random.uniform(-8, 8)
            price = max(CENTER_PRICE - 200, min(CENTER_PRICE + 200, price))
            atr = random.uniform(15, 35)
            bar_open = price + random.uniform(-3, 3)
            bar_high = bar_open + random.uniform(2, atr * 0.5)
            bar_low  = bar_open - random.uniform(2, atr * 0.5)
            bar_close = random.uniform(bar_low + 0.5, bar_high - 0.5)
            volume = random.randint(800, 4000)
            delta = random.uniform(-volume * 0.35, volume * 0.35)

            bars.append({
                "timestamp": bar_dt,
                "datetime":  bar_dt,
                "open": bar_open, "high": bar_high,
                "low": bar_low,   "close": bar_close,
                "volume": volume, "delta": delta,
            })
            if len(bars) > 1000:
                bars = bars[-1000:]

            self.bars_df = pd.DataFrame(bars)
            try:
                self.bars_df = enrich_bars(self.bars_df)
            except Exception as e:
                log.warning("enrich_bars error: %s", e)
            self.bars_df["cum_delta"] = self.bars_df["delta"].cumsum()

            price = bar_close
            self.current_price = price
            self._bar_delta_pct = random.uniform(-25, 25)
            self.bar_count += 1

            self._evaluate_and_broadcast()
            time.sleep(5)

    # ── Main Run ──

    async def run(self):
        """Start WebSocket server and TCP reader."""
        log.info("=" * 60)
        log.info("  📡 SIGNAL DASHBOARD SERVER")
        log.info("  TCP: %s:%d | WS: localhost:%d",
                 self.tcp_host, self.tcp_port, self.ws_port)
        log.info("  Mode: %s", "DEMO" if self.demo else "LIVE")
        log.info("=" * 60)

        # Store main loop for thread-safe broadcasting
        self.loop = asyncio.get_running_loop()

        # Start TCP reader in background
        self._start_tcp_reader()

        # Start WebSocket server
        if not HAS_WS:
            log.error("websockets package required! pip install websockets")
            return

        async with websockets.server.serve(
            self._ws_handler,
            "0.0.0.0",
            self.ws_port,
        ):
            log.info("🌐 WebSocket server listening on ws://0.0.0.0:%d", self.ws_port)
            log.info("📊 Open dashboard: http://localhost:%d", self.ws_port + 1)
            await asyncio.Future()  # run forever


def main():
    parser = argparse.ArgumentParser(description="Signal Dashboard Server")
    parser.add_argument("--host", default="127.0.0.1", help="TCP host")
    parser.add_argument("--port", type=int, default=5557, help="TCP port (TickStreamer)")
    parser.add_argument("--ws-port", type=int, default=8080, help="WebSocket port")
    parser.add_argument("--demo", action="store_true", help="Demo mode (no NT8)")
    parser.add_argument("--relay-url", default=None, help="URL to push updates to (e.g. https://my-relay.railway.app/push)")
    parser.add_argument("--relay-secret", default=None, help="Secret token for push")
    parser.add_argument("--log-level", default="INFO")
    args = parser.parse_args()

    logging.basicConfig(level=getattr(logging, args.log_level), format=LOG_FMT)

    server = SignalDashboardServer(
        tcp_host=args.host, tcp_port=args.port,
        ws_port=args.ws_port, demo=args.demo,
        relay_url=args.relay_url, relay_secret=args.relay_secret
    )
    asyncio.run(server.run())


if __name__ == "__main__":
    main()
