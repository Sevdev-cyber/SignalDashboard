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
import os
import sys
import threading
import time
import urllib.request
import urllib.error
from concurrent.futures import ThreadPoolExecutor
from collections import defaultdict, deque
from datetime import datetime
from pathlib import Path

import pandas as pd

sys.path.insert(0, str(Path(__file__).parent))

import compat  # noqa: F401 — patches dataclass for Python 3.9 (before hsb)
from signal_engine import SignalEngine
from bar_builder import get_target_tf_min

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

    def __init__(
        self,
        *,
        tcp_host: str,
        tcp_port: int,
        ws_port: int,
        demo: bool = False,
        relay_url: str = None,
        relay_secret: str = None,
        account_name: str | None = None,
    ):
        self.tcp_host = tcp_host
        self.tcp_port = tcp_port
        self.ws_port = ws_port
        self.demo = demo
        self.relay_url = relay_url
        self.relay_secret = relay_secret
        self.account_name = (account_name or "").strip() or None
        self.executor = ThreadPoolExecutor(max_workers=2)
        self.bar_tf_min = get_target_tf_min()

        self.engine = SignalEngine()
        self.bars_df = pd.DataFrame()
        self.current_price = 0.0
        self.bar_count = 0

        # Tick delta tracking (Bookmap style: raw volume, not percentage)
        self._bar_buy_vol = 0
        self._bar_sell_vol = 0
        self._bar_delta_pct = 0.0      # legacy: still used by engine
        self._bar_delta_raw = 0        # buy_vol - sell_vol (Bookmap style)
        self._session_cvd = 0          # session cumulative delta
        self._bar_trade_value = 0.0    # sum(price * vol) for tick-level VWAP

        # WebSocket clients
        self._ws_clients: set = set()
        self.active_signals: dict = {}
        self.resolved_signals: list[dict] = []  # signals that hit TP/SL (kept for chart markers)
        self._latest_state: dict = {}
        self._latest_signals: list[dict] = []
        self._latest_ghost_signals: list[dict] = []
        self._latest_engine_debug: dict = {}
        self._latest_zones: dict = {}
        self._recent_ticks = deque(maxlen=1200)
        self._l2_display_state = {
            "bias": "neutral",
            "confidence": 0,
            "candidate": None,
            "candidate_count": 0,
            "updated_at": 0.0,
        }
        self._last_local_emit = 0.0
        self._last_relay_emit = 0.0
        self._min_local_emit_sec = 1.0
        self._min_relay_emit_sec = 1.0
        self.loop = None

    # ── WebSocket Server ──

    def _safe_json(self, data: dict) -> str:
        """Serialize to JSON, replacing NaN/Infinity with null."""
        return json.dumps(data, allow_nan=True, default=self._json_fallback).replace('NaN', 'null').replace('Infinity', 'null').replace('-Infinity', 'null')

    @staticmethod
    def _json_fallback(obj):
        """Handle non-serializable types (Timestamp, numpy, etc.)."""
        import numpy as np
        if hasattr(obj, 'timestamp'):  # pandas.Timestamp, datetime
            return int(obj.timestamp())
        if hasattr(obj, 'isoformat'):  # datetime
            return obj.isoformat()
        if isinstance(obj, (np.integer,)):
            return int(obj)
        if isinstance(obj, (np.floating,)):
            return float(obj)
        if isinstance(obj, (np.ndarray,)):
            return obj.tolist()
        if isinstance(obj, set):
            return list(obj)
        return str(obj)

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
                    "ghost_signals": self._latest_ghost_signals,
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
                        "ghost_signals": self._latest_ghost_signals,
                        "resolved": self.resolved_signals,
                        "zones": self._latest_zones,
                        "history": self.engine.get_history(),
                    }))
        except websockets.exceptions.ConnectionClosed:
            pass
        finally:
            self._ws_clients.discard(ws)
            log.info("🌐 WS client disconnected: %s", peer)

    async def _broadcast_split(self, full_payload: dict, all_bars: list):
        """Send full data to local WS clients, reduced data to relay."""
        now = time.time()
        full_msg = self._safe_json(full_payload)

        # Relay gets reduced payload: top 15 signals, last 200 bars, no history
        if self.relay_url and (now - self._last_relay_emit >= self._min_relay_emit_sec):
            relay_payload = dict(full_payload)
            relay_payload["signals"] = full_payload.get("signals", [])[:15]
            relay_payload["bars"] = all_bars[-200:] if all_bars else []
            relay_payload["history"] = []
            relay_payload["resolved"] = full_payload.get("resolved", [])[-20:]
            relay_msg = self._safe_json(relay_payload)
            log.info("Relay payload: %d bytes (signals=%d, bars=%d)",
                     len(relay_msg), len(relay_payload["signals"]), len(relay_payload["bars"]))
            try:
                loop = self.loop or asyncio.get_event_loop()
                loop.run_in_executor(self.executor, self._push_to_relay, relay_msg)
                self._last_relay_emit = now
            except Exception as e:
                log.error("Relay dispatch error: %s", e)

        # Local WS gets full payload
        if now - self._last_local_emit < self._min_local_emit_sec:
            return

        dead = set()
        for ws in list(self._ws_clients):
            try:
                await ws.send(full_msg)
            except Exception:
                dead.add(ws)
        self._ws_clients -= dead
        self._last_local_emit = now

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
            import ssl
            ctx = ssl.create_default_context()
            req = urllib.request.Request(self.relay_url, data=payload.encode('utf-8'))
            req.add_header('Content-Type', 'application/json')
            if self.relay_secret:
                req.add_header('X-Push-Secret', self.relay_secret)

            with urllib.request.urlopen(req, timeout=10, context=ctx) as response:
                if response.status in (200, 201):
                    log.info("✅ Relay push OK (%d bytes)", len(payload))
                else:
                    log.warning("Relay push failed: HTTP %s", response.status)
        except urllib.error.URLError as e:
            log.error("Relay push failed (URLError): reason=%r | %s", e.reason, e)
        except Exception as e:
            log.error("Relay push failed: %s: %s", type(e).__name__, e)

    # ── TCP Feed (reuses tcp_adapter patterns) ──

    def _start_tcp_reader(self):
        """Start TCP reader thread (same protocol as existing bots)."""
        if self.demo:
            threading.Thread(target=self._demo_loop, daemon=True).start()
            return

        from tcp_adapter import TickStreamerAdapter
        from bar_builder import warmup_bars_to_df, append_bar, apply_tick_deltas, BarAccumulator, enrich_bars

        bar_accum = BarAccumulator(target_min=self.bar_tf_min)

        adapter = TickStreamerAdapter(host=self.tcp_host, port=self.tcp_port, dry_run=True)

        def on_warmup():
            # Always update bars (reconnect may have newer data)
            self.bars_df = warmup_bars_to_df(adapter.warmup_bars)

            # Always apply tick-based flow on warmup if ticks are available.
            if adapter.warmup_ticks:
                log.info("🎯 Processing %d warmup ticks for real delta/CVD/VWAP...", len(adapter.warmup_ticks))
                self.bars_df = apply_tick_deltas(self.bars_df, adapter.warmup_ticks)
                log.info("✅ Tick-based delta applied to %d bars", len(self.bars_df))
            else:
                log.info("⏭️ No warmup ticks available; using bar-derived flow fallback")

            self.bar_count = len(self.bars_df)
            self._bar_buy_vol = 0
            self._bar_sell_vol = 0
            self._bar_trade_value = 0.0
            if not self.bars_df.empty:
                self.current_price = float(self.bars_df.iloc[-1]["close"])
                if "cum_delta" in self.bars_df.columns:
                    self._session_cvd = int(float(self.bars_df.iloc[-1].get("cum_delta", 0)))
                    log.info("📊 Session CVD initialized at %+d from warmup", self._session_cvd)
            log.info("✅ Warmup: %d bars loaded | price=%.2f", self.bar_count, self.current_price)
            # Schedule heavy signal evaluation OFF the TCP thread (non-blocking)
            self.executor.submit(self._evaluate_and_broadcast)

        def on_bar_close(bar):
            if self.bars_df.empty:
                return

            # Compute actual delta from tick data (Bookmap style: raw contracts)
            buy_v = self._bar_buy_vol
            sell_v = self._bar_sell_vol
            true_delta = buy_v - sell_v
            trade_value = self._bar_trade_value
            self._bar_delta_raw = true_delta

            total_vol = buy_v + sell_v
            if total_vol > 0:
                self._bar_delta_pct = (true_delta) / total_vol * 100

            # Accumulate sub-target bars into target candles
            completed = bar_accum.add_bar(bar, true_delta=true_delta,
                                          buy_vol=float(buy_v), sell_vol=float(sell_v),
                                          trade_value=float(trade_value))

            # Reset tick accumulators for next input bar
            self._bar_buy_vol = 0
            self._bar_sell_vol = 0
            self._bar_trade_value = 0.0

            # Update current price from every input bar (responsive)
            self.current_price = bar.close

            if completed is None:
                # Still accumulating — don't emit bar yet
                log.debug("Accumulating bar: C=%.2f Δ=%+d", bar.close, true_delta)
                return

            # ── target timeframe bar completed ──
            new_row = completed  # already a dict with all fields
            df = pd.concat([self.bars_df, pd.DataFrame([new_row])], ignore_index=True)
            self.bars_df = enrich_bars(df)
            self.bar_count += 1
            last = self.bars_df.iloc[-1]
            self._session_cvd = int(float(last.get("cum_delta", self._session_cvd)))

            self.current_price = float(last["close"])
            log.info("%dMIN BAR %d | C=%.2f | ATR=%.1f | Δ=%+d (buy=%.0f sell=%.0f) | CVD=%+d | VWAP=%.2f",
                     self.bar_tf_min, self.bar_count, self.current_price,
                     float(last.get("atr", 0)), int(completed["delta"]),
                     completed.get("buy_volume", 0), completed.get("sell_volume", 0),
                     self._session_cvd, float(last.get("vwap", self.current_price)))

            now = time.time()
            if not hasattr(self, '_last_full_broadcast'):
                self._last_full_broadcast = 0

            if now - self._last_full_broadcast >= 1:
                self._last_full_broadcast = now
                # Schedule heavy signal evaluation OFF the TCP thread (non-blocking)
                self.executor.submit(self._evaluate_and_broadcast)

        self._last_tick_broadcast = time.time()

        def on_tick(tick):
            self.current_price = tick.price
            self._recent_ticks.append(
                {
                    "ts": time.time(),
                    "price": float(tick.price),
                    "size": int(tick.size),
                    "aggressor": int(tick.aggressor),
                    "bid": float(tick.bid),
                    "ask": float(tick.ask),
                }
            )
            if tick.aggressor == 1:
                self._bar_buy_vol += tick.size
            elif tick.aggressor == 2:
                self._bar_sell_vol += tick.size
            # Track trade value for tick-level VWAP (Bookmap style)
            self._bar_trade_value += tick.price * tick.size

            # Track min/max price per active signal (detects SL/TP even if price bounces back)
            p = tick.price
            for sid, s in self.active_signals.items():
                if "price_min" not in s:
                    s["price_min"] = p
                    s["price_max"] = p
                s["price_min"] = min(s["price_min"], p)
                s["price_max"] = max(s["price_max"], p)
                # Track if entry was touched (limit filled)
                if not s.get("entry_touched"):
                    if s["direction"] == "long" and p <= s["entry"]:
                        s["entry_touched"] = True
                        s["entry_touched_time"] = int(time.time())
                    elif s["direction"] == "short" and p >= s["entry"]:
                        s["entry_touched"] = True
                        s["entry_touched_time"] = int(time.time())

            # Broadcast tick update every 1 second
            now = time.time()
            if now - self._last_tick_broadcast >= 1:
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
                    if self.account_name:
                        log.info("Account preference requested: %s", self.account_name)
                        adapter.set_account(self.account_name)
                    adapter.get_account()
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
        ghost_signals = self.engine.get_ghost_signals() if hasattr(self.engine, "get_ghost_signals") else []
        engine_debug = self.engine.get_debug_info() if hasattr(self.engine, "get_debug_info") else {}

        now_ts = int(time.time() * 1000)
        price = self.current_price
        atr = float(self.bars_df.iloc[-1].get("atr", 20)) if not self.bars_df.empty else 20

        # ── PERSISTENT SIGNAL MANAGEMENT ──
        # Signals persist until SL/TP hit or price moves >2.5 ATR away.
        # Engine may stop regenerating a signal (pattern conditions shifted)
        # but the TRADE SETUP is still valid if levels haven't been breached.

        # Use last bar time as reference (not wall clock — important for replay)
        last_bar_time = 0
        if not self.bars_df.empty:
            dt = self.bars_df.iloc[-1].get("datetime")
            if dt is not None:
                try:
                    last_bar_time = int(pd.to_datetime(dt).timestamp())
                except Exception:
                    last_bar_time = int(now_ts / 1000)
            else:
                last_bar_time = int(now_ts / 1000)
        else:
            last_bar_time = int(now_ts / 1000)

        def _is_dead(s, p, a):
            """Check if signal's trade setup has been invalidated.

            Uses price_min/price_max to detect SL/TP hits even if price bounced back.
            """
            pmin = s.get("price_min", p)
            pmax = s.get("price_max", p)

            if s["direction"] == "long":
                # SL hit if price ever went below SL
                if pmin <= s["sl"]:
                    return "sl_hit"
                # TP hit if price ever went above TP1
                if pmax >= s["tp1"]:
                    return "tp_hit"
            else:
                # SL hit if price ever went above SL
                if pmax >= s["sl"]:
                    return "sl_hit"
                # TP hit if price ever went below TP1
                if pmin <= s["tp1"]:
                    return "tp_hit"

            # Per-signal hold time (scalps expire fast, swings stay longer)
            max_hold = s.get("max_hold_bars", 48)  # from SIGNAL_TIME_PROFILE
            max_hold_sec = max_hold * self.bar_tf_min * 60
            age_s = last_bar_time - s.get("origin_time", last_bar_time)
            if age_s > max_hold_sec:
                return "expired"
            return None

        # Step 1: Carry forward existing signals that are still alive
        carried = 0
        expired = 0
        merged = {}
        for sid, s in self.active_signals.items():
            death = _is_dead(s, price, atr)
            if death:
                expired += 1
                log.info("Signal expired: %s %s — reason: %s",
                         s["direction"], s["name"], death)
            else:
                merged[sid] = s
                carried += 1

        # Step 2: Add/update with freshly generated signals
        added = 0
        skipped_dead = 0
        dead_reasons = {}
        for s in new_signals:
            death = _is_dead(s, price, atr)
            if death:
                skipped_dead += 1
                dead_reasons[death] = dead_reasons.get(death, 0) + 1
                continue

            # Preserve creation_time + origin_time from existing signal
            if s["id"] in merged:
                s["creation_time"] = merged[s["id"]].get("creation_time", now_ts)
                s["origin_time"] = merged[s["id"]].get("origin_time", int(time.time()))
                s["origin_bar"] = merged[s["id"]].get("origin_bar", int(self.bar_count))
            elif s["id"] in self.active_signals:
                s["creation_time"] = self.active_signals[s["id"]].get("creation_time", now_ts)
                s["origin_time"] = self.active_signals[s["id"]].get("origin_time", int(time.time()))
                s["origin_bar"] = self.active_signals[s["id"]].get("origin_bar", int(self.bar_count))
            else:
                s["creation_time"] = now_ts
                s["origin_bar"] = int(self.bar_count)
                if "origin_time" not in s:
                    s["origin_time"] = int(time.time())

            if s["id"] not in merged:
                added += 1
            merged[s["id"]] = s

        self.active_signals = merged

        log.info("Signals: %d active (carried=%d, new=%d, expired=%d, dead=%d %s) | bar=%d | price=%.2f",
                 len(self.active_signals), carried, added, expired, skipped_dead,
                 dict(dead_reasons) if dead_reasons else "",
                 self.bar_count, self.current_price)
        if skipped_dead > 0 and added == 0 and len(new_signals) > 0:
            # Debug: show first few dead signals to diagnose
            sample = new_signals[:3]
            for s in sample:
                log.info("  DEAD sample: %s %s entry=%.2f sl=%.2f tp1=%.2f | price=%.2f | reason=%s",
                         s["direction"], s["name"], s["entry"], s["sl"], s["tp1"],
                         price, _is_dead(s, price, atr))

        state = self.engine.get_market_state(
            self.bars_df,
            current_price=self.current_price,
            bar_delta_pct=self._bar_delta_pct,
        )
        state["engine_debug"] = engine_debug
        self._inject_l2_guide(state)

        # Convert dictionary to descending sorted list for broadcast
        signals_payload = list(self.active_signals.values())
        signals_payload.sort(key=lambda x: x["confidence_pct"], reverse=True)
        
        zones = self.engine.compute_weighted_zones(signals_payload)

        self._latest_state = state
        self._latest_signals = signals_payload
        self._latest_ghost_signals = ghost_signals
        self._latest_engine_debug = engine_debug
        self._latest_zones = zones

        # Extract recent candlestick history for TradingView chart
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
                # NT8 bar timestamps are CLOSE time.
                # Lightweight Charts expects OPEN time → shift back by active TF.
                bars_for_chart.append({
                    "time": ts - self.bar_tf_min * 60,
                    "open": float(row["open"]),
                    "high": float(row["high"]),
                    "low": float(row["low"]),
                    "close": float(row["close"]),
                    "cum_delta": float(row.get("cum_delta", 0.0)),
                })

        full_payload = {
            "type": "full_update",
            "state": state,
            "signals": signals_payload,
            "ghost_signals": ghost_signals,
            "resolved": self.resolved_signals,
            "zones": zones,
            "history": self.engine.get_history(),
            "bars": bars_for_chart,
        }

        # Schedule broadcast on the event loop safely from the TCP thread
        if self.loop:
            asyncio.run_coroutine_threadsafe(
                self._broadcast_split(full_payload, bars_for_chart),
                self.loop,
            )

    def _tick_update(self):
        """Broadcast lightweight tick update every second."""
        if not self._latest_state:
            return

        now = time.time()
        if now - self._last_local_emit < self._min_local_emit_sec:
            return

        # Recompute state with current tick price
        state = self.engine.get_market_state(
            self.bars_df,
            current_price=self.current_price,
            bar_delta_pct=self._bar_delta_pct,
        )
        state["engine_debug"] = self._latest_engine_debug
        self._inject_l2_guide(state)
        self._latest_state = state

        signals_payload = list(self.active_signals.values())
        signals_payload.sort(key=lambda x: x["confidence_pct"], reverse=True)
        self._latest_signals = signals_payload

        # Local WS: full tick payload (signals, zones, etc.)
        local_payload = {
            "type": "tick_update",
            "state": state,
            "signals": signals_payload[:15],
            "ghost_signals": self._latest_ghost_signals[:8],
            "resolved": self.resolved_signals[-10:],
            "zones": self._latest_zones,
        }
        if self.loop:
            msg = self._safe_json(local_payload)
            asyncio.run_coroutine_threadsafe(self._ws_only(msg), self.loop)
            self._last_local_emit = now

            # Relay: lightweight — state + signals (so Railway dashboard stays in sync)
            if now - self._last_relay_emit >= 2:
                self._last_relay_emit = now
                relay_tick = {
                    "type": "tick_update",
                    "state": state,
                    "signals": signals_payload[:10],
                    "ghost_signals": self._latest_ghost_signals[:4],
                }
                asyncio.run_coroutine_threadsafe(self._relay_push(relay_tick), self.loop)

    async def _relay_push(self, payload: dict):
        """Push lightweight payload to relay (async, non-blocking)."""
        if not self.relay_url:
            return
        try:
            msg = self._safe_json(payload)
            loop = self.loop or asyncio.get_event_loop()
            loop.run_in_executor(self.executor, self._push_to_relay, msg)
        except Exception as e:
            log.debug("Relay tick push error: %s", e)

    async def _ws_only(self, msg: str):
        """Send to local WebSocket clients only (no relay)."""
        dead = set()
        for ws in list(self._ws_clients):
            try:
                await ws.send(msg)
            except Exception:
                dead.add(ws)
        self._ws_clients -= dead

    def _inject_l2_guide(self, state: dict) -> None:
        if not state:
            return
        raw_guide = self._build_l2_guide(current_price=float(state.get("price", self.current_price or 0.0)))
        guide = self._stabilize_l2_guide(raw_guide)
        state["l2_guide"] = guide
        trader_guide = dict(state.get("trader_guide") or {})
        trader_guide["l2"] = guide
        if guide.get("zones"):
            merged_zones = list(trader_guide.get("zones") or [])
            merged_zones.extend(guide["zones"])
            trader_guide["zones"] = merged_zones[:6]
        if guide.get("summary"):
            trader_guide["summary"] = f"{trader_guide.get('summary', '')} Flow: {guide['summary']}".strip()
        state["trader_guide"] = trader_guide

    def _stabilize_l2_guide(self, guide: dict) -> dict:
        """Smooth micro-flow so UI doesn't flip direction on every burst of ticks."""
        state = dict(self._l2_display_state)
        now = time.time()
        raw_bias = str(guide.get("micro_bias") or "neutral")
        raw_conf = int(guide.get("confidence") or 0)
        shown_bias = str(state.get("bias") or "neutral")
        shown_conf = int(state.get("confidence") or 0)
        candidate = state.get("candidate")
        candidate_count = int(state.get("candidate_count") or 0)

        if raw_bias == shown_bias or raw_bias == "neutral":
            candidate = None
            candidate_count = 0
        elif raw_bias == candidate:
            candidate_count += 1
        else:
            candidate = raw_bias
            candidate_count = 1

        first_assignment = shown_bias == "neutral" and shown_conf == 0
        strong_override = raw_conf >= max(72, shown_conf + 18)
        confirmed_flip = candidate_count >= 3 and raw_conf >= max(50, shown_conf - 8)
        time_release = (now - float(state.get("updated_at") or 0.0)) >= 4.0 and raw_conf >= max(58, shown_conf)

        if raw_bias != "neutral" and (first_assignment or strong_override or confirmed_flip or time_release):
            shown_bias = raw_bias
            candidate = None
            candidate_count = 0

        if first_assignment:
            shown_conf = raw_conf
        else:
            shown_conf = int(round(shown_conf * 0.72 + raw_conf * 0.28))
            if shown_bias != raw_bias:
                shown_conf = min(shown_conf, max(40, raw_conf))
        shown_conf = max(0, min(90, shown_conf))

        flow_state = "balanced"
        if shown_bias == raw_bias and shown_bias != "neutral" and raw_conf >= 60:
            flow_state = "confirmed"
        elif shown_bias != raw_bias and raw_bias != "neutral":
            flow_state = "probing"
        elif shown_bias != "neutral":
            flow_state = "leaning"

        stabilized = dict(guide)
        stabilized["raw_micro_bias"] = raw_bias
        stabilized["raw_confidence"] = raw_conf
        stabilized["micro_bias"] = shown_bias
        stabilized["confidence"] = shown_conf
        stabilized["flow_state"] = flow_state

        self._l2_display_state = {
            "bias": shown_bias,
            "confidence": shown_conf,
            "candidate": candidate,
            "candidate_count": candidate_count,
            "updated_at": now,
        }
        return stabilized

    def _build_l2_guide(self, *, current_price: float) -> dict:
        ticks = list(self._recent_ticks)
        if len(ticks) < 25 or current_price <= 0:
            return {
                "micro_bias": "neutral",
                "confidence": 0,
                "summary": "Waiting for enough live bid/ask ticks.",
                "spread": None,
                "support_zone": None,
                "resistance_zone": None,
                "zones": [],
            }

        recent = ticks[-400:]
        spreads = [max(0.0, t["ask"] - t["bid"]) for t in recent if t["bid"] > 0 and t["ask"] > 0]
        avg_spread = round(sum(spreads) / len(spreads), 3) if spreads else None

        buy_vol = sum(t["size"] for t in recent if t["aggressor"] == 1)
        sell_vol = sum(t["size"] for t in recent if t["aggressor"] == 2)
        total_aggr = buy_vol + sell_vol
        imbalance = ((buy_vol - sell_vol) / total_aggr) if total_aggr else 0.0

        bid_presence = defaultdict(float)
        ask_presence = defaultdict(float)
        bid_hits = defaultdict(float)
        ask_hits = defaultdict(float)
        large_buy = 0
        large_sell = 0

        for t in recent:
            bid = self._round_tick(t["bid"])
            ask = self._round_tick(t["ask"])
            size = float(t["size"])
            if bid > 0:
                bid_presence[bid] += 1.0
                if t["aggressor"] == 2 and abs(t["price"] - bid) <= 0.5:
                    bid_hits[bid] += size
                    if size >= 10:
                        large_sell += 1
            if ask > 0:
                ask_presence[ask] += 1.0
                if t["aggressor"] == 1 and abs(t["price"] - ask) <= 0.5:
                    ask_hits[ask] += size
                    if size >= 10:
                        large_buy += 1

        support_zone = self._pick_l2_zone(
            side="support",
            current_price=current_price,
            presence=bid_presence,
            hits=bid_hits,
        )
        resistance_zone = self._pick_l2_zone(
            side="resistance",
            current_price=current_price,
            presence=ask_presence,
            hits=ask_hits,
        )

        if imbalance >= 0.18:
            micro_bias = "long"
        elif imbalance <= -0.18:
            micro_bias = "short"
        else:
            micro_bias = "neutral"
        confidence = min(90, int(round(45 + abs(imbalance) * 120 + max(large_buy, large_sell) * 1.5)))

        parts = []
        if support_zone:
            parts.append(
                f"defended bid near {support_zone['low']:.2f}-{support_zone['high']:.2f}"
            )
        if resistance_zone:
            parts.append(
                f"defended ask near {resistance_zone['low']:.2f}-{resistance_zone['high']:.2f}"
            )
        if micro_bias == "long":
            lead = "Micro bias leans LONG."
        elif micro_bias == "short":
            lead = "Micro bias leans SHORT."
        else:
            lead = "Micro bias is balanced."
        summary = lead
        if parts:
            summary += " Watch " + " and ".join(parts) + "."

        zones = []
        if support_zone:
            zones.append(
                {
                    "label": "L2 support reaction",
                    "direction": "long",
                    "low": support_zone["low"],
                    "high": support_zone["high"],
                    "why": support_zone["note"],
                }
            )
        if resistance_zone:
            zones.append(
                {
                    "label": "L2 resistance reaction",
                    "direction": "short",
                    "low": resistance_zone["low"],
                    "high": resistance_zone["high"],
                    "why": resistance_zone["note"],
                }
            )

        return {
            "micro_bias": micro_bias,
            "confidence": confidence,
            "summary": summary,
            "spread": avg_spread,
            "imbalance": round(imbalance, 3),
            "support_zone": support_zone,
            "resistance_zone": resistance_zone,
            "large_buy": large_buy,
            "large_sell": large_sell,
            "zones": zones,
        }

    def _pick_l2_zone(self, *, side: str, current_price: float, presence: dict, hits: dict) -> dict | None:
        candidates = []
        for level, seen in presence.items():
            if side == "support" and level > current_price + 1.0:
                continue
            if side == "resistance" and level < current_price - 1.0:
                continue
            if abs(level - current_price) > 18.0:
                continue
            score = seen + hits.get(level, 0.0) * 0.08
            if score < 8:
                continue
            candidates.append((score, level, seen, hits.get(level, 0.0)))
        if not candidates:
            return None
        candidates.sort(key=lambda item: (item[0], -abs(item[1] - current_price)), reverse=True)
        _, level, seen, hit_vol = candidates[0]
        low = round(level - 0.5, 2)
        high = round(level + 0.5, 2)
        if side == "support":
            note = (
                f"Repeated bid holding near {level:.2f}; sellers hit bid {int(hit_vol)} contracts here. "
                f"Long reaction is more likely if this zone holds."
            )
        else:
            note = (
                f"Repeated ask holding near {level:.2f}; buyers lifted offer {int(hit_vol)} contracts here. "
                f"Short reaction is more likely if this zone rejects."
            )
        return {
            "level": round(level, 2),
            "low": low,
            "high": high,
            "touches": int(seen),
            "hit_volume": int(hit_vol),
            "note": note,
        }

    @staticmethod
    def _round_tick(price: float) -> float:
        if price <= 0:
            return 0.0
        return round(round(price / 0.25) * 0.25, 2)

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

        # Prefill historical bars at the configured dashboard TF.
        BAR_SECS = self.bar_tf_min * 60
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
            time.sleep(max(1, self.bar_tf_min))

    # ── Main Run ──

    async def run(self):
        """Start WebSocket server and TCP reader."""
        log.info("=" * 60)
        log.info("  📡 SIGNAL DASHBOARD SERVER")
        log.info("  TCP: %s:%d | WS: localhost:%d",
                 self.tcp_host, self.tcp_port, self.ws_port)
        log.info("  Mode: %s", "DEMO" if self.demo else "LIVE")
        log.info("  Bars TF: %dmin | Engine: %s",
                 self.bar_tf_min, getattr(self.engine, "engine_mode", "unknown"))
        log.info("  NT Account: %s", self.account_name or "chart default")
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
    parser.add_argument("--account", default=None, help="Preferred NT account, e.g. Playback101 or Sim101")
    parser.add_argument("--log-level", default="INFO")
    args = parser.parse_args()

    # Log to both console and file
    log_level = getattr(logging, args.log_level)
    root = logging.getLogger()
    root.setLevel(log_level)
    root.handlers.clear()
    console_h = logging.StreamHandler()
    console_h.setFormatter(logging.Formatter(LOG_FMT))
    root.addHandler(console_h)
    file_h = logging.FileHandler(str(Path(__file__).parent / "server.log"), mode="w", encoding="utf-8")
    file_h.setFormatter(logging.Formatter(LOG_FMT))
    root.addHandler(file_h)

    server = SignalDashboardServer(
        tcp_host=args.host, tcp_port=args.port,
        ws_port=args.ws_port, demo=args.demo,
        relay_url=args.relay_url,
        relay_secret=args.relay_secret,
        account_name=args.account or os.getenv("NT_ACCOUNT_NAME"),
    )
    asyncio.run(server.run())


if __name__ == "__main__":
    main()
