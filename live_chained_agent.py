"""
Live Chained Agent — runs alongside signal_server.py
Produces the exact same event log format as deepseek_chained_agent.py backtest.

Architecture:
  1. Macro Boss   — triggered by events (market open, sweeps, vol shocks, hourly)
  2. Zone Watcher — triggered when price enters an interest zone
  3. Micro Hunter — triggered when a micro alert level is hit

Output: rolling list of formatted log lines, pushed to dashboard via state payload.
"""

from __future__ import annotations

import json
import logging
import os
import time
import threading
from collections import deque
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any

from env_bootstrap import load_project_env

load_project_env()

log = logging.getLogger("chained_agent")

# ---------------------------------------------------------------------------
# LLM client (reuses market_snapshot_llm infrastructure)
# ---------------------------------------------------------------------------

def _call_deepseek(system_prompt: str, user_prompt: str, memory: dict, smc_data: dict | None = None) -> dict:
    """Call DeepSeek API and return parsed JSON dict."""
    try:
        from market_snapshot_llm import MarketSnapshotLLMClient
        client = MarketSnapshotLLMClient()

        smc_block = f"\nCALCULATED SMC INDICATORS:\n{json.dumps(smc_data, indent=2)}" if smc_data else ""
        full_system = f"""{system_prompt}

SHARED MEMORY STATE:
{json.dumps(memory, indent=2)}{smc_block}"""

        resp = client.call(full_system, user_prompt, temperature=0.1)
        if resp.success and resp.parsed_json:
            return resp.parsed_json
        log.debug("Chained agent LLM call failed: %s", resp.error)
        return {}
    except Exception as e:
        log.error("Chained agent LLM error: %s", e)
        return {}


# ---------------------------------------------------------------------------
# SMC calculator (same as backtest)
# ---------------------------------------------------------------------------

def _calculate_smc(bars: list[dict], lookback: int = 30) -> dict:
    """Calculate ATR, swing H/L, FVG from recent bars."""
    if len(bars) < 5:
        return {"atr": 10.0, "swing_high": 0.0, "swing_low": 0.0, "fvg_bull": [], "fvg_bear": []}

    recent = bars[-lookback:] if len(bars) >= lookback else bars
    highs = [b["high"] for b in recent]
    lows = [b["low"] for b in recent]
    closes = [b["close"] for b in recent]

    # ATR(14)
    trs = []
    for i in range(1, len(recent)):
        hl = highs[i] - lows[i]
        hc = abs(highs[i] - closes[i - 1])
        lc = abs(lows[i] - closes[i - 1])
        trs.append(max(hl, hc, lc))
    atr = sum(trs[-14:]) / min(14, len(trs)) if trs else 10.0

    last15 = recent[-15:]
    swing_high = max(b["high"] for b in last15)
    swing_low = min(b["low"] for b in last15)

    fvg_bull, fvg_bear = [], []
    for i in range(2, len(recent)):
        b1_h = recent[i - 2]["high"]
        b3_l = recent[i]["low"]
        b1_l = recent[i - 2]["low"]
        b3_h = recent[i]["high"]
        if b1_h < b3_l:
            fvg_bull.append([round(b1_h, 2), round(b3_l, 2)])
        elif b1_l > b3_h:
            fvg_bear.append([round(b3_h, 2), round(b1_l, 2)])

    return {
        "atr": round(atr, 2),
        "swing_high": round(swing_high, 2),
        "swing_low": round(swing_low, 2),
        "fvg_bull": fvg_bull[-2:],
        "fvg_bear": fvg_bear[-2:],
    }


# ---------------------------------------------------------------------------
# System prompt (same philosophy as backtest)
# ---------------------------------------------------------------------------

SYSTEM_PROMPT = """You are the {agent_type} (Hybrid SMC + Flow Engine v11.5) MNQ prop-trading agent.
You strictly output JSON.

CORE PHILOSOPHY (v11.5):
1. HTF Alignment: Trades must align with Higher Time Frame structure.
2. Market Regime: Trend, Compression, Rebuild, Distribution, Accumulation.
3. Liquidity & SMC: We do not trade blind breakouts. We look for Liquidity Sweeps, CHOCH, and FVG.
4. STRICT DIRECTIONAL RULE: If the shared memory 'hourly_bias' is BEARISH, you may ONLY execute SHORT trades. If it is BULLISH, you may ONLY execute LONG trades. Counter-trend trading is STRICTLY FORBIDDEN.

PLAYBOOKS AVAILABLE:
1. "LIQUIDITY_SWEEP_REVERSAL": Price sweeps a prominent liquidity pool. We use the Calculated SMC data to validate entry.
2. "BOS_CONTINUATION": In a strong trend, price breaks structure, pulls back to a calculated FVG, and reclaims direction.
3. "NONE": Choppy or non-directional without clear pullbacks.

RISK MANAGEMENT RULES:
1. SL: For LONGs, place SL below Swing Low minus half ATR. For SHORTs, above Swing High plus half ATR.
2. FVG Buffer: If there is an active FVG below entry, you can place SL behind the FVG for tighter risk.
3. TP1: Minimum R:R is 1:1.5. If impossible, decision: WAIT.
"""


# ---------------------------------------------------------------------------
# LiveChainedAgent
# ---------------------------------------------------------------------------

class LiveChainedAgent:
    """Event-driven chained agent that produces backtest-style log output."""

    def __init__(self, max_log_entries: int = 150):
        self._enabled = os.environ.get("CHAINED_AGENT_ENABLED", "1").strip().lower() not in {"0", "false", "no"}
        self._max_log = max_log_entries
        self._log: deque[str] = deque(maxlen=max_log_entries)
        self._lock = threading.Lock()

        # Shared memory (same as backtest)
        self._memory = {
            "mode": "DORMANT",
            "hourly_bias": "NONE",
            "active_playbook": "NONE",
            "rationale": "Waiting for market open.",
            "interest_zones": [],
            "micro_alerts": [],
        }

        # Trade state
        self._in_trade = False
        self._trade_dir = ""
        self._entry = 0.0
        self._sl = 0.0
        self._tp1 = 0.0
        self._tp2 = 0.0
        self._tp3 = 0.0

        # Trigger trackers
        self._pdh = 0.0
        self._pdl = 0.0
        self._onh = 0.0
        self._onl = 0.0
        self._touched_pdh = False
        self._touched_pdl = False
        self._touched_onh = False
        self._touched_onl = False
        self._last_shock_bar = -15
        self._bar_index = 0
        self._session_date = ""

        # Bar history for context
        self._bars: list[dict] = []

        # Daily context
        self._daily_context_lines: list[str] = []

        # Cooldown: don't call LLM too fast
        self._last_macro_call = 0.0
        self._last_micro_call = 0.0

        log.info("LiveChainedAgent initialized (enabled=%s)", self._enabled)

    # -- Public API --

    def get_event_log(self) -> list[str]:
        """Return current event log as list of strings."""
        with self._lock:
            return list(self._log)

    def set_session_levels(self, pdh: float, pdl: float, onh: float, onl: float):
        """Set previous day and overnight levels for sweep detection."""
        if pdh != self._pdh or pdl != self._pdl:
            self._pdh = pdh
            self._pdl = pdl
            self._touched_pdh = False
            self._touched_pdl = False
        if onh != self._onh or onl != self._onl:
            self._onh = onh
            self._onl = onl
            self._touched_onh = False
            self._touched_onl = False

    def on_bar(self, bar: dict):
        """Called on each completed 1-min bar. bar must have: open, high, low, close, volume, datetime (str HH:MM or full)."""
        if not self._enabled:
            return

        self._bar_index += 1
        self._bars.append(bar)
        if len(self._bars) > 200:
            self._bars = self._bars[-200:]

        time_str = self._extract_time(bar)
        hour = int(time_str.split(":")[0]) if ":" in time_str else 0
        minute = int(time_str.split(":")[1]) if ":" in time_str else 0

        # Check session date change
        bar_date = bar.get("date", "")
        if bar_date and bar_date != self._session_date:
            self._session_date = bar_date
            self._emit(f"\n{'=' * 50}\n MARKET DATE: {bar_date}\n{'=' * 50}")
            self._touched_pdh = False
            self._touched_pdl = False
            self._touched_onh = False
            self._touched_onl = False
            self._memory["mode"] = "DORMANT"
            self._memory["interest_zones"] = []
            self._memory["micro_alerts"] = []
            self._in_trade = False

        price = float(bar.get("close", 0))
        high = float(bar.get("high", 0))
        low = float(bar.get("low", 0))

        # -- Trade management (mechanical) --
        if self._in_trade:
            self._check_trade(time_str, high, low)
            return  # no new analysis while in trade

        if hour >= 15 and minute >= 45:
            return  # end of day

        # -- 1. Event detection (Macro Boss triggers) --
        trigger, reason = self._detect_event(time_str, hour, minute, bar)
        if trigger:
            self._run_macro_boss(time_str, reason, bar)

        # -- 2. Zone entry detection --
        if self._memory["mode"] == "DORMANT" and self._memory["interest_zones"]:
            for z in list(self._memory["interest_zones"]):
                if len(z) >= 2 and z[0] <= price <= z[1]:
                    self._run_zone_agent(time_str, z, price)
                    break

        # -- 3. Micro alert detection --
        if self._memory["mode"] == "HUNTING" and self._memory.get("micro_alerts"):
            prev_bar = self._bars[-2] if len(self._bars) >= 2 else bar
            for ma in self._memory["micro_alerts"]:
                prev_lo = float(prev_bar.get("low", 0))
                prev_hi = float(prev_bar.get("high", 0))
                if min(low, prev_lo) <= ma <= max(high, prev_hi):
                    self._run_micro_hunter(time_str, ma, bar)
                    break

    # -- Private --

    def _emit(self, line: str):
        with self._lock:
            self._log.append(line)
        log.info("[ChainedAgent] %s", line.strip())

    def _extract_time(self, bar: dict) -> str:
        dt = bar.get("datetime", bar.get("time_str", ""))
        if isinstance(dt, str) and ":" in dt:
            # Could be "HH:MM" or "2026-04-16 10:30:00"
            parts = dt.split(" ")
            time_part = parts[-1] if len(parts) > 1 else parts[0]
            return time_part[:5]  # HH:MM
        return "00:00"

    def _detect_event(self, time_str: str, hour: int, minute: int, bar: dict) -> tuple[bool, str]:
        high = float(bar.get("high", 0))
        low = float(bar.get("low", 0))
        candle_range = high - low

        # Time-based
        if time_str == "09:20":
            return True, "MARKET_OPEN"
        if minute == 30:
            return True, "HOURLY_SYNC"
        if time_str in ("10:02", "14:02"):
            return True, "POST_NEWS_DIGEST"

        # Volatility shock (40pt range)
        if candle_range >= 40.0 and (self._bar_index - self._last_shock_bar) >= 15:
            self._last_shock_bar = self._bar_index
            return True, "VOLATILITY_SHOCK"

        # Liquidity sweeps
        if self._pdh > 0 and high >= self._pdh and not self._touched_pdh:
            self._touched_pdh = True
            return True, "PDH_SWEEP"
        if self._pdl > 0 and low <= self._pdl and not self._touched_pdl:
            self._touched_pdl = True
            return True, "PDL_SWEEP"
        if self._onh > 0 and high >= self._onh and not self._touched_onh:
            self._touched_onh = True
            return True, "ONH_SWEEP"
        if self._onl > 0 and low <= self._onl and not self._touched_onl:
            self._touched_onl = True
            return True, "ONL_SWEEP"

        return False, ""

    def _run_macro_boss(self, time_str: str, reason: str, bar: dict):
        now = time.monotonic()
        if now - self._last_macro_call < 20:  # cooldown 20s
            return
        self._last_macro_call = now

        self._emit(f"[{time_str}] 🚨 EVENT TRIGGERED: {reason}. WAKING MACRO BOSS...")

        price = float(bar.get("close", 0))
        # Build context from recent bars
        recent = self._bars[-60:]
        if not recent:
            return

        r_open = recent[0]["open"]
        r_high = max(b["high"] for b in recent)
        r_low = min(b["low"] for b in recent)
        r_close = recent[-1]["close"]

        prompt = f"""ANALYZE CURRENT MULTI-TIMEFRAME CONTEXT AND SMC:
EVENT TRIGGER: {reason}
Time: {time_str} ET
Current Price: {price}

Institutional Levels:
Previous Day High (PDH): {self._pdh}
Previous Day Low (PDL): {self._pdl}
Overnight High (ONH): {self._onh}
Overnight Low (ONL): {self._onl}

Current Market context (Last hour summary):
Open: {r_open} High: {r_high} Low: {r_low} Close: {r_close}

Update the shared memory. Decide on a playbook. Draw 1 or 2 interest_zones near CURRENT PRICE.
OUTPUT JSON ONLY:
{{
  "hourly_bias": "BULLISH|BEARISH|NEUTRAL",
  "active_playbook": "LIQUIDITY_SWEEP_REVERSAL|BOS_CONTINUATION|NONE",
  "rationale": "...",
  "interest_zones": [ [minPx, maxPx], ... ]
}}"""

        ans = _call_deepseek(
            SYSTEM_PROMPT.format(agent_type="Hourly Macro"),
            prompt,
            self._memory,
        )

        if ans.get("hourly_bias"):
            self._memory.update({
                "hourly_bias": ans.get("hourly_bias", "NEUTRAL"),
                "active_playbook": ans.get("active_playbook", "NONE"),
                "rationale": ans.get("rationale", ""),
                "interest_zones": ans.get("interest_zones", []),
                "mode": "DORMANT",
            })

        self._emit(f"   -> Bias: {self._memory['hourly_bias']} | Playbook: {self._memory['active_playbook']}")
        self._emit(f"   -> Zones: {self._memory['interest_zones']}")

    def _run_zone_agent(self, time_str: str, zone: list, price: float):
        self._emit(f"[{time_str}] ⚠️ ZONE ENTERED {zone}. RUNNING ZONE EVENT AGENT...")

        recent = self._bars[-15:]
        recent_summary = ", ".join(f"{b.get('close', 0):.2f}" for b in recent[-5:])

        prompt = f"""Price just entered zone {zone}. Current price {price}.
Recent closes: {recent_summary}

Based on our active playbook {self._memory['active_playbook']}, do we initiate a Hunt?
If yes, set micro_alerts (at least 10-15 pts from current price).
OUTPUT JSON ONLY:
{{
  "mode": "HUNTING|DORMANT",
  "rationale": "...",
  "micro_alerts": [float]
}}"""

        ans = _call_deepseek(
            SYSTEM_PROMPT.format(agent_type="Zone Event"),
            prompt,
            self._memory,
        )

        new_mode = ans.get("mode", "DORMANT")
        self._memory.update({
            "mode": new_mode,
            "rationale_zone": ans.get("rationale", ""),
            "micro_alerts": ans.get("micro_alerts", []),
        })
        if new_mode == "DORMANT" and zone in self._memory["interest_zones"]:
            self._memory["interest_zones"].remove(zone)

        self._emit(f"   -> State changed to {self._memory['mode']}. Alerts: {self._memory['micro_alerts']}")

    def _run_micro_hunter(self, time_str: str, alert_level: float, bar: dict):
        now = time.monotonic()
        if now - self._last_micro_call < 10:  # cooldown 10s
            return
        self._last_micro_call = now

        self._emit(f"[{time_str}] 🎯 MICRO ALERT {alert_level} HIT... RUNNING MICRO AGENT...")

        recent = self._bars[-10:]
        recent_lines = "\n".join(
            f"  {b.get('close', 0):.2f} H={b.get('high', 0):.2f} L={b.get('low', 0):.2f}"
            for b in recent
        )
        smc = _calculate_smc(self._bars)

        prompt = f"""We are in HUNTING mode executing {self._memory['active_playbook']}.
Price just hit Micro Alert {alert_level}. Current close is {bar['close']}.
Last 10 bars:
{recent_lines}

Evaluate the pattern. Do we have full confirmation to EXECUTE TRADE?
If WAIT, supply new_micro_alerts to wait for.
OUTPUT JSON ONLY:
{{
  "decision": "ENTER_LONG|ENTER_SHORT|WAIT|ABORT",
  "rationale": "SMC Analysis: ...",
  "entry_px": float,
  "sl_px": float,
  "tp1_px": float,
  "tp2_px": float,
  "tp3_px": float,
  "probability": "HIGH|MEDIUM|LOW",
  "status": "potwierdzony|niepewny|w trakcie",
  "new_micro_alerts": [float]
}}"""

        ans = _call_deepseek(
            SYSTEM_PROMPT.format(agent_type="Micro Hunter"),
            prompt,
            self._memory,
            smc_data=smc,
        )

        dec = ans.get("decision", "WAIT")
        rationale = ans.get("rationale", "")
        self._emit(f"   -> Micro Decision: {dec} | {rationale}")

        if dec in ("ENTER_LONG", "ENTER_SHORT"):
            self._in_trade = True
            self._trade_dir = "LONG" if dec == "ENTER_LONG" else "SHORT"
            self._entry = float(ans.get("entry_px", bar["close"]))
            self._sl = float(ans.get("sl_px", 0))
            self._tp1 = float(ans.get("tp1_px", 0))
            self._tp2 = float(ans.get("tp2_px", 0))
            self._tp3 = float(ans.get("tp3_px", 0))
            self._emit(
                f"[{time_str}] 🚀 TRADE EXECUTED: {self._trade_dir} at {self._entry}. "
                f"SL: {self._sl}, TP1: {self._tp1}, TP2: {self._tp2}, TP3: {self._tp3}"
            )
            self._emit(f"   -> Status: {ans.get('status', '?')} | Prob: {ans.get('probability', '?')}")
            self._memory["mode"] = "IN_TRADE"

        elif dec == "ABORT":
            self._memory["mode"] = "DORMANT"

        elif dec == "WAIT":
            new_alerts = ans.get("new_micro_alerts", [])
            if new_alerts:
                self._memory["micro_alerts"] = [float(x) for x in new_alerts if x]
                self._emit(f"   -> Placed new Micro Alerts: {self._memory['micro_alerts']}")
            else:
                self._emit("   -> No new alerts provided, reverting to DORMANT.")
                self._memory["mode"] = "DORMANT"

    def _check_trade(self, time_str: str, high: float, low: float):
        if self._trade_dir == "LONG":
            if low <= self._sl:
                pts = round(self._entry - self._sl, 2)
                self._emit(f"[{time_str}] 🔴 STOP LOSS HIT (Long). Points: -{pts}")
                self._close_trade()
            elif high >= self._tp1:
                pts = round(self._tp1 - self._entry, 2)
                self._emit(f"[{time_str}] 🟢 TAKE PROFIT HIT (Long). Points: +{pts}")
                self._close_trade()
        else:
            if high >= self._sl:
                pts = round(self._sl - self._entry, 2)
                self._emit(f"[{time_str}] 🔴 STOP LOSS HIT (Short). Points: -{pts}")
                self._close_trade()
            elif low <= self._tp1:
                pts = round(self._entry - self._tp1, 2)
                self._emit(f"[{time_str}] 🟢 TAKE PROFIT HIT (Short). Points: +{pts}")
                self._close_trade()

    def _close_trade(self):
        self._in_trade = False
        self._trade_dir = ""
        self._memory["mode"] = "DORMANT"
