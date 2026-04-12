"""Signal Engine — wraps HSB pipeline for dashboard display.

Returns ALL signal candidates (not just champion) with enriched metadata:
- confidence_pct: 0-100% normalized confidence
- confluence_count: how many other signals confirm this direction
- confirming_signals: list of confirming signal names
- tp_target: price target
- invalidation: price that cancels the signal
- regime_match: whether signal matches current regime
- time_edge: time-of-day bonus/penalty description
"""

from __future__ import annotations

import compat  # noqa: F401 — patches dataclass for Python 3.9

import logging
import math
import time
from collections import defaultdict
from datetime import datetime
from typing import Any

log = logging.getLogger("signal_dash")

import pandas as pd

from hsb.signals.composite import CompositeGenerator, CONFLUENCE_BOOST
from hsb.pipeline.context_builder import ContextBuilder
from hsb.pipeline.regime import infer_regime
from hsb.domain.enums import Direction


# Historical signal quality weights (from 37,803 signal events study)
# Positive = proven profitable, negative = SL hit > TP hit
# Format: (signal_name_contains, direction) → multiplier
# >1.0 = boost confidence, <1.0 = penalize
SIGNAL_QUALITY = {
    # ── PROVEN WINNERS (TP >> SL, positive PnL) ──
    ("ib_break", "short"):      1.30,   # 71% WR, SL 53%, +5.81 pts — best high-volume
    ("ib_retest", "short"):     1.35,   # 100% WR (small sample but stellar)
    ("ib_retest", "long"):      1.25,   # 77% WR, +43 pts@60b
    ("trend", "short"):         1.25,   # 61% WR, SL only 44%, +12.57 pts
    ("trend", "long"):          1.15,   # 68% WR, +4.06 pts
    ("delta_div", "short"):     1.30,   # 71% WR, SL 53%, +27.41 pts — best PnL
    ("vwap_bounce", "long"):    1.20,   # 72% WR, +25.11 pts
    ("vwap_mr", "long"):        1.20,   # 78% WR, +5.89 pts
    ("vwap_mr", "short"):       1.10,   # 65% WR, +1.68 pts
    ("delta_div", "long"):      1.05,   # 68% WR, +3.89 pts — decent but not stellar
    ("pullback", "long"):       1.10,   # fast, proven in v6.7
    ("pullback", "short"):      1.10,

    # ── NEUTRAL (mixed results) ──
    ("exhaust", "short"):       1.00,   # 61% WR, +3.24 pts — OK
    ("exhaust", "long"):        0.85,   # 66% WR but SL 66%, -1.54 pts — SL=TP
    ("sell_exhaust", "long"):   0.80,   # 66% WR but SL 61%, -2.42 pts — slightly losing
    ("delta_accel", "short"):   1.00,   # mixed
    ("delta_accel", "long"):    0.90,   # mixed

    # ── PROVEN LOSERS (SL >> TP, negative PnL) — penalize hard ──
    ("ib_break", "long"):       0.65,   # 65% WR but SL 66%!, -5.32 pts — trap signal
    ("ema_bounce", "long"):     0.70,   # 67% WR but SL 64%, -2.81 pts — stop hunted
    ("ema_bounce", "short"):    0.75,   # 62% WR, SL 51%, -0.51 pts — slightly losing
    ("vwap_bounce", "short"):   0.70,   # 66% WR but SL only 38%... yet -2.86 pts PnL??
    ("volspike", "long"):       0.60,   # 67% WR but SL 61%, -6.04 pts — worst PnL
    ("streak", "short"):        0.65,   # 63% WR, SL 60%, -0.63 pts — high volume loser
    ("streak", "long"):         0.75,   # 56% WR, SL 60% — unreliable
}

# Regime multipliers: signal type → regime → multiplier
REGIME_MULT = {
    "exhaustion":    {"range": 1.3, "transition": 1.0, "trend_up": 0.5, "trend_down": 0.5, "chop": 0.8},
    "delta_div":     {"range": 1.2, "transition": 1.0, "trend_up": 0.7, "trend_down": 0.7, "chop": 0.9},
    "vwap_bounce":   {"range": 1.2, "transition": 1.1, "trend_up": 1.0, "trend_down": 1.0, "chop": 0.8},
    "waterfall":     {"range": 0.7, "transition": 1.0, "trend_up": 1.3, "trend_down": 1.3, "chop": 0.6},
    "trend_cont":    {"range": 0.6, "transition": 1.0, "trend_up": 1.3, "trend_down": 1.3, "chop": 0.5},
    "pullback":      {"range": 0.8, "transition": 1.0, "trend_up": 1.2, "trend_down": 1.2, "chop": 0.7},
    "micro_smc":     {"range": 1.0, "transition": 1.0, "trend_up": 1.0, "trend_down": 1.0, "chop": 0.8},
    "delta_accel":   {"range": 0.9, "transition": 1.0, "trend_up": 1.1, "trend_down": 1.1, "chop": 0.7},
}

# Time-of-day edges (from PATTERN_DISCOVERIES.md)
TIME_EDGES = {
    (9,): {"label": "Pre-market noise", "mult": 0.85},
    (10,): {"label": "10AM long window", "mult": 1.10},
    (11,): {"label": "Mid-morning fade", "mult": 0.95},
    (12,): {"label": "Lunch chop", "mult": 0.80},
    (13,): {"label": "Afternoon prep", "mult": 0.95},
    (14,): {"label": "POWER HOUR ⚡", "mult": 1.20},
    (15,): {"label": "Power hour cont.", "mult": 1.15},
    (16,): {"label": "Extended hours", "mult": 1.05},
}

# Signal time profile: expected bars to TP1 resolution (from champion_trades.csv + 120d profiling)
# Used by dashboard to project TP/SL dots at realistic distance
# Values = typical bars on 5-min chart
SIGNAL_TIME_PROFILE = {
    "pullback":      {"bars_to_tp": 2,  "label": "Fast"},       # instant, 0-1 bar median
    "exhaustion":    {"bars_to_tp": 6,  "label": "Quick"},      # 5-8 bars, mean reversion
    "delta_div":     {"bars_to_tp": 8,  "label": "Quick"},      # ~6-10 bars, divergence resolves
    "delta_accel":   {"bars_to_tp": 5,  "label": "Quick"},      # momentum burst, fast
    "vwap_bounce":   {"bars_to_tp": 6,  "label": "Quick"},      # bounce from VWAP, medium
    "ema_bounce":    {"bars_to_tp": 7,  "label": "Quick"},      # EMA rejection, medium
    "trend_cont":    {"bars_to_tp": 10, "label": "Medium"},     # continuation, needs time
    "waterfall":     {"bars_to_tp": 4,  "label": "Fast"},       # cascade, resolves quickly
    "micro_smc":     {"bars_to_tp": 14, "label": "Slow"},       # BOS/CHOCH, structural
    "break_retest":  {"bars_to_tp": 18, "label": "Slow"},       # retest of broken level
    "reclaim":       {"bars_to_tp": 16, "label": "Slow"},       # reclaim pattern
    "sweep":         {"bars_to_tp": 25, "label": "Very Slow"},  # sweep+reclaim, longest
    "ib_break":      {"bars_to_tp": 11, "label": "Medium"},     # initial balance breakout
    "orb":           {"bars_to_tp": 12, "label": "Medium"},     # opening range breakout
    "vwap_loss":     {"bars_to_tp": 8,  "label": "Quick"},      # VWAP loss, medium
}

# Day-of-week edges
DAY_EDGES = {
    0: {"label": "Monday LONG bias", "long_mult": 1.15, "short_mult": 0.90},
    1: {"label": "Tuesday SHORT bias", "long_mult": 0.85, "short_mult": 1.20},
    2: {"label": "Wednesday neutral", "long_mult": 1.0, "short_mult": 1.0},
    3: {"label": "Thursday neutral", "long_mult": 1.0, "short_mult": 1.0},
    4: {"label": "Friday standard", "long_mult": 1.0, "short_mult": 1.0},
}


class SignalEngine:
    """Evaluates all signals and returns enriched candidates for dashboard."""

    def __init__(self):
        self.composite = CompositeGenerator()
        self.ctx_builder = ContextBuilder()
        self._last_signals: list[dict] = []
        self._signal_history: list[dict] = []  # last 50 signals for tracking

    def evaluate(
        self,
        bars_df: pd.DataFrame,
        *,
        bar_delta_pct: float = 0.0,
        current_price: float = 0.0,
        now: datetime | None = None,
    ) -> list[dict]:
        """Run all generators and return enriched signal dicts.

        Returns list of dicts, each with:
        - name, direction, score, confidence_pct
        - entry, sl, tp1, tp3, invalidation
        - confluence_count, confirming_signals
        - regime, regime_match, time_edge, day_edge
        - reasons, source_type
        """
        if bars_df.empty or len(bars_df) < 15:
            return []

        now = now or datetime.now()

        # Build context
        try:
            ctx = self.ctx_builder.build(
                bars_df=bars_df, session="bars",
                day=now.strftime("%Y%m%d"),
                live_mode=True,
            )
        except Exception:
            return []

        if ctx is None:
            return []

        # Get ALL candidates (no champion filter, no director gate)
        try:
            candidates = list(self.composite.generate(ctx))
        except Exception as e:
            log.warning("composite.generate() error: %s", e)
            return []

        log.info("Raw candidates: %d | bars: %d", len(candidates), len(bars_df))
        if not candidates:
            return []

        # LIVE ONLY: We no longer arbitrarily drop old signals (like yesterday's FVGs).
        # HSB generators yield signals that are ACTUALLY computationally pending.
        # So whatever the generator yields, we pass it to the UI!
        # (Generator itself is responsible for expiring dead setups)
        
        # Extract regime info
        regime = ctx.regime.regime if ctx.regime else "unknown"
        atr = ctx.atr if ctx.atr else 20.0

        # Build enriched signals
        enriched = []
        for cand in candidates:
            if cand is None:
                continue
            if math.isnan(cand.sl_price) or math.isnan(cand.entry_price):
                continue

            direction = "long" if cand.direction == Direction.LONG else "short"
            source_type = cand.features.get("source_type", "") if hasattr(cand, "features") else ""
            # Clean source_type for display
            display_name = source_type.replace("derived_", "").replace("_long", "").replace("_short", "").upper()

            risk = abs(cand.entry_price - cand.sl_price)
            if risk <= 0:
                continue

            # TP targets
            if direction == "long":
                tp1 = cand.entry_price + risk * 1.5
                tp3 = cand.entry_price + risk * 4.0
                invalidation = cand.sl_price
            else:
                tp1 = cand.entry_price - risk * 1.5
                tp3 = cand.entry_price - risk * 4.0
                invalidation = cand.sl_price

            # Use candidate's own TP if available AND directionally valid
            if hasattr(cand, "tp1_price") and not math.isnan(cand.tp1_price):
                cand_tp1 = cand.tp1_price
                # Sanity: LONG target must be ABOVE entry, SHORT target must be BELOW entry
                if direction == "long" and cand_tp1 > cand.entry_price:
                    tp1 = cand_tp1
                elif direction == "short" and cand_tp1 < cand.entry_price:
                    tp1 = cand_tp1
                # else: keep the risk-based default tp1 (which is always directionally correct)

            # Regime multiplier
            base_source = source_type.split("_")[1] if "_" in source_type and len(source_type.split("_")) > 1 else source_type
            regime_mult = REGIME_MULT.get(base_source, {}).get(regime, 1.0)
            regime_match = regime_mult >= 1.0

            # Time-of-day edge
            hour = now.hour
            time_info = TIME_EDGES.get((hour,), {"label": "Standard", "mult": 1.0})
            time_mult = time_info["mult"]

            # Day-of-week edge
            dow = now.weekday()
            day_info = DAY_EDGES.get(dow, {"label": "Unknown", "long_mult": 1.0, "short_mult": 1.0})
            day_mult = day_info["long_mult"] if direction == "long" else day_info["short_mult"]

            # Delta alignment
            if direction == "long":
                delta_mult = 1.2 if bar_delta_pct > 5 else (0.8 if bar_delta_pct < -5 else 1.0)
            else:
                delta_mult = 1.2 if bar_delta_pct < -5 else (0.8 if bar_delta_pct > 5 else 1.0)

            # Historical quality weight (from 37K signal study)
            quality_mult = 1.0
            quality_grade = "B"  # default
            for (sig_tag, sig_dir), qmult in SIGNAL_QUALITY.items():
                if sig_tag in source_type.lower() and sig_dir == direction:
                    quality_mult = qmult
                    break
            if quality_mult >= 1.20:
                quality_grade = "A+"
            elif quality_mult >= 1.10:
                quality_grade = "A"
            elif quality_mult >= 0.95:
                quality_grade = "B"
            elif quality_mult >= 0.75:
                quality_grade = "C"
            else:
                quality_grade = "D"

            # Compute confidence
            raw_conf = cand.score * regime_mult * time_mult * day_mult * delta_mult * quality_mult
            confidence_pct = min(100, max(0, int(raw_conf * 100 / 0.75)))

            retrace_offset = cand.features.get("retrace_offset", 0) if hasattr(cand, "features") else 0

            # Time profile: how many bars this signal type typically needs
            time_profile = SIGNAL_TIME_PROFILE.get(base_source, {"bars_to_tp": 10, "label": "Medium"})

            enriched.append({
                "id": cand.id,
                "name": display_name or "UNKNOWN",
                "source_type": source_type,
                "direction": direction,
                "score": round(cand.score, 3),
                "confidence_pct": confidence_pct,
                "origin_time": int(cand.timestamp.timestamp()),
                "entry": round(cand.entry_price, 2),
                "sl": round(cand.sl_price, 2),
                "tp1": round(tp1, 2),
                "tp3": round(tp3, 2),
                "invalidation": round(invalidation, 2),
                "risk_pts": round(risk, 2),
                "rr_ratio": round((abs(tp3 - cand.entry_price) / risk) if risk > 0 else 0, 1),
                "regime": regime,
                "regime_match": regime_match,
                "regime_mult": round(regime_mult, 2),
                "time_edge": time_info["label"],
                "time_mult": round(time_mult, 2),
                "day_edge": day_info["label"],
                "day_mult": round(day_mult, 2),
                "delta_pct": round(bar_delta_pct, 1),
                "delta_aligned": delta_mult >= 1.0,
                "atr": round(atr, 1),
                "retrace_offset": round(retrace_offset, 2),
                "entry_type": "limit",
                "bars_to_tp": time_profile["bars_to_tp"],
                "speed_label": time_profile["label"],
                "quality_grade": quality_grade,
                "quality_mult": round(quality_mult, 2),
                "reasons": cand.reasons if hasattr(cand, "reasons") else [],
                "confluence_count": 0,
                "confirming_signals": [],
                "timestamp": now.isoformat(),
            })

        # Compute confluence: count how many signals agree on direction
        for sig in enriched:
            confirming = []
            for other in enriched:
                if other["id"] == sig["id"]:
                    continue
                if other["direction"] == sig["direction"]:
                    confirming.append(other["name"])
            sig["confluence_count"] = len(confirming) + 1  # Wliczamy samych siebie
            sig["confirming_signals"] = confirming

        # Compute EMA50 slope for trend filtering
        ema50_trend = "flat"
        vwap_price = 0.0
        close_price = 0.0
        if "ema_50" in bars_df.columns and len(bars_df) >= 3:
            ema50_last = bars_df["ema_50"].iloc[-1]
            ema50_prev = bars_df["ema_50"].iloc[-3]
            if ema50_last < ema50_prev - 1.5:
                ema50_trend = "down"
            elif ema50_last > ema50_prev + 1.5:
                ema50_trend = "up"
        if "vwap" in bars_df.columns and not bars_df.empty:
            vwap_price = bars_df["vwap"].iloc[-1]
        if "close" in bars_df.columns and not bars_df.empty:
            close_price = bars_df["close"].iloc[-1]

        # ── GOLD TIER DETECTION (from 37K signal study) ──
        # Multi-filter combos that historically hit 65-88% net win rate
        hour = now.hour
        is_prime_hour = hour in (10, 11)  # 10-11 ET = best liquidity window

        # Regime alignment check
        def _regime_aligned(sig_dir, reg):
            if sig_dir == "short" and reg in ("trend_down",):
                return True
            if sig_dir == "long" and reg in ("trend_up",):
                return True
            if reg in ("range",):
                return True  # range works for both
            return False

        for sig in enriched:
            conf_count = sig["confluence_count"]
            dir_ = sig["direction"]
            name = sig["name"]
            src = sig["source_type"].lower()
            regime_ok = _regime_aligned(dir_, regime)

            # ── GOLD TIER: confluence ≥ 2 + prime hours + regime aligned ──
            # DELTA_DIV_SHORT: 88% WR, +76 pts (N=8)
            # VWAP_BOUNCE_SHORT: 71% WR, +12.5 pts (N=231)
            # TREND_SHORT: 71% WR (N=324)
            # IB_BREAK_SHORT: 64% WR, +24.8 pts (N=166)
            # EMA_BOUNCE_SHORT: 62% WR, +24.6 pts (N=247)
            gold_tier = False
            gold_label = ""

            if conf_count >= 2 and is_prime_hour and regime_ok:
                # Full gold: all 3 filters met
                gold_tier = True
                gold_label = "GOLD"
                sig["confidence_pct"] = min(100, int(sig["confidence_pct"] * 1.35 + 8))
            elif conf_count >= 2 and is_prime_hour:
                # Silver: 2 of 3 filters
                gold_label = "SILVER"
                sig["confidence_pct"] = min(100, int(sig["confidence_pct"] * 1.20 + 5))
            elif conf_count >= 2 and regime_ok:
                # Silver: 2 of 3 filters
                gold_label = "SILVER"
                sig["confidence_pct"] = min(100, int(sig["confidence_pct"] * 1.15 + 3))
            elif conf_count >= 3:
                # High confluence alone
                gold_label = "SILVER"
                sig["confidence_pct"] = min(100, int(sig["confidence_pct"] * 1.15))

            sig["gold_tier"] = gold_tier
            sig["tier_label"] = gold_label

        # POST-PROCESSING: Wzmacnianie i dławienie sygnałów według wyników testu 120-dniowego
        for sig in enriched:
            # 0. Twarde wycięcie trucizn (FVG_FILL, BOS_BULL)
            if "FVG_FILL" in sig["name"] or "BOS_BULL" in sig["name"]:
                sig["confidence_pct"] = 0
                continue

            # 0b. Trend filter dla PULLBACK
            if "PULLBACK" in sig["name"]:
                if sig["direction"] == "long" and ema50_trend == "down" and close_price < vwap_price:
                    sig["confidence_pct"] = 0
                    continue
                if sig["direction"] == "short" and ema50_trend == "up" and close_price > vwap_price:
                    sig["confidence_pct"] = 0
                    continue

            # 1. Confluence penalty (non-gold only — gold already boosted)
            if not sig.get("gold_tier") and sig["confluence_count"] < 4:
                sig["confidence_pct"] = int(sig["confidence_pct"] * 0.5)

            # 2. Boost dla PULLBACK z odrzuceniem EMA (historycznie 98.9% WR)
            if "PULLBACK" in sig["name"]:
                reasons_str = "|".join(sig.get("reasons", [])).upper()
                confirms_str = "|".join(sig["confirming_signals"]).upper()
                if "EMA" in reasons_str or "EMA" in confirms_str:
                    sig["confidence_pct"] = min(100, int(sig["confidence_pct"] * 1.3 + 10))

        # Odrzucamy drastycznie osłabione sygnały by odśmiecić wizjonera
        pre_filter = len(enriched)
        enriched = [s for s in enriched if s["confidence_pct"] >= 50]
        if pre_filter > 0:
            log.info("Confidence filter: %d -> %d (dropped %d)", pre_filter, len(enriched), pre_filter - len(enriched))

        # ── DIRECTION CONFLICT RESOLUTION ──
        # If both LONG and SHORT signals exist, resolve the conflict
        enriched = self._resolve_direction_conflict(enriched, regime, ema50_trend, close_price, vwap_price)

        # Sort by confidence (highest first)
        enriched.sort(key=lambda s: s["confidence_pct"], reverse=True)

        # Store for history
        self._last_signals = enriched
        for sig in enriched[:3]:  # store top 3 per evaluation
            self._signal_history.append(sig)
        self._signal_history = self._signal_history[-50:]  # keep last 50

        return enriched

    def _resolve_direction_conflict(
        self,
        signals: list[dict],
        regime: str,
        ema50_trend: str,
        close_price: float,
        vwap_price: float,
    ) -> list[dict]:
        """Resolve conflicting LONG/SHORT signals.

        Strategy:
        1. Calculate weighted directional strength (confidence × count)
        2. Use regime, EMA trend, VWAP position as tiebreakers
        3. If one direction dominates (>60%), suppress the minority
        4. If balanced (<60/40), mark all as 'conflicted' — let user decide
        """
        longs = [s for s in signals if s["direction"] == "long"]
        shorts = [s for s in signals if s["direction"] == "short"]

        if not longs or not shorts:
            # No conflict — tag winning direction as dominant
            for s in signals:
                s["dir_bias"] = "dominant"
                s["conflicted"] = False
            return signals

        # Weighted strength = sum of confidence² (rewards high-conviction signals)
        long_str = sum(s["confidence_pct"] ** 2 for s in longs)
        short_str = sum(s["confidence_pct"] ** 2 for s in shorts)
        total_str = long_str + short_str

        if total_str == 0:
            return signals

        long_pct = long_str / total_str
        short_pct = short_str / total_str

        # Structural tiebreakers (each adds 5% bias)
        bias_adj = 0.0  # positive = favors LONG, negative = favors SHORT

        # EMA50 trend
        if ema50_trend == "up":
            bias_adj += 0.07
        elif ema50_trend == "down":
            bias_adj -= 0.07

        # VWAP position
        if close_price > vwap_price and vwap_price > 0:
            bias_adj += 0.05
        elif close_price < vwap_price and vwap_price > 0:
            bias_adj -= 0.05

        # Regime
        if regime in ("trend_up",):
            bias_adj += 0.08
        elif regime in ("trend_down",):
            bias_adj -= 0.08

        # Apply bias adjustment
        long_pct_adj = long_pct + bias_adj
        short_pct_adj = short_pct - bias_adj

        # Determine dominant direction
        DOMINANCE_THRESHOLD = 0.55  # 55% = clear bias

        dominant = None
        if long_pct_adj >= DOMINANCE_THRESHOLD:
            dominant = "long"
        elif short_pct_adj >= DOMINANCE_THRESHOLD:
            dominant = "short"

        if dominant:
            # Suppress minority direction: crush confidence so they drop off
            for s in signals:
                if s["direction"] == dominant:
                    s["dir_bias"] = "dominant"
                    s["conflicted"] = False
                else:
                    # Kill minority signals — multiply by (1 - dominance)
                    suppress_factor = 0.25  # keep 25% of original confidence
                    old_conf = s["confidence_pct"]
                    s["confidence_pct"] = int(s["confidence_pct"] * suppress_factor)
                    s["dir_bias"] = "suppressed"
                    s["conflicted"] = False
                    s["suppression_reason"] = (
                        f"{dominant.upper()} dominuje "
                        f"({'↑' if dominant == 'long' else '↓'} "
                        f"{long_pct_adj:.0%}/{short_pct_adj:.0%})"
                    )
            # Re-filter: remove signals that fell below 30% after suppression
            before = len(signals)
            signals = [s for s in signals if s["confidence_pct"] >= 30]
            suppressed_count = before - len(signals)
            log.info("Direction bias: %s dominates (%.0f%% vs %.0f%%) — %d signals suppressed",
                     dominant.upper(), long_pct_adj * 100, short_pct_adj * 100, suppressed_count)
        else:
            # Balanced conflict — mark all, let user see both sides
            log.info("⚖️ Direction conflict: LONG %.0f%% vs SHORT %.0f%% (balanced, showing both)",
                     long_pct_adj * 100, short_pct_adj * 100)
            for s in signals:
                s["conflicted"] = True
                s["dir_bias"] = "contested"
                s["conflict_note"] = (
                    f"LONG {long_pct_adj:.0%} vs SHORT {short_pct_adj:.0%} — "
                    f"czekaj na rozstrzygnięcie"
                )

        return signals

    def get_history(self) -> list[dict]:
        """Return last 50 signal evaluations."""
        return list(reversed(self._signal_history))

    def get_market_state(
        self,
        bars_df: pd.DataFrame,
        *,
        current_price: float = 0.0,
        bar_delta_pct: float = 0.0,
        now: datetime | None = None,
    ) -> dict:
        """Return current market state with ALL bot indicators."""
        now = now or datetime.now()
        if bars_df.empty:
            return {}

        last = bars_df.iloc[-1]
        atr = float(last.get("atr", 0))
        vwap = float(last.get("vwap", 0))
        ema20 = float(last.get("ema_20", 0))
        ema50 = float(last.get("ema_50", 0))
        ema100 = float(last.get("ema_100", 0)) if "ema_100" in bars_df.columns else 0
        rsi = float(last.get("rsi", 50)) if "rsi" in bars_df.columns else 50
        cum_delta = float(last.get("cum_delta", 0)) if "cum_delta" in bars_df.columns else 0
        delta_raw = float(last.get("delta", 0)) if "delta" in bars_df.columns else 0
        volume = float(last.get("volume", 0)) if "volume" in bars_df.columns else 0
        bar_high = float(last.get("high", current_price))
        bar_low = float(last.get("low", current_price))
        bar_close = float(last.get("close", current_price))
        bar_open = float(last.get("open", current_price))
        bar_range = bar_high - bar_low

        # Regime
        regime_info = self._get_regime(bars_df)

        # EMA stack direction
        if ema20 > ema50:
            ema_stack = "BULLISH"
        elif ema20 < ema50:
            ema_stack = "BEARISH"
        else:
            ema_stack = "NEUTRAL"

        # Price vs VWAP
        vwap_pos = "ABOVE" if current_price > vwap else "BELOW"
        vwap_dist = round(current_price - vwap, 2) if vwap > 0 else 0

        # Volume relative to avg
        if "volume" in bars_df.columns and len(bars_df) > 20:
            vol_avg = float(bars_df["volume"].tail(20).mean())
            vol_ratio = round(volume / vol_avg, 2) if vol_avg > 0 else 1.0
        else:
            vol_avg = volume
            vol_ratio = 1.0

        # CVD trend (last 5 bars)
        if "cum_delta" in bars_df.columns and len(bars_df) >= 5:
            cd5 = bars_df["cum_delta"].tail(5)
            cvd_trend = "RISING" if float(cd5.iloc[-1]) > float(cd5.iloc[0]) else "FALLING"
            cvd_change = round(float(cd5.iloc[-1]) - float(cd5.iloc[0]), 0)
        else:
            cvd_trend = "FLAT"
            cvd_change = 0

        # Delta streak (consecutive buy/sell bars)
        delta_streak = 0
        if "delta" in bars_df.columns and len(bars_df) >= 2:
            for i in range(len(bars_df) - 1, max(0, len(bars_df) - 10), -1):
                d = float(bars_df.iloc[i].get("delta", 0))
                if delta_raw > 0 and d > 0:
                    delta_streak += 1
                elif delta_raw < 0 and d < 0:
                    delta_streak -= 1
                else:
                    break

        # Bar candle type
        if bar_range > 0:
            body_pct = abs(bar_close - bar_open) / bar_range
            close_pos = (bar_close - bar_low) / bar_range
        else:
            body_pct = 0
            close_pos = 0.5

        # Time/day info
        hour = now.hour
        dow = now.weekday()
        time_info = TIME_EDGES.get((hour,), {"label": "Standard", "mult": 1.0})
        day_info = DAY_EDGES.get(dow, {"label": "Unknown", "long_mult": 1.0, "short_mult": 1.0})

        return {
            "price": round(current_price, 2),
            # --- Core indicators ---
            "atr": round(atr, 1),
            "vwap": round(vwap, 2),
            "vwap_pos": vwap_pos,
            "vwap_dist": vwap_dist,
            "ema20": round(ema20, 2),
            "ema50": round(ema50, 2),
            "ema100": round(ema100, 2),
            "ema_stack": ema_stack,
            "rsi": round(rsi, 1),
            # --- Delta / Flow ---
            "delta_pct": round(bar_delta_pct, 1),
            "delta_raw": round(delta_raw, 0),
            "cum_delta": round(cum_delta, 0),
            "cvd_trend": cvd_trend,
            "cvd_change": cvd_change,
            "delta_streak": delta_streak,
            # --- Volume ---
            "volume": round(volume, 0),
            "vol_avg": round(vol_avg, 0),
            "vol_ratio": vol_ratio,
            # --- Bar info ---
            "bar_range": round(bar_range, 2),
            "bar_body_pct": round(body_pct * 100, 0),
            "bar_close_pos": round(close_pos * 100, 0),
            # --- Context ---
            "regime": regime_info,
            "time_label": time_info["label"],
            "day_label": day_info["label"],
            "hour": hour,
            "dow": dow,
            "timestamp": now.isoformat(),
        }

    def compute_weighted_zones(self, signals: list[dict]) -> dict:
        """Compute weighted average entry/TP/SL across all signals.

        Weights = confidence_pct. Returns separate zones for LONG and SHORT.
        """
        result = {"long": None, "short": None}

        for direction in ("long", "short"):
            sigs = [s for s in signals if s["direction"] == direction and s.get("confidence_pct", 0) > 0]
            if not sigs:
                continue

            total_w = sum(s["confidence_pct"] for s in sigs)
            if total_w <= 0:
                continue

            w_entry = sum(s["entry"] * s["confidence_pct"] for s in sigs) / total_w
            w_tp1 = sum(s["tp1"] * s["confidence_pct"] for s in sigs) / total_w
            w_tp3 = sum(s["tp3"] * s["confidence_pct"] for s in sigs) / total_w
            w_sl = sum(s["sl"] * s["confidence_pct"] for s in sigs) / total_w

            # Also compute min/max range
            entries = [s["entry"] for s in sigs]
            tp1s = [s["tp1"] for s in sigs]
            sls = [s["sl"] for s in sigs]

            result[direction] = {
                "count": len(sigs),
                "avg_conf": round(total_w / len(sigs), 0),
                "entry": round(w_entry, 2),
                "entry_min": round(min(entries), 2),
                "entry_max": round(max(entries), 2),
                "tp1": round(w_tp1, 2),
                "tp1_min": round(min(tp1s), 2),
                "tp1_max": round(max(tp1s), 2),
                "tp3": round(w_tp3, 2),
                "sl": round(w_sl, 2),
                "sl_min": round(min(sls), 2),
                "sl_max": round(max(sls), 2),
            }

        return result

    def _get_regime(self, bars_df: pd.DataFrame) -> str:
        if len(bars_df) < 2:
            return "unknown"
        try:
            first = bars_df.iloc[0]
            last = bars_df.iloc[-1]
            op = float(first.get("open", 0))
            cl = float(last.get("close", 0))
            move = cl - op
            closes = pd.to_numeric(bars_df["close"], errors="coerce").dropna()
            total_path = float(closes.diff().abs().sum()) if len(closes) > 1 else abs(move)
            info = infer_regime(
                move_from_open=move, total_path=total_path,
                current_close=cl, open_price=op,
            )
            return info.regime
        except Exception:
            return "unknown"
