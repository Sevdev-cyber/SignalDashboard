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

import os
import sys
import compat  # noqa: F401 — patches dataclass for Python 3.9

import logging
import math
import time
from collections import defaultdict
from datetime import datetime
from pathlib import Path
from typing import Any

log = logging.getLogger("signal_dash")

import pandas as pd

THIS_DIR = Path(__file__).resolve().parent
NEW_SIGNAL_DIR = THIS_DIR.parent / "NewSignal"
if str(NEW_SIGNAL_DIR) not in sys.path:
    sys.path.insert(0, str(NEW_SIGNAL_DIR))

from hsb.signals.composite import CompositeGenerator, CONFLUENCE_BOOST
from hsb.pipeline.context_builder import ContextBuilder
from hsb.pipeline.regime import infer_regime
from hsb.domain.enums import Direction
from final_signal_engine import FinalSignalEngine


# Historical signal quality scores (from 37,803 signal events, composite metric)
# Score 0-100: net_WR (40%) + MFE/MAE ratio (30%) + forward PnL (30%)
# Used to scale confidence_pct for final ranking
SIGNAL_SCORE = {
    # ── TOP TIER (score 65-80) — best risk-adjusted signals ──
    ("delta_div", "short"):     77,   # MFE/MAE=17.1, +27.4pts@60m, netWR 47%
    ("trend", "short"):         73,   # MFE/MAE=23.6, +12.6pts, netWR 56% — most reliable
    ("vwap_bounce", "short"):   68,   # MFE/MAE=24.6, high net WR 62%
    ("ib_break", "short"):      66,   # MFE/MAE=17.3, +5.8pts, netWR 47%
    ("ema_bounce", "short"):    64,   # MFE/MAE=16.6, netWR 49%

    # ── MID TIER (score 40-55) — decent, use with confluence ──
    ("ib_retest", "long"):      49,   # small sample but +43pts@60m
    ("exhaust", "short"):       46,   # MFE/MAE=2.9, +3.2pts
    ("vwap_bounce", "long"):    46,   # +25pts@60m but low MFE/MAE
    ("vwap_mr", "long"):        42,   # 78% TP hit, +5.9pts
    ("streak", "short"):        40,   # 12K events, marginal
    ("pullback", "long"):       45,   # fast execution, proven v6.7
    ("pullback", "short"):      45,
    ("delta_accel", "short"):   42,
    ("delta_accel", "long"):    38,
    ("vwap_mr", "short"):       37,
    ("streak", "long"):         50,   # 5+ sell streak → LONG: Tier 1 (90% WR, rare)

    # ── LOW TIER (score 25-35) — use only with gold tier filters ──
    ("trend", "long"):          35,   # OK WR but low MFE/MAE
    ("delta_div", "long"):      34,   # +3.9pts but high risk
    ("exhaust", "long"):        30,   # SL=TP, breakeven
    ("sell_exhaust", "long"):   30,   # marginal

    # ── BOTTOM TIER (score <30) — consider disabling ──
    ("ema_bounce", "long"):     29,   # stop-hunted, -2.8pts
    ("ib_break", "long"):       29,   # trap signal, -5.3pts
    ("volspike", "long"):       28,   # worst PnL -6.0pts
}

# Confluence boost per signal (from study: solo vs conf WR delta)
CONFLUENCE_EFFECT = {
    ("vwap_bounce", "short"):   20,   # +20% WR with confluence!
    ("vwap_mr", "short"):       18,   # +18%
    ("trend", "long"):          17,   # +17% — huge boost
    ("trend", "short"):         5,
    ("ib_break", "short"):      3,
    ("delta_div", "long"):      3,
    # Negative confluence (worse with more signals):
    ("vwap_bounce", "long"):   -14,   # gets WORSE with confluence
    ("delta_div", "short"):    -7,
    ("volspike", "long"):      -6,
    ("ib_break", "long"):      -5,
    ("streak", "long"):        -5,
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
    # New signals
    "ib_break":      {"range": 1.1, "transition": 1.2, "trend_up": 1.0, "trend_down": 1.3, "chop": 0.7},
    "ib_retest":     {"range": 1.2, "transition": 1.3, "trend_up": 1.0, "trend_down": 1.3, "chop": 0.8},
    "ema_bounce":    {"range": 1.0, "transition": 1.0, "trend_up": 0.8, "trend_down": 0.8, "chop": 0.6},
    "streak_rev":    {"range": 1.3, "transition": 1.1, "trend_up": 0.7, "trend_down": 0.7, "chop": 1.0},
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
# Signal time profile: bars_to_tp (display), max_hold (expiry), optimal_tf (from study)
# optimal_tf: the forward window (minutes) where PnL is best
SIGNAL_TIME_PROFILE = {
    "pullback":      {"bars_to_tp": 2,  "label": "Fast",      "max_hold": 12,  "optimal_min": 5},
    "exhaustion":    {"bars_to_tp": 6,  "label": "Quick",     "max_hold": 24,  "optimal_min": 60},
    "sell_exhaust":  {"bars_to_tp": 2,  "label": "Scalp",     "max_hold": 6,   "optimal_min": 5},   # best at 1-5min!
    "delta_div":     {"bars_to_tp": 12, "label": "Swing",     "max_hold": 48,  "optimal_min": 60},  # best at 60min
    "delta_accel":   {"bars_to_tp": 5,  "label": "Quick",     "max_hold": 20,  "optimal_min": 20},
    "vwap_bounce":   {"bars_to_tp": 6,  "label": "Quick",     "max_hold": 24,  "optimal_min": 10},
    "ema_bounce":    {"bars_to_tp": 7,  "label": "Quick",     "max_hold": 24,  "optimal_min": 20},
    "trend_cont":    {"bars_to_tp": 4,  "label": "Fast",      "max_hold": 16,  "optimal_min": 10},  # best at 10min
    "waterfall":     {"bars_to_tp": 4,  "label": "Fast",      "max_hold": 12,  "optimal_min": 5},
    "micro_smc":     {"bars_to_tp": 14, "label": "Slow",      "max_hold": 48,  "optimal_min": 60},
    "break_retest":  {"bars_to_tp": 18, "label": "Slow",      "max_hold": 48,  "optimal_min": 60},
    "reclaim":       {"bars_to_tp": 16, "label": "Slow",      "max_hold": 36,  "optimal_min": 60},
    "sweep":         {"bars_to_tp": 25, "label": "Very Slow", "max_hold": 48,  "optimal_min": 60},
    "ib_break":      {"bars_to_tp": 11, "label": "Medium",    "max_hold": 36,  "optimal_min": 60},  # grows with time
    "ib_retest":     {"bars_to_tp": 12, "label": "Swing",     "max_hold": 48,  "optimal_min": 60},  # +43pts @60min
    "orb":           {"bars_to_tp": 12, "label": "Medium",    "max_hold": 36,  "optimal_min": 30},
    "vwap_loss":     {"bars_to_tp": 8,  "label": "Quick",     "max_hold": 20,  "optimal_min": 20},
    "streak_rev":    {"bars_to_tp": 6,  "label": "Quick",     "max_hold": 24,  "optimal_min": 30},  # 90% WR sell streak
    "vwap_mr":       {"bars_to_tp": 12, "label": "Swing",     "max_hold": 48,  "optimal_min": 60},  # 78% LONG
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
        self.engine_mode = os.getenv("SIGNAL_ENGINE_MODE", "final_mtf_v3").strip().lower()
        if self.engine_mode == "final_mtf_v2":
            final_variant = "v2"
        elif self.engine_mode == "final_mtf_v3":
            final_variant = "v3"
        else:
            final_variant = "v1"
        self.final_engine = FinalSignalEngine(variant=final_variant)

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
        if self.engine_mode in {"final_mtf", "final_mtf_v2", "final_mtf_v3"}:
            return self._evaluate_final_mtf(
                bars_df,
                bar_delta_pct=bar_delta_pct,
                current_price=current_price,
                now=now,
            )

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
            # Extract base signal type from source_type like "derived_ib_break_short"
            # Try matching longest key in SIGNAL_TIME_PROFILE first
            _st = source_type.lower().replace("derived_", "")
            base_source = source_type
            for key in sorted(SIGNAL_TIME_PROFILE.keys(), key=len, reverse=True):
                if key in _st:
                    base_source = key
                    break
            else:
                # Fallback: second token
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

            # Historical quality score (from 37K signal study, 0-100 composite)
            base_quality = 40  # default if not in table
            for (sig_tag, sig_dir), qscore in SIGNAL_SCORE.items():
                if sig_tag in _st and sig_dir == direction:
                    base_quality = qscore
                    break

            # Grade from score
            if base_quality >= 65:
                quality_grade = "A+"
            elif base_quality >= 50:
                quality_grade = "A"
            elif base_quality >= 40:
                quality_grade = "B"
            elif base_quality >= 30:
                quality_grade = "C"
            else:
                quality_grade = "D"

            # Quality multiplier: score/50 so 50=neutral, 77=1.54x, 28=0.56x
            quality_mult = base_quality / 50.0

            # Compute confidence (wider range: use multipliers but keep 0-100)
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
                "max_hold_bars": time_profile.get("max_hold", 48),
                "optimal_min": time_profile.get("optimal_min", 20),
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

            # Count UNIQUE signal types confirming (not total count)
            unique_confirms = len(set(sig.get("confirming_signals", [])))

            if unique_confirms >= 2 and is_prime_hour and regime_ok:
                gold_tier = True
                gold_label = "GOLD"
                sig["confidence_pct"] = min(100, int(sig["confidence_pct"] * 1.30 + 5))
            elif unique_confirms >= 2 and is_prime_hour:
                gold_label = "SILVER"
                sig["confidence_pct"] = min(100, int(sig["confidence_pct"] * 1.15 + 3))
            elif unique_confirms >= 2 and regime_ok:
                gold_label = "SILVER"
                sig["confidence_pct"] = min(100, int(sig["confidence_pct"] * 1.10 + 2))
            elif unique_confirms >= 3:
                gold_label = "SILVER"
                sig["confidence_pct"] = min(100, int(sig["confidence_pct"] * 1.10))

            sig["gold_tier"] = gold_tier
            sig["tier_label"] = gold_label

        # POST-PROCESSING: Wzmacnianie i dławienie sygnałów według wyników testu 120-dniowego
        for sig in enriched:
            # 0. Twarde wycięcie trucizn (FVG_FILL, BOS_BULL)
            if "FVG_FILL" in sig["name"] or "BOS_BULL" in sig["name"]:
                sig["confidence_pct"] = 0
                continue

            # 0a. Minimum RR filter — reject signals with risk > reward
            rr = sig["rr_ratio"]
            if sig["risk_pts"] > 0:
                actual_rr = abs(sig["tp1"] - sig["entry"]) / sig["risk_pts"]
                if actual_rr < 1.0:
                    sig["confidence_pct"] = int(sig["confidence_pct"] * 0.4)  # heavy penalty

            # 0b. Trend filter dla PULLBACK
            if "PULLBACK" in sig["name"]:
                if sig["direction"] == "long" and ema50_trend == "down" and close_price < vwap_price:
                    sig["confidence_pct"] = 0
                    continue
                if sig["direction"] == "short" and ema50_trend == "up" and close_price > vwap_price:
                    sig["confidence_pct"] = 0
                    continue

            # 1. Confluence effect (data-driven per signal type)
            src = sig["source_type"].lower().replace("derived_", "")
            conf_effect = 0
            for (sig_tag, sig_dir), eff in CONFLUENCE_EFFECT.items():
                if sig_tag in src and sig_dir == sig["direction"]:
                    conf_effect = eff
                    break

            has_confluence = sig["confluence_count"] >= 2
            if has_confluence and conf_effect > 0:
                # Positive confluence: boost
                sig["confidence_pct"] = min(100, sig["confidence_pct"] + conf_effect // 2)
            elif has_confluence and conf_effect < 0:
                # Negative confluence: penalty (signal gets worse with more signals)
                sig["confidence_pct"] = max(10, sig["confidence_pct"] + conf_effect)
            elif not has_confluence and not sig.get("gold_tier"):
                # No confluence, no gold: moderate penalty
                sig["confidence_pct"] = int(sig["confidence_pct"] * 0.7)

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

    def _evaluate_final_mtf(
        self,
        bars_df: pd.DataFrame,
        *,
        bar_delta_pct: float = 0.0,
        current_price: float = 0.0,
        now: datetime | None = None,
    ) -> list[dict]:
        now = now or datetime.now()
        raw = self.final_engine.evaluate(
            bars_df,
            bar_delta_pct=bar_delta_pct,
            current_price=current_price,
            now=now,
        )
        if not raw:
            self._last_signals = []
            return []

        enriched = []
        for sig in raw:
            item = dict(sig)
            contributors = list(item.get("contributors", []))
            item["confluence_count"] = len(contributors) + 1
            item["confirming_signals"] = contributors
            item["gold_tier"] = item.get("confidence_pct", 0) >= 90
            item["tier_label"] = "GOLD" if item["confidence_pct"] >= 90 else "SILVER" if item["confidence_pct"] >= 80 else ""
            item["regime_match"] = True
            item["regime_mult"] = 1.0
            item["time_edge"] = f"Final MTF {item.get('lead_timeframe_min', '?')}m"
            item["time_mult"] = 1.0
            item["day_edge"] = "Final engine"
            item["day_mult"] = 1.0
            item["delta_pct"] = round(bar_delta_pct, 1)
            item["delta_aligned"] = True
            item["quality_mult"] = round(item.get("confidence_pct", 0) / 100.0, 2)
            item["dir_bias"] = "dominant"
            item["conflicted"] = False
            item["entry_type"] = item.get("entry_type", "limit")
            enriched.append(item)

        enriched.sort(key=lambda s: s["confidence_pct"], reverse=True)
        self._last_signals = enriched
        for sig in enriched[:3]:
            self._signal_history.append(sig)
        self._signal_history = self._signal_history[-50:]
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
            # Tag but DON'T suppress — user wants to see all signals
            for s in signals:
                if s["direction"] == dominant:
                    s["dir_bias"] = "dominant"
                    s["conflicted"] = False
                else:
                    s["dir_bias"] = "minority"
                    s["conflicted"] = False
            log.info("Direction bias: %s (%.0f%% vs %.0f%%) — all signals kept",
                     dominant.upper(), long_pct_adj * 100, short_pct_adj * 100)
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
        if self.engine_mode in {"final_mtf", "final_mtf_v2", "final_mtf_v3"}:
            return self.final_engine.get_history()
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
            "trader_guide": self._build_trader_guide(bars_df, current_price=current_price),
        }

    def _build_trader_guide(self, bars_df: pd.DataFrame, *, current_price: float) -> dict:
        guide_5m = self._compute_tf_guide(bars_df, timeframe_min=5, current_price=current_price)
        guide_15m = self._compute_tf_guide(bars_df, timeframe_min=15, current_price=current_price)

        score = 0
        if guide_15m["bias"] == "long":
            score += 2
        elif guide_15m["bias"] == "short":
            score -= 2
        if guide_5m["bias"] == "long":
            score += 1
        elif guide_5m["bias"] == "short":
            score -= 1

        if score >= 2:
            overall_bias = "long"
        elif score <= -2:
            overall_bias = "short"
        elif score > 0:
            overall_bias = "neutral_to_long"
        elif score < 0:
            overall_bias = "neutral_to_short"
        else:
            overall_bias = "neutral"

        alignment_bonus = 12 if guide_5m["bias"] == guide_15m["bias"] and guide_5m["bias"] != "neutral" else 0
        overall_confidence = min(95, max(40, 52 + abs(score) * 10 + alignment_bonus))

        zones = []
        for tf_guide in (guide_5m, guide_15m):
            for zone in tf_guide.get("guide_zones", []):
                zones.append(zone)

        nearest_zones = sorted(
            zones,
            key=lambda item: min(abs(item["low"] - current_price), abs(item["high"] - current_price)),
        )[:4]

        if overall_bias == "long":
            summary = "Prefer LONG continuation while 15m holds value. Short only on confirmed rejection / loss of 5m trigger."
        elif overall_bias == "short":
            summary = "Prefer SHORT continuation while 15m stays heavy. Long only on confirmed reclaim / absorption."
        elif overall_bias == "neutral_to_long":
            summary = "Bias slightly long, but 5m confirmation matters. Best entries are pullbacks into value."
        elif overall_bias == "neutral_to_short":
            summary = "Bias slightly short, but 5m confirmation matters. Best entries are pops into resistance."
        else:
            summary = "No clean dominance. Treat current area as rotational until 5m and 15m align."

        best_long_zone = self._pick_directional_zone("long", guide_5m, guide_15m, current_price=current_price)
        best_short_zone = self._pick_directional_zone("short", guide_5m, guide_15m, current_price=current_price)
        continuation = self._build_continuation_summary(overall_bias, guide_5m, guide_15m, current_price=current_price)
        prediction = self._build_prediction_summary(overall_bias, guide_5m, guide_15m, current_price=current_price)

        return {
            "overall_bias": overall_bias,
            "confidence": overall_confidence,
            "summary": summary,
            "prediction": prediction,
            "continuation": continuation,
            "best_long_zone": best_long_zone,
            "best_short_zone": best_short_zone,
            "tf_5m": guide_5m,
            "tf_15m": guide_15m,
            "zones": nearest_zones,
        }

    def _compute_tf_guide(self, bars_df: pd.DataFrame, *, timeframe_min: int, current_price: float) -> dict:
        frame = self._resample_for_guide(bars_df, timeframe_min)
        empty = {
            "timeframe": f"{timeframe_min}m",
            "bias": "neutral",
            "strength": 0,
            "reversal_risk": "unknown",
            "reversal_side": "neutral",
            "continuation_valid": False,
            "invalidation_level": round(current_price, 2) if current_price else 0.0,
            "continuation_zone": None,
            "reversal_zone": None,
            "continuation_note": "Not enough bars.",
            "reversal_note": "Not enough bars.",
            "trigger_level": round(current_price, 2) if current_price else 0.0,
        }
        if frame.empty or len(frame) < 20:
            return empty

        last = frame.iloc[-1]
        prev = frame.iloc[-2]
        close = float(last["close"])
        high = float(last["high"])
        low = float(last["low"])
        ema20 = float(last["ema20"])
        ema50 = float(last["ema50"])
        vwap = float(last["vwap"])
        atr = max(0.25, float(last["atr"]))
        rsi = float(last["rsi"])
        close_pos = 50.0 if high == low else ((close - low) / (high - low)) * 100.0

        long_score = sum([
            close >= ema20,
            ema20 >= ema50,
            close >= vwap,
            close >= float(prev["close"]),
        ])
        short_score = sum([
            close <= ema20,
            ema20 <= ema50,
            close <= vwap,
            close <= float(prev["close"]),
        ])
        if long_score >= 3 and long_score > short_score:
            bias = "long"
            strength = long_score
        elif short_score >= 3 and short_score > long_score:
            bias = "short"
            strength = short_score
        else:
            bias = "neutral"
            strength = max(long_score, short_score)

        recent = frame.tail(7 if timeframe_min == 5 else 5).iloc[:-1]
        swing_high = float(recent["high"].max()) if not recent.empty else high
        swing_low = float(recent["low"].min()) if not recent.empty else low
        extension = abs(close - ema20) / atr

        continuation_center = (ema20 + vwap) / 2.0
        continuation_half = max(atr * 0.35, abs(ema20 - vwap) / 2.0, 1.0)
        continuation_zone = {
            "low": round(min(continuation_center - continuation_half, continuation_center + continuation_half), 2),
            "high": round(max(continuation_center - continuation_half, continuation_center + continuation_half), 2),
        }
        trigger_half = max(atr * 0.18, 0.75)

        if bias == "long":
            reversal_side = "short"
            reversal_center = swing_high
            reversal_score = sum([
                rsi >= 67,
                extension >= 1.1,
                close_pos <= 45,
                close < float(prev["close"]),
            ])
            trigger_level = round(max(swing_low, ema20), 2)
            continuation_valid = close >= trigger_level
            invalidation_level = trigger_level
            continuation_note = f"Best LONG area is pullback into VWAP/EMA20 cluster while above {trigger_level:.2f}."
            prediction_note = (
                f"Predicted LONG only if pullback holds value and reclaims {trigger_level:.2f}. "
                f"Do not chase extension away from value."
            )
            reversal_note = (
                f"{timeframe_min}m SHORT reversal becomes meaningful only after rejection of {reversal_center:.2f} "
                f"and loss of {trigger_level:.2f}."
            )
        elif bias == "short":
            reversal_side = "long"
            reversal_center = swing_low
            reversal_score = sum([
                rsi <= 33,
                extension >= 1.1,
                close_pos >= 55,
                close > float(prev["close"]),
            ])
            trigger_level = round(min(swing_high, ema20), 2)
            continuation_valid = close <= trigger_level
            invalidation_level = trigger_level
            continuation_note = f"Best SHORT area is pop into VWAP/EMA20 cluster while below {trigger_level:.2f}."
            prediction_note = (
                f"Predicted SHORT only if pop into value gets rejected and price stays below {trigger_level:.2f}. "
                f"Do not short the hole away from value."
            )
            reversal_note = (
                f"{timeframe_min}m LONG reversal becomes meaningful only after reclaim of {reversal_center:.2f} "
                f"and push back above {trigger_level:.2f}."
            )
        else:
            reversal_side = "neutral"
            reversal_center = close
            reversal_score = 1
            trigger_level = round((swing_high + swing_low) / 2.0, 2)
            continuation_valid = False
            invalidation_level = trigger_level
            continuation_note = "No dominant side. Treat VWAP/EMA20 area as rotation zone."
            prediction_note = f"Wait for break/reclaim of {trigger_level:.2f}. No clean predictive edge yet."
            reversal_note = f"Wait for break/reclaim of {trigger_level:.2f} before leaning directional."

        reversal_half = max(atr * 0.3, 1.0)
        reversal_zone = {
            "low": round(reversal_center - reversal_half, 2),
            "high": round(reversal_center + reversal_half, 2),
        }
        reversal_risk = "high" if reversal_score >= 3 else "medium" if reversal_score == 2 else "low"
        trigger_zone = {
            "low": round(trigger_level - trigger_half, 2),
            "high": round(trigger_level + trigger_half, 2),
        }
        guide_zones = []
        if continuation_zone:
            guide_zones.append(
                {
                    "label": f"{timeframe_min}m predicted area",
                    "stage": "predicted",
                    "direction": bias if bias != "neutral" else "neutral",
                    "low": continuation_zone["low"],
                    "high": continuation_zone["high"],
                    "trigger": trigger_level,
                    "why": prediction_note,
                }
            )
            guide_zones.append(
                {
                    "label": f"{timeframe_min}m watch area",
                    "stage": "watch",
                    "direction": bias if bias != "neutral" else "neutral",
                    "low": continuation_zone["low"],
                    "high": continuation_zone["high"],
                    "trigger": trigger_level,
                    "why": continuation_note,
                }
            )
        if bias != "neutral":
            guide_zones.append(
                {
                    "label": f"{timeframe_min}m trigger",
                    "stage": "trigger",
                    "direction": bias,
                    "low": trigger_zone["low"],
                    "high": trigger_zone["high"],
                    "trigger": trigger_level,
                    "why": (
                        f"{timeframe_min}m {bias.upper()} stays valid only "
                        f"{'above' if bias == 'long' else 'below'} {trigger_level:.2f}."
                    ),
                }
            )
        if reversal_zone:
            guide_zones.append(
                {
                    "label": f"{timeframe_min}m reversal watch",
                    "stage": "reversal",
                    "direction": reversal_side,
                    "low": reversal_zone["low"],
                    "high": reversal_zone["high"],
                    "trigger": trigger_level,
                    "why": reversal_note,
                }
            )

        return {
            "timeframe": f"{timeframe_min}m",
            "bias": bias,
            "strength": int(strength),
            "reversal_risk": reversal_risk,
            "reversal_side": reversal_side,
            "continuation_valid": continuation_valid,
            "invalidation_level": invalidation_level,
            "continuation_zone": continuation_zone,
            "reversal_zone": reversal_zone,
            "continuation_note": continuation_note,
            "prediction_note": prediction_note,
            "reversal_note": reversal_note,
            "trigger_level": trigger_level,
            "trigger_zone": trigger_zone,
            "guide_zones": guide_zones,
        }

    def _pick_directional_zone(
        self,
        direction: str,
        guide_5m: dict,
        guide_15m: dict,
        *,
        current_price: float,
    ) -> dict | None:
        candidates: list[dict] = []
        for tf_guide in (guide_5m, guide_15m):
            if direction == "long" and tf_guide["bias"] == "long" and tf_guide["continuation_zone"]:
                candidates.append(
                    {
                        "label": f"Best {tf_guide['timeframe']} LONG zone",
                        "timeframe": tf_guide["timeframe"],
                        "low": tf_guide["continuation_zone"]["low"],
                        "high": tf_guide["continuation_zone"]["high"],
                        "invalidation": tf_guide["invalidation_level"],
                        "why": tf_guide["continuation_note"],
                    }
                )
            if direction == "short" and tf_guide["bias"] == "short" and tf_guide["continuation_zone"]:
                candidates.append(
                    {
                        "label": f"Best {tf_guide['timeframe']} SHORT zone",
                        "timeframe": tf_guide["timeframe"],
                        "low": tf_guide["continuation_zone"]["low"],
                        "high": tf_guide["continuation_zone"]["high"],
                        "invalidation": tf_guide["invalidation_level"],
                        "why": tf_guide["continuation_note"],
                    }
                )
        if not candidates:
            return None
        candidates.sort(key=lambda item: min(abs(item["low"] - current_price), abs(item["high"] - current_price)))
        return candidates[0]

    def _build_continuation_summary(
        self,
        overall_bias: str,
        guide_5m: dict,
        guide_15m: dict,
        *,
        current_price: float,
    ) -> dict:
        if overall_bias in {"long", "neutral_to_long"}:
            primary = guide_5m if guide_5m["bias"] == "long" else guide_15m
            return {
                "side": "long",
                "valid": bool(primary.get("continuation_valid")),
                "invalidation": primary.get("invalidation_level"),
                "message": (
                    f"LONG continuation is {'valid' if primary.get('continuation_valid') else 'fragile'} "
                    f"while price holds above {float(primary.get('invalidation_level', current_price)):.2f}."
                ),
            }
        if overall_bias in {"short", "neutral_to_short"}:
            primary = guide_5m if guide_5m["bias"] == "short" else guide_15m
            return {
                "side": "short",
                "valid": bool(primary.get("continuation_valid")),
                "invalidation": primary.get("invalidation_level"),
                "message": (
                    f"SHORT continuation is {'valid' if primary.get('continuation_valid') else 'fragile'} "
                    f"while price stays below {float(primary.get('invalidation_level', current_price)):.2f}."
                ),
            }
        return {
            "side": "neutral",
            "valid": False,
            "invalidation": round(current_price, 2),
            "message": "No continuation edge right now. Wait for 5m and 15m to align.",
        }

    def _build_prediction_summary(
        self,
        overall_bias: str,
        guide_5m: dict,
        guide_15m: dict,
        *,
        current_price: float,
    ) -> dict:
        if overall_bias in {"short", "neutral_to_short"}:
            primary = guide_5m if guide_5m["bias"] == "short" else guide_15m
            return {
                "side": "short",
                "watch_low": primary.get("continuation_zone", {}).get("low", round(current_price, 2)),
                "watch_high": primary.get("continuation_zone", {}).get("high", round(current_price, 2)),
                "trigger": primary.get("trigger_level", round(current_price, 2)),
                "message": (
                    f"Predicted SHORT = wait for pop into {float(primary.get('continuation_zone', {}).get('low', current_price)):.2f}-"
                    f"{float(primary.get('continuation_zone', {}).get('high', current_price)):.2f} and rejection back "
                    f"below {float(primary.get('trigger_level', current_price)):.2f}."
                ),
            }
        if overall_bias in {"long", "neutral_to_long"}:
            primary = guide_5m if guide_5m["bias"] == "long" else guide_15m
            return {
                "side": "long",
                "watch_low": primary.get("continuation_zone", {}).get("low", round(current_price, 2)),
                "watch_high": primary.get("continuation_zone", {}).get("high", round(current_price, 2)),
                "trigger": primary.get("trigger_level", round(current_price, 2)),
                "message": (
                    f"Predicted LONG = wait for dip into {float(primary.get('continuation_zone', {}).get('low', current_price)):.2f}-"
                    f"{float(primary.get('continuation_zone', {}).get('high', current_price)):.2f} and reclaim back "
                    f"above {float(primary.get('trigger_level', current_price)):.2f}."
                ),
            }
        return {
            "side": "neutral",
            "watch_low": round(current_price, 2),
            "watch_high": round(current_price, 2),
            "trigger": round(current_price, 2),
            "message": "No clean predicted area yet. Wait for a structured move into value first.",
        }

    def _resample_for_guide(self, bars_df: pd.DataFrame, timeframe_min: int) -> pd.DataFrame:
        frame = bars_df.copy()
        if "timestamp" not in frame.columns and "datetime" in frame.columns:
            frame["timestamp"] = pd.to_datetime(frame["datetime"], errors="coerce")
        elif "timestamp" in frame.columns:
            frame["timestamp"] = pd.to_datetime(frame["timestamp"], errors="coerce")
        if "timestamp" not in frame.columns:
            return pd.DataFrame()
        frame = frame.dropna(subset=["timestamp"]).sort_values("timestamp")
        frame = frame.set_index("timestamp")
        agg = {
            "open": "first",
            "high": "max",
            "low": "min",
            "close": "last",
            "volume": "sum",
        }
        if "delta" in frame.columns:
            agg["delta"] = "sum"
        out = frame.resample(f"{timeframe_min}min").agg(agg).dropna(subset=["open"]).copy()
        if out.empty:
            return out

        out["ema20"] = out["close"].ewm(span=20, adjust=False).mean()
        out["ema50"] = out["close"].ewm(span=50, adjust=False).mean()
        typical = (out["high"] + out["low"] + out["close"]) / 3.0
        cum_vol = out["volume"].cumsum().replace(0, pd.NA)
        out["vwap"] = (typical * out["volume"]).cumsum() / cum_vol
        delta = out["close"].diff()
        gain = delta.clip(lower=0).rolling(14).mean()
        loss = (-delta.clip(upper=0)).rolling(14).mean().replace(0, pd.NA)
        rs = gain / loss
        out["rsi"] = (100 - (100 / (1 + rs))).fillna(50)
        prev_close = out["close"].shift(1)
        tr = pd.concat(
            [
                out["high"] - out["low"],
                (out["high"] - prev_close).abs(),
                (out["low"] - prev_close).abs(),
            ],
            axis=1,
        ).max(axis=1)
        out["atr"] = tr.rolling(14).mean().fillna(tr.expanding().mean()).fillna(0.0)
        out.reset_index(inplace=True)
        return out

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
