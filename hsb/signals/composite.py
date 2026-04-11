"""Composite signal generator — ported from V1 + V4 signals.

Produces LONG and SHORT candidates based on VWAP reclaims, pullbacks,
ORB breakouts, session level sweeps, VWAP loss continuation, break & retest,
delta acceleration (V4), and micro SMC (V4: BOS, CHOCH, FVG).

Cross-signal confluence boosting is applied when multiple generators
fire on the same bar+direction (ported from V4 CompositeScorer).

This module has **no side effects** — it reads the context and returns a
list of :class:`SignalCandidate` instances.
"""

from __future__ import annotations

import uuid
from collections import defaultdict
from datetime import datetime

import numpy as np
import pandas as pd

from hsb.domain.context import AnalysisContext
from hsb.domain.enums import CandidateFamily, Direction
from hsb.domain.models import SignalCandidate
from hsb.signals.delta_acceleration import DeltaAccelerationGenerator
from hsb.signals.exhaustion import ExhaustionGenerator
from hsb.signals.micro_smc import MicroSMCGenerator
from hsb.signals.trend_continuation import TrendContinuationGenerator
from hsb.signals.waterfall import WaterfallGenerator
from hsb.signals.delta_divergence import DeltaDivergenceGenerator
from hsb.signals.vwap_bounce import VWAPBounceGenerator
from hsb.signals.ema_bounce import EMABounceGenerator


# Confluence boost matrix (ported from V4 CompositeScorer).
# When two different source_types fire on the same bar+direction, boost score.
CONFLUENCE_BOOST = {
    ("sweep", "delta_accel"): 0.20,
    ("sweep", "micro_smc"): 0.20,
    ("sweep", "vwap_loss"): 0.15,
    ("pullback", "delta_accel"): 0.15,
    ("pullback", "micro_smc"): 0.15,
    ("vwap_loss", "delta_accel"): 0.20,
    ("vwap_loss", "micro_smc"): 0.15,
    ("break_retest", "micro_smc"): 0.20,
    ("break_retest", "delta_accel"): 0.15,
    ("delta_accel", "micro_smc"): 0.25,
    ("reclaim", "micro_smc"): 0.15,
    ("orb", "delta_accel"): 0.20,
    # New generators
    ("exhaustion", "delta_accel"): 0.20,
    ("exhaustion", "micro_smc"): 0.15,
    ("trend_cont", "delta_accel"): 0.20,
    ("trend_cont", "micro_smc"): 0.15,
    ("trend_cont", "break_retest"): 0.15,
    # Waterfall
    ("waterfall", "delta_accel"): 0.25,
    ("waterfall", "micro_smc"): 0.20,
    ("waterfall", "vwap_loss"): 0.20,
    ("waterfall", "sweep"): 0.15,
    # Delta divergence
    ("delta_div", "exhaustion"): 0.25,
    ("delta_div", "micro_smc"): 0.20,
    ("delta_div", "vwap_loss"): 0.15,
    ("delta_div", "break_retest"): 0.15,
    ("delta_div", "vwap_bounce"): 0.20,
    # VWAP bounce
    ("vwap_bounce", "delta_accel"): 0.20,
    ("vwap_bounce", "micro_smc"): 0.15,
    ("vwap_bounce", "exhaustion"): 0.20,
    ("vwap_bounce", "delta_div"): 0.20,
    # EMA bounce
    ("ema_bounce", "delta_accel"): 0.20,
    ("ema_bounce", "delta_div"): 0.15,
    ("ema_bounce", "micro_smc"): 0.15,
    ("ema_bounce", "break_retest"): 0.15,
}


class CompositeGenerator:
    """Deterministic candidate generator with V4 signal modules and confluence."""

    def __init__(self, config: dict | None = None) -> None:
        cfg = config or {}
        # V4 ported generators
        self._delta_accel = DeltaAccelerationGenerator(cfg.get("delta_acceleration"))
        self._micro_smc = MicroSMCGenerator(cfg.get("micro_smc"))
        # Gap-fill generators (from day analysis)
        self._trend_cont = TrendContinuationGenerator(cfg.get("trend_continuation"))
        self._exhaustion = ExhaustionGenerator(cfg.get("exhaustion"))
        self._waterfall = WaterfallGenerator(cfg.get("waterfall"))
        self._delta_div = DeltaDivergenceGenerator(cfg.get("delta_divergence"))
        self._vwap_bounce = VWAPBounceGenerator(cfg.get("vwap_bounce"))
        self._ema_bounce = EMABounceGenerator(cfg.get("ema_bounce"))

    # ------------------------------------------------------------------
    # Public API (matches CandidateGenerator protocol)
    # ------------------------------------------------------------------

    def generate(self, context: AnalysisContext) -> list[SignalCandidate]:
        bars = context.bar_data.bars_df
        if bars.empty or len(bars) < 3:
            return []

        bars = self._ensure_numeric(bars)
        regime = context.regime.regime

        candidates: list[SignalCandidate] = []

        # Original V1/V2 generators
        candidates.extend(self._build_reclaim_candidates(bars, context, regime))
        candidates.extend(self._build_pullback_candidates(bars, context, regime))
        candidates.extend(self._build_orb_candidates(bars, context, regime))
        # DISABLED: sweep — PF=0.90 even with +2pts padding. -$126 on 126d.
        # candidates.extend(self._build_sweep_candidates(bars, context, regime))
        candidates.extend(self._build_vwap_loss_candidates(bars, context, regime))
        candidates.extend(self._build_break_retest_candidates(bars, context, regime))

        # V4 ported generators
        candidates.extend(self._delta_accel.generate(bars, context))
        candidates.extend(self._micro_smc.generate(bars, context))

        # Gap-fill generators (from day analysis)
        candidates.extend(self._trend_cont.generate(bars, context))
        candidates.extend(self._exhaustion.generate(bars, context))
        candidates.extend(self._waterfall.generate(bars, context))
        candidates.extend(self._delta_div.generate(bars, context))
        candidates.extend(self._vwap_bounce.generate(bars, context))
        # Re-enabled V11: with +5pts SL padding, ema_bounce is 43% WR, +$2,247
        candidates.extend(self._ema_bounce.generate(bars, context))

        # Filter invalid candidates (NaN SL/entry from edge cases)
        import math
        candidates = [
            c for c in candidates
            if c is not None
            and not math.isnan(c.sl_price)
            and not math.isnan(c.entry_price)
            and not math.isnan(c.tp1_price)
        ]

        # Apply cross-signal confluence boosting (V4 logic)
        candidates = self._apply_confluence(candidates)

        # Time-weighted scoring:
        # 14:00-16:00 = 50% big-move rate → bonus
        #  9:00-12:00 =  7% big-move rate → penalty
        candidates = self._apply_time_weight(candidates, bars)

        return self._dedupe(candidates)

    def _apply_time_weight(
        self, candidates: list[SignalCandidate], bars: pd.DataFrame
    ) -> list[SignalCandidate]:
        """Adjust scores based on time-of-day edge.

        MNQ has a massive structural edge during RTH power hours (14-16 EST)
        where 50%+ of bars produce >30pt moves. Morning hours (9-12) produce
        big moves only 7% of the time → lower expected RR.
        """
        ts_col = None
        for col in ("datetime", "timestamp"):
            if col in bars.columns:
                ts_col = col
                break
        if ts_col is None:
            return candidates

        for c in candidates:
            bar_idx = c.features.get("bar_index")
            if bar_idx is None or bar_idx >= len(bars):
                continue
            ts = bars.iloc[bar_idx].get(ts_col)
            if ts is None or not hasattr(ts, "hour"):
                continue

            hour = ts.hour
            if 14 <= hour <= 15:
                # Peak power hours: 50% big-move rate
                c.score = min(1.0, c.score + 0.05)
                c.reasons.append("peak_hour")
            elif 16 <= hour <= 17:
                # Extended hours: 32% big-move rate (still good)
                c.score = min(1.0, c.score + 0.03)
            elif 9 <= hour <= 11:
                # Quiet morning: 7% big-move rate
                c.score = max(0.1, c.score - 0.05)
                c.reasons.append("quiet_hour")

        return candidates

    # ------------------------------------------------------------------
    # VWAP reclaim candidates
    # ------------------------------------------------------------------

    def _build_reclaim_candidates(
        self,
        bars: pd.DataFrame,
        ctx: AnalysisContext,
        regime: str,
    ) -> list[SignalCandidate]:
        # DISABLED: 19% WR across 73 days (6W/25L = -$273).
        # VWAP cross alone is too noisy without additional confirmation.
        # The vwap_loss generator handles the displacement-confirmed version.
        return []

        row = bars.iloc[-1]
        prev = bars.iloc[-2]
        ts = self._bar_timestamp(row, ctx)
        close = float(row.get("close", 0.0))
        vwap = float(row.get("vwap", close))
        prev_close = float(prev.get("close", close))
        prev_vwap = float(prev.get("vwap", prev_close))
        ema20 = float(row.get("ema_20", close))
        ema50 = float(row.get("ema_50", ema20))
        atr = self._safe(row.get("atr"), 20.0)
        recent = bars.iloc[max(0, len(bars) - 6):]
        swing_lo = float(recent["low"].min()) if "low" in recent.columns else close - atr
        swing_hi = float(recent["high"].max()) if "high" in recent.columns else close + atr
        delta_sum = float(recent["delta"].sum()) if "delta" in recent.columns else 0.0
        candidates: list[SignalCandidate] = []

        # Long reclaim
        if (
            close > vwap
            and prev_close <= prev_vwap
            and ema20 >= ema50
            and (delta_sum > 0 or regime in {"trend_up", "transition"})
        ):
            entry = close
            sl = min(swing_lo, vwap - atr * 0.35)
            risk = max(entry - sl, max(atr * 0.35, 2.0))
            sl = entry - risk
            candidates.append(
                self._make(
                    bars=bars,
                    timestamp=ts,
                    direction=Direction.LONG,
                    entry=entry,
                    sl=sl,
                    score=0.63 if regime == "transition" else 0.68,
                    reasons=["vwap_reclaim", "bullish_reclaim", "accept_above_vwap"],
                    bar_index=len(bars) - 1,
                    source_type="derived_vwap_reclaim_long",
                )
            )

        # Short reclaim
        if (
            close < vwap
            and prev_close >= prev_vwap
            and ema20 <= ema50
            and (delta_sum < 0 or regime in {"trend_down", "transition"})
        ):
            entry = close
            sl = max(swing_hi, vwap + atr * 0.35)
            risk = max(sl - entry, max(atr * 0.35, 2.0))
            sl = entry + risk
            candidates.append(
                self._make(
                    bars=bars,
                    timestamp=ts,
                    direction=Direction.SHORT,
                    entry=entry,
                    sl=sl,
                    score=0.63 if regime == "transition" else 0.68,
                    reasons=["vwap_reclaim", "bearish_reclaim", "accept_below_vwap"],
                    bar_index=len(bars) - 1,
                    source_type="derived_vwap_reclaim_short",
                )
            )

        return candidates

    # ------------------------------------------------------------------
    # Pullback candidates
    # ------------------------------------------------------------------

    def _build_pullback_candidates(
        self,
        bars: pd.DataFrame,
        ctx: AnalysisContext,
        regime: str,
    ) -> list[SignalCandidate]:
        if len(bars) < 5:
            return []

        row = bars.iloc[-1]
        ts = self._bar_timestamp(row, ctx)
        close = float(row.get("close", 0.0))
        low = float(row.get("low", close))
        high = float(row.get("high", close))
        vwap = float(row.get("vwap", close))
        ema20 = float(row.get("ema_20", close))
        ema50 = float(row.get("ema_50", ema20))
        atr = self._safe(row.get("atr"), 20.0)
        recent = bars.iloc[max(0, len(bars) - 8):]
        box_lo = float(recent["low"].min()) if "low" in recent.columns else low
        box_hi = float(recent["high"].max()) if "high" in recent.columns else high
        if box_hi <= box_lo:
            return []
        box_pos = (close - box_lo) / max(box_hi - box_lo, 1e-9)
        delta_3 = float(recent["delta"].tail(3).sum()) if "delta" in recent.columns else 0.0
        candidates: list[SignalCandidate] = []

        # Long pullback
        if (
            regime in {"trend_up", "transition"}
            and ema20 >= ema50
            and close >= vwap
            and box_pos <= 0.45
            and low <= ema20
        ):
            entry = close
            sl = min(box_lo, ema20 - atr * 0.45)
            risk = max(entry - sl, max(atr * 0.4, 2.0))
            sl = entry - risk
            score = 0.62 if regime == "transition" else 0.67
            if delta_3 > 0:
                score += 0.03
            candidates.append(
                self._make(
                    bars=bars,
                    timestamp=ts,
                    direction=Direction.LONG,
                    entry=entry,
                    sl=sl,
                    score=score,
                    reasons=["pullback", "lower_box", "reload_long", "ema_reclaim"],
                    bar_index=len(bars) - 1,
                    source_type="derived_pullback_long",
                    family=CandidateFamily.MTF_CONFLUENCE,
                )
            )

        # Short pullback
        if (
            regime in {"trend_down", "transition"}
            and ema20 <= ema50
            and close <= vwap
            and box_pos >= 0.55
            and high >= ema20
        ):
            entry = close
            sl = max(box_hi, ema20 + atr * 0.45)
            risk = max(sl - entry, max(atr * 0.4, 2.0))
            sl = entry + risk
            score = 0.62 if regime == "transition" else 0.67
            if delta_3 < 0:
                score += 0.03
            candidates.append(
                self._make(
                    bars=bars,
                    timestamp=ts,
                    direction=Direction.SHORT,
                    entry=entry,
                    sl=sl,
                    score=score,
                    reasons=["pullback", "upper_box", "reload_short", "ema_reject"],
                    bar_index=len(bars) - 1,
                    source_type="derived_pullback_short",
                    family=CandidateFamily.MTF_CONFLUENCE,
                )
            )

        return candidates

    # ------------------------------------------------------------------
    # ORB candidates
    # ------------------------------------------------------------------

    def _build_orb_candidates(
        self,
        bars: pd.DataFrame,
        ctx: AnalysisContext,
        regime: str,
    ) -> list[SignalCandidate]:
        if len(bars) < 4:
            return []

        orb_hi = ctx.session_levels.extras.get("orb_high")
        orb_lo = ctx.session_levels.extras.get("orb_low")
        if orb_hi is None or orb_lo is None:
            orb_hi = getattr(ctx.session_levels, "orb_high", None)
            orb_lo = getattr(ctx.session_levels, "orb_low", None)
        if orb_hi is None or orb_lo is None or float(orb_hi) <= float(orb_lo):
            return []
        orb_hi, orb_lo = float(orb_hi), float(orb_lo)

        row = bars.iloc[-1]
        prev = bars.iloc[-2]
        close = float(row.get("close", 0.0))
        high = float(row.get("high", close))
        low = float(row.get("low", close))
        prev_close = float(prev.get("close", close))
        atr = self._safe(row.get("atr"), 20.0)
        ts = self._bar_timestamp(row, ctx)
        candidates: list[SignalCandidate] = []

        # Long ORB breakout
        if (
            high >= orb_hi
            and close > orb_hi
            and prev_close <= orb_hi + atr * 0.1
            and regime in {"trend_up", "transition"}
        ):
            candidates.append(
                self._make(
                    bars=bars,
                    timestamp=ts,
                    direction=Direction.LONG,
                    entry=close,
                    sl=min(orb_lo, close - max(atr * 0.45, 2.0)),
                    score=0.66 if regime == "trend_up" else 0.61,
                    reasons=["orb", "opening_range_breakout", "breakout_retest", "ny_open"],
                    bar_index=len(bars) - 1,
                    source_type="derived_orb_long",
                    family=CandidateFamily.MTF_CONFLUENCE,
                )
            )

        # Short ORB breakdown
        if (
            low <= orb_lo
            and close < orb_lo
            and prev_close >= orb_lo - atr * 0.1
            and regime in {"trend_down", "transition"}
        ):
            candidates.append(
                self._make(
                    bars=bars,
                    timestamp=ts,
                    direction=Direction.SHORT,
                    entry=close,
                    sl=max(orb_hi, close + max(atr * 0.45, 2.0)),
                    score=0.66 if regime == "trend_down" else 0.61,
                    reasons=["orb", "opening_range_breakdown", "breakout_retest", "ny_open"],
                    bar_index=len(bars) - 1,
                    source_type="derived_orb_short",
                    family=CandidateFamily.MTF_CONFLUENCE,
                )
            )

        return candidates

    # ------------------------------------------------------------------
    # Sweep candidates
    # ------------------------------------------------------------------

    def _build_sweep_candidates(
        self,
        bars: pd.DataFrame,
        ctx: AnalysisContext,
        regime: str,
    ) -> list[SignalCandidate]:
        if len(bars) < 6:
            return []

        row = bars.iloc[-1]
        ts = self._bar_timestamp(row, ctx)
        close = float(row.get("close", 0.0))
        low = float(row.get("low", close))
        high = float(row.get("high", close))
        atr = self._safe(row.get("atr"), 20.0)
        lookback = bars.iloc[max(0, len(bars) - 20): len(bars) - 1]
        if lookback.empty:
            return []

        prior_low = float(lookback["low"].min()) if "low" in lookback.columns else low
        prior_high = float(lookback["high"].max()) if "high" in lookback.columns else high

        # Flow data for score enhancement
        recent = bars.iloc[max(0, len(bars) - 4):]
        delta_sum = float(recent["delta"].sum()) if "delta" in recent.columns else 0.0

        candidates: list[SignalCandidate] = []

        # Long sweep (sweep prior low, close back above)
        if low < prior_low and close > prior_low and close > low + atr * 0.15:
            entry = close
            sl = low - atr * 0.2
            risk = max(entry - sl, max(atr * 0.35, 2.0))
            sl = entry - risk

            # Score based on quality
            score = 0.64
            boosts = 0
            reasons = ["sweep", "liquidity_grab", "reclaim"]
            # Flow confirmation: delta turning positive after sweep = buyers stepping in
            if delta_sum > 0:
                score += 0.04
                boosts += 1
                reasons.append("flow_confirmed")
            # With-trend sweep is higher quality
            if regime in {"trend_up", "transition"}:
                score += 0.03
                boosts += 1
                reasons.append("with_trend")
            # Displacement: big reclaim = strong rejection
            if close > low + atr * 0.4:
                score += 0.02
                boosts += 1
                reasons.append("strong_rejection")

            # Quality gate: need at least 2 of 3 confirmations
            # Without this, sweep alone is 24% WR (84 losers in 73 days)
            if boosts < 2:
                return candidates

            candidates.append(
                self._make(
                    bars=bars,
                    timestamp=ts,
                    direction=Direction.LONG,
                    entry=entry,
                    sl=sl,
                    score=score,
                    reasons=reasons,
                    bar_index=len(bars) - 1,
                    source_type="derived_sweep_long",
                )
            )

        # Short sweep (sweep prior high, close back below)
        if high > prior_high and close < prior_high and close < high - atr * 0.15:
            entry = close
            sl = high + atr * 0.2
            risk = max(sl - entry, max(atr * 0.35, 2.0))
            sl = entry + risk

            score = 0.64
            boosts = 0
            reasons = ["sweep", "liquidity_grab", "reclaim"]
            if delta_sum < 0:
                score += 0.04
                boosts += 1
                reasons.append("flow_confirmed")
            if regime in {"trend_down", "transition"}:
                score += 0.03
                boosts += 1
                reasons.append("with_trend")
            if close < high - atr * 0.4:
                score += 0.02
                boosts += 1
                reasons.append("strong_rejection")

            if boosts < 2:
                return candidates

            candidates.append(
                self._make(
                    bars=bars,
                    timestamp=ts,
                    direction=Direction.SHORT,
                    entry=entry,
                    sl=sl,
                    score=score,
                    reasons=reasons,
                    bar_index=len(bars) - 1,
                    source_type="derived_sweep_short",
                )
            )

        return candidates

    # ------------------------------------------------------------------
    # VWAP loss / rejection continuation
    # ------------------------------------------------------------------

    def _build_vwap_loss_candidates(
        self,
        bars: pd.DataFrame,
        ctx: AnalysisContext,
        regime: str,
    ) -> list[SignalCandidate]:
        """Catch continuation after losing VWAP — the big move the old bot missed.

        Logic:
        - Price was above VWAP, now closes below with displacement
        - EMA structure confirms direction  
        - Delta confirms selling/buying pressure
        - This catches the "VWAP rejection → continuation" pattern
        """
        if len(bars) < 6:
            return []

        row = bars.iloc[-1]
        prev = bars.iloc[-2]
        ts = self._bar_timestamp(row, ctx)
        close = float(row.get("close", 0.0))
        vwap = float(row.get("vwap", close))
        high = float(row.get("high", close))
        low = float(row.get("low", close))
        prev_close = float(prev.get("close", close))
        prev_vwap = float(prev.get("vwap", prev_close))
        ema20 = float(row.get("ema_20", close))
        ema50 = float(row.get("ema_50", ema20))
        atr = self._safe(row.get("atr"), 20.0)
        recent = bars.iloc[max(0, len(bars) - 6):]
        delta_sum = float(recent["delta"].sum()) if "delta" in recent.columns else 0.0
        candidates: list[SignalCandidate] = []

        # Displacement = bar range > 0.7 ATR (real move, not noise)
        # Increased from 0.5 to reduce false signals (was 24% WR)
        bar_range = high - low
        displaced = bar_range > atr * 0.7

        # SHORT: Lost VWAP with displacement + bearish structure
        if (
            close < vwap
            and prev_close > prev_vwap  # was above VWAP
            and close < ema20             # below EMA20 too
            and displaced
            and delta_sum < 0             # selling is real
            and regime not in {"trend_up"}  # don't fight strong uptrend
        ):
            entry = close
            sl = max(vwap + atr * 0.15, high)
            risk = max(sl - entry, max(atr * 0.4, 2.0))
            sl = entry + risk

            # Higher score if EMA stack is bearish
            score = 0.68
            if ema20 < ema50:
                score = 0.72
            if regime == "trend_down":
                score += 0.03

            candidates.append(
                self._make(
                    bars=bars,
                    timestamp=ts,
                    direction=Direction.SHORT,
                    entry=entry,
                    sl=sl,
                    score=score,
                    reasons=["vwap_loss", "displacement", "bearish_continuation", "accept_below_vwap"],
                    bar_index=len(bars) - 1,
                    source_type="derived_vwap_loss_short",
                    family=CandidateFamily.MTF_CONFLUENCE,
                )
            )

        # LONG: Reclaimed VWAP with displacement + bullish structure
        if (
            close > vwap
            and prev_close < prev_vwap  # was below VWAP
            and close > ema20             # above EMA20
            and displaced
            and delta_sum > 0             # buying is real
            and regime not in {"trend_down"}
        ):
            entry = close
            sl = min(vwap - atr * 0.15, low)
            risk = max(entry - sl, max(atr * 0.4, 2.0))
            sl = entry - risk

            score = 0.68
            if ema20 > ema50:
                score = 0.72
            if regime == "trend_up":
                score += 0.03

            candidates.append(
                self._make(
                    bars=bars,
                    timestamp=ts,
                    direction=Direction.LONG,
                    entry=entry,
                    sl=sl,
                    score=score,
                    reasons=["vwap_reclaim", "displacement", "bullish_continuation", "accept_above_vwap"],
                    bar_index=len(bars) - 1,
                    source_type="derived_vwap_loss_long",
                    family=CandidateFamily.MTF_CONFLUENCE,
                )
            )

        return candidates

    # ------------------------------------------------------------------
    # Break & Retest (lower high / higher low)
    # ------------------------------------------------------------------

    def _build_break_retest_candidates(
        self,
        bars: pd.DataFrame,
        ctx: AnalysisContext,
        regime: str,
    ) -> list[SignalCandidate]:
        """Catch the break & retest pattern — first lower high or higher low.

        Logic:
        - Find a recent swing high then a confirmed LOWER high → short
        - Find a recent swing low then a confirmed HIGHER low → long 
        - Need the retest to fail (close back in direction of break)
        """
        if len(bars) < 10:
            return []

        row = bars.iloc[-1]
        ts = self._bar_timestamp(row, ctx)
        close = float(row.get("close", 0.0))
        high = float(row.get("high", close))
        low = float(row.get("low", close))
        atr = self._safe(row.get("atr"), 20.0)
        recent = bars.iloc[max(0, len(bars) - 15):]
        delta_3 = float(recent["delta"].tail(3).sum()) if "delta" in recent.columns else 0.0
        candidates: list[SignalCandidate] = []

        # Find swing highs and lows in last 15 bars
        swing_highs = self._find_swing_points(recent, "high", compare="max")
        swing_lows = self._find_swing_points(recent, "low", compare="min")

        # SHORT: lower high pattern
        # Re-enabled V11: with +5pts SL padding → 42% WR, +$4,335
        # Need at least 2 swing highs, second one lower than first
        if (
            len(swing_highs) >= 2
            and swing_highs[-1][1] < swing_highs[-2][1] - atr * 0.1  # clear lower high
            and close < swing_highs[-1][1]  # closing below the lower high
            and high >= swing_highs[-1][1] * 0.999  # tested the area
            and regime not in {"trend_up"}
        ):
            lower_high_price = swing_highs[-1][1]
            entry = close
            sl = lower_high_price + atr * 0.25
            risk = max(sl - entry, max(atr * 0.4, 2.0))
            sl = entry + risk

            score = 0.66
            if delta_3 < 0:
                score += 0.04
            if regime == "trend_down":
                score += 0.03

            candidates.append(
                self._make(
                    bars=bars,
                    timestamp=ts,
                    direction=Direction.SHORT,
                    entry=entry,
                    sl=sl,
                    score=score,
                    reasons=["lower_high", "break_retest", "structure_shift", "rejection"],
                    bar_index=len(bars) - 1,
                    source_type="derived_break_retest_short",
                    family=CandidateFamily.MTF_CONFLUENCE,
                )
            )

        # LONG: higher low pattern
        if (
            len(swing_lows) >= 2
            and swing_lows[-1][1] > swing_lows[-2][1] + atr * 0.1  # clear higher low
            and close > swing_lows[-1][1]  # closing above the higher low
            and low <= swing_lows[-1][1] * 1.001  # tested the area
            and regime not in {"trend_down"}
        ):
            higher_low_price = swing_lows[-1][1]
            entry = close
            sl = higher_low_price - atr * 0.25
            risk = max(entry - sl, max(atr * 0.4, 2.0))
            sl = entry - risk

            score = 0.66
            if delta_3 > 0:
                score += 0.04
            if regime == "trend_up":
                score += 0.03

            candidates.append(
                self._make(
                    bars=bars,
                    timestamp=ts,
                    direction=Direction.LONG,
                    entry=entry,
                    sl=sl,
                    score=score,
                    reasons=["higher_low", "break_retest", "structure_shift", "reclaim"],
                    bar_index=len(bars) - 1,
                    source_type="derived_break_retest_long",
                    family=CandidateFamily.MTF_CONFLUENCE,
                )
            )

        return candidates

    def _find_swing_points(
        self,
        bars: pd.DataFrame,
        col: str,
        compare: str,
    ) -> list[tuple[int, float]]:
        """Find swing highs or lows: local extremes with 1-bar confirmation."""
        if len(bars) < 3 or col not in bars.columns:
            return []
        vals = pd.to_numeric(bars[col], errors="coerce")
        points = []
        for i in range(1, len(vals) - 1):
            v = float(vals.iloc[i])
            prev_v = float(vals.iloc[i - 1])
            next_v = float(vals.iloc[i + 1])
            if compare == "max" and v > prev_v and v >= next_v:
                points.append((i, v))
            elif compare == "min" and v < prev_v and v <= next_v:
                points.append((i, v))
        return points

    # ------------------------------------------------------------------
    # Candidate construction with multi-target levels
    # ------------------------------------------------------------------

    # Retrace entry: zamiast market order na bar close, ustawiamy limit
    # order X pts w "złą" stronę. Dane z event study (3900+ sygnałów)
    # pokazują że 76-86% sygnałów daje retrace >= 3-5 pts przed TP1.
    _RETRACE_OFFSET_ATR = 0.15  # retrace = 15% ATR (~3-5 pts przy ATR 20-30)

    def _make(
        self,
        *,
        bars: pd.DataFrame,
        timestamp: datetime,
        direction: Direction,
        entry: float,
        sl: float,
        score: float,
        reasons: list[str],
        bar_index: int,
        source_type: str,
        family: CandidateFamily = CandidateFamily.COMPOSITE,
    ) -> SignalCandidate:
        # --- Retrace entry adjustment ---
        atr = self._safe(bars.iloc[-1].get("atr") if len(bars) > 0 else None, 20.0)
        retrace = atr * self._RETRACE_OFFSET_ATR
        if direction == Direction.LONG:
            entry = entry - retrace   # kupuj niżej
        else:
            entry = entry + retrace   # sprzedaj wyżej

        # Per-signal SL padding (same as _helpers.py)
        _PADDING = {
            "delta_div": 5.0, "delta_accel": 5.0, "exhaustion": 5.0,
            "ema_bounce": 5.0, "vwap_bounce": 5.0, "fvg": 5.0,
            "break_retest": 2.0, "sweep": 2.0, "reclaim": 2.0,
            "pullback": 3.0, "vwap_loss": 3.0, "trend_cont": 3.0,
            "waterfall": 0.0,
        }
        pad = 0.0
        for tag, p in _PADDING.items():
            if tag in source_type:
                pad = p
                break
        if pad > 0:
            if direction == Direction.LONG:
                sl = sl - pad
            else:
                sl = sl + pad

        risk = abs(entry - sl)
        targets = self._target_levels(bars=bars, direction=direction, entry=entry, risk=risk)
        protections = self._protective_levels(bars=bars, direction=direction, entry=entry, seed_risk=risk)

        # Assign TP levels from targets
        tp1 = targets[0] if targets else (entry + risk * 1.5 if direction == Direction.LONG else entry - risk * 1.5)
        tp2 = targets[1] if len(targets) > 1 else (entry + risk * 2.5 if direction == Direction.LONG else entry - risk * 2.5)
        tp3 = targets[2] if len(targets) > 2 else (entry + risk * 4.0 if direction == Direction.LONG else entry - risk * 4.0)

        # Refine SL from protective levels
        if protections:
            best_sl = protections[0]
            if direction == Direction.LONG and best_sl < entry:
                sl = best_sl
            elif direction == Direction.SHORT and best_sl > entry:
                sl = best_sl

        return SignalCandidate(
            id=f"{source_type}_{bar_index}_{uuid.uuid4().hex[:8]}",
            timestamp=timestamp,
            direction=direction,
            family=family,
            entry_price=round(entry, 2),
            sl_price=round(sl, 2),
            tp1_price=round(tp1, 2),
            tp2_price=round(tp2, 2),
            tp3_price=round(tp3, 2),
            score=round(score, 4),
            reasons=reasons,
            features={
                "bar_index": bar_index,
                "risk": round(risk, 2),
                "source_type": source_type,
                "retrace_offset": round(retrace, 2),
                "entry_type": "limit",
            },
        )

    # ------------------------------------------------------------------
    # Target and protective level discovery
    # ------------------------------------------------------------------

    def _target_levels(self, *, bars: pd.DataFrame, direction: Direction, entry: float, risk: float) -> list[float]:
        recent = bars.tail(min(len(bars), 48))
        min_dist = max(risk * 0.75, 1.0)
        levels: list[float] = []

        if direction == Direction.LONG:
            levels.extend(self._swing_highs(recent, floor=entry + min_dist))
            if "high" in recent.columns:
                hi = float(recent["high"].max())
                if hi >= entry + min_dist:
                    levels.append(hi)
        else:
            levels.extend(self._swing_lows(recent, ceiling=entry - min_dist))
            if "low" in recent.columns:
                lo = float(recent["low"].min())
                if lo <= entry - min_dist:
                    levels.append(lo)

        levels.extend(self._date_extremes(bars, direction))

        unique: list[float] = []
        seen: set[float] = set()
        # Final directional filter: for LONG, keep only levels ABOVE entry; for SHORT, only BELOW entry
        if direction == Direction.LONG:
            levels = [lv for lv in levels if float(lv) > entry]
        else:
            levels = [lv for lv in levels if float(lv) < entry]
        ordered = sorted(levels) if direction == Direction.LONG else sorted(levels, reverse=True)
        for lv in ordered:
            r = round(float(lv), 2)
            if r not in seen:
                seen.add(r)
                unique.append(r)
        return unique

    def _protective_levels(self, *, bars: pd.DataFrame, direction: Direction, entry: float, seed_risk: float) -> list[float]:
        recent = bars.tail(min(len(bars), 48))
        current = bars.iloc[-1]
        atr = self._safe(current.get("atr"), seed_risk)
        min_dist = max(min(seed_risk * 0.5, atr * 0.35), 0.5)
        levels: list[float] = []

        if direction == Direction.LONG:
            levels.extend(self._swing_lows(recent, ceiling=entry - min_dist))
            for col in ("vwap", "ema_20"):
                val = current.get(col)
                if val is not None and not pd.isna(val) and float(val) <= entry - min_dist:
                    levels.append(float(val))
            return sorted({round(float(lv), 2) for lv in levels if float(lv) < entry - min_dist}, reverse=True)
        else:
            levels.extend(self._swing_highs(recent, floor=entry + min_dist))
            for col in ("vwap", "ema_20"):
                val = current.get(col)
                if val is not None and not pd.isna(val) and float(val) >= entry + min_dist:
                    levels.append(float(val))
            return sorted({round(float(lv), 2) for lv in levels if float(lv) > entry + min_dist})

    # ------------------------------------------------------------------
    # Swing detection helpers
    # ------------------------------------------------------------------

    def _swing_highs(self, bars: pd.DataFrame, *, floor: float) -> list[float]:
        if len(bars) < 3 or "high" not in bars.columns:
            return []
        highs = pd.to_numeric(bars["high"], errors="coerce")
        prior = highs.shift(1)
        nxt = highs.shift(-1)
        mask = (highs > prior) & (highs >= nxt) & (highs > floor)
        return [float(v) for v in highs[mask].dropna().tolist()]

    def _swing_lows(self, bars: pd.DataFrame, *, ceiling: float) -> list[float]:
        if len(bars) < 3 or "low" not in bars.columns:
            return []
        lows = pd.to_numeric(bars["low"], errors="coerce")
        prior = lows.shift(1)
        nxt = lows.shift(-1)
        mask = (lows < prior) & (lows <= nxt) & (lows < ceiling)
        return [float(v) for v in lows[mask].dropna().tolist()]

    def _date_extremes(self, bars: pd.DataFrame, direction: Direction) -> list[float]:
        if "date" not in bars.columns or "timestamp" not in bars.columns:
            return []
        frame = bars.copy()
        if "date" not in frame.columns:
            frame["date"] = pd.to_datetime(frame["timestamp"], utc=True).dt.date
        grouped = frame.groupby("date")
        if len(grouped) < 2:
            return []
        history = frame.iloc[:-1]
        if history.empty:
            return []
        if direction == Direction.LONG and "high" in history.columns:
            return [float(v) for v in history.groupby("date")["high"].max().tail(2).tolist()]
        if direction == Direction.SHORT and "low" in history.columns:
            return [float(v) for v in history.groupby("date")["low"].min().tail(2).tolist()]
        return []

    # ------------------------------------------------------------------
    # Utilities
    # ------------------------------------------------------------------

    def _ensure_numeric(self, bars: pd.DataFrame) -> pd.DataFrame:
        df = bars.copy()
        for col in ("open", "high", "low", "close", "vwap", "ema_20", "ema_50", "atr", "delta"):
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors="coerce")
        if "timestamp" in df.columns:
            df["timestamp"] = pd.to_datetime(df["timestamp"], utc=True, errors="coerce")
        if "date" not in df.columns and "timestamp" in df.columns:
            df["date"] = df["timestamp"].dt.date
        return df

    def _bar_timestamp(self, row: pd.Series, ctx: AnalysisContext) -> datetime:
        ts = row.get("timestamp")
        if ts is not None and hasattr(ts, "to_pydatetime"):
            return ts.to_pydatetime()
        return ctx.timestamp

    def _apply_confluence(self, candidates: list[SignalCandidate]) -> list[SignalCandidate]:
        """Boost signals that have confluence from multiple source types at same bar.

        Ported from V4 CompositeScorer._apply_confluence(). When two different
        signal generators fire on the same bar+direction, both get a score boost
        from the CONFLUENCE_BOOST matrix.
        """
        # Group by (bar_index, direction)
        groups: dict[tuple, list[SignalCandidate]] = defaultdict(list)
        for c in candidates:
            bar_idx = int(c.features.get("bar_index", -1) or -1)
            groups[(bar_idx, c.direction.value)].append(c)

        boosted = []
        for key, group in groups.items():
            if len(group) > 1:
                # Multiple signals at same bar + direction → confluence!
                source_types = set()
                for c in group:
                    st = c.features.get("source_type", "")
                    # Normalize source type for lookup (e.g. "derived_sweep_long" → "sweep")
                    for tag in ("sweep", "pullback", "reclaim", "orb", "vwap_loss",
                                "break_retest", "delta_accel", "micro_smc", "fvg", "bos", "choch",
                                "trend_cont", "exhaustion", "waterfall", "delta_div", "vwap_bounce",
                                "ema_bounce"):
                        if tag in st:
                            source_types.add(tag)
                            break

                for c in group:
                    c_type = ""
                    st = c.features.get("source_type", "")
                    for tag in ("sweep", "pullback", "reclaim", "orb", "vwap_loss",
                                "break_retest", "delta_accel", "micro_smc", "fvg", "bos", "choch",
                                "trend_cont", "exhaustion", "waterfall"):
                        if tag in st:
                            c_type = tag
                            break

                    boost = 0.0
                    for other_type in source_types:
                        if other_type != c_type:
                            pair = tuple(sorted([c_type, other_type]))
                            boost += CONFLUENCE_BOOST.get(pair, 0.1)

                    if boost > 0:
                        c.score = min(1.0, c.score + boost)
                        c.reasons.append(f"confluence_{len(source_types)}x")

            boosted.extend(group)

        return boosted

    def _dedupe(self, candidates: list[SignalCandidate]) -> list[SignalCandidate]:
        """Keep best signal per bar per direction per source type family."""
        seen: set[tuple] = set()
        result: list[SignalCandidate] = []
        for c in candidates:
            # Include source_type family in key so different generators
            # on same bar are kept (for confluence) but exact duplicates aren't
            st = c.features.get("source_type", "unknown")
            key = (
                c.direction.value,
                st,
                round(c.entry_price, 2),
                int(c.features.get("bar_index", -1) or -1),
            )
            if key not in seen:
                seen.add(key)
                result.append(c)
        return result

    @staticmethod
    def _safe(value: object, default: float) -> float:
        try:
            v = float(value)  # type: ignore[arg-type]
        except (TypeError, ValueError):
            return default
        return v if v > 0 else default

