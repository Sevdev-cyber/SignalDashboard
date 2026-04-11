"""
Trend Continuation Signal
═════════════════════════
Catches moves when price is far above/below VWAP but structure is intact.

Problem it solves: existing generators require proximity to VWAP or EMA,
so they miss continuation moves in strong trends where price is extended
but still grinding in one direction with structure intact.

Logic:
- Price > VWAP + 1.5×ATR (extended but trending)
- Close > EMA20 (still supported by near-term average)
- No lower_high (long) / higher_low (short) in last 5 bars → structure intact
- Delta sum > 0 in last 3 bars → flow confirming direction
- Score is deliberately lower (0.55-0.65) since entry is extended
"""
from __future__ import annotations

import numpy as np
import pandas as pd

from hsb.domain.context import AnalysisContext
from hsb.domain.enums import CandidateFamily, Direction
from hsb.domain.models import SignalCandidate

from hsb.signals._helpers import make_candidate


class TrendContinuationGenerator:
    """Catch continuation moves in strong trends (far from VWAP)."""

    def __init__(self, config: dict | None = None) -> None:
        cfg = config or {}
        self.vwap_distance_atr = cfg.get("vwap_distance_atr", 1.5)  # min distance from VWAP in ATR
        self.structure_lookback = cfg.get("structure_lookback", 5)
        self.sl_atr_mult = cfg.get("sl_atr_mult", 1.2)

    def generate(self, bars: pd.DataFrame, ctx: AnalysisContext) -> list[SignalCandidate]:
        """Scan for trend continuation setups in extended markets.

        Requires: close, high, low, vwap, ema_20, atr, delta
        """
        candidates = []
        if len(bars) < 12:
            return candidates

        closes = bars["close"].values.astype(float)
        highs = bars["high"].values.astype(float)
        lows = bars["low"].values.astype(float)
        vwaps = bars["vwap"].values.astype(float) if "vwap" in bars.columns else closes.copy()
        ema20s = bars["ema_20"].values.astype(float) if "ema_20" in bars.columns else closes.copy()
        atrs = bars["atr"].values.astype(float) if "atr" in bars.columns else np.full(len(bars), 20.0)
        deltas = bars["delta"].values.astype(float) if "delta" in bars.columns else np.zeros(len(bars))

        i = len(bars) - 1
        close = closes[i]
        vwap = vwaps[i]
        ema20 = ema20s[i]
        atr = max(atrs[i], 1.0)
        vwap_dist = (close - vwap) / atr  # positive = above VWAP

        # Delta sum over last 3 bars
        delta_3 = float(np.sum(deltas[max(0, i - 2) : i + 1]))

        # ----- LONG continuation: far ABOVE VWAP, structure intact -----
        if (
            vwap_dist > self.vwap_distance_atr
            and close > ema20  # still above EMA support
            and delta_3 > 0  # flow confirming longs
        ):
            # Check structure intact: no lower high in last N bars
            lb = min(self.structure_lookback, i)
            recent_highs = highs[i - lb : i + 1]
            structure_intact = True
            for j in range(1, len(recent_highs)):
                # If a high is significantly lower than the previous high → lower_high
                if recent_highs[j] < recent_highs[j - 1] - atr * 0.15:
                    # Check if it's a confirmed lower high (next bar also lower)
                    if j + 1 < len(recent_highs) and recent_highs[j + 1] < recent_highs[j - 1] - atr * 0.1:
                        structure_intact = False
                        break

            if structure_intact:
                entry = close
                # SL at recent swing low or EMA20
                recent_lows = lows[max(0, i - lb) : i + 1]
                sl = max(float(np.min(recent_lows)) - 0.25, ema20 - atr * 0.3)
                risk = entry - sl
                if risk > 0 and risk <= atr * self.sl_atr_mult:
                    # Score based on trend strength — but capped lower since extended
                    score = 0.55
                    reasons = ["trend_continuation", f"vwap_dist={vwap_dist:.1f}ATR"]

                    # Boost for strong delta
                    if delta_3 > np.mean(np.abs(deltas[max(0, i - 10) : i])) * 1.5:
                        score += 0.05
                        reasons.append("strong_flow")

                    # Boost for tight consolidation (low range = coiling)
                    recent_ranges = highs[max(0, i - 3) : i + 1] - lows[max(0, i - 3) : i + 1]
                    avg_range = float(np.mean(recent_ranges))
                    if avg_range < atr * 0.6:
                        score += 0.05
                        reasons.append("tight_consolidation")

                    candidates.append(make_candidate(
                        bars=bars, ctx=ctx, bar_index=i,
                        direction=Direction.LONG,
                        entry=entry, sl=sl, score=score,
                        reasons=reasons,
                        source_type="derived_trend_cont_long",
                        family=CandidateFamily.MTF_CONFLUENCE,
                        meta={"vwap_dist_atr": float(vwap_dist), "delta_3": float(delta_3)},
                    ))

        # ----- SHORT continuation: far BELOW VWAP, structure intact -----
        if (
            vwap_dist < -self.vwap_distance_atr
            and close < ema20  # still below EMA
            and delta_3 < 0  # flow confirming shorts
        ):
            lb = min(self.structure_lookback, i)
            recent_lows = lows[i - lb : i + 1]
            structure_intact = True
            for j in range(1, len(recent_lows)):
                if recent_lows[j] > recent_lows[j - 1] + atr * 0.15:
                    if j + 1 < len(recent_lows) and recent_lows[j + 1] > recent_lows[j - 1] + atr * 0.1:
                        structure_intact = False
                        break

            if structure_intact:
                entry = close
                recent_highs = highs[max(0, i - lb) : i + 1]
                sl = min(float(np.max(recent_highs)) + 0.25, ema20 + atr * 0.3)
                risk = sl - entry
                if risk > 0 and risk <= atr * self.sl_atr_mult:
                    score = 0.55
                    reasons = ["trend_continuation", f"vwap_dist={abs(vwap_dist):.1f}ATR"]

                    if abs(delta_3) > np.mean(np.abs(deltas[max(0, i - 10) : i])) * 1.5:
                        score += 0.05
                        reasons.append("strong_flow")

                    recent_ranges = highs[max(0, i - 3) : i + 1] - lows[max(0, i - 3) : i + 1]
                    avg_range = float(np.mean(recent_ranges))
                    if avg_range < atr * 0.6:
                        score += 0.05
                        reasons.append("tight_consolidation")

                    candidates.append(make_candidate(
                        bars=bars, ctx=ctx, bar_index=i,
                        direction=Direction.SHORT,
                        entry=entry, sl=sl, score=score,
                        reasons=reasons,
                        source_type="derived_trend_cont_short",
                        family=CandidateFamily.MTF_CONFLUENCE,
                        meta={"vwap_dist_atr": float(abs(vwap_dist)), "delta_3": float(delta_3)},
                    ))

        return candidates
