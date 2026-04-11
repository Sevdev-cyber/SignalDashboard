"""
Momentum Exhaustion Signal
══════════════════════════
Detects exhaustion at the end of a strong move — when rally/selloff is
losing steam and a reversal is likely.

Problem it solves: after a 200+ pt explosion, price often pulls back 50-80pts.
No existing generator catches this because there's no swing break yet.

Logic (from Trading Kompendium - "exhaustion" regime):
- 3+ consecutive bars of SHRINKING range (each bar smaller than previous)
- Delta weakening (abs value decreasing on each push)
- Price far from VWAP (> 2×ATR distance)
- Direction = AGAINST the prior move

V4 had MOMENTUM_EXHAUSTION in its SignalType enum but never implemented it.
This is the first full implementation.
"""
from __future__ import annotations

import numpy as np
import pandas as pd

from hsb.domain.context import AnalysisContext
from hsb.domain.enums import CandidateFamily, Direction
from hsb.domain.models import SignalCandidate

from hsb.signals._helpers import make_candidate


class ExhaustionGenerator:
    """Detect momentum exhaustion for fade/reversal entries."""

    def __init__(self, config: dict | None = None) -> None:
        cfg = config or {}
        self.min_shrink_bars = cfg.get("min_shrink_bars", 3)
        self.vwap_min_dist_atr = cfg.get("vwap_min_dist_atr", 1.5)
        self.sl_atr_mult = cfg.get("sl_atr_mult", 1.2)

    def generate(self, bars: pd.DataFrame, ctx: AnalysisContext) -> list[SignalCandidate]:
        """Scan for exhaustion patterns.

        Requires: close, high, low, vwap, atr, delta
        """
        candidates = []
        min_bars = self.min_shrink_bars + 3
        if len(bars) < min_bars:
            return candidates

        closes = bars["close"].values.astype(float)
        highs = bars["high"].values.astype(float)
        lows = bars["low"].values.astype(float)
        vwaps = bars["vwap"].values.astype(float) if "vwap" in bars.columns else closes.copy()
        atrs = bars["atr"].values.astype(float) if "atr" in bars.columns else np.full(len(bars), 20.0)
        deltas = bars["delta"].values.astype(float) if "delta" in bars.columns else np.zeros(len(bars))

        i = len(bars) - 1
        close = closes[i]
        vwap = vwaps[i]
        atr = max(atrs[i], 1.0)
        vwap_dist = (close - vwap) / atr  # positive = above, negative = below

        # Need to be extended from VWAP
        if abs(vwap_dist) < self.vwap_min_dist_atr:
            return candidates

        # Check for shrinking ranges (each bar's range smaller than previous)
        n = self.min_shrink_bars
        ranges = highs[i - n + 1 : i + 1] - lows[i - n + 1 : i + 1]
        shrinking_ranges = all(ranges[j] <= ranges[j - 1] for j in range(1, len(ranges)))
        if not shrinking_ranges:
            return candidates

        # Check for weakening delta (absolute value decreasing)
        recent_deltas = deltas[i - n + 1 : i + 1]
        abs_deltas = np.abs(recent_deltas)
        weakening_delta = all(abs_deltas[j] <= abs_deltas[j - 1] * 1.1 for j in range(1, len(abs_deltas)))
        # Allow 10% tolerance since delta can be noisy

        # Determine prior move direction from VWAP position
        # If price is way above VWAP → prior move was UP → exhaustion = short
        # If price is way below VWAP → prior move was DOWN → exhaustion = long

        if vwap_dist > self.vwap_min_dist_atr:
            # Exhaustion of UP move → SHORT entry
            # Confirm: delta is turning negative or weakening
            last_delta_negative = deltas[i] < 0 or weakening_delta

            if last_delta_negative:
                entry = close
                # SL above recent high + buffer
                recent_high = float(np.max(highs[max(0, i - n) : i + 1]))
                sl = recent_high + atr * 0.2
                risk = sl - entry
                if risk > 0 and risk <= atr * self.sl_atr_mult:
                    score = 0.57
                    reasons = ["exhaustion", "shrinking_ranges", f"vwap_dist={vwap_dist:.1f}ATR"]

                    if weakening_delta:
                        score += 0.04
                        reasons.append("weakening_delta")
                    if deltas[i] < 0:
                        score += 0.04
                        reasons.append("delta_turned_negative")

                    # Extra: check if prior move was very large (>100pts in 5 bars)
                    prior_move = close - float(np.min(lows[max(0, i - 8) : i + 1]))
                    if prior_move > atr * 3:
                        score += 0.03
                        reasons.append(f"after_big_move_{prior_move:.0f}pts")

                    candidates.append(make_candidate(
                        bars=bars, ctx=ctx, bar_index=i,
                        direction=Direction.SHORT,
                        entry=entry, sl=sl, score=min(1.0, score),
                        reasons=reasons,
                        source_type="derived_exhaustion_short",
                        family=CandidateFamily.COMPOSITE,
                        meta={"vwap_dist_atr": float(vwap_dist), "range_shrink": float(ranges[-1] / ranges[0])},
                    ))

        elif vwap_dist < -self.vwap_min_dist_atr:
            # Exhaustion of DOWN move → LONG entry
            last_delta_positive = deltas[i] > 0 or weakening_delta

            if last_delta_positive:
                entry = close
                recent_low = float(np.min(lows[max(0, i - n) : i + 1]))
                sl = recent_low - atr * 0.2
                risk = entry - sl
                if risk > 0 and risk <= atr * self.sl_atr_mult:
                    score = 0.57
                    reasons = ["exhaustion", "shrinking_ranges", f"vwap_dist={abs(vwap_dist):.1f}ATR"]

                    if weakening_delta:
                        score += 0.04
                        reasons.append("weakening_delta")
                    if deltas[i] > 0:
                        score += 0.04
                        reasons.append("delta_turned_positive")

                    prior_move = float(np.max(highs[max(0, i - 8) : i + 1])) - close
                    if prior_move > atr * 3:
                        score += 0.03
                        reasons.append(f"after_big_move_{prior_move:.0f}pts")

                    candidates.append(make_candidate(
                        bars=bars, ctx=ctx, bar_index=i,
                        direction=Direction.LONG,
                        entry=entry, sl=sl, score=min(1.0, score),
                        reasons=reasons,
                        source_type="derived_exhaustion_long",
                        family=CandidateFamily.COMPOSITE,
                        meta={"vwap_dist_atr": float(abs(vwap_dist)), "range_shrink": float(ranges[-1] / ranges[0])},
                    ))

        return candidates
