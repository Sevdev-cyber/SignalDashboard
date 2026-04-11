"""
EMA Bounce Signal
═════════════════
Detects price touching EMA20 and bouncing with delta confirmation.

Pattern:
  - Price consolidates near EMA20 (within 0.3 * ATR)
  - Bar shows rejection (long wick in the direction of bounce)
  - Delta confirms direction

This catches the "EMA tight → explosion" pattern that other
generators miss — 15 moves, 965pts identified in gap analysis.
"""
from __future__ import annotations

import numpy as np
import pandas as pd

from hsb.domain.context import AnalysisContext
from hsb.domain.enums import CandidateFamily, Direction
from hsb.domain.models import SignalCandidate

from hsb.signals._helpers import make_candidate


class EMABounceGenerator:
    """Detect EMA20 touch + bounce with delta confirmation.

    Catches trending moves that originate from EMA20 – the most
    common dynamic support/resistance level on 5-min charts.
    """

    def __init__(self, config: dict | None = None) -> None:
        cfg = config or {}
        self.ema_touch_threshold = cfg.get("ema_touch_atr_ratio", 0.35)
        self.sl_atr_mult = cfg.get("sl_atr_mult", 1.5)
        self.max_risk_atr = cfg.get("max_risk_atr", 2.0)
        self.max_risk_abs = cfg.get("max_risk_abs", 80.0)

    def generate(self, bars: pd.DataFrame, ctx: AnalysisContext) -> list[SignalCandidate]:
        """Scan bars for EMA bounce setups.

        Requires: close, high, low, open, ema_20, ema_50, delta, atr, vwap
        """
        candidates: list[SignalCandidate] = []
        if len(bars) < 12:
            return candidates

        highs = bars["high"].values.astype(float)
        lows = bars["low"].values.astype(float)
        closes = bars["close"].values.astype(float)
        opens = bars["open"].values.astype(float) if "open" in bars.columns else closes.copy()
        ema20 = bars["ema_20"].values.astype(float) if "ema_20" in bars.columns else None
        ema50 = bars["ema_50"].values.astype(float) if "ema_50" in bars.columns else None
        atrs = bars["atr"].values.astype(float) if "atr" in bars.columns else np.full(len(bars), 20.0)
        deltas = bars["delta"].values.astype(float) if "delta" in bars.columns else np.zeros(len(bars))
        vwap = bars["vwap"].values.astype(float) if "vwap" in bars.columns else closes.copy()

        if ema20 is None:
            return candidates

        for i in range(10, len(bars)):
            atr = max(atrs[i], 0.5)
            bar_range = highs[i] - lows[i]
            if bar_range < atr * 0.2:
                continue

            # How close is price to EMA20?
            ema_dist = abs(closes[i] - ema20[i])
            touch_threshold = atr * self.ema_touch_threshold

            # Was the bar touching EMA20? (low touched for long, high touched for short)
            low_touched_ema = abs(lows[i] - ema20[i]) < touch_threshold
            high_touched_ema = abs(highs[i] - ema20[i]) < touch_threshold

            # ── LONG bounce off EMA20 (EMA20 as support) ──
            if low_touched_ema and closes[i] > ema20[i]:
                # Rejection: close in upper portion of bar
                close_pos = (closes[i] - lows[i]) / bar_range
                if close_pos < 0.55:
                    continue

                score = 0.0
                reasons = ["ema_bounce", "ema20_support"]

                #  Rejection candle (close well above low)
                if close_pos > 0.65:
                    score += 0.25
                    reasons.append("rejection_candle")

                # Delta confirms buyers
                if deltas[i] > 0:
                    score += 0.15
                    reasons.append("delta_positive")

                # EMA20 > EMA50 (uptrend structure)
                if ema50 is not None and ema20[i] > ema50[i]:
                    score += 0.15
                    reasons.append("ema_bullish")

                # Price above VWAP (trend alignment)
                if closes[i] > vwap[i]:
                    score += 0.10
                    reasons.append("above_vwap")

                # Multiple touches (EMA20 held as support in prior bars)
                prior_touches = sum(1 for j in range(max(0, i-5), i)
                                   if abs(lows[j] - ema20[j]) < touch_threshold)
                if prior_touches >= 1:
                    score += 0.10
                    reasons.append(f"multi_touch={prior_touches+1}")

                if score >= 0.45:
                    entry = closes[i]
                    sl = min(lows[i], ema20[i]) - atr * 0.2
                    risk = entry - sl
                    if risk <= 0 or risk > atr * self.max_risk_atr or risk > self.max_risk_abs:
                        continue

                    cand = make_candidate(
                        bars=bars, ctx=ctx, bar_index=i,
                        direction=Direction.LONG, entry=entry, sl=sl,
                        score=min(1.0, score), reasons=reasons,
                        source_type="derived_ema_bounce_long",
                        family=CandidateFamily.COMPOSITE,
                        meta={"ema20": round(float(ema20[i]), 2),
                              "ema_dist": round(float(ema_dist), 2)},
                    )
                    if cand is not None:
                        candidates.append(cand)

            # ── SHORT bounce off EMA20 (EMA20 as resistance) ──
            if high_touched_ema and closes[i] < ema20[i]:
                close_pos = (closes[i] - lows[i]) / bar_range
                if close_pos > 0.45:
                    continue

                score = 0.0
                reasons = ["ema_bounce", "ema20_resistance"]

                if close_pos < 0.35:
                    score += 0.25
                    reasons.append("rejection_candle")

                if deltas[i] < 0:
                    score += 0.15
                    reasons.append("delta_negative")

                if ema50 is not None and ema20[i] < ema50[i]:
                    score += 0.15
                    reasons.append("ema_bearish")

                if closes[i] < vwap[i]:
                    score += 0.10
                    reasons.append("below_vwap")

                prior_touches = sum(1 for j in range(max(0, i-5), i)
                                   if abs(highs[j] - ema20[j]) < touch_threshold)
                if prior_touches >= 1:
                    score += 0.10
                    reasons.append(f"multi_touch={prior_touches+1}")

                if score >= 0.45:
                    entry = closes[i]
                    sl = max(highs[i], ema20[i]) + atr * 0.2
                    risk = sl - entry
                    if risk <= 0 or risk > atr * self.max_risk_atr or risk > self.max_risk_abs:
                        continue

                    cand = make_candidate(
                        bars=bars, ctx=ctx, bar_index=i,
                        direction=Direction.SHORT, entry=entry, sl=sl,
                        score=min(1.0, score), reasons=reasons,
                        source_type="derived_ema_bounce_short",
                        family=CandidateFamily.COMPOSITE,
                        meta={"ema20": round(float(ema20[i]), 2),
                              "ema_dist": round(float(ema_dist), 2)},
                    )
                    if cand is not None:
                        candidates.append(cand)

        return candidates
