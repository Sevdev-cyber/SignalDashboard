"""
VWAP Bounce Signal — ported from Scalper V4
════════════════════════════════════════════
Mean reversion off VWAP bands (±2σ).

- LONG: Price touches −2σ band, rejection candle, delta turning positive
- SHORT: Price touches +2σ band, rejection candle, delta turning negative

Ported from V4 VWAPBounceSignal, adapted to 5-min bars.
The V4 version used 1-second bars; here we adjust thresholds for 5-min.
"""
from __future__ import annotations

import numpy as np
import pandas as pd

from hsb.domain.context import AnalysisContext
from hsb.domain.enums import CandidateFamily, Direction
from hsb.domain.models import SignalCandidate

from hsb.signals._helpers import make_candidate


class VWAPBounceGenerator:
    """VWAP band touch + reversal for mean-reversion entries.

    Ported from V4. On 5-min bars, we use expanding VWAP std bands
    and require rejection candle + delta confirmation.
    """

    def __init__(self, config: dict | None = None) -> None:
        cfg = config or {}
        self.band_sigma = cfg.get("band_sigma", 2.0)
        self.rejection_ratio = cfg.get("rejection_ratio", 0.6)
        self.sl_atr_mult = cfg.get("sl_atr_mult", 1.5)
        self.max_risk_atr = cfg.get("max_risk_atr", 2.0)
        self.max_risk_abs = cfg.get("max_risk_abs", 80.0)

    def generate(self, bars: pd.DataFrame, ctx: AnalysisContext) -> list[SignalCandidate]:
        """Scan bars for VWAP bounce setups.

        Requires: close, high, low, open, vwap, delta, atr
        """
        candidates: list[SignalCandidate] = []
        if len(bars) < 25:
            return candidates

        highs = bars["high"].values.astype(float)
        lows = bars["low"].values.astype(float)
        closes = bars["close"].values.astype(float)
        opens = bars["open"].values.astype(float) if "open" in bars.columns else closes.copy()
        vwap = bars["vwap"].values.astype(float) if "vwap" in bars.columns else closes.copy()
        deltas = bars["delta"].values.astype(float) if "delta" in bars.columns else np.zeros(len(bars))
        atrs = bars["atr"].values.astype(float) if "atr" in bars.columns else np.full(len(bars), 20.0)

        # Compute VWAP bands (expanding std from session start)
        tp = (highs + lows + closes) / 3.0
        sq_diff = (tp - vwap) ** 2

        # Expanding variance (session-based VWAP bands)
        cum_sq = np.cumsum(sq_diff)
        counts = np.arange(1, len(bars) + 1, dtype=float)
        variance = cum_sq / np.maximum(counts, 5)
        std = np.sqrt(variance)

        upper = vwap + self.band_sigma * std
        lower = vwap - self.band_sigma * std

        for i in range(20, len(bars)):
            atr = max(atrs[i], 0.5)
            bar_range = highs[i] - lows[i]
            if bar_range < 0.5:
                continue

            # Band width sanity check (need meaningful bands)
            band_width = upper[i] - lower[i]
            if band_width < atr * 0.5:
                continue

            # ── LONG: Touch lower band ──
            if lows[i] <= lower[i]:
                close_pos = (closes[i] - lows[i]) / bar_range
                is_rejection = close_pos > self.rejection_ratio
                is_delta_turn = deltas[i] > 0

                score = 0.0
                reasons = ["vwap_bounce", "touch_lower_band"]

                if is_rejection:
                    score += 0.30
                    reasons.append("rejection_candle")
                if is_delta_turn:
                    score += 0.20
                    reasons.append("delta_positive")
                # Close back above band
                if closes[i] > lower[i]:
                    score += 0.15
                    reasons.append("close_above_band")
                # Deep touch bonus (price went well below band)
                touch_depth = (lower[i] - lows[i]) / max(atr, 1)
                if touch_depth > 0.3:
                    score += 0.10
                    reasons.append("deep_touch")

                if score >= 0.45:
                    entry = closes[i]
                    sl = lows[i] - atr * 0.15
                    risk = entry - sl
                    if risk <= 0 or risk > atr * self.max_risk_atr or risk > self.max_risk_abs:
                        continue

                    # TP towards VWAP (mean reversion target)
                    cand = make_candidate(
                        bars=bars,
                        ctx=ctx,
                        bar_index=i,
                        direction=Direction.LONG,
                        entry=entry,
                        sl=sl,
                        score=min(1.0, score),
                        reasons=reasons,
                        source_type="derived_vwap_bounce_long",
                        family=CandidateFamily.COMPOSITE,
                        meta={"band": "lower", "band_dist": round(float(lower[i] - lows[i]), 2),
                              "vwap_target": round(float(vwap[i]), 2)},
                    )
                    if cand is not None:
                        candidates.append(cand)

            # ── SHORT: Touch upper band ──
            if highs[i] >= upper[i]:
                close_pos = (closes[i] - lows[i]) / bar_range
                is_rejection = close_pos < (1 - self.rejection_ratio)
                is_delta_turn = deltas[i] < 0

                score = 0.0
                reasons = ["vwap_bounce", "touch_upper_band"]

                if is_rejection:
                    score += 0.30
                    reasons.append("rejection_candle")
                if is_delta_turn:
                    score += 0.20
                    reasons.append("delta_negative")
                if closes[i] < upper[i]:
                    score += 0.15
                    reasons.append("close_below_band")
                touch_depth = (highs[i] - upper[i]) / max(atr, 1)
                if touch_depth > 0.3:
                    score += 0.10
                    reasons.append("deep_touch")

                if score >= 0.45:
                    entry = closes[i]
                    sl = highs[i] + atr * 0.15
                    risk = sl - entry
                    if risk <= 0 or risk > atr * self.max_risk_atr or risk > self.max_risk_abs:
                        continue

                    cand = make_candidate(
                        bars=bars,
                        ctx=ctx,
                        bar_index=i,
                        direction=Direction.SHORT,
                        entry=entry,
                        sl=sl,
                        score=min(1.0, score),
                        reasons=reasons,
                        source_type="derived_vwap_bounce_short",
                        family=CandidateFamily.COMPOSITE,
                        meta={"band": "upper", "band_dist": round(float(highs[i] - upper[i]), 2),
                              "vwap_target": round(float(vwap[i]), 2)},
                    )
                    if cand is not None:
                        candidates.append(cand)

        return candidates
