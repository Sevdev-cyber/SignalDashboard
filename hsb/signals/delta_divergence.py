"""
Delta Divergence Signal — ported from Scalper V4
═════════════════════════════════════════════════
Price makes new high/low but cumulative delta diverges.
Classic order flow reversal signal.

Example:
  - Price makes higher high → cum_delta makes lower high → bearish divergence
  - Price makes lower low  → cum_delta makes higher low  → bullish divergence

Ported 1:1 from V4 DeltaDivergenceSignal, adapted to 5-min bars.
"""
from __future__ import annotations

import numpy as np
import pandas as pd

from hsb.domain.context import AnalysisContext
from hsb.domain.enums import CandidateFamily, Direction
from hsb.domain.models import SignalCandidate

from hsb.signals._helpers import make_candidate


class DeltaDivergenceGenerator:
    """Detect delta divergence on 5-min bars.

    Ported 1:1 from V4. Price diverging from cumulative delta is a
    classic order flow reversal signal.
    """

    def __init__(self, config: dict | None = None) -> None:
        cfg = config or {}
        self.lookbacks = cfg.get("lookback_bars_list", [15, 25])
        self.min_div_bars = cfg.get("min_divergence_bars", 3)
        self.sl_atr_mult = cfg.get("sl_atr_mult", 1.5)
        self.max_risk_atr = cfg.get("max_risk_atr", 2.0)
        self.max_risk_abs = cfg.get("max_risk_abs", 100.0)

    def generate(self, bars: pd.DataFrame, ctx: AnalysisContext) -> list[SignalCandidate]:
        """Scan bars for delta divergence + delta exhaustion reversal.

        Two modes:
        1. Classic divergence: price HH + delta LH (or inverse)
        2. Delta exhaustion: cum_delta extreme + price reversal bar
        """
        candidates: list[SignalCandidate] = []
        min_lb = min(self.lookbacks)
        if len(bars) < min_lb + 5:
            return candidates

        highs = bars["high"].values.astype(float)
        lows = bars["low"].values.astype(float)
        closes = bars["close"].values.astype(float)
        opens = bars["open"].values.astype(float) if "open" in bars.columns else closes.copy()
        atrs = bars["atr"].values.astype(float) if "atr" in bars.columns else np.full(len(bars), 20.0)
        deltas = bars["delta"].values.astype(float) if "delta" in bars.columns else np.zeros(len(bars))

        cum_delta = np.cumsum(deltas)
        seen = set()  # avoid duplicate (bar_index, direction)

        for lookback in self.lookbacks:
            if len(bars) < lookback + 5:
                continue

            for i in range(lookback, len(bars)):
                atr = max(atrs[i], 0.5)
                w_start = i - lookback
                w_highs = highs[w_start:i + 1]
                w_lows = lows[w_start:i + 1]
                w_cd = cum_delta[w_start:i + 1]

                # ── Bearish divergence: price higher high, cum_delta lower high ──
                price_hh = self._is_higher_high(w_highs)
                delta_lh = self._is_lower_high(w_cd)

                if price_hh and delta_lh and (i, "short") not in seen:
                    entry = closes[i]
                    sl = float(np.max(highs[max(0, i - 5):i + 1])) + atr * 0.2
                    risk = sl - entry
                    if risk <= 0 or risk > atr * self.max_risk_atr or risk > self.max_risk_abs:
                        pass  # skip but don't break loop
                    else:
                        div_strength = self._div_strength(w_highs, w_cd, "bear")
                        score = 0.55 + div_strength * 0.15
                        if deltas[i] < 0:
                            score += 0.05
                        bar_range = highs[i] - lows[i]
                        if bar_range > 0 and (closes[i] - lows[i]) / bar_range < 0.4:
                            score += 0.05

                        reasons = ["delta_divergence", "price_HH", "delta_LH",
                                   f"div={div_strength:.2f}", f"lb={lookback}"]
                        cand = make_candidate(
                            bars=bars, ctx=ctx, bar_index=i,
                            direction=Direction.SHORT, entry=entry, sl=sl,
                            score=min(1.0, score), reasons=reasons,
                            source_type="derived_delta_div_short",
                            family=CandidateFamily.COMPOSITE,
                            meta={"divergence": "bearish", "div_strength": round(div_strength, 3)},
                        )
                        if cand is not None:
                            candidates.append(cand)
                            seen.add((i, "short"))

                # ── Bullish divergence: price lower low, cum_delta higher low ──
                price_ll = self._is_lower_low(w_lows)
                delta_hl = self._is_higher_low(w_cd)

                if price_ll and delta_hl and (i, "long") not in seen:
                    entry = closes[i]
                    sl = float(np.min(lows[max(0, i - 5):i + 1])) - atr * 0.2
                    risk = entry - sl
                    if risk <= 0 or risk > atr * self.max_risk_atr or risk > self.max_risk_abs:
                        pass
                    else:
                        div_strength = self._div_strength(w_lows, w_cd, "bull")
                        score = 0.55 + div_strength * 0.15
                        if deltas[i] > 0:
                            score += 0.05
                        bar_range = highs[i] - lows[i]
                        if bar_range > 0 and (closes[i] - lows[i]) / bar_range > 0.6:
                            score += 0.05

                        reasons = ["delta_divergence", "price_LL", "delta_HL",
                                   f"div={div_strength:.2f}", f"lb={lookback}"]
                        cand = make_candidate(
                            bars=bars, ctx=ctx, bar_index=i,
                            direction=Direction.LONG, entry=entry, sl=sl,
                            score=min(1.0, score), reasons=reasons,
                            source_type="derived_delta_div_long",
                            family=CandidateFamily.COMPOSITE,
                            meta={"divergence": "bullish", "div_strength": round(div_strength, 3)},
                        )
                        if cand is not None:
                            candidates.append(cand)
                            seen.add((i, "long"))

        # ── Delta exhaustion reversal ──
        # When cumulative delta is extreme (>2.5 std) and price reverses
        candidates.extend(self._detect_exhaustion_reversal(
            bars, ctx, closes, opens, highs, lows, atrs, deltas, cum_delta, seen
        ))

        return candidates

    def _detect_exhaustion_reversal(
        self, bars, ctx, closes, opens, highs, lows, atrs, deltas, cum_delta, seen
    ) -> list[SignalCandidate]:
        """Detect extreme delta exhaustion + price reversal.

        When cumulative delta is massively one-sided but the current bar
        reverses direction → exhaustion reversal. This catches the big
        turns that standard divergence misses.
        """
        candidates = []
        if len(bars) < 15:
            return candidates

        for i in range(15, len(bars)):
            atr = max(atrs[i], 0.5)
            cd_window = cum_delta[max(0, i - 30):i + 1]
            if len(cd_window) < 10:
                continue

            cd_mean = np.mean(cd_window[:-1])
            cd_std = max(np.std(cd_window[:-1]), 1)
            cd_z = (cum_delta[i] - cd_mean) / cd_std

            # Current bar is a reversal bar?
            bar_range = highs[i] - lows[i]
            if bar_range < atr * 0.3:
                continue

            close_pos = (closes[i] - lows[i]) / bar_range

            # Bearish exhaustion: massive positive delta but bearish reversal bar
            if cd_z > 2.5 and close_pos < 0.35 and deltas[i] < 0 and (i, "short") not in seen:
                entry = closes[i]
                sl = highs[i] + atr * 0.15
                risk = sl - entry
                if 0 < risk <= min(atr * self.max_risk_atr, self.max_risk_abs):
                    score = min(0.75, 0.55 + abs(cd_z) * 0.04)
                    reasons = ["delta_exhaustion", "bearish_reversal",
                               f"cd_z={cd_z:.1f}", "extreme_delta"]
                    cand = make_candidate(
                        bars=bars, ctx=ctx, bar_index=i,
                        direction=Direction.SHORT, entry=entry, sl=sl,
                        score=score, reasons=reasons,
                        source_type="derived_delta_div_short",
                        family=CandidateFamily.COMPOSITE,
                        meta={"exhaustion": True, "cd_z": round(cd_z, 2)},
                    )
                    if cand is not None:
                        candidates.append(cand)
                        seen.add((i, "short"))

            # Bullish exhaustion: massive negative delta but bullish reversal bar
            if cd_z < -2.5 and close_pos > 0.65 and deltas[i] > 0 and (i, "long") not in seen:
                entry = closes[i]
                sl = lows[i] - atr * 0.15
                risk = entry - sl
                if 0 < risk <= min(atr * self.max_risk_atr, self.max_risk_abs):
                    score = min(0.75, 0.55 + abs(cd_z) * 0.04)
                    reasons = ["delta_exhaustion", "bullish_reversal",
                               f"cd_z={cd_z:.1f}", "extreme_delta"]
                    cand = make_candidate(
                        bars=bars, ctx=ctx, bar_index=i,
                        direction=Direction.LONG, entry=entry, sl=sl,
                        score=score, reasons=reasons,
                        source_type="derived_delta_div_long",
                        family=CandidateFamily.COMPOSITE,
                        meta={"exhaustion": True, "cd_z": round(cd_z, 2)},
                    )
                    if cand is not None:
                        candidates.append(cand)
                        seen.add((i, "long"))

        return candidates

    # ------------------------------------------------------------------
    # Half-window comparison (exact V4 logic)
    # ------------------------------------------------------------------

    def _is_higher_high(self, arr: np.ndarray) -> bool:
        """Recent half has higher high than first half."""
        mid = len(arr) // 2
        if mid < 2:
            return False
        first_max = arr[:mid].max()
        second_max = arr[mid:].max()
        return second_max > first_max and arr[-1] >= second_max * 0.998

    def _is_lower_high(self, arr: np.ndarray) -> bool:
        mid = len(arr) // 2
        if mid < 2:
            return False
        return arr[mid:].max() < arr[:mid].max()

    def _is_lower_low(self, arr: np.ndarray) -> bool:
        mid = len(arr) // 2
        if mid < 2:
            return False
        first_min = arr[:mid].min()
        second_min = arr[mid:].min()
        return second_min < first_min and arr[-1] <= second_min * 1.002

    def _is_higher_low(self, arr: np.ndarray) -> bool:
        mid = len(arr) // 2
        if mid < 2:
            return False
        return arr[mid:].min() > arr[:mid].min()

    def _div_strength(self, price_arr: np.ndarray, delta_arr: np.ndarray,
                       direction: str) -> float:
        """Quantify divergence strength 0-1.

        Measures how strongly price and delta are diverging.
        """
        mid = len(price_arr) // 2
        if mid < 2:
            return 0.0

        if direction == "bear":
            # Price going up, delta going down
            price_change = (price_arr[mid:].max() - price_arr[:mid].max()) / max(abs(price_arr[:mid].max()), 1)
            delta_change = (delta_arr[:mid].max() - delta_arr[mid:].max()) / max(abs(delta_arr[:mid].max()), 1)
        else:
            # Price going down, delta going up
            price_change = (price_arr[:mid].min() - price_arr[mid:].min()) / max(abs(price_arr[:mid].min()), 1)
            delta_change = (delta_arr[mid:].min() - delta_arr[:mid].min()) / max(abs(delta_arr[:mid].min()), 1)

        # Both should be positive for a valid divergence
        strength = min(abs(price_change) * 100, 1.0) * min(abs(delta_change), 1.0)
        return min(1.0, strength)
