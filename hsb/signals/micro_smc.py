"""
Micro SMC Signal — ported from Scalper V4
══════════════════════════════════════════
Adapted from Sacred Forest SMCDetector but on 5-minute bars.
Detects micro structure: BOS, CHOCH, FVG fill, OB retest.

This bridges the Sacred Forest SMC intelligence into the V2 framework.
We detect the same patterns but on bar data — giving us structural entries
with tight stops.

Patterns:
- BOS (Break of Structure) — continuation signal in trend direction
- CHOCH (Change of Character) — reversal signal against prior trend
- FVG (Fair Value Gap) fill — rebalance entry when price returns to gap
"""
from __future__ import annotations

import numpy as np
import pandas as pd

from hsb.domain.context import AnalysisContext
from hsb.domain.enums import CandidateFamily, Direction
from hsb.domain.models import SignalCandidate

from hsb.signals._helpers import make_candidate


class MicroSMCGenerator:
    """Micro-timeframe SMC pattern detection.

    Ported 1:1 from V4 MicroSMCSignal with full BOS/CHOCH and FVG
    detection. All logic is preserved.
    """

    def __init__(self, config: dict | None = None) -> None:
        cfg = config or {}
        self.swing_lookback = cfg.get("swing_lookback", 3)
        self.fvg_min_gap_ticks = cfg.get("fvg_min_gap_ticks", 2)
        self.fvg_min_gap = self.fvg_min_gap_ticks * 0.25
        self.use_bos = cfg.get("use_bos", True)
        self.use_fvg = cfg.get("use_fvg", True)
        self.sl_atr_mult = cfg.get("sl_atr_mult", 1.5)
        self.max_risk_atr = cfg.get("max_risk_atr", 2.0)
        self.max_risk_abs = cfg.get("max_risk_abs", 100.0)  # absolute cap (pts)

    def generate(self, bars: pd.DataFrame, ctx: AnalysisContext) -> list[SignalCandidate]:
        """Scan bars for SMC patterns: BOS, CHOCH, FVG fill.

        Requires: close, high, low, atr, delta (optional)
        """
        candidates = []
        if len(bars) < 20:
            return candidates

        # Find swing points
        swing_highs, swing_lows = self._find_swings(bars)

        # Detect BOS/CHOCH
        if self.use_bos:
            candidates.extend(self._detect_bos_choch(bars, ctx, swing_highs, swing_lows))

        # Detect FVG fills
        if self.use_fvg:
            candidates.extend(self._detect_fvg_fills(bars, ctx))

        return candidates

    # ------------------------------------------------------------------
    # Swing detection (exact V4 logic)
    # ------------------------------------------------------------------

    def _find_swings(self, bars: pd.DataFrame) -> tuple[list, list]:
        """Find swing highs and lows on bars."""
        highs = bars["high"].values.astype(float)
        lows = bars["low"].values.astype(float)
        lb = self.swing_lookback

        swing_highs = []  # (index, price)
        swing_lows = []

        for i in range(lb, len(bars) - lb):
            # Swing high: highest in window
            window_highs = highs[i - lb : i + lb + 1]
            if highs[i] == window_highs.max() and highs[i] > highs[i - 1] and highs[i] > highs[i + 1]:
                swing_highs.append((i, highs[i]))

            # Swing low
            window_lows = lows[i - lb : i + lb + 1]
            if lows[i] == window_lows.min() and lows[i] < lows[i - 1] and lows[i] < lows[i + 1]:
                swing_lows.append((i, lows[i]))

        return swing_highs, swing_lows

    # ------------------------------------------------------------------
    # BOS / CHOCH detection (exact V4 logic)
    # ------------------------------------------------------------------

    def _detect_bos_choch(
        self,
        bars: pd.DataFrame,
        ctx: AnalysisContext,
        swing_highs: list,
        swing_lows: list,
    ) -> list[SignalCandidate]:
        """Detect Break of Structure (BOS) and Change of Character (CHOCH).

        BOS: Continuation — price breaks a swing in the trend direction
        CHOCH: Reversal — price breaks a swing AGAINST the trend
        """
        candidates = []
        closes = bars["close"].values.astype(float)
        highs = bars["high"].values.astype(float)
        lows = bars["low"].values.astype(float)
        atrs = bars["atr"].values.astype(float) if "atr" in bars.columns else np.full(len(bars), 20.0)
        deltas = bars["delta"].values.astype(float) if "delta" in bars.columns else np.zeros(len(bars))

        # Track trend from swings
        trend = "neutral"  # "up", "down", "neutral"

        for i in range(10, len(bars)):
            atr = max(atrs[i], 0.5)

            # Check for bullish BOS (break above recent swing high)
            recent_sh = [sh for sh in swing_highs if sh[0] < i and sh[0] > i - 30]
            if recent_sh:
                last_sh_idx, last_sh_price = recent_sh[-1]
                if closes[i] > last_sh_price and closes[i - 1] <= last_sh_price:
                    # BOS bull confirmed
                    is_choch = trend == "down"
                    pattern = "choch_bull" if is_choch else "bos_bull"

                    # Find nearest swing low for SL (exact V4 logic: use last 3 swing lows)
                    recent_sl_levels = [sl for sl in swing_lows if sl[0] < i and sl[0] > i - 20]
                    if recent_sl_levels:
                        sl_price = min(s[1] for s in recent_sl_levels[-3:]) - 0.25
                    else:
                        sl_price = lows[i] - atr

                    entry = closes[i]
                    risk = entry - sl_price
                    if risk <= 0 or risk > atr * self.max_risk_atr or risk > self.max_risk_abs:
                        trend = "up"
                        continue

                    # Score: CHOCH higher than BOS (reversal = more informative)
                    score = 0.6 if is_choch else 0.5
                    reasons = [pattern, f"swing_broken={last_sh_price:.2f}"]
                    if deltas[i] > 0:
                        score += 0.15
                        reasons.append("delta_confirm")
                    # Displacement check (V2 addition): strong move = better BOS
                    bar_range = highs[i] - lows[i]
                    if bar_range > atr * 0.6:
                        score += 0.05
                        reasons.append("displacement")

                    candidates.append(make_candidate(
                        bars=bars,
                        ctx=ctx,
                        bar_index=i,
                        direction=Direction.LONG,
                        entry=entry,
                        sl=sl_price,
                        score=min(1.0, score),
                        reasons=reasons,
                        source_type=f"derived_{pattern}_long",
                        family=CandidateFamily.COMPOSITE,
                        meta={"pattern": pattern, "swing_broken": float(last_sh_price)},
                    ))
                    trend = "up"

            # Check for bearish BOS (break below recent swing low)
            recent_sl = [sl for sl in swing_lows if sl[0] < i and sl[0] > i - 30]
            if recent_sl:
                last_sl_idx, last_sl_price = recent_sl[-1]
                if closes[i] < last_sl_price and closes[i - 1] >= last_sl_price:
                    is_choch = trend == "up"
                    pattern = "choch_bear" if is_choch else "bos_bear"

                    recent_sh_levels = [sh for sh in swing_highs if sh[0] < i and sh[0] > i - 20]
                    if recent_sh_levels:
                        sl_price = max(s[1] for s in recent_sh_levels[-3:]) + 0.25
                    else:
                        sl_price = highs[i] + atr

                    entry = closes[i]
                    risk = sl_price - entry
                    if risk <= 0 or risk > atr * self.max_risk_atr or risk > self.max_risk_abs:
                        trend = "down"
                        continue

                    score = 0.6 if is_choch else 0.5
                    reasons = [pattern, f"swing_broken={last_sl_price:.2f}"]
                    if deltas[i] < 0:
                        score += 0.15
                        reasons.append("delta_confirm")
                    bar_range = highs[i] - lows[i]
                    if bar_range > atr * 0.6:
                        score += 0.05
                        reasons.append("displacement")

                    candidates.append(make_candidate(
                        bars=bars,
                        ctx=ctx,
                        bar_index=i,
                        direction=Direction.SHORT,
                        entry=entry,
                        sl=sl_price,
                        score=min(1.0, score),
                        reasons=reasons,
                        source_type=f"derived_{pattern}_short",
                        family=CandidateFamily.COMPOSITE,
                        meta={"pattern": pattern, "swing_broken": float(last_sl_price)},
                    ))
                    trend = "down"

        return candidates

    # ------------------------------------------------------------------
    # FVG (Fair Value Gap) detection and fill (exact V4 logic)
    # ------------------------------------------------------------------

    def _detect_fvg_fills(self, bars: pd.DataFrame, ctx: AnalysisContext) -> list[SignalCandidate]:
        """Detect FVG (Fair Value Gap) creation and fill — entry on fill.

        Bull FVG: bar[i-2].high < bar[i].low → gap filled when price returns
        Bear FVG: bar[i-2].low > bar[i].high → gap filled when price returns
        """
        candidates = []
        highs = bars["high"].values.astype(float)
        lows = bars["low"].values.astype(float)
        closes = bars["close"].values.astype(float)
        atrs = bars["atr"].values.astype(float) if "atr" in bars.columns else np.full(len(bars), 20.0)

        # Track open FVGs: (type, top, bottom, created_idx)
        open_fvgs: list[tuple[str, float, float, int]] = []

        for i in range(2, len(bars)):
            atr = max(atrs[i], 0.5)

            # Detect new FVGs
            # Bull FVG: gap between bar[i-2].high and bar[i].low
            if lows[i] > highs[i - 2] + self.fvg_min_gap:
                open_fvgs.append(("bull", lows[i], highs[i - 2], i))

            # Bear FVG
            if highs[i] < lows[i - 2] - self.fvg_min_gap:
                open_fvgs.append(("bear", lows[i - 2], highs[i], i))

            # Check fills of existing FVGs
            new_fvgs = []
            for fvg_type, top, bottom, created_idx in open_fvgs:
                age = i - created_idx
                if age > 50:  # FVG expired
                    continue

                if fvg_type == "bull" and lows[i] <= top:
                    # Bull FVG being filled → long entry (buy the fill)
                    entry = closes[i]
                    sl = bottom - 0.5
                    risk = entry - sl
                    if risk > 0 and risk <= atr * self.max_risk_atr:
                        gap_size = top - bottom
                        candidates.append(make_candidate(
                            bars=bars,
                            ctx=ctx,
                            bar_index=i,
                            direction=Direction.LONG,
                            entry=entry,
                            sl=sl,
                            score=0.55,
                            reasons=["fvg_bull_fill", f"gap={gap_size:.2f}", f"age={age}bars"],
                            source_type="derived_fvg_fill_long",
                            family=CandidateFamily.COMPOSITE,
                            meta={"fvg_type": "bull", "gap_size": float(gap_size), "age": age},
                        ))
                    continue  # FVG consumed

                if fvg_type == "bear" and highs[i] >= bottom:
                    # Bear FVG being filled → short entry
                    entry = closes[i]
                    sl = top + 0.5
                    risk = sl - entry
                    if risk > 0 and risk <= atr * self.max_risk_atr:
                        gap_size = top - bottom
                        candidates.append(make_candidate(
                            bars=bars,
                            ctx=ctx,
                            bar_index=i,
                            direction=Direction.SHORT,
                            entry=entry,
                            sl=sl,
                            score=0.55,
                            reasons=["fvg_bear_fill", f"gap={gap_size:.2f}", f"age={age}bars"],
                            source_type="derived_fvg_fill_short",
                            family=CandidateFamily.COMPOSITE,
                            meta={"fvg_type": "bear", "gap_size": float(gap_size), "age": age},
                        ))
                    continue  # FVG consumed

                new_fvgs.append((fvg_type, top, bottom, created_idx))

            open_fvgs = new_fvgs

        return candidates
