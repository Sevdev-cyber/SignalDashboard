"""
Waterfall / Momentum Cascade Signal
════════════════════════════════════
Detects sustained one-directional price movement without meaningful
pullback — "waterfall" selloffs or "melt-up" rallies.

Problem it solves: in a strong crash (like Feb 3, -782pts), price
falls bar after bar without pullback. No existing generator catches
this because:
- break_retest waits for a pullback that never comes
- BOS waits for swing confirmation that's too slow
- delta_accel fires on spikes, not on sustained pressure

Logic:
- N consecutive bearish/bullish closes (streak ≥ min_streak)
- Cumulative delta strongly confirms direction over the streak
- Price is below EMA20 (shorts) / above EMA20 (longs) — momentum confirmed
- Entry on current close, SL at recent swing against direction
- Score increases with streak length and delta magnitude
"""
from __future__ import annotations

import numpy as np
import pandas as pd

from hsb.domain.context import AnalysisContext
from hsb.domain.enums import CandidateFamily, Direction
from hsb.domain.models import SignalCandidate

from hsb.signals._helpers import make_candidate


class WaterfallGenerator:
    """Detect sustained momentum cascades (waterfall selloff / melt-up rally)."""

    def __init__(self, config: dict | None = None) -> None:
        cfg = config or {}
        self.min_streak = cfg.get("min_streak", 3)          # min consecutive same-direction closes
        self.min_cum_delta_mult = cfg.get("min_cum_delta_mult", 1.5)  # cum delta vs avg
        self.sl_atr_mult = cfg.get("sl_atr_mult", 1.5)
        self.cooldown_bars = cfg.get("cooldown_bars", 3)     # don't fire again within N bars

    def generate(self, bars: pd.DataFrame, ctx: AnalysisContext) -> list[SignalCandidate]:
        """Scan for waterfall/cascade patterns.

        Requires: open, close, high, low, delta, ema_20, atr
        """
        candidates = []
        if len(bars) < self.min_streak + 5:
            return candidates

        opens = bars["open"].values.astype(float)
        closes = bars["close"].values.astype(float)
        highs = bars["high"].values.astype(float)
        lows = bars["low"].values.astype(float)
        deltas = bars["delta"].values.astype(float) if "delta" in bars.columns else np.zeros(len(bars))
        ema20s = bars["ema_20"].values.astype(float) if "ema_20" in bars.columns else closes.copy()
        atrs = bars["atr"].values.astype(float) if "atr" in bars.columns else np.full(len(bars), 20.0)

        i = len(bars) - 1
        close = closes[i]
        atr = max(atrs[i], 1.0)
        ema20 = ema20s[i]

        # Count consecutive bearish/bullish closes
        bear_streak = 0
        bull_streak = 0
        for j in range(i, max(0, i - 15), -1):
            if closes[j] < opens[j]:
                if bull_streak > 0:
                    break
                bear_streak += 1
            elif closes[j] > opens[j]:
                if bear_streak > 0:
                    break
                bull_streak += 1
            else:
                break

        # ─── BEARISH WATERFALL ───
        # DISABLED: PF=0.85, 5W/7L = -$73 on 126d. Momentum SL correctly
        # tight (0pts padding) but signal itself is unreliable.
        if False and bear_streak >= self.min_streak and close < ema20:
            streak_start = i - bear_streak + 1

            # Cumulative delta over streak (should be strongly negative)
            streak_deltas = deltas[streak_start:i + 1]
            cum_delta = float(np.sum(streak_deltas))
            avg_bar_delta = float(np.mean(np.abs(deltas[max(0, i - 20):i]))) if i > 5 else 1.0

            # Delta must confirm: cumulative delta negative and significant
            if cum_delta < 0 and abs(cum_delta) > avg_bar_delta * self.min_cum_delta_mult:
                # Total move during streak
                streak_move = float(opens[streak_start] - closes[i])
                if streak_move < atr * 0.5:
                    return candidates  # too small

                entry = close
                # SL: use only last 2 bars' high (not entire streak — too far in a crash)
                # In a waterfall, the streak high is way above and risk would be huge
                recent_sl_window = max(2, min(3, bear_streak))
                recent_highs = highs[max(streak_start, i - recent_sl_window + 1):i + 1]
                sl = float(np.max(recent_highs)) + atr * 0.15
                risk = sl - entry
                # Use actual bar range as volatility proxy (ATR lags in crash days)
                actual_vol = float(np.mean(highs[max(0,i-3):i+1] - lows[max(0,i-3):i+1]))
                max_risk = max(atr * self.sl_atr_mult, actual_vol * 1.5)
                if risk <= 0 or risk > max_risk:
                    return candidates

                # Score scales with streak length and delta magnitude
                streak_score = min(0.15, (bear_streak - self.min_streak) * 0.05)
                delta_score = min(0.15, abs(cum_delta) / max(avg_bar_delta * 10, 1) * 0.15)
                score = 0.55 + streak_score + delta_score

                reasons = [
                    "waterfall_short",
                    f"streak={bear_streak}",
                    f"cum_delta={cum_delta:.0f}",
                    f"move={streak_move:.0f}pts",
                ]

                # Extra: acceleration check — are bars getting BIGGER?
                streak_ranges = highs[streak_start:i + 1] - lows[streak_start:i + 1]
                if len(streak_ranges) >= 3:
                    if streak_ranges[-1] > streak_ranges[-2]:
                        score += 0.05
                        reasons.append("accelerating")

                # Check if each bar's low is lower than previous (pure waterfall)
                pure = all(lows[streak_start + j + 1] < lows[streak_start + j]
                           for j in range(min(bear_streak - 1, len(lows) - streak_start - 1)))
                if pure:
                    score += 0.05
                    reasons.append("pure_cascade")

                candidates.append(make_candidate(
                    bars=bars, ctx=ctx, bar_index=i,
                    direction=Direction.SHORT,
                    entry=entry, sl=sl, score=min(1.0, score),
                    reasons=reasons,
                    source_type="derived_waterfall_short",
                    family=CandidateFamily.COMPOSITE,
                    meta={
                        "streak": bear_streak,
                        "cum_delta": float(cum_delta),
                        "streak_move": float(streak_move),
                    },
                ))

        # ─── BULLISH MELT-UP ───
        # DISABLED: PF=0.47, -$338 on 126d. Even with 0pts padding (correct
        # for momentum), signal is unreliable for long direction.
        if False and bull_streak >= self.min_streak and close > ema20:
            streak_start = i - bull_streak + 1

            streak_deltas = deltas[streak_start:i + 1]
            cum_delta = float(np.sum(streak_deltas))
            avg_bar_delta = float(np.mean(np.abs(deltas[max(0, i - 20):i]))) if i > 5 else 1.0

            if cum_delta > 0 and abs(cum_delta) > avg_bar_delta * self.min_cum_delta_mult:
                streak_move = float(closes[i] - opens[streak_start])
                if streak_move < atr * 0.5:
                    return candidates

                entry = close
                recent_sl_window = max(2, min(3, bull_streak))
                recent_lows = lows[max(streak_start, i - recent_sl_window + 1):i + 1]
                sl = float(np.min(recent_lows)) - atr * 0.15
                risk = entry - sl
                actual_vol = float(np.mean(highs[max(0,i-3):i+1] - lows[max(0,i-3):i+1]))
                max_risk = max(atr * self.sl_atr_mult, actual_vol * 1.5)
                if risk <= 0 or risk > max_risk:
                    return candidates

                streak_score = min(0.15, (bull_streak - self.min_streak) * 0.05)
                delta_score = min(0.15, abs(cum_delta) / max(avg_bar_delta * 10, 1) * 0.15)
                score = 0.55 + streak_score + delta_score

                reasons = [
                    "waterfall_long",
                    f"streak={bull_streak}",
                    f"cum_delta={cum_delta:.0f}",
                    f"move={streak_move:.0f}pts",
                ]

                streak_ranges = highs[streak_start:i + 1] - lows[streak_start:i + 1]
                if len(streak_ranges) >= 3:
                    if streak_ranges[-1] > streak_ranges[-2]:
                        score += 0.05
                        reasons.append("accelerating")

                pure = all(highs[streak_start + j + 1] > highs[streak_start + j]
                           for j in range(min(bull_streak - 1, len(highs) - streak_start - 1)))
                if pure:
                    score += 0.05
                    reasons.append("pure_cascade")

                candidates.append(make_candidate(
                    bars=bars, ctx=ctx, bar_index=i,
                    direction=Direction.LONG,
                    entry=entry, sl=sl, score=min(1.0, score),
                    reasons=reasons,
                    source_type="derived_waterfall_long",
                    family=CandidateFamily.COMPOSITE,
                    meta={
                        "streak": bull_streak,
                        "cum_delta": float(cum_delta),
                        "streak_move": float(streak_move),
                    },
                ))

        return candidates
