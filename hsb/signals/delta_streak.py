"""Delta Streak Reversal detector.

From PATTERN_DISCOVERIES Tier 1 #1:
- 6× consecutive sell delta bars → LONG reversal: 90% win, +117 pts/30min (N=10)
- 2× consecutive buy delta bars → SHORT reversal: 65.3% (larger sample)

Also from 37K signal study:
- STREAK_REVERSAL_SHORT: 12,147 events, 63% WR (but SL 60%, losing)
- STREAK_REVERSAL_LONG: 1,167 events, 56% WR (marginal)

Key insight: SHORT streaks work poorly (too common, noisy).
LONG reversal after extended SELL streaks works great (rare, high conviction).
We only fire on 5+ consecutive same-direction delta bars (not 2-3).
"""

from __future__ import annotations

import logging

import pandas as pd
import numpy as np

from hsb.domain.enums import Direction
from hsb.domain.models import SignalCandidate

log = logging.getLogger("jajcus.delta_streak")


class DeltaStreakGenerator:
    """Detect delta streak reversals on 5-min bars."""

    def __init__(self, config: dict | None = None):
        cfg = config or {}
        # Minimum consecutive bars with same-sign delta for signal
        self.min_sell_streak = cfg.get("min_sell_streak", 5)  # 5+ sell bars → LONG
        self.min_buy_streak = cfg.get("min_buy_streak", 6)    # 6+ buy bars → SHORT (rarer)
        self.buy_streak_enabled = cfg.get("buy_streak_enabled", True)

    def generate(self, bars: pd.DataFrame, context=None) -> list[SignalCandidate]:
        """Scan for delta streak reversals."""
        if len(bars) < 15 or "delta" not in bars.columns:
            return []

        candidates = []
        delta = bars["delta"].values
        close = bars["close"].values
        high = bars["high"].values
        low = bars["low"].values
        atr_col = bars["atr"].values if "atr" in bars.columns else np.full(len(bars), 20.0)

        # Only check last 3 bars for fresh signals (not entire history)
        for i in range(max(10, len(bars) - 3), len(bars)):
            atr = float(atr_col[i]) if atr_col[i] > 0 else 20.0
            bar_idx = i
            ts = bars.iloc[i].get("datetime") or bars.iloc[i].get("timestamp")
            if ts is None:
                continue
            timestamp = pd.to_datetime(ts)

            # Count consecutive sell delta bars ending at bar i
            sell_streak = 0
            for j in range(i, max(i - 12, -1), -1):
                if delta[j] < 0:
                    sell_streak += 1
                else:
                    break

            # Count consecutive buy delta bars
            buy_streak = 0
            for j in range(i, max(i - 12, -1), -1):
                if delta[j] > 0:
                    buy_streak += 1
                else:
                    break

            # ── LONG reversal after sell streak ──
            if sell_streak >= self.min_sell_streak:
                # Reversal bar: current bar should show buying pressure
                reversal_bar = close[i] > (low[i] + (high[i] - low[i]) * 0.4)

                entry = float(close[i])
                # SL below the streak low
                streak_low = float(np.min(low[max(0, i - sell_streak):i + 1]))
                sl = streak_low - atr * 0.15
                risk = entry - sl
                tp1 = entry + risk * 2.0  # 2:1 RR

                # Score based on streak length
                base_score = 0.60 + min(sell_streak - self.min_sell_streak, 3) * 0.05
                if reversal_bar:
                    base_score += 0.08

                if 0 < risk <= atr * 2.5:
                    candidates.append(SignalCandidate(
                        id=f"derived_streak_rev_long_{bar_idx}_{Direction.LONG.value}_{round(entry, 1)}",
                        timestamp=timestamp,
                        direction=Direction.LONG,
                        family="streak_reversal",
                        entry_price=round(entry, 2),
                        sl_price=round(sl, 2),
                        tp1_price=round(tp1, 2),
                        tp2_price=round(entry + risk * 4.0, 2),
                        score=min(base_score, 0.85),
                        signal_name="STREAK_REV_LONG",
                        reasons=[f"sell_streak_{sell_streak}",
                                 "reversal_bar" if reversal_bar else "no_reversal"],
                        confluences=set(),
                        features={
                            "bar_index": bar_idx,
                            "risk": round(risk, 2),
                            "source_type": "derived_streak_rev_long",
                            "streak_length": sell_streak,
                            "retrace_offset": round(atr * 0.15, 2),
                            "entry_type": "limit",
                        },
                    ))
                    log.debug("STREAK_REV_LONG: %d sell bars, score=%.2f, entry=%.2f",
                              sell_streak, base_score, entry)

            # ── SHORT reversal after buy streak ──
            if self.buy_streak_enabled and buy_streak >= self.min_buy_streak:
                reversal_bar = close[i] < (high[i] - (high[i] - low[i]) * 0.4)

                entry = float(close[i])
                streak_high = float(np.max(high[max(0, i - buy_streak):i + 1]))
                sl = streak_high + atr * 0.15
                risk = sl - entry
                tp1 = entry - risk * 2.0

                base_score = 0.55 + min(buy_streak - self.min_buy_streak, 3) * 0.05
                if reversal_bar:
                    base_score += 0.08

                if 0 < risk <= atr * 2.5:
                    candidates.append(SignalCandidate(
                        id=f"derived_streak_rev_short_{bar_idx}_{Direction.SHORT.value}_{round(entry, 1)}",
                        timestamp=timestamp,
                        direction=Direction.SHORT,
                        family="streak_reversal",
                        entry_price=round(entry, 2),
                        sl_price=round(sl, 2),
                        tp1_price=round(tp1, 2),
                        tp2_price=round(entry - risk * 4.0, 2),
                        score=min(base_score, 0.80),
                        signal_name="STREAK_REV_SHORT",
                        reasons=[f"buy_streak_{buy_streak}",
                                 "reversal_bar" if reversal_bar else "no_reversal"],
                        confluences=set(),
                        features={
                            "bar_index": bar_idx,
                            "risk": round(risk, 2),
                            "source_type": "derived_streak_rev_short",
                            "streak_length": buy_streak,
                            "retrace_offset": round(atr * 0.15, 2),
                            "entry_type": "limit",
                        },
                    ))

        return candidates
