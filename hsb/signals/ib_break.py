"""Initial Balance Break & Retest detector.

From 37K signal study:
- IB_BREAK_SHORT: 71.2% WR, +5.81 pts/trade (N=2805) — best high-volume signal
- IB_RETEST_SHORT: 100% WR (N=4, small sample but stellar)
- IB_BREAK_LONG: 65% WR but SL 66% — DISABLED by default (trap signal)

IB = first 30 minutes of RTH (9:30-10:00 ET). High/Low of that period define the range.
Breakout below IB with delta confirmation = strong SHORT.
Retest of broken level after breakout = higher conviction entry.
"""

from __future__ import annotations

import logging
from datetime import datetime
from typing import Optional

import pandas as pd

from hsb.domain.enums import Direction
from hsb.domain.models import SignalCandidate

log = logging.getLogger("jajcus.ib_break")


class IBBreakGenerator:
    """Detect Initial Balance breakouts and retests on 5-min bars."""

    def __init__(self, config: dict | None = None):
        cfg = config or {}
        self.ib_minutes = cfg.get("ib_minutes", 30)  # first 30 min of RTH
        self.min_ib_range_pts = cfg.get("min_ib_range", 5.0)  # minimum IB range
        self.max_ib_range_pts = cfg.get("max_ib_range", 80.0)  # skip if IB too wide
        self.long_enabled = cfg.get("long_enabled", False)  # disabled: 34% net WR
        self.sl_ib_fraction = cfg.get("sl_ib_fraction", 0.25)  # SL at 25% of IB range

    def generate(self, bars: pd.DataFrame, context=None) -> list[SignalCandidate]:
        """Scan bars for IB break/retest patterns."""
        if len(bars) < 10:
            return []

        candidates = []

        # Detect IB period
        ib = self._find_ib(bars)
        if ib is None:
            return []

        ib_high, ib_low, ib_range, ib_end_idx = ib

        if ib_range < self.min_ib_range_pts or ib_range > self.max_ib_range_pts:
            return []

        # Scan bars AFTER IB formation for breakouts
        broken_up = False
        broken_down = False
        atr = float(bars.iloc[-1].get("atr", 20)) if "atr" in bars.columns else 20.0

        for i in range(ib_end_idx + 1, len(bars)):
            bar = bars.iloc[i]
            close = float(bar["close"])
            high = float(bar["high"])
            low = float(bar["low"])
            delta = float(bar.get("delta", 0))
            bar_idx = i
            ts = bar.get("datetime") or bar.get("timestamp")
            if ts is None:
                continue
            timestamp = pd.to_datetime(ts)

            # ── BREAKOUT DOWN ──
            if close < ib_low and not broken_up and not broken_down:
                broken_down = True
                if delta < 0:  # delta confirms selling
                    entry = close
                    sl = ib_low + self.sl_ib_fraction * ib_range
                    tp1 = entry - 0.5 * ib_range  # target: 50% extension
                    risk = sl - entry
                    if risk > 0 and risk <= atr * 2:
                        candidates.append(SignalCandidate(
                            id=f"derived_ib_break_short_{bar_idx}_{Direction.SHORT.value}_{round(entry, 1)}",
                            timestamp=timestamp,
                            direction=Direction.SHORT,
                            family="ib_break",
                            entry_price=round(entry, 2),
                            sl_price=round(sl, 2),
                            tp1_price=round(tp1, 2),
                            tp2_price=round(entry - ib_range, 2),
                            score=0.72,  # 71.2% WR from study
                            signal_name="IB_BREAK_SHORT",
                            reasons=["ib_breakout", "delta_negative"],
                            confluences=set(),
                            features={
                                "bar_index": bar_idx,
                                "risk": round(risk, 2),
                                "source_type": "derived_ib_break_short",
                                "ib_high": round(ib_high, 2),
                                "ib_low": round(ib_low, 2),
                                "ib_range": round(ib_range, 2),
                                "retrace_offset": round(atr * 0.15, 2),
                                "entry_type": "limit",
                            },
                        ))

            # ── BREAKOUT UP (disabled by default — trap signal) ──
            elif close > ib_high and not broken_down and not broken_up:
                broken_up = True
                if self.long_enabled and delta > 0:
                    entry = close
                    sl = ib_high - self.sl_ib_fraction * ib_range
                    tp1 = entry + 0.5 * ib_range
                    risk = entry - sl
                    if risk > 0 and risk <= atr * 2:
                        candidates.append(SignalCandidate(
                            id=f"derived_ib_break_long_{bar_idx}_{Direction.LONG.value}_{round(entry, 1)}",
                            timestamp=timestamp,
                            direction=Direction.LONG,
                            family="ib_break",
                            entry_price=round(entry, 2),
                            sl_price=round(sl, 2),
                            tp1_price=round(tp1, 2),
                            tp2_price=round(entry + ib_range, 2),
                            score=0.55,  # penalized: 34% net WR
                            signal_name="IB_BREAK_LONG",
                            reasons=["ib_breakout", "delta_positive"],
                            confluences=set(),
                            features={
                                "bar_index": bar_idx,
                                "risk": round(risk, 2),
                                "source_type": "derived_ib_break_long",
                                "ib_high": round(ib_high, 2),
                                "ib_low": round(ib_low, 2),
                                "ib_range": round(ib_range, 2),
                                "retrace_offset": round(atr * 0.15, 2),
                                "entry_type": "limit",
                            },
                        ))

            # ── RETEST after breakout ──
            if broken_down and not broken_up:
                # Price pulled back to IB low (retest zone: within 2pts)
                if abs(close - ib_low) <= 2.0 and close < ib_low + 1.0:
                    if delta < 0:  # sellers still in control
                        entry = close
                        sl = ib_low + 2.0  # tight SL above IB low
                        tp1 = entry - 0.5 * ib_range
                        risk = sl - entry
                        if risk > 0 and risk <= atr * 1.5:
                            candidates.append(SignalCandidate(
                                id=f"derived_ib_retest_short_{bar_idx}_{Direction.SHORT.value}_{round(entry, 1)}",
                                timestamp=timestamp,
                                direction=Direction.SHORT,
                                family="ib_retest",
                                entry_price=round(entry, 2),
                                sl_price=round(sl, 2),
                                tp1_price=round(tp1, 2),
                                tp2_price=round(entry - ib_range, 2),
                                score=0.80,  # highest conviction
                                signal_name="IB_RETEST_SHORT",
                                reasons=["ib_retest", "delta_negative"],
                                confluences=set(),
                                features={
                                    "bar_index": bar_idx,
                                    "risk": round(risk, 2),
                                    "source_type": "derived_ib_retest_short",
                                    "retrace_offset": round(atr * 0.15, 2),
                                    "entry_type": "limit",
                                },
                            ))

            if broken_up and not broken_down:
                if abs(close - ib_high) <= 2.0 and close > ib_high - 1.0:
                    if delta > 0 and self.long_enabled:
                        entry = close
                        sl = ib_high - 2.0
                        tp1 = entry + 0.5 * ib_range
                        risk = entry - sl
                        if risk > 0 and risk <= atr * 1.5:
                            candidates.append(SignalCandidate(
                                id=f"derived_ib_retest_long_{bar_idx}_{Direction.LONG.value}_{round(entry, 1)}",
                                timestamp=timestamp,
                                direction=Direction.LONG,
                                family="ib_retest",
                                entry_price=round(entry, 2),
                                sl_price=round(sl, 2),
                                tp1_price=round(tp1, 2),
                                tp2_price=round(entry + ib_range, 2),
                                score=0.75,
                                signal_name="IB_RETEST_LONG",
                                reasons=["ib_retest", "delta_positive"],
                                confluences=set(),
                                features={
                                    "bar_index": bar_idx,
                                    "risk": round(risk, 2),
                                    "source_type": "derived_ib_retest_long",
                                    "retrace_offset": round(atr * 0.15, 2),
                                    "entry_type": "limit",
                                },
                            ))

        return candidates

    def _find_ib(self, bars: pd.DataFrame) -> Optional[tuple]:
        """Find Initial Balance (first 30min of RTH).

        Returns (ib_high, ib_low, ib_range, end_idx) or None.
        IB = 9:30-10:00 ET = first 6 bars on 5-min chart.
        """
        if "datetime" not in bars.columns and "timestamp" not in bars.columns:
            return None

        dt_col = "datetime" if "datetime" in bars.columns else "timestamp"
        times = pd.to_datetime(bars[dt_col])

        # Find bars in 9:30-10:00 window (IB period)
        # NT8 bar timestamps are CLOSE times, so 9:35 bar covers 9:30-9:35
        ib_mask = []
        ib_high = -float("inf")
        ib_low = float("inf")
        ib_end_idx = None

        for i, t in enumerate(times):
            hour = t.hour
            minute = t.minute
            # Bars closing at 9:35, 9:40, 9:45, 9:50, 9:55, 10:00
            if hour == 9 and minute >= 35:
                ib_high = max(ib_high, float(bars.iloc[i]["high"]))
                ib_low = min(ib_low, float(bars.iloc[i]["low"]))
                ib_end_idx = i
            elif hour == 10 and minute == 0:
                ib_high = max(ib_high, float(bars.iloc[i]["high"]))
                ib_low = min(ib_low, float(bars.iloc[i]["low"]))
                ib_end_idx = i

        if ib_end_idx is None or ib_high == -float("inf"):
            # Fallback: use first 6 bars as IB proxy
            if len(bars) < 8:
                return None
            ib_end_idx = 5
            for i in range(6):
                ib_high = max(ib_high, float(bars.iloc[i]["high"]))
                ib_low = min(ib_low, float(bars.iloc[i]["low"]))

        ib_range = ib_high - ib_low
        if ib_range <= 0:
            return None

        return (ib_high, ib_low, ib_range, ib_end_idx)
