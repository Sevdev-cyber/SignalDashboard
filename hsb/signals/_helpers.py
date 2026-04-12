"""Shared helper for building SignalCandidate instances from V4-ported signals."""
from __future__ import annotations

import uuid
from datetime import datetime

import pandas as pd

from hsb.domain.context import AnalysisContext
from hsb.domain.enums import CandidateFamily, Direction
from hsb.domain.models import SignalCandidate


def make_candidate(
    *,
    bars: pd.DataFrame,
    ctx: AnalysisContext,
    bar_index: int,
    direction: Direction,
    entry: float,
    sl: float,
    score: float,
    reasons: list[str],
    source_type: str,
    family: CandidateFamily = CandidateFamily.COMPOSITE,
    meta: dict | None = None,
) -> SignalCandidate:
    """Build a SignalCandidate with multi-target levels.

    Uses the same target-finding logic as CompositeGenerator._make()
    but exposed as a standalone function for ported V4 signals.
    """
    import math
    # Guard: if entry/sl are NaN/inf, use entry ± 20 as safe fallback
    # The candidate will have a bad score and get filtered downstream
    if math.isnan(entry) or math.isinf(entry):
        return None
    if math.isnan(sl) or math.isinf(sl):
        if direction == Direction.LONG:
            sl = entry - 20.0
        else:
            sl = entry + 20.0
    # Per-signal SL padding: different signal types need different SL room.
    # Reversal signals get stop-hunted → need wider SL.
    # Momentum signals should have tight SL → if cascade stops, you're wrong.
    # Structure signals need SL at the structure level → small buffer only.
    _PADDING_BY_TYPE = {
        # Reversal/mean-reversion: +5pts (86% quick-SL would-have-won)
        "delta_div": 5.0, "delta_accel": 5.0, "exhaustion": 5.0,
        "ema_bounce": 5.0, "vwap_bounce": 5.0, "fvg": 5.0,
        # Structure: +2pts (only noise buffer)
        "break_retest": 2.0, "sweep": 2.0, "reclaim": 2.0,
        "pullback": 3.0, "vwap_loss": 3.0, "trend_cont": 3.0,
        # Momentum: 0pts (tight SL is correct)
        "waterfall": 0.0,
    }
    sl_padding = 0.0
    for tag, pad in _PADDING_BY_TYPE.items():
        if tag in source_type:
            sl_padding = pad
            break
    if sl_padding > 0:
        if direction == Direction.LONG:
            sl = sl - sl_padding
        else:
            sl = sl + sl_padding

    risk = abs(entry - sl)
    if risk <= 0:
        risk = 20.0
        sl = entry - risk if direction == Direction.LONG else entry + risk

    # Target levels from swing structure
    targets = _target_levels(bars, direction, entry, risk)
    tp1 = targets[0] if targets else (entry + risk * 1.5 if direction == Direction.LONG else entry - risk * 1.5)
    tp2 = targets[1] if len(targets) > 1 else (entry + risk * 2.5 if direction == Direction.LONG else entry - risk * 2.5)
    tp3 = targets[2] if len(targets) > 2 else (entry + risk * 4.0 if direction == Direction.LONG else entry - risk * 4.0)

    # Timestamp
    row = bars.iloc[bar_index] if bar_index < len(bars) else bars.iloc[-1]
    ts = row.get("timestamp")
    if ts is not None and hasattr(ts, "to_pydatetime"):
        timestamp = ts.to_pydatetime()
    else:
        timestamp = ctx.timestamp

    features = {"bar_index": bar_index, "risk": round(risk, 2), "source_type": source_type}
    if meta:
        features.update(meta)

    return SignalCandidate(
        id=f"{source_type}_{bar_index}_{direction.value}_{round(entry, 1)}",
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
        features=features,
    )


def _target_levels(bars: pd.DataFrame, direction: Direction, entry: float, risk: float) -> list[float]:
    """Find target levels from swing structure."""
    recent = bars.tail(min(len(bars), 48))
    min_dist = max(risk * 0.75, 1.0)
    levels: list[float] = []

    if direction == Direction.LONG:
        if "high" in recent.columns:
            highs = pd.to_numeric(recent["high"], errors="coerce")
            for i in range(1, len(highs) - 1):
                v = float(highs.iloc[i])
                if v > float(highs.iloc[i - 1]) and v >= float(highs.iloc[i + 1]) and v > entry + min_dist:
                    levels.append(v)
            hi = float(highs.max())
            if hi >= entry + min_dist:
                levels.append(hi)
    else:
        if "low" in recent.columns:
            lows = pd.to_numeric(recent["low"], errors="coerce")
            for i in range(1, len(lows) - 1):
                v = float(lows.iloc[i])
                if v < float(lows.iloc[i - 1]) and v <= float(lows.iloc[i + 1]) and v < entry - min_dist:
                    levels.append(v)
            lo = float(lows.min())
            if lo <= entry - min_dist:
                levels.append(lo)

    unique: list[float] = []
    seen: set[float] = set()
    ordered = sorted(levels) if direction == Direction.LONG else sorted(levels, reverse=True)
    for lv in ordered:
        r = round(float(lv), 2)
        if r not in seen:
            seen.add(r)
            unique.append(r)
    return unique
