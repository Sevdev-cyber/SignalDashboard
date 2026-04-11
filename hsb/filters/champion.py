"""Champion filter — quality gate for signal candidates.

Ported from V1 champion_filter.py with typed context access.
"""

from __future__ import annotations

import logging

import pandas as pd

from hsb.domain.context import AnalysisContext
from hsb.domain.enums import Direction
from hsb.domain.models import SignalCandidate

log = logging.getLogger(__name__)


class ChampionFilter:
    """Filters candidates based on score, RR, risk, displacement, and structure."""

    def __init__(
        self,
        *,
        min_score: float = 0.45,
        min_rr: float = 1.3,
        max_risk_points: float | None = None,
        adaptive_risk: bool = True,
        adaptive_risk_mult: float = 1.5,
        adaptive_risk_cap: float = 75.0,
        require_displacement: bool = True,
        displacement_lookback: int = 5,
        displacement_atr_pct: float = 0.30,
        block_countertrend: bool = True,
    ) -> None:
        self.min_score = min_score
        self.min_rr = min_rr
        self.max_risk_points = max_risk_points
        self.adaptive_risk = adaptive_risk
        self.adaptive_risk_mult = adaptive_risk_mult
        self.adaptive_risk_cap = adaptive_risk_cap
        self.require_displacement = require_displacement
        self.displacement_lookback = displacement_lookback
        self.displacement_atr_pct = displacement_atr_pct
        self.block_countertrend = block_countertrend

    def filter(
        self,
        candidates: list[SignalCandidate],
        context: AnalysisContext,
    ) -> list[SignalCandidate]:
        passed: list[SignalCandidate] = []
        bars = context.bar_data.bars_df

        for c in candidates:
            reason = self._check(c, context, bars)
            if reason is not None:
                log.debug("Filtered %s: %s", c.id, reason)
                continue
            passed.append(c)

        return passed

    def _check(
        self,
        c: SignalCandidate,
        ctx: AnalysisContext,
        bars: pd.DataFrame,
    ) -> str | None:
        # Score gate
        if c.score < self.min_score:
            return f"score {c.score:.2f} < {self.min_score}"

        # Risk/reward
        risk = abs(c.entry_price - c.sl_price)
        if risk <= 0:
            return "zero risk"
        reward = abs(c.tp1_price - c.entry_price)
        rr = reward / risk
        if rr < self.min_rr:
            return f"RR {rr:.2f} < {self.min_rr}"

        # Max risk
        max_risk = self._max_risk(ctx.atr)
        if risk > max_risk:
            return f"risk {risk:.1f} > max {max_risk:.1f}"

        # Displacement check
        if self.require_displacement and not bars.empty:
            if not self._has_displacement(bars, c.direction, ctx.atr):
                return "no displacement"

        # Countertrend block
        if self.block_countertrend and not bars.empty:
            blocked = self._is_countertrend(c, bars)
            if blocked:
                return "countertrend without reversal pattern"

        return None

    def _max_risk(self, atr: float) -> float:
        if self.max_risk_points is not None:
            return self.max_risk_points
        if self.adaptive_risk and atr > 0:
            return min(atr * self.adaptive_risk_mult, self.adaptive_risk_cap)
        return 75.0

    def _has_displacement(self, bars: pd.DataFrame, direction: Direction, atr: float) -> bool:
        if len(bars) < 2:
            return True
        lookback = bars.tail(self.displacement_lookback)
        threshold = atr * self.displacement_atr_pct
        if threshold <= 0:
            return True

        for i in range(len(lookback)):
            row = lookback.iloc[i]
            body = abs(float(row.get("close", 0.0)) - float(row.get("open", 0.0)))
            if direction == Direction.LONG:
                if float(row.get("close", 0.0)) > float(row.get("open", 0.0)) and body >= threshold:
                    return True
            else:
                if float(row.get("close", 0.0)) < float(row.get("open", 0.0)) and body >= threshold:
                    return True
        return False

    def _is_countertrend(self, c: SignalCandidate, bars: pd.DataFrame) -> bool:
        if len(bars) < 3:
            return False
        current = bars.iloc[-1]
        close = float(current.get("close", 0.0))
        vwap = float(current.get("vwap", close))
        ema20 = float(current.get("ema_20", close))
        ema50 = float(current.get("ema_50", ema20))

        # Check for reversal signal patterns (exempt from countertrend block)
        reversal_reasons = {"sweep", "reclaim", "choch", "reversal", "failed_breakout"}
        if any(r in reversal_reasons for r in c.reasons):
            return False

        if c.direction == Direction.LONG and close < vwap and ema20 < ema50:
            return True
        if c.direction == Direction.SHORT and close > vwap and ema20 > ema50:
            return True
        return False
