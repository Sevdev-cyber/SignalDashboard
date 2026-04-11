"""Protocol for execution policies."""

from __future__ import annotations

from typing import Protocol

from hsb.domain.context import AnalysisContext
from hsb.domain.models import TradePlan, TradeResult


class ExecutionPolicy(Protocol):
    """Simulates a trade plan through a price path, applying its management rules."""

    name: str

    def apply(self, plan: TradePlan, context: AnalysisContext) -> TradeResult:
        ...
