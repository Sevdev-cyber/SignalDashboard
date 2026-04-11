"""Protocol definitions for the director layer.

The director layer acts as a gatekeeper between signal generation and
execution.  It has three tiers:

1. **Macro planning** — called every ~15 minutes to classify the day regime.
2. **Micro planning** — called every ~5 minutes to activate playbooks.
3. **Event callback** — called after meaningful events (TP1 hit, SL, sweep).

Any implementation of these protocols can be injected into the pipeline.
"""

from __future__ import annotations

from typing import Protocol

from hsb.domain.context import AnalysisContext
from hsb.domain.models import (
    DirectorDecision,
    EventUpdate,
    MacroPlan,
    MicroPlan,
    SignalCandidate,
)


class Director(Protocol):
    """Unified director interface."""

    def decide(
        self,
        candidate: SignalCandidate,
        context: AnalysisContext,
    ) -> DirectorDecision:
        """Gate a single candidate — allow, block, or reduce_size."""
        ...

    def macro_plan(self, context: AnalysisContext) -> MacroPlan:
        """Produce the macro regime plan for the session."""
        ...

    def micro_plan(self, context: AnalysisContext) -> MicroPlan:
        """Produce the micro tactical plan (active playbooks)."""
        ...

    def on_event(self, event_type: str, context: AnalysisContext) -> EventUpdate:
        """React to a pipeline event."""
        ...
