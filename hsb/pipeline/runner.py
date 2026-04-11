"""Pipeline runner — thin orchestrator replacing V1's God class ComparisonRunner.

This module coordinates the pipeline phases but owns NONE of the logic:

1. Build context           → ``ContextBuilder``
2. Generate candidates     → ``CandidateGenerator`` (injected)
3. Filter candidates       → ``CandidateFilter`` (injected)
4. Gate via director       → ``Director`` (injected)
5. Build plan + simulate   → ``PolicyEngine``

All components are injected via constructor — making the runner fully testable
with stubs, mocks, or real implementations.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass, field
from datetime import datetime

import pandas as pd

from hsb.director.fallback import FallbackDirector
from hsb.domain.context import AnalysisContext, PositionState
from hsb.domain.enums import DirectorAction
from hsb.execution.policy_engine import PolicyEngine
from hsb.filters.champion import ChampionFilter
from hsb.filters.dedupe import DedupeFilter
from hsb.pipeline.context_builder import ContextBuilder
from hsb.signals.composite import CompositeGenerator

log = logging.getLogger(__name__)


@dataclass(slots=True)
class PipelineDecision:
    """One decision from the pipeline — candidate + director action + result."""

    candidate_id: str
    candidate_timestamp: str
    stage: str = "director"
    trade_plan: dict = field(default_factory=dict)
    trade_result: dict = field(default_factory=dict)


@dataclass(slots=True)
class PipelineResult:
    """Full result of a single pipeline run."""

    candidate_count: int = 0
    blocked_count: int = 0
    decisions: list[PipelineDecision] = field(default_factory=list)
    context_summary: dict = field(default_factory=dict)


class PipelineRunner:
    """Dependency-injected pipeline coordinator.

    All components have sensible defaults but can be swapped via constructor:

    >>> runner = PipelineRunner(
    ...     generator=MyCustomGenerator(),
    ...     director=LLMDirector(api_key="..."),
    ... )
    """

    def __init__(
        self,
        *,
        generator: object | None = None,
        filters: list[object] | None = None,
        director: object | None = None,
        policy_engine: PolicyEngine | None = None,
        context_builder: ContextBuilder | None = None,
        base_contracts: int = 1,
    ) -> None:
        self.generator = generator or CompositeGenerator()
        self.filters = filters or [ChampionFilter(), DedupeFilter()]
        self.director = director or FallbackDirector()
        self.policy_engine = policy_engine or PolicyEngine()
        self.context_builder = context_builder or ContextBuilder()
        self.base_contracts = base_contracts

    def run(
        self,
        *,
        bars_df: pd.DataFrame,
        ticks_df: pd.DataFrame | None = None,
        macro_bars_df: pd.DataFrame | None = None,
        micro_bars_df: pd.DataFrame | None = None,
        session: str = "bars",
        day: str = "",
        source: str = "",
        position: PositionState | None = None,
        require_flat_position: bool = True,
        gate_mode: str = "off",
        live_mode: bool = False,
    ) -> PipelineResult:
        """Execute the full pipeline once and return the result."""

        # 1. Build context
        context = self.context_builder.build(
            bars_df=bars_df,
            ticks_df=ticks_df,
            macro_bars_df=macro_bars_df,
            micro_bars_df=micro_bars_df,
            session=session,
            day=day,
            source=source,
            position=position,
            require_flat_position=require_flat_position,
            gate_mode=gate_mode,
            live_mode=live_mode,
        )

        # 2. Generate candidates
        candidates = self.generator.generate(context)  # type: ignore[union-attr]
        log.info("Generated %d candidates", len(candidates))

        # 3. Filter
        filtered = candidates
        for f in self.filters:
            filtered = f.filter(filtered, context)  # type: ignore[union-attr]
        log.info("After filters: %d candidates", len(filtered))

        # 4. Gate + simulate each candidate
        decisions: list[PipelineDecision] = []
        blocked = 0

        for candidate in filtered:
            decision = self.director.decide(candidate, context)  # type: ignore[union-attr]

            if decision.action == DirectorAction.BLOCK:
                blocked += 1
                decisions.append(PipelineDecision(
                    candidate_id=candidate.id,
                    candidate_timestamp=candidate.timestamp.isoformat(),
                    trade_plan={},
                    trade_result={"status": "blocked", "pnl": 0.0},
                ))
                continue

            # Build plan
            plan = self.policy_engine.build_plan(candidate, decision, self.base_contracts)

            # Simulate
            result = self.policy_engine.simulate(plan, context)

            decisions.append(PipelineDecision(
                candidate_id=candidate.id,
                candidate_timestamp=candidate.timestamp.isoformat(),
                trade_plan={
                    "candidate_id": plan.candidate_id,
                    "direction": plan.direction.value,
                    "entry_price": plan.entry_price,
                    "sl_price": plan.sl_price,
                    "tp1_price": plan.tp1_price,
                    "tp2_price": plan.tp2_price,
                    "tp3_price": plan.tp3_price,
                    "contracts": plan.contracts,
                    "policy": plan.policy.value,
                    "strategy": plan.strategy.value,
                    "search_zone": None if plan.search_zone is None else {
                        "lo": plan.search_zone.lo,
                        "hi": plan.search_zone.hi,
                    },
                    "metadata": plan.metadata,
                },
                trade_result={
                    "status": result.status.value,
                    "pnl": result.pnl,
                    "bars_held": result.bars_held,
                    "events": result.events,
                    "metadata": result.metadata,
                },
            ))

        return PipelineResult(
            candidate_count=len(candidates),
            blocked_count=blocked,
            decisions=decisions,
            context_summary={
                "timestamp": context.timestamp.isoformat(),
                "session": context.session,
                "day": context.day,
                "regime": context.regime.regime,
                "atr": context.atr,
                "move_from_open": context.regime.move_from_open,
                "efficiency": context.regime.directional_efficiency,
                "position": context.position.raw,
                "bars_count": len(context.bar_data.bars_df),
                "gate_profile": context.gate.profile,
            },
        )
