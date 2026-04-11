"""Policy engine — maps PolicyName to concrete execution policies.

Uses a single shared :class:`Simulator` instance instead of V1's pattern
where each policy created its own simulator.
"""

from __future__ import annotations

from hsb.domain.context import AnalysisContext
from hsb.domain.enums import DirectorAction, PolicyName
from hsb.domain.models import DirectorDecision, SignalCandidate, TradePlan, TradeResult
from hsb.execution.simulator import Simulator


class PolicyEngine:
    """Builds trade plans and simulates them through the selected policy."""

    def __init__(self) -> None:
        self._sim = Simulator()

    def build_plan(
        self,
        candidate: SignalCandidate,
        decision: DirectorDecision,
        base_contracts: int = 1,
    ) -> TradePlan:
        contracts = max(1, int(base_contracts * decision.size_multiplier))
        return TradePlan(
            candidate_id=candidate.id,
            direction=candidate.direction,
            entry_price=candidate.entry_price,
            sl_price=candidate.sl_price,
            tp1_price=candidate.tp1_price,
            tp2_price=candidate.tp2_price,
            tp3_price=candidate.tp3_price,
            contracts=contracts,
            policy=decision.policy,
            search_zone=decision.entry_zone,
            trigger=decision.trigger,
            strategy=decision.strategy,
            metadata={"director_reason": decision.reason},
        )

    def simulate(self, plan: TradePlan, context: AnalysisContext) -> TradeResult:
        if plan.policy == PolicyName.BE_TRAIL:
            result = self._sim.simulate_be_trail(plan, context)
            result.events = ["policy:be_trail"] + result.events
        elif plan.policy == PolicyName.TP1_LOCK:
            result = self._sim.simulate_tp1_lock(plan, context)
            result.events = ["policy:tp1_lock"] + result.events
        elif plan.policy == PolicyName.RANGE_QUICK_EXIT:
            result = self._sim.simulate_be_trail(
                plan, context,
                be_buffer_ticks=2,
                trail_mult=0.15,
                trail_min_points=1.5,
                max_bars=10,
            )
            result.events = ["policy:range_quick_exit"] + result.events
        else:
            # Basic — no simulation, just mark as open
            from hsb.domain.enums import TradeStatus
            result = TradeResult(
                plan_id=plan.candidate_id,
                status=TradeStatus.OPEN,
                events=["policy:basic"],
            )
        return result
