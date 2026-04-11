"""Fallback director — simple score-based gating without LLM.

This is the default director used when ``api_mode=fallback``.  It mirrors
the V1 ``llm/director.py`` MetaDirector logic exactly.
"""

from __future__ import annotations

from hsb.domain.context import AnalysisContext
from hsb.domain.enums import DirectorAction, PolicyName
from hsb.domain.models import (
    DirectorDecision,
    EventUpdate,
    MacroPlan,
    MicroPlan,
    SignalCandidate,
)


class FallbackDirector:
    """Deterministic director — no API calls."""

    def decide(
        self,
        candidate: SignalCandidate,
        context: AnalysisContext,
    ) -> DirectorDecision:
        regime = context.regime.regime

        if candidate.score >= 0.6:
            return DirectorDecision(
                action=DirectorAction.ALLOW,
                policy=PolicyName.BE_TRAIL,
                reason=f"score {candidate.score:.2f} >= 0.6 → be_trail",
            )

        if candidate.score < 0.45:
            return DirectorDecision(
                action=DirectorAction.BLOCK,
                reason=f"score {candidate.score:.2f} < 0.45 → blocked",
            )

        if regime == "transition":
            return DirectorDecision(
                action=DirectorAction.REDUCE_SIZE,
                policy=PolicyName.BASIC,
                size_multiplier=0.5,
                reason=f"score {candidate.score:.2f} in transition → reduce_size",
            )

        return DirectorDecision(
            action=DirectorAction.ALLOW,
            policy=PolicyName.BASIC,
            reason=f"score {candidate.score:.2f} → allow basic",
        )

    def macro_plan(self, context: AnalysisContext) -> MacroPlan:
        """Deterministic macro plan based on regime inference."""
        regime = context.regime
        from hsb.domain.enums import MacroRegime, RiskMode

        regime_map = {
            "trend_up": MacroRegime.TREND_UP,
            "trend_down": MacroRegime.TREND_DOWN,
            "chop": MacroRegime.CHOP_DAY,
            "transition": MacroRegime.TRANSITION,
        }
        macro = regime_map.get(regime.regime, MacroRegime.TRANSITION)

        sides = ["long", "short"]
        if macro == MacroRegime.TREND_UP:
            sides = ["long"]
        elif macro == MacroRegime.TREND_DOWN:
            sides = ["short"]

        return MacroPlan(
            macro_regime=macro,
            day_bias=regime.bias,
            confidence=min(regime.directional_efficiency * 3, 1.0),
            allowed_sides=sides,
            risk_mode=RiskMode.REDUCED if macro == MacroRegime.CHOP_DAY else RiskMode.NORMAL,
        )

    def micro_plan(self, context: AnalysisContext) -> MicroPlan:
        """No-op micro plan — fallback has no playbooks."""
        return MicroPlan()

    def on_event(self, event_type: str, context: AnalysisContext) -> EventUpdate:
        """No-op event handler."""
        return EventUpdate(reason=f"fallback: no action for {event_type}")
