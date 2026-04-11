"""LLM response parser — converts raw JSON from DeepSeek into domain models."""

from __future__ import annotations

import logging

from hsb.domain.enums import (
    DirectorAction,
    MacroRegime,
    PolicyName,
    RiskMode,
)
from hsb.domain.models import DirectorDecision, MacroPlan

log = logging.getLogger(__name__)


def parse_macro_plan(data: dict) -> MacroPlan:
    """Parse macro plan JSON from LLM response."""
    regime_map = {
        "trend_up": MacroRegime.TREND_UP,
        "trend_down": MacroRegime.TREND_DOWN,
        "range_day": MacroRegime.RANGE_DAY,
        "chop_day": MacroRegime.CHOP_DAY,
        "transition": MacroRegime.TRANSITION,
    }
    risk_map = {
        "normal": RiskMode.NORMAL,
        "reduced": RiskMode.REDUCED,
        "defensive": RiskMode.DEFENSIVE,
    }

    return MacroPlan(
        macro_regime=regime_map.get(data.get("macro_regime", ""), MacroRegime.TRANSITION),
        day_bias=data.get("day_bias", "neutral"),
        confidence=float(data.get("confidence", 0.5)),
        allowed_sides=data.get("allowed_sides", ["long", "short"]),
        preferred_strategies=data.get("preferred_strategies", []),
        disabled_strategies=data.get("disabled_strategies", []),
        policy_bias=data.get("policy_bias", "be_trail"),
        risk_mode=risk_map.get(data.get("risk_mode", "normal"), RiskMode.NORMAL),
    )


def parse_candidate_review(data: dict) -> DirectorDecision:
    """Parse per-candidate review JSON from LLM response."""
    action_map = {
        "allow": DirectorAction.ALLOW,
        "block": DirectorAction.BLOCK,
        "reduce_size": DirectorAction.REDUCE_SIZE,
    }
    policy_map = {
        "be_trail": PolicyName.BE_TRAIL,
        "tp1_lock": PolicyName.TP1_LOCK,
        "basic": PolicyName.BASIC,
        "range_quick_exit": PolicyName.RANGE_QUICK_EXIT,
    }

    action = action_map.get(data.get("action", "block"), DirectorAction.BLOCK)
    policy = policy_map.get(data.get("policy", "basic"), PolicyName.BASIC)
    multiplier = float(data.get("size_multiplier", 1.0))
    reason = data.get("reasoning", "")

    return DirectorDecision(
        action=action,
        policy=policy,
        size_multiplier=max(0.0, min(multiplier, 2.0)),  # clamp 0-2
        reason=reason,
    )
