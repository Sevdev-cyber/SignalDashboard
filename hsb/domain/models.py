"""Domain models — immutable data structures flowing through the pipeline.

All models are frozen dataclasses with __slots__ for performance and safety.
No business logic lives here — only data shapes.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime

from hsb.domain.enums import (
    CandidateFamily,
    Direction,
    DirectorAction,
    MacroRegime,
    MicroRegime,
    PolicyName,
    RiskMode,
    StrategyName,
    TradeStatus,
    TriggerType,
)


# ---------------------------------------------------------------------------
# Price zone
# ---------------------------------------------------------------------------

@dataclass(slots=True, frozen=True)
class PriceZone:
    lo: float
    hi: float
    label: str = ""


# ---------------------------------------------------------------------------
# Signal layer
# ---------------------------------------------------------------------------

@dataclass(slots=True)
class SignalCandidate:
    id: str
    timestamp: datetime
    direction: Direction
    family: CandidateFamily
    entry_price: float
    sl_price: float
    tp1_price: float
    tp2_price: float = 0.0
    tp3_price: float = 0.0
    score: float = 0.0
    reasons: list[str] = field(default_factory=list)
    features: dict[str, object] = field(default_factory=dict)


# ---------------------------------------------------------------------------
# Director layer
# ---------------------------------------------------------------------------

@dataclass(slots=True)
class DirectorDecision:
    action: DirectorAction
    policy: PolicyName = PolicyName.BASIC
    size_multiplier: float = 1.0
    entry_zone: PriceZone | None = None
    invalidation_zone: PriceZone | None = None
    trigger: TriggerType = TriggerType.TOUCH
    strategy: StrategyName = StrategyName.NO_TRADE
    reason: str = ""


@dataclass(slots=True)
class Playbook:
    id: str
    strategy: StrategyName
    direction: Direction
    priority: int = 1
    entry_zone: PriceZone | None = None
    invalidation_zone: PriceZone | None = None
    targets: list[float] = field(default_factory=list)
    trigger: TriggerType = TriggerType.TOUCH
    execution_policy: PolicyName = PolicyName.BE_TRAIL
    notes: str = ""


@dataclass(slots=True)
class MacroPlan:
    macro_regime: MacroRegime = MacroRegime.TRANSITION
    day_bias: str = "neutral"
    confidence: float = 0.5
    htf_bias: str = "neutral"
    allowed_sides: list[str] = field(default_factory=lambda: ["long", "short"])
    preferred_strategies: list[str] = field(default_factory=list)
    disabled_strategies: list[str] = field(default_factory=list)
    policy_bias: str = "be_trail"
    risk_mode: RiskMode = RiskMode.NORMAL


@dataclass(slots=True)
class MicroPlan:
    micro_regime: MicroRegime = MicroRegime.NO_TRADE
    strategy_mode: str = "no_trade"
    active_playbooks: list[Playbook] = field(default_factory=list)
    avoid: list[str] = field(default_factory=list)
    review_after_bars: int = 2
    reason: str = ""


@dataclass(slots=True)
class EventUpdate:
    update_action: str = "keep"
    playbook_update: list[Playbook] = field(default_factory=list)
    cancel_playbooks: list[str] = field(default_factory=list)
    risk_mode: RiskMode = RiskMode.NORMAL
    close_position_now: bool = False
    close_reason: str = ""
    reason: str = ""


# ---------------------------------------------------------------------------
# Execution layer
# ---------------------------------------------------------------------------

@dataclass(slots=True)
class TradePlan:
    candidate_id: str
    direction: Direction
    entry_price: float
    sl_price: float
    tp1_price: float
    tp2_price: float = 0.0
    tp3_price: float = 0.0
    contracts: int = 1
    policy: PolicyName = PolicyName.BASIC
    search_zone: PriceZone | None = None
    trigger: TriggerType = TriggerType.TOUCH
    strategy: StrategyName = StrategyName.NO_TRADE
    metadata: dict[str, object] = field(default_factory=dict)


@dataclass(slots=True)
class TradeResult:
    plan_id: str
    status: TradeStatus
    pnl: float = 0.0
    bars_held: int = 0
    events: list[str] = field(default_factory=list)
    metadata: dict[str, object] = field(default_factory=dict)
