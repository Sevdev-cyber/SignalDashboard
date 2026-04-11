"""Bridge domain models — order intents and submission tracking."""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime

from hsb.domain.enums import Direction, PolicyName, StrategyName


@dataclass(slots=True)
class OrderIntent:
    """An intent to submit an order to the broker."""

    candidate_id: str
    timestamp: datetime
    direction: Direction
    contracts: int
    entry_price: float
    sl_price: float
    tp_price: float
    tp2_price: float = 0.0
    tp3_price: float = 0.0
    order_type: str = "MARKET"  # MARKET | LIMIT
    policy: PolicyName = PolicyName.BASIC
    strategy: StrategyName = StrategyName.NO_TRADE
    metadata: dict = field(default_factory=dict)


@dataclass(slots=True)
class BridgeSubmission:
    """Result of submitting an order intent."""

    accepted: bool
    bridge_order_id: str = ""
    mode: str = ""
    reason: str = ""
    metadata: dict = field(default_factory=dict)
