"""Typed event payloads for the telemetry event stream."""

from __future__ import annotations

from dataclasses import asdict, dataclass, field
from datetime import datetime, timezone


@dataclass(slots=True)
class BaseEvent:
    event_type: str = ""
    timestamp: str = field(default_factory=lambda: datetime.now(timezone.utc).isoformat())

    def to_dict(self) -> dict:
        return asdict(self)


@dataclass(slots=True)
class SessionStartEvent(BaseEvent):
    event_type: str = "session_start"
    profile: str = ""
    variant: str = ""
    mode: str = ""


@dataclass(slots=True)
class FeedReadyEvent(BaseEvent):
    event_type: str = "feed_ready"
    bars_count: int = 0
    ticks_count: int = 0
    warmup_seconds: float = 0.0


@dataclass(slots=True)
class CycleCompleteEvent(BaseEvent):
    event_type: str = "cycle_complete"
    candidate_count: int = 0
    blocked_count: int = 0
    submitted_orders: int = 0
    accepted_orders: int = 0
    latest_bar_timestamp: str = ""
    cycle_duration_ms: float = 0.0


@dataclass(slots=True)
class OrderSubmissionEvent(BaseEvent):
    event_type: str = "order_submission"
    candidate_id: str = ""
    direction: str = ""
    contracts: int = 0
    entry_price: float = 0.0
    order_type: str = ""
    accepted: bool = False
    reason: str = ""


@dataclass(slots=True)
class FillSeenEvent(BaseEvent):
    event_type: str = "fill_seen"
    signal_name: str = ""
    action: str = ""
    qty: int = 0
    price: float = 0.0


@dataclass(slots=True)
class ErrorEvent(BaseEvent):
    event_type: str = "runner_error"
    error: str = ""
    traceback: str = ""
