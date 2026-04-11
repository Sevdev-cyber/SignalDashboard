"""Intent extractor — converts pipeline decisions into order intents.

Extracted from V1's bridge_runtime.py (~200 line function) into a focused module.
"""

from __future__ import annotations

from datetime import datetime, timezone

from hsb.bridge.models import OrderIntent
from hsb.domain.enums import Direction, PolicyName, StrategyName
from hsb.pipeline.runner import PipelineResult


def extract_intents(
    result: PipelineResult,
    *,
    submitted_ids: set[str],
    not_before: datetime,
    limit: int = 1,
    prefer_latest: bool = False,
) -> list[OrderIntent]:
    """Extract actionable order intents from pipeline decisions.

    Skips blocked decisions, duplicates, old timestamps, and zero-contract plans.
    """
    not_before_utc = not_before.replace(tzinfo=timezone.utc) if not_before.tzinfo is None else not_before
    decisions = list(result.decisions)
    if prefer_latest:
        decisions = list(reversed(decisions))

    intents: list[OrderIntent] = []

    for d in decisions:
        # Skip non-director or blocked
        if d.stage != "director":
            continue
        tp = d.trade_plan
        tr = d.trade_result
        if not tp or tr.get("status") == "blocked":
            continue

        cid = str(tp.get("candidate_id", ""))
        if not cid or cid in submitted_ids:
            continue

        # Timestamp check
        ts_raw = d.candidate_timestamp
        if not ts_raw:
            continue
        ts = datetime.fromisoformat(ts_raw)
        if ts.tzinfo is None:
            ts = ts.replace(tzinfo=timezone.utc)
        if ts <= not_before_utc:
            continue

        # Contracts check
        contracts = int(tp.get("contracts", 0) or 0)
        if contracts <= 0:
            continue

        # Resolve order type
        order_type = _resolve_order_type(tp)

        direction = tp.get("direction", "neutral")
        policy = tp.get("policy", "basic")
        strategy = tp.get("strategy", "no_trade")

        intents.append(OrderIntent(
            candidate_id=cid,
            timestamp=ts,
            direction=Direction(direction),
            contracts=contracts,
            entry_price=float(tp.get("entry_price", 0.0) or 0.0),
            sl_price=float(tp.get("sl_price", 0.0) or 0.0),
            tp_price=float(tp.get("tp1_price", 0.0) or 0.0),
            tp2_price=float(tp.get("tp2_price", 0.0) or 0.0),
            tp3_price=float(tp.get("tp3_price", 0.0) or 0.0),
            order_type=order_type,
            policy=PolicyName(policy),
            strategy=StrategyName(strategy),
            metadata={
                "director_reason": (tp.get("metadata") or {}).get("director_reason", ""),
                "search_zone": tp.get("search_zone"),
            },
        ))

        if len(intents) >= limit:
            break

    return intents


def _resolve_order_type(plan: dict) -> str:
    strategy = plan.get("strategy", "")
    policy = plan.get("policy", "")
    if strategy in {"breakout_retest", "range_reversal"}:
        return "MARKET"
    if policy == "range_quick_exit":
        return "MARKET"
    if plan.get("search_zone"):
        return "LIMIT"
    return "MARKET"
