"""Regime inference — deterministic classification of the trading day.

Ported from V1 data/regime.py. Pure function, no side effects.
"""

from __future__ import annotations

from hsb.domain.context import RegimeInfo


def infer_regime(
    *,
    move_from_open: float,
    total_path: float,
    current_close: float,
    open_price: float,
) -> RegimeInfo:
    """Classify the current day into a regime based on price action efficiency.

    Returns a :class:`RegimeInfo` with the resolved regime string and metrics.
    """
    if total_path <= 0:
        return RegimeInfo(regime="transition")

    efficiency = abs(move_from_open) / total_path
    abs_move = abs(move_from_open)

    # Determine regime
    if efficiency < 0.06:
        regime = "chop" if abs_move < 40 else "transition"
    elif efficiency > 0.25 or (abs_move > 80 and efficiency > 0.12):
        regime = "trend_up" if move_from_open > 0 else "trend_down"
    elif efficiency < 0.12:
        regime = "chop"
    else:
        regime = "transition"

    # Determine bias
    if regime in ("trend_up", "trend_down"):
        bias = "bullish" if move_from_open > 0 else "bearish"
    elif abs(move_from_open) > 20:
        bias = "bullish" if move_from_open > 0 else "bearish"
    else:
        bias = "neutral"

    return RegimeInfo(
        regime=regime,
        move_from_open=round(move_from_open, 2),
        directional_efficiency=round(efficiency, 4),
        total_path=round(total_path, 2),
        bias=bias,
    )
