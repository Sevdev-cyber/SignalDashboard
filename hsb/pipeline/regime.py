"""Regime inference — enhanced classification of the trading session.

V2: Uses EMA slope, VWAP position, and ADX alongside path efficiency.
From PATTERN_DISCOVERIES: regime matters 40% swing on signal effectiveness.
"""

from __future__ import annotations

from hsb.domain.context import RegimeInfo


def infer_regime(
    *,
    move_from_open: float,
    total_path: float,
    current_close: float,
    open_price: float,
    # V2 enhanced inputs (optional — backwards compatible)
    ema20: float = 0.0,
    ema50: float = 0.0,
    ema20_prev: float = 0.0,
    ema50_prev: float = 0.0,
    vwap: float = 0.0,
    adx: float = 0.0,
    atr: float = 20.0,
) -> RegimeInfo:
    """Classify current session into a regime.

    V2 uses multiple inputs for more accurate classification:
    - Path efficiency (V1 method)
    - EMA20/50 slope and crossover
    - Price vs VWAP position
    - ADX for trend strength
    """
    if total_path <= 0:
        return RegimeInfo(regime="transition")

    efficiency = abs(move_from_open) / total_path
    abs_move = abs(move_from_open)

    # ── V1 base classification (path efficiency) ──
    if efficiency < 0.06:
        base_regime = "chop" if abs_move < 40 else "transition"
    elif efficiency > 0.25 or (abs_move > 80 and efficiency > 0.12):
        base_regime = "trend_up" if move_from_open > 0 else "trend_down"
    elif efficiency < 0.12:
        base_regime = "chop"
    else:
        base_regime = "transition"

    # ── V2 enhanced: use EMA/VWAP/ADX if available ──
    regime = base_regime

    if ema20 > 0 and ema50 > 0 and vwap > 0:
        # EMA slope (20-bar momentum)
        ema20_slope = (ema20 - ema20_prev) / atr if ema20_prev > 0 and atr > 0 else 0
        ema50_slope = (ema50 - ema50_prev) / atr if ema50_prev > 0 and atr > 0 else 0

        # EMA alignment score: +1 for each bullish signal, -1 for bearish
        ema_score = 0
        if current_close > ema20:
            ema_score += 1
        else:
            ema_score -= 1
        if ema20 > ema50:
            ema_score += 1  # bullish cross
        else:
            ema_score -= 1
        if ema20_slope > 0.05:
            ema_score += 1  # rising fast
        elif ema20_slope < -0.05:
            ema_score -= 1
        if current_close > vwap:
            ema_score += 1
        else:
            ema_score -= 1

        # Override base regime with EMA evidence
        if base_regime in ("transition", "chop"):
            if ema_score >= 3:
                regime = "trend_up"
            elif ema_score <= -3:
                regime = "trend_down"
            elif abs(ema_score) <= 1 and abs_move < 30:
                regime = "range"  # new! tight EMA convergence = range-bound
            # else keep base_regime

        elif base_regime in ("trend_up", "trend_down"):
            # Validate: if EMAs disagree strongly, downgrade to transition
            if base_regime == "trend_up" and ema_score <= -2:
                regime = "transition"  # path says up but EMAs say no
            elif base_regime == "trend_down" and ema_score >= 2:
                regime = "transition"

    # ── Determine bias ──
    if regime in ("trend_up",):
        bias = "bullish"
    elif regime in ("trend_down",):
        bias = "bearish"
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
