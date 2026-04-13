"""Market snapshot decision module for MNQ.

This module consumes a structural market snapshot instead of a chart screenshot.
It is intentionally deterministic:

- the engine reads state, VWAP, CVD, EMA stack, regime, signals, and zones
- the bot returns a trading scenario, entry/invalid/target levels, and a summary
- an optional LLM prompt can be generated from the same snapshot

The module is standalone on purpose. It can be wired later to:
- SignalDashboard websocket state
- a JSON file exported from the backend
- a relay or API payload

No model calls are made here. This is the decision layer and prompt builder only.
"""

from __future__ import annotations

import argparse
import json
import math
import sys
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Iterable


def _num(value: Any, default: float = 0.0) -> float:
    try:
        if value is None:
            return default
        if isinstance(value, bool):
            return float(value)
        out = float(value)
        if math.isnan(out) or math.isinf(out):
            return default
        return out
    except Exception:
        return default


def _int(value: Any, default: int = 0) -> int:
    try:
        if value is None:
            return default
        return int(float(value))
    except Exception:
        return default


def _norm_str(value: Any, default: str = "") -> str:
    if value is None:
        return default
    text = str(value).strip()
    return text if text else default


def _as_list(value: Any) -> list[dict[str, Any]]:
    if not value:
        return []
    if isinstance(value, list):
        return [x for x in value if isinstance(x, dict)]
    return []


@dataclass(slots=True)
class MarketSnapshot:
    """Normalized structural state from the dashboard/backend."""

    price: float = 0.0
    atr: float = 0.0
    vwap: float = 0.0
    vwap_dist: float = 0.0
    vwap_dist_atr: float = 0.0
    vwap_state: str = "UNKNOWN"
    vwap_pos: str = "NEUTRAL"
    ema20: float = 0.0
    ema50: float = 0.0
    ema100: float = 0.0
    ema_stack: str = "NEUTRAL"
    regime: str = "unknown"
    rsi: float = 50.0
    delta_raw: float = 0.0
    cum_delta: float = 0.0
    cvd_trend: str = "FLAT"
    delta_streak: int = 0
    volume: float = 0.0
    vol_ratio: float = 1.0
    flow_source: str = "unknown"
    flow_quality: str = "unknown"
    real_tick_coverage: float = 0.0
    bar_range: float = 0.0
    bar_close_pos: float = 50.0
    time_label: str = "Standard"
    day_label: str = "Unknown"
    updated_at: str = ""
    trader_guide: dict[str, Any] = field(default_factory=dict)
    signals: list[dict[str, Any]] = field(default_factory=list)
    zones: list[dict[str, Any]] = field(default_factory=list)
    ghost_signals: list[dict[str, Any]] = field(default_factory=list)

    @classmethod
    def from_payload(cls, payload: dict[str, Any]) -> "MarketSnapshot":
        state = payload.get("state") if isinstance(payload.get("state"), dict) else payload
        guide = state.get("trader_guide") if isinstance(state.get("trader_guide"), dict) else {}
        return cls(
            price=_num(state.get("price")),
            atr=_num(state.get("atr")),
            vwap=_num(state.get("vwap")),
            vwap_dist=_num(state.get("vwap_dist")),
            vwap_dist_atr=_num(state.get("vwap_dist_atr")),
            vwap_state=_norm_str(state.get("vwap_state"), "UNKNOWN"),
            vwap_pos=_norm_str(state.get("vwap_pos"), "NEUTRAL"),
            ema20=_num(state.get("ema20")),
            ema50=_num(state.get("ema50")),
            ema100=_num(state.get("ema100")),
            ema_stack=_norm_str(state.get("ema_stack"), "NEUTRAL"),
            regime=_norm_str(state.get("regime"), "unknown"),
            rsi=_num(state.get("rsi"), 50.0),
            delta_raw=_num(state.get("delta_raw")),
            cum_delta=_num(state.get("cum_delta")),
            cvd_trend=_norm_str(state.get("cvd_trend"), "FLAT"),
            delta_streak=_int(state.get("delta_streak")),
            volume=_num(state.get("volume")),
            vol_ratio=_num(state.get("vol_ratio"), 1.0),
            flow_source=_norm_str(state.get("flow_source"), "unknown"),
            flow_quality=_norm_str(state.get("flow_quality"), "unknown"),
            real_tick_coverage=_num(state.get("real_tick_coverage")),
            bar_range=_num(state.get("bar_range")),
            bar_close_pos=_num(state.get("bar_close_pos"), 50.0),
            time_label=_norm_str(state.get("time_label"), "Standard"),
            day_label=_norm_str(state.get("day_label"), "Unknown"),
            updated_at=_norm_str(state.get("timestamp"), ""),
            trader_guide=guide,
            signals=_as_list(payload.get("signals") or state.get("signals")),
            zones=_as_list(payload.get("zones") or state.get("zones")),
            ghost_signals=_as_list(payload.get("ghost_signals") or state.get("ghost_signals")),
        )

    def to_dict(self) -> dict[str, Any]:
        return {
            "price": self.price,
            "atr": self.atr,
            "vwap": self.vwap,
            "vwap_dist": self.vwap_dist,
            "vwap_dist_atr": self.vwap_dist_atr,
            "vwap_state": self.vwap_state,
            "vwap_pos": self.vwap_pos,
            "ema20": self.ema20,
            "ema50": self.ema50,
            "ema100": self.ema100,
            "ema_stack": self.ema_stack,
            "regime": self.regime,
            "rsi": self.rsi,
            "delta_raw": self.delta_raw,
            "cum_delta": self.cum_delta,
            "cvd_trend": self.cvd_trend,
            "delta_streak": self.delta_streak,
            "volume": self.volume,
            "vol_ratio": self.vol_ratio,
            "flow_source": self.flow_source,
            "flow_quality": self.flow_quality,
            "real_tick_coverage": self.real_tick_coverage,
            "bar_range": self.bar_range,
            "bar_close_pos": self.bar_close_pos,
            "time_label": self.time_label,
            "day_label": self.day_label,
            "updated_at": self.updated_at,
            "trader_guide": self.trader_guide,
            "signals": self.signals,
            "zones": self.zones,
            "ghost_signals": self.ghost_signals,
        }


@dataclass(slots=True)
class TradeDecision:
    """Structured output from the decision layer."""

    action: str
    bias: str
    confidence: int
    scenario: str
    summary: str
    reasons: list[str] = field(default_factory=list)
    warnings: list[str] = field(default_factory=list)
    trigger_level: float | None = None
    invalidation_level: float | None = None
    entry_zone: dict[str, Any] | None = None
    target_zone: dict[str, Any] | None = None
    supporting_signals: list[str] = field(default_factory=list)
    prompt: str = ""

    def to_dict(self) -> dict[str, Any]:
        return {
            "action": self.action,
            "bias": self.bias,
            "confidence": self.confidence,
            "scenario": self.scenario,
            "summary": self.summary,
            "reasons": self.reasons,
            "warnings": self.warnings,
            "trigger_level": self.trigger_level,
            "invalidation_level": self.invalidation_level,
            "entry_zone": self.entry_zone,
            "target_zone": self.target_zone,
            "supporting_signals": self.supporting_signals,
            "prompt": self.prompt,
        }


class MarketSnapshotBot:
    """Deterministic scenario interpreter for MNQ market snapshots."""

    def __init__(
        self,
        *,
        min_confidence_to_act: int = 66,
        long_vwap_reclaim_atr: float = -0.35,
        short_vwap_reject_atr: float = 0.35,
        extended_vwap_atr: float = 1.0,
        near_vwap_atr: float = 0.25,
    ) -> None:
        self.min_confidence_to_act = min_confidence_to_act
        self.long_vwap_reclaim_atr = long_vwap_reclaim_atr
        self.short_vwap_reject_atr = short_vwap_reject_atr
        self.extended_vwap_atr = extended_vwap_atr
        self.near_vwap_atr = near_vwap_atr

    def analyze(self, payload: dict[str, Any]) -> TradeDecision:
        snapshot = MarketSnapshot.from_payload(payload)
        guide = snapshot.trader_guide or {}
        tf5 = guide.get("tf_5m") or {}
        tf15 = guide.get("tf_15m") or {}

        long_score = 0.0
        short_score = 0.0
        reasons: list[str] = []
        warnings: list[str] = []

        def add(side: str, pts: float, reason: str) -> None:
            nonlocal long_score, short_score
            if pts <= 0:
                return
            if side == "long":
                long_score += pts
            else:
                short_score += pts
            reasons.append(reason)

        self._score_regime(snapshot, add)
        self._score_vwap(snapshot, add)
        self._score_flow(snapshot, add)
        self._score_guide(snapshot, guide, tf5, tf15, add)
        self._score_signals(snapshot, add)

        bias, confidence = self._derive_bias(long_score, short_score)
        if confidence < self.min_confidence_to_act:
            action = "wait"
        else:
            action = self._derive_action(snapshot, bias)

        scenario, summary, trigger_level, invalidation_level, entry_zone, target_zone = self._build_scenario(
            snapshot,
            guide,
            bias=bias,
            action=action,
        )

        if snapshot.vwap_state == "UNKNOWN" or snapshot.vwap <= 0:
            warnings.append("VWAP unavailable or not initialized.")
        if snapshot.flow_source == "bar_fallback":
            warnings.append("Flow is estimated from bars, not tick-level data.")
        if snapshot.regime in {"chop", "transition"} and confidence < 75:
            warnings.append("Market is rotational; wait for cleaner alignment.")

        prompt = self.build_llm_prompt(snapshot, scenario=scenario, action=action, bias=bias, confidence=confidence)

        return TradeDecision(
            action=action,
            bias=bias,
            confidence=confidence,
            scenario=scenario,
            summary=summary,
            reasons=self._dedupe(reasons),
            warnings=self._dedupe(warnings),
            trigger_level=trigger_level,
            invalidation_level=invalidation_level,
            entry_zone=entry_zone,
            target_zone=target_zone,
            supporting_signals=self._supporting_signals(snapshot, bias),
            prompt=prompt,
        )

    def _score_regime(self, s: MarketSnapshot, add) -> None:
        if s.regime == "trend_up":
            add("long", 3, "Regime is trend_up.")
        elif s.regime == "trend_down":
            add("short", 3, "Regime is trend_down.")
        elif s.regime == "range":
            add("long", 1, "Regime is range; mean reversion has some edge.")
            add("short", 1, "Regime is range; mean reversion has some edge.")
        elif s.regime == "transition":
            add("long", 0.5, "Regime is transition; wait for confirmation.")
            add("short", 0.5, "Regime is transition; wait for confirmation.")

        if s.ema_stack == "BULLISH":
            add("long", 2, "EMA stack is bullish.")
        elif s.ema_stack == "BEARISH":
            add("short", 2, "EMA stack is bearish.")

    def _score_vwap(self, s: MarketSnapshot, add) -> None:
        if s.vwap_state == "EXTENDED_ABOVE" or s.vwap_dist_atr >= self.extended_vwap_atr:
            add("long", 1.5, "Price is extended above VWAP.")
            add("short", 1.0, "Price is stretched far from VWAP; fade risk rises.")
        elif s.vwap_state == "EXTENDED_BELOW" or s.vwap_dist_atr <= -self.extended_vwap_atr:
            add("short", 1.5, "Price is extended below VWAP.")
            add("long", 1.0, "Price is stretched far from VWAP; reclaim risk rises.")
        elif s.vwap_dist_atr >= self.short_vwap_reject_atr:
            add("long", 1.0, "Price is above VWAP.")
        elif s.vwap_dist_atr <= self.long_vwap_reclaim_atr:
            add("short", 1.0, "Price is below VWAP.")
        else:
            add("long", 0.5, "Price is near VWAP; continuation can still develop.")
            add("short", 0.5, "Price is near VWAP; continuation can still develop.")

        if abs(s.vwap_dist_atr) <= self.near_vwap_atr:
            add("long", 0.5, "Price is close to VWAP.")
            add("short", 0.5, "Price is close to VWAP.")

        if s.vwap_dist_atr > 0.0:
            add("long", min(1.5, max(0.25, s.vwap_dist_atr)), "Trade is trading above VWAP.")
        elif s.vwap_dist_atr < 0.0:
            add("short", min(1.5, max(0.25, abs(s.vwap_dist_atr))), "Trade is trading below VWAP.")

    def _score_flow(self, s: MarketSnapshot, add) -> None:
        if s.cvd_trend == "RISING":
            add("long", 1.0, "CVD is rising.")
        elif s.cvd_trend == "FALLING":
            add("short", 1.0, "CVD is falling.")

        if s.delta_streak >= 3:
            add("long", 1.0, f"Delta streak is positive ({s.delta_streak}).")
        elif s.delta_streak <= -3:
            add("short", 1.0, f"Delta streak is negative ({s.delta_streak}).")

        if s.delta_raw > 0:
            add("long", 0.5, "Current bar delta is positive.")
        elif s.delta_raw < 0:
            add("short", 0.5, "Current bar delta is negative.")

        if s.rsi >= 68:
            add("short", 0.5, "RSI is stretched to the upside.")
        elif s.rsi <= 32:
            add("long", 0.5, "RSI is stretched to the downside.")

    def _score_guide(self, s: MarketSnapshot, guide: dict[str, Any], tf5: dict[str, Any], tf15: dict[str, Any], add) -> None:
        overall_bias = _norm_str(guide.get("overall_bias"), "neutral")
        if overall_bias in {"long", "neutral_to_long"}:
            add("long", 2.0 if overall_bias == "long" else 1.0, "Trader guide leans long.")
        elif overall_bias in {"short", "neutral_to_short"}:
            add("short", 2.0 if overall_bias == "short" else 1.0, "Trader guide leans short.")

        if tf5.get("bias") == "long":
            add("long", 1.0, "5m guide is long.")
        elif tf5.get("bias") == "short":
            add("short", 1.0, "5m guide is short.")

        if tf15.get("bias") == "long":
            add("long", 1.5, "15m guide is long.")
        elif tf15.get("bias") == "short":
            add("short", 1.5, "15m guide is short.")

        best_long = guide.get("best_long_zone") if isinstance(guide.get("best_long_zone"), dict) else {}
        best_short = guide.get("best_short_zone") if isinstance(guide.get("best_short_zone"), dict) else {}
        if best_long and s.price >= _num(best_long.get("low")) and s.price <= _num(best_long.get("high")):
            add("long", 1.5, "Price is in the preferred long zone.")
        if best_short and s.price >= _num(best_short.get("low")) and s.price <= _num(best_short.get("high")):
            add("short", 1.5, "Price is in the preferred short zone.")

    def _score_signals(self, s: MarketSnapshot, add) -> None:
        longs = 0
        shorts = 0
        aligned_3x = 0
        for sig in s.signals:
            direction = _norm_str(sig.get("direction")).lower()
            aligned = _int(sig.get("confluence_count") or sig.get("same_dir_family_count") or 0)
            if direction == "long":
                longs += 1
                if aligned >= 3:
                    aligned_3x += 1
            elif direction == "short":
                shorts += 1
                if aligned >= 3:
                    aligned_3x += 1

        if longs > shorts:
            add("long", min(1.5, 0.3 * longs), f"Active signals lean long ({longs} vs {shorts}).")
        elif shorts > longs:
            add("short", min(1.5, 0.3 * shorts), f"Active signals lean short ({shorts} vs {longs}).")

        if aligned_3x >= 1:
            if longs > shorts:
                add("long", 1.25, "At least one 3x+ aligned long cluster is active.")
            elif shorts > longs:
                add("short", 1.25, "At least one 3x+ aligned short cluster is active.")

    def _derive_bias(self, long_score: float, short_score: float) -> tuple[str, int]:
        best = max(long_score, short_score)
        gap = abs(long_score - short_score)
        if best < 3.0:
            return "neutral", int(round(min(70, 40 + best * 6)))
        if gap < 1.25:
            return "rotation", int(round(min(72, 48 + best * 5)))
        if long_score > short_score:
            return "long", int(round(min(95, 50 + long_score * 6 + gap * 4)))
        return "short", int(round(min(95, 50 + short_score * 6 + gap * 4)))

    def _derive_action(self, s: MarketSnapshot, bias: str) -> str:
        if bias == "neutral":
            return "wait"
        if bias == "rotation":
            return "watch_rotation"

        if bias == "long":
            if s.vwap_dist_atr <= self.long_vwap_reclaim_atr:
                return "watch_long_reclaim"
            if s.vwap_dist_atr >= self.short_vwap_reject_atr:
                return "watch_long_continuation"
            return "watch_long"

        if s.vwap_dist_atr >= self.short_vwap_reject_atr:
            return "watch_short_reject"
        if s.vwap_dist_atr <= self.long_vwap_reclaim_atr:
            return "watch_short_continuation"
        return "watch_short"

    def _build_scenario(
        self,
        s: MarketSnapshot,
        guide: dict[str, Any],
        *,
        bias: str,
        action: str,
    ) -> tuple[str, str, float | None, float | None, dict[str, Any] | None, dict[str, Any] | None]:
        tf5 = guide.get("tf_5m") or {}
        tf15 = guide.get("tf_15m") or {}
        best_long = guide.get("best_long_zone") if isinstance(guide.get("best_long_zone"), dict) else {}
        best_short = guide.get("best_short_zone") if isinstance(guide.get("best_short_zone"), dict) else {}

        trigger_level = None
        invalidation_level = None
        entry_zone = None
        target_zone = None

        if bias == "long":
            trigger_level = _num(tf5.get("trigger_level"), s.vwap)
            invalidation_level = _num(tf5.get("invalidation_level"), s.vwap - max(1.0, s.atr * 0.55))
            entry_zone = self._zone_from(best_long, fallback_center=trigger_level or s.price, fallback_atr=s.atr, direction="long")
            target_zone = self._directional_target_zone(
                tf15.get("continuation_zone") if isinstance(tf15.get("continuation_zone"), dict) else None,
                direction="long",
                current_price=s.price,
                fallback_center=s.price + max(1.0, s.atr * 1.5),
                fallback_atr=s.atr,
            )
            if s.vwap_dist_atr <= self.long_vwap_reclaim_atr:
                scenario = "long reclaim"
                summary = f"Look for reclaim above {trigger_level:.2f} and continuation back toward value."
            elif s.vwap_dist_atr >= self.short_vwap_reject_atr:
                scenario = "long continuation"
                summary = f"Trend is long and price is extended above value; buy pullbacks only."
            else:
                scenario = "long rotation"
                summary = f"Long bias is intact, but price is still rotating near value. Wait for a clean trigger."
        elif bias == "short":
            trigger_level = _num(tf5.get("trigger_level"), s.vwap)
            invalidation_level = _num(tf5.get("invalidation_level"), s.vwap + max(1.0, s.atr * 0.55))
            entry_zone = self._zone_from(best_short, fallback_center=trigger_level or s.price, fallback_atr=s.atr, direction="short")
            target_zone = self._directional_target_zone(
                tf15.get("continuation_zone") if isinstance(tf15.get("continuation_zone"), dict) else None,
                direction="short",
                current_price=s.price,
                fallback_center=s.price - max(1.0, s.atr * 1.5),
                fallback_atr=s.atr,
            )
            if s.vwap_dist_atr >= self.short_vwap_reject_atr:
                scenario = "short reject"
                summary = f"Look for rejection below {trigger_level:.2f} and continuation back under value."
            elif s.vwap_dist_atr <= self.long_vwap_reclaim_atr:
                scenario = "short continuation"
                summary = f"Trend is short and price is extended below value; sell pops only."
            else:
                scenario = "short rotation"
                summary = f"Short bias is intact, but price is still rotating near value. Wait for a clean trigger."
        elif bias == "rotation":
            center = s.vwap if s.vwap > 0 else s.price
            trigger_level = center
            invalidation_level = center
            entry_zone = self._zone_from(None, fallback_center=center, fallback_atr=s.atr, direction="long")
            target_zone = self._zone_from(None, fallback_center=center, fallback_atr=s.atr, direction="short")
            scenario = "rotation"
            summary = "No clean dominance. Treat VWAP/EMA area as rotational until a reclaim or rejection confirms direction."
        else:
            center = s.vwap if s.vwap > 0 else s.price
            trigger_level = center
            invalidation_level = center
            entry_zone = self._zone_from(None, fallback_center=center, fallback_atr=s.atr, direction="long")
            target_zone = self._zone_from(None, fallback_center=center, fallback_atr=s.atr, direction="short")
            scenario = "wait"
            summary = "No clean edge. Wait for 5m and 15m alignment, plus VWAP reaction."

        if action == "wait":
            summary = "Wait for a cleaner trigger. Current structure is not strong enough."

        return scenario, summary, trigger_level, invalidation_level, entry_zone, target_zone

    def _zone_from(
        self,
        zone: dict[str, Any] | None,
        *,
        fallback_center: float,
        fallback_atr: float,
        direction: str,
    ) -> dict[str, Any]:
        if zone and {"low", "high"}.issubset(zone):
            out = {
                "low": _num(zone.get("low")),
                "high": _num(zone.get("high")),
                "label": _norm_str(zone.get("label"), f"{direction} zone"),
                "why": _norm_str(zone.get("why"), ""),
            }
            if "trigger" in zone:
                out["trigger"] = _num(zone.get("trigger"))
            if "timeframe" in zone:
                out["timeframe"] = zone.get("timeframe")
            return out

        half = max(0.75, fallback_atr * 0.22)
        return {
            "low": round(fallback_center - half, 2),
            "high": round(fallback_center + half, 2),
            "label": f"{direction} fallback zone",
            "why": "Derived from VWAP/ATR context.",
        }

    def _directional_target_zone(
        self,
        zone: dict[str, Any] | None,
        *,
        direction: str,
        current_price: float,
        fallback_center: float,
        fallback_atr: float,
    ) -> dict[str, Any]:
        """Choose a target zone that actually lies in the trade direction."""
        candidate = self._zone_from(zone, fallback_center=fallback_center, fallback_atr=fallback_atr, direction=direction)
        if direction == "long" and candidate["high"] > current_price:
            return candidate
        if direction == "short" and candidate["low"] < current_price:
            return candidate
        return self._zone_from(None, fallback_center=fallback_center, fallback_atr=fallback_atr, direction=direction)

    def _supporting_signals(self, s: MarketSnapshot, bias: str) -> list[str]:
        out: list[str] = []
        for sig in s.signals[:8]:
            direction = _norm_str(sig.get("direction")).lower()
            aligned = _int(sig.get("confluence_count") or sig.get("same_dir_family_count") or 0)
            name = _norm_str(sig.get("name"), _norm_str(sig.get("signal_kind"), "signal"))
            if bias in {"long", "rotation"} and direction == "long":
                out.append(f"{name}:{aligned}x")
            elif bias == "short" and direction == "short":
                out.append(f"{name}:{aligned}x")
        return self._dedupe(out)[:6]

    def build_llm_prompt(
        self,
        snapshot: MarketSnapshot,
        *,
        scenario: str,
        action: str,
        bias: str,
        confidence: int,
    ) -> str:
        """Build a compact prompt for a future LLM call.

        The prompt asks for a JSON answer and never includes screenshot data.
        """
        payload = {
            "market": snapshot.to_dict(),
            "scenario": scenario,
            "action": action,
            "bias": bias,
            "confidence": confidence,
            "rules": {
                "do_not_use_screenshot": True,
                "prefer_structured_state": True,
                "output_format": {
                    "bias": "long|short|rotation|wait",
                    "scenario": "short text",
                    "entry": "number or null",
                    "invalidation": "number or null",
                    "target": "number or null",
                    "reasons": "array of strings",
                    "warnings": "array of strings",
                },
            },
        }
        return (
            "You are a MNQ trading assistant. "
            "Analyze the structured snapshot below and return only JSON. "
            "Do not infer from screenshots. "
            "Prefer the structural data over any visual guess.\n\n"
            f"{json.dumps(payload, ensure_ascii=True, indent=2, default=str)}"
        )

    @staticmethod
    def _dedupe(items: Iterable[str]) -> list[str]:
        out: list[str] = []
        seen: set[str] = set()
        for item in items:
            text = _norm_str(item)
            if not text or text in seen:
                continue
            seen.add(text)
            out.append(text)
        return out

    def analyze_json(self, text: str) -> TradeDecision:
        payload = json.loads(text)
        if not isinstance(payload, dict):
            raise TypeError("Snapshot JSON must decode to an object.")
        return self.analyze(payload)


def _read_input(path: str | None) -> dict[str, Any]:
    if path:
        return json.loads(Path(path).read_text(encoding="utf-8"))
    raw = sys.stdin.read()
    if not raw.strip():
        raise SystemExit("No JSON snapshot provided on stdin or via --snapshot.")
    return json.loads(raw)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Interpret an MNQ market snapshot.")
    parser.add_argument("--snapshot", help="Path to a JSON snapshot file.")
    parser.add_argument("--pretty", action="store_true", help="Pretty-print the decision JSON.")
    parser.add_argument("--prompt", action="store_true", help="Print the LLM prompt instead of the decision JSON.")
    args = parser.parse_args(argv)

    payload = _read_input(args.snapshot)
    bot = MarketSnapshotBot()
    decision = bot.analyze(payload)

    if args.prompt:
        print(decision.prompt)
        return 0

    if args.pretty:
        print(json.dumps(decision.to_dict(), ensure_ascii=True, indent=2))
    else:
        print(json.dumps(decision.to_dict(), ensure_ascii=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
