"""Execution layer over dashboard signals and market-decision context.

This module is intentionally separate from the websocket server.
It consumes:
  - enriched signal candidates from ``SignalEngine.evaluate()``
  - deterministic market context from ``MarketSnapshotBot.analyze()``

It produces:
  - pending limit orders
  - managed open positions
  - early exits when context flips hard enough

The first implementation is conservative and keeps at most one position open.
That is a deliberate choice for backtest clarity before wiring the logic to
live order routing.
"""

from __future__ import annotations

from collections import Counter
from dataclasses import asdict, dataclass, field
from typing import Any


def _f(value: Any, default: float = 0.0) -> float:
    try:
        if value is None:
            return default
        return float(value)
    except Exception:
        return default


def _i(value: Any, default: int = 0) -> int:
    try:
        if value is None:
            return default
        return int(float(value))
    except Exception:
        return default


def _cluster(sig: dict[str, Any]) -> int:
    return max(
        _i(sig.get("same_dir_family_count")),
        _i(sig.get("confluence_count")),
        1 if _f(sig.get("confidence_pct")) > 0 else 0,
    )


def _quality_bonus(sig: dict[str, Any]) -> float:
    return {
        "A+": 8.0,
        "A": 5.0,
        "B": 2.0,
        "C": -2.0,
        "D": -6.0,
    }.get(str(sig.get("quality_grade") or "").upper(), 0.0)


@dataclass(slots=True)
class ExecutionConfig:
    min_signal_confidence: int = 72
    soft_signal_confidence: int = 62
    min_decision_confidence: int = 66
    max_entry_dist_atr: float = 0.85
    max_initial_risk_points: float = 25.0
    max_confluence_risk_points: float = 15.0
    min_target_r_multiple: float = 0.15
    max_pending_bars: int = 3
    max_position_bars: int = 24
    be_progress_r: float = 0.50
    lock_progress_r: float = 0.90
    soft_exit_progress_r: float = 0.45
    hard_flip_confidence: int = 82
    soft_flip_confidence: int = 74
    strong_cluster: int = 3
    soft_cluster: int = 2
    break_even_buffer: float = 0.25
    lock_profit_r: float = 0.35
    allow_legacy_fallback: bool = False
    session_trade_cap: int = 10
    session_direction_cap: int = 6
    family_cooldown_win_bars: int = 8
    family_cooldown_loss_bars: int = 16
    zone_cooldown_bars: int = 20
    zone_reuse_points: float = 8.0


@dataclass(slots=True)
class PendingOrder:
    signal_id: str
    signal_name: str
    source_type: str
    direction: str
    entry_price: float
    sl_price: float
    tp_price: float
    risk_pts: float
    confidence: int
    cluster: int
    created_bar: int
    created_time: str
    decision_bias: str
    decision_confidence: int
    selection_score: float
    notes: list[str] = field(default_factory=list)


@dataclass(slots=True)
class ManagedPosition:
    signal_id: str
    signal_name: str
    source_type: str
    direction: str
    entry_price: float
    sl_price: float
    tp_price: float
    initial_risk: float
    confidence: int
    cluster: int
    opened_bar: int
    opened_time: str
    decision_bias: str
    decision_confidence: int
    current_sl: float
    current_tp: float
    bars_held: int = 0
    max_favorable_price: float = 0.0
    max_adverse_price: float = 0.0
    exit_candidates_seen: int = 0
    notes: list[str] = field(default_factory=list)


@dataclass(slots=True)
class ClosedTrade:
    signal_id: str
    signal_name: str
    source_type: str
    direction: str
    entry_time: str
    exit_time: str
    entry_price: float
    exit_price: float
    sl_price: float
    tp_price: float
    initial_risk: float
    bars_held: int
    confidence: int
    cluster: int
    gross_points: float
    gross_r: float
    outcome: str
    exit_reason: str
    mfe_points: float
    mae_points: float
    decision_bias: str
    decision_confidence: int
    notes: list[str] = field(default_factory=list)


class SignalExecutionBot:
    """Selects signals, manages pending entries, and exits on context flips."""

    def __init__(self, config: ExecutionConfig | None = None) -> None:
        self.config = config or ExecutionConfig()
        self.pending: PendingOrder | None = None
        self.position: ManagedPosition | None = None
        self.closed_trades: list[ClosedTrade] = []
        self.event_log: list[dict[str, Any]] = []
        self.session_tag: str | None = None
        self.session_trade_count = 0
        self.session_direction_counts: Counter[str] = Counter()
        self._family_last_close_bar: dict[tuple[str, str], int] = {}
        self._family_last_outcome: dict[tuple[str, str], str] = {}
        self._zone_last_close_bar: dict[tuple[str, str, int], int] = {}

    def has_risk(self) -> bool:
        return self.pending is not None or self.position is not None

    def start_session(self, tag: str) -> None:
        if self.session_tag == tag:
            return
        self.session_tag = tag
        self.session_trade_count = 0
        self.session_direction_counts = Counter()

    def on_tick(self, price: float, timestamp: str, bar_index: int) -> list[ClosedTrade]:
        closed: list[ClosedTrade] = []
        if self.pending is not None and self._pending_filled(self.pending, price):
            self.position = self._open_from_pending(self.pending, price, timestamp, bar_index)
            self.session_trade_count += 1
            self.session_direction_counts[self.position.direction] += 1
            self._log_event(
                "fill",
                bar_index,
                timestamp,
                {
                    "signal": self.position.signal_name,
                    "direction": self.position.direction,
                    "entry": self.position.entry_price,
                    "sl": self.position.sl_price,
                    "tp": self.position.tp_price,
                },
            )
            self.pending = None

        if self.position is None:
            return closed

        self._update_excursions(self.position, price)

        if self.position.direction == "long":
            if price <= self.position.current_sl:
                closed.append(self._close_position(price, timestamp, "loss", "stop_hit"))
            elif price >= self.position.current_tp:
                closed.append(self._close_position(price, timestamp, "win", "tp_hit"))
        else:
            if price >= self.position.current_sl:
                closed.append(self._close_position(price, timestamp, "loss", "stop_hit"))
            elif price <= self.position.current_tp:
                closed.append(self._close_position(price, timestamp, "win", "tp_hit"))
        return closed

    def on_bar_close(
        self,
        *,
        bar_index: int,
        timestamp: str,
        price: float,
        atr: float,
        signals: list[dict[str, Any]],
        market_decision: dict[str, Any],
        state: dict[str, Any],
    ) -> list[ClosedTrade]:
        closed: list[ClosedTrade] = []

        if self.pending is not None and self._should_cancel_pending(
            self.pending,
            price=price,
            atr=atr,
            bar_index=bar_index,
            market_decision=market_decision,
            signals=signals,
        ):
            self._log_event(
                "cancel_pending",
                bar_index,
                timestamp,
                {
                    "signal": self.pending.signal_name,
                    "direction": self.pending.direction,
                    "entry": self.pending.entry_price,
                },
            )
            self.pending = None

        if self.position is not None:
            self.position.bars_held += 1
            self._tighten_risk(self.position, price)
            exit_reason = self._context_exit_reason(
                self.position,
                price=price,
                atr=atr,
                market_decision=market_decision,
                signals=signals,
                state=state,
            )
            if exit_reason:
                outcome = "win" if self._signed_pnl(self.position.direction, self.position.entry_price, price) > 0 else "scratch"
                closed.append(self._close_position(price, timestamp, outcome, exit_reason))

        if self.has_risk():
            return closed

        candidate = self._choose_signal(
            signals=signals,
            market_decision=market_decision,
            price=price,
            atr=atr,
            bar_index=bar_index,
        )
        if candidate is None:
            return closed

        self.pending = PendingOrder(
            signal_id=str(candidate["id"]),
            signal_name=str(candidate.get("name") or "UNKNOWN"),
            source_type=str(candidate.get("source_type") or ""),
            direction=str(candidate["direction"]),
            entry_price=round(_f(candidate["entry"]), 2),
            sl_price=round(_f(candidate["sl"]), 2),
            tp_price=round(_f(candidate["tp1"]), 2),
            risk_pts=round(abs(_f(candidate["entry"]) - _f(candidate["sl"])), 2),
            confidence=_i(candidate.get("confidence_pct")),
            cluster=_cluster(candidate),
            created_bar=bar_index,
            created_time=timestamp,
            decision_bias=str(market_decision.get("bias") or "neutral"),
            decision_confidence=_i(market_decision.get("confidence")),
            selection_score=round(_f(candidate.get("_exec_score")), 2),
            notes=list(candidate.get("_exec_notes") or []),
        )
        self._log_event(
            "place_pending",
            bar_index,
            timestamp,
            {
                "signal": self.pending.signal_name,
                "direction": self.pending.direction,
                "entry": self.pending.entry_price,
                "sl": self.pending.sl_price,
                "tp": self.pending.tp_price,
                "score": self.pending.selection_score,
            },
        )
        return closed

    def flatten(self, *, price: float, timestamp: str, reason: str) -> list[ClosedTrade]:
        closed: list[ClosedTrade] = []
        if self.pending is not None:
            self.pending = None
        if self.position is not None:
            outcome = "win" if self._signed_pnl(self.position.direction, self.position.entry_price, price) > 0 else "scratch"
            closed.append(self._close_position(price, timestamp, outcome, reason))
        return closed

    def _choose_signal(
        self,
        *,
        signals: list[dict[str, Any]],
        market_decision: dict[str, Any],
        price: float,
        atr: float,
        bar_index: int,
    ) -> dict[str, Any] | None:
        decision_bias = str(market_decision.get("bias") or "neutral").lower()
        decision_conf = _i(market_decision.get("confidence"))
        action = str(market_decision.get("action") or "wait").lower()
        if decision_bias not in {"long", "short"}:
            return None
        if action in {"wait", "watch_rotation", "unavailable"}:
            return None
        if self.session_trade_count >= self.config.session_trade_cap:
            return None
        if self.session_direction_counts[decision_bias] >= self.config.session_direction_cap:
            return None

        eligible = self.rank_signals(
            signals=signals,
            market_decision=market_decision,
            price=price,
            atr=atr,
            bar_index=bar_index,
        )
        if not eligible:
            return None
        return eligible[0]

    def rank_signals(
        self,
        *,
        signals: list[dict[str, Any]],
        market_decision: dict[str, Any],
        price: float,
        atr: float,
        bar_index: int,
    ) -> list[dict[str, Any]]:
        decision_bias = str(market_decision.get("bias") or "neutral").lower()
        decision_conf = _i(market_decision.get("confidence"))
        action = str(market_decision.get("action") or "wait").lower()
        if decision_bias not in {"long", "short"}:
            return []
        if action in {"wait", "watch_rotation", "unavailable"}:
            return []
        if self.session_trade_count >= self.config.session_trade_cap:
            return []
        if self.session_direction_counts[decision_bias] >= self.config.session_direction_cap:
            return []

        eligible: list[dict[str, Any]] = []
        for sig in signals:
            if str(sig.get("direction") or "").lower() != decision_bias:
                continue
            if not self._is_tradeable_signal(sig):
                continue
            if not self._has_directional_levels(sig):
                continue

            risk_pts = abs(_f(sig.get("entry")) - _f(sig.get("sl")))
            if risk_pts <= 0:
                continue
            if risk_pts > self.config.max_initial_risk_points:
                continue

            lead_kind = str(sig.get("lead_signal_kind") or sig.get("signal_kind") or "").lower()
            if lead_kind == "ns_mtf_confluence" and risk_pts > self.config.max_confluence_risk_points:
                continue

            target_pts = abs(_f(sig.get("tp1")) - _f(sig.get("entry")))
            target_r = target_pts / max(risk_pts, 0.25)
            if target_r < self.config.min_target_r_multiple:
                continue

            confidence = _i(sig.get("confidence_pct"))
            cluster = _cluster(sig)
            dist_atr = abs(_f(sig.get("entry")) - price) / max(atr, 0.25)
            notes: list[str] = []

            if self._on_cooldown(sig, bar_index=bar_index):
                continue

            strict_ok = (
                confidence >= self.config.min_signal_confidence
                and decision_conf >= self.config.min_decision_confidence
                and dist_atr <= self.config.max_entry_dist_atr
            )
            soft_ok = (
                confidence >= self.config.soft_signal_confidence
                and cluster >= self.config.soft_cluster
                and decision_conf >= self.config.min_decision_confidence
                and dist_atr <= (self.config.max_entry_dist_atr + 0.35)
            )
            if not strict_ok and not soft_ok:
                continue

            score = float(confidence)
            score += decision_conf * 0.35
            score += cluster * 4.0
            score += _quality_bonus(sig)
            score -= dist_atr * 9.0
            score -= max(0.0, risk_pts - max(atr * 0.85, 2.0)) * 2.0
            score += min(target_r, 0.75) * 8.0

            source_type = str(sig.get("source_type") or "").lower()
            if "break_retest" in source_type:
                score += 4.0
                notes.append("break_retest")
            if "fvg_fill" in source_type:
                score += 3.0
                notes.append("fvg_fill")
            if "pullback" in source_type or "reclaim" in source_type:
                score += 2.0
                notes.append("pullback_or_reclaim")
            if cluster >= self.config.strong_cluster:
                score += 6.0
                notes.append("strong_cluster")
            if not strict_ok:
                score -= 5.0
                notes.append("soft_fallback")
            if "macro_swing" in source_type and decision_conf < 78:
                score -= 8.0
                notes.append("macro_swing_needs_stronger_context")
            if target_r < 0.25:
                score -= 3.0
                notes.append("tight_target")

            item = dict(sig)
            item["_exec_score"] = round(score, 2)
            item["_exec_notes"] = notes
            eligible.append(item)

        eligible.sort(
            key=lambda s: (
                _f(s.get("_exec_score")),
                _i(s.get("confidence_pct")),
                _cluster(s),
            ),
            reverse=True,
        )
        return eligible

    def _is_tradeable_signal(self, sig: dict[str, Any]) -> bool:
        source_type = str(sig.get("source_type") or "").lower()
        signal_name = str(sig.get("name") or "").upper()
        entry_mode = str(sig.get("entry_mode") or sig.get("entry_type") or "").lower()
        engine_mode = str(sig.get("engine_mode") or "").lower()
        lead_kind = str(sig.get("lead_signal_kind") or sig.get("signal_kind") or "").lower()

        if not self.config.allow_legacy_fallback:
            if "legacy_fallback" in engine_mode or entry_mode == "legacy_composite_fallback":
                return False
            if signal_name and not signal_name.startswith("FINAL_MTF_"):
                return False
        if entry_mode in {"micro_confirm_wait"}:
            return False
        if "pending" in engine_mode:
            return False

        allowed_kinds = {
            "ns_mtf_confluence",
            "ns_ifvg_reclaim",
            "ns_macro_swing",
            "ns_break_retest_pro",
            "ns_absorption_sweep",
            "ns_delta_streak",
            "ns_vwap_mean_reversion",
        }
        if lead_kind and lead_kind not in allowed_kinds:
            return False
        if lead_kind == "ns_vwap_mean_reversion" and str(sig.get("direction") or "").lower() == "long":
            return False
        return True

    def _has_directional_levels(self, sig: dict[str, Any]) -> bool:
        direction = str(sig.get("direction") or "").lower()
        entry = _f(sig.get("entry"))
        sl = _f(sig.get("sl"))
        tp = _f(sig.get("tp1"))
        if direction == "long":
            return tp > entry and sl < entry
        if direction == "short":
            return tp < entry and sl > entry
        return False

    def _on_cooldown(self, sig: dict[str, Any], *, bar_index: int) -> bool:
        direction = str(sig.get("direction") or "").lower()
        family = self._family_key(sig)
        fam_key = (family, direction)
        if fam_key in self._family_last_close_bar:
            last_bar = self._family_last_close_bar[fam_key]
            last_outcome = self._family_last_outcome.get(fam_key, "loss")
            cooldown = (
                self.config.family_cooldown_win_bars
                if last_outcome in {"win", "scratch"}
                else self.config.family_cooldown_loss_bars
            )
            if (bar_index - last_bar) < cooldown:
                return True

        zone_bucket = self._zone_bucket(sig)
        zone_key = (family, direction, zone_bucket)
        last_zone_bar = self._zone_last_close_bar.get(zone_key)
        if last_zone_bar is not None and (bar_index - last_zone_bar) < self.config.zone_cooldown_bars:
            return True
        return False

    def _family_key(self, sig: dict[str, Any] | ManagedPosition) -> str:
        if isinstance(sig, ManagedPosition):
            source_type = sig.source_type
            signal_name = sig.signal_name
        else:
            source_type = str(sig.get("lead_signal_kind") or sig.get("signal_kind") or sig.get("source_type") or "")
            signal_name = str(sig.get("name") or "")
        return (source_type or signal_name).lower()

    def _zone_bucket(self, sig: dict[str, Any] | ManagedPosition) -> int:
        entry = sig.entry_price if isinstance(sig, ManagedPosition) else _f(sig.get("entry"))
        width = max(self.config.zone_reuse_points, 1.0)
        return int(round(entry / width))

    def _should_cancel_pending(
        self,
        pending: PendingOrder,
        *,
        price: float,
        atr: float,
        bar_index: int,
        market_decision: dict[str, Any],
        signals: list[dict[str, Any]],
    ) -> bool:
        if (bar_index - pending.created_bar) >= self.config.max_pending_bars:
            return True
        if abs(price - pending.entry_price) / max(atr, 0.25) > (self.config.max_entry_dist_atr + 0.9):
            return True

        decision_bias = str(market_decision.get("bias") or "neutral").lower()
        decision_conf = _i(market_decision.get("confidence"))
        if decision_bias and decision_bias != pending.direction and decision_conf >= self.config.soft_flip_confidence:
            return True

        opp_best = 0
        opp_cluster = 0
        for sig in signals:
            if str(sig.get("direction") or "").lower() == pending.direction:
                continue
            opp_best = max(opp_best, _i(sig.get("confidence_pct")))
            opp_cluster = max(opp_cluster, _cluster(sig))
        if opp_best >= self.config.hard_flip_confidence or (
            opp_best >= self.config.soft_flip_confidence and opp_cluster >= self.config.strong_cluster
        ):
            return True
        return False

    @staticmethod
    def _pending_filled(order: PendingOrder, price: float) -> bool:
        if order.direction == "long":
            return price <= order.entry_price
        return price >= order.entry_price

    def _open_from_pending(
        self,
        pending: PendingOrder,
        fill_price: float,
        timestamp: str,
        bar_index: int,
    ) -> ManagedPosition:
        entry = pending.entry_price
        return ManagedPosition(
            signal_id=pending.signal_id,
            signal_name=pending.signal_name,
            source_type=pending.source_type,
            direction=pending.direction,
            entry_price=entry,
            sl_price=pending.sl_price,
            tp_price=pending.tp_price,
            initial_risk=pending.risk_pts,
            confidence=pending.confidence,
            cluster=pending.cluster,
            opened_bar=bar_index,
            opened_time=timestamp,
            decision_bias=pending.decision_bias,
            decision_confidence=pending.decision_confidence,
            current_sl=pending.sl_price,
            current_tp=pending.tp_price,
            max_favorable_price=fill_price,
            max_adverse_price=fill_price,
            notes=list(pending.notes),
        )

    def _update_excursions(self, pos: ManagedPosition, price: float) -> None:
        if pos.direction == "long":
            pos.max_favorable_price = max(pos.max_favorable_price, price)
            pos.max_adverse_price = min(pos.max_adverse_price, price)
        else:
            if pos.max_favorable_price == 0.0:
                pos.max_favorable_price = price
            if pos.max_adverse_price == 0.0:
                pos.max_adverse_price = price
            pos.max_favorable_price = min(pos.max_favorable_price, price)
            pos.max_adverse_price = max(pos.max_adverse_price, price)

    def _tighten_risk(self, pos: ManagedPosition, price: float) -> None:
        progress_r = self._progress_r(pos, price)
        if progress_r >= self.config.be_progress_r:
            if pos.direction == "long":
                pos.current_sl = max(pos.current_sl, pos.entry_price + self.config.break_even_buffer)
            else:
                pos.current_sl = min(pos.current_sl, pos.entry_price - self.config.break_even_buffer)
        if progress_r >= self.config.lock_progress_r:
            lock_points = max(self.config.lock_profit_r * pos.initial_risk, 0.5)
            if pos.direction == "long":
                pos.current_sl = max(pos.current_sl, pos.entry_price + lock_points)
            else:
                pos.current_sl = min(pos.current_sl, pos.entry_price - lock_points)

    def _context_exit_reason(
        self,
        pos: ManagedPosition,
        *,
        price: float,
        atr: float,
        market_decision: dict[str, Any],
        signals: list[dict[str, Any]],
        state: dict[str, Any],
    ) -> str | None:
        progress_r = self._progress_r(pos, price)
        decision_bias = str(market_decision.get("bias") or "neutral").lower()
        decision_conf = _i(market_decision.get("confidence"))
        action = str(market_decision.get("action") or "wait").lower()

        same_best = 0
        same_cluster = 0
        opp_best = 0
        opp_cluster = 0
        for sig in signals:
            direction = str(sig.get("direction") or "").lower()
            if direction == pos.direction:
                same_best = max(same_best, _i(sig.get("confidence_pct")))
                same_cluster = max(same_cluster, _cluster(sig))
            else:
                opp_best = max(opp_best, _i(sig.get("confidence_pct")))
                opp_cluster = max(opp_cluster, _cluster(sig))

        hard_flip = (
            decision_bias not in {pos.direction, "rotation", "neutral"}
            and decision_conf >= self.config.hard_flip_confidence
        )
        if hard_flip and (opp_best >= self.config.hard_flip_confidence or opp_cluster >= self.config.strong_cluster):
            return "opposite_hard_flip"

        if pos.bars_held >= self.config.max_position_bars:
            return "time_exit"

        if progress_r >= self.config.soft_exit_progress_r:
            weak_same_side = same_best < max(60, pos.confidence - 18)
            soft_flip = (
                decision_bias not in {pos.direction}
                and decision_conf >= self.config.soft_flip_confidence
            )
            if soft_flip and (opp_best >= self.config.soft_flip_confidence or opp_cluster >= self.config.soft_cluster):
                return "protect_profit_flip"
            if action in {"wait", "watch_rotation"} and weak_same_side:
                return "protect_profit_rotation"

        if progress_r > 0 and same_best == 0 and opp_best >= self.config.soft_flip_confidence:
            return "fade_exit"

        continuation = (state.get("trader_guide") or {}).get("continuation") or {}
        cont_side = str(continuation.get("side") or "neutral").lower()
        cont_valid = bool(continuation.get("valid"))
        if cont_side != pos.direction and not cont_valid and progress_r > 0:
            return "guide_continuation_lost"

        if abs(price - _f(state.get("vwap"))) / max(atr, 0.25) > 2.4 and progress_r < -0.35:
            return "stretched_against"
        return None

    def _close_position(
        self,
        price: float,
        timestamp: str,
        outcome: str,
        reason: str,
    ) -> ClosedTrade:
        if self.position is None:
            raise RuntimeError("No open position to close.")
        pos = self.position
        signed = self._signed_pnl(pos.direction, pos.entry_price, price)
        gross_r = signed / max(pos.initial_risk, 0.25)
        if pos.direction == "long":
            mfe = pos.max_favorable_price - pos.entry_price
            mae = pos.entry_price - pos.max_adverse_price
        else:
            mfe = pos.entry_price - pos.max_favorable_price
            mae = pos.max_adverse_price - pos.entry_price
        closed = ClosedTrade(
            signal_id=pos.signal_id,
            signal_name=pos.signal_name,
            source_type=pos.source_type,
            direction=pos.direction,
            entry_time=pos.opened_time,
            exit_time=timestamp,
            entry_price=round(pos.entry_price, 2),
            exit_price=round(price, 2),
            sl_price=round(pos.sl_price, 2),
            tp_price=round(pos.tp_price, 2),
            initial_risk=round(pos.initial_risk, 2),
            bars_held=pos.bars_held,
            confidence=pos.confidence,
            cluster=pos.cluster,
            gross_points=round(signed, 2),
            gross_r=round(gross_r, 2),
            outcome=outcome,
            exit_reason=reason,
            mfe_points=round(max(mfe, 0.0), 2),
            mae_points=round(max(mae, 0.0), 2),
            decision_bias=pos.decision_bias,
            decision_confidence=pos.decision_confidence,
            notes=list(pos.notes),
        )
        self.position = None
        self.closed_trades.append(closed)
        fam_key = (self._family_key(pos), pos.direction)
        self._family_last_close_bar[fam_key] = pos.opened_bar + pos.bars_held
        self._family_last_outcome[fam_key] = "win" if closed.gross_points > 0 else "loss"
        zone_key = (self._family_key(pos), pos.direction, self._zone_bucket(pos))
        self._zone_last_close_bar[zone_key] = pos.opened_bar + pos.bars_held
        self._log_event(
            "close",
            pos.opened_bar + pos.bars_held,
            timestamp,
            {
                "signal": closed.signal_name,
                "direction": closed.direction,
                "exit_reason": closed.exit_reason,
                "gross_points": closed.gross_points,
                "gross_r": closed.gross_r,
            },
        )
        return closed

    @staticmethod
    def _signed_pnl(direction: str, entry_price: float, exit_price: float) -> float:
        if direction == "long":
            return exit_price - entry_price
        return entry_price - exit_price

    def _progress_r(self, pos: ManagedPosition, price: float) -> float:
        return self._signed_pnl(pos.direction, pos.entry_price, price) / max(pos.initial_risk, 0.25)

    def _log_event(self, kind: str, bar_index: int, timestamp: str, payload: dict[str, Any]) -> None:
        item = {"kind": kind, "bar_index": int(bar_index), "timestamp": timestamp}
        item.update(payload)
        self.event_log.append(item)
        self.event_log = self.event_log[-500:]

    def export_state(self) -> dict[str, Any]:
        return {
            "pending": asdict(self.pending) if self.pending else None,
            "position": asdict(self.position) if self.position else None,
            "closed_trades": [asdict(t) for t in self.closed_trades[-20:]],
            "events": list(self.event_log[-50:]),
        }
