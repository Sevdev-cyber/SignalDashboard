"""Bar-level price path simulator — shared engine for all policies.

MNQ tick constants:
    TICK_SIZE  = 0.25
    TICK_VALUE = 0.50  (per tick per contract)
    COMM_PER_SIDE = 0.62
"""

from __future__ import annotations

from dataclasses import dataclass, field

import pandas as pd

from hsb.domain.context import AnalysisContext
from hsb.domain.enums import Direction, TradeStatus
from hsb.domain.models import TradePlan, TradeResult

TICK_SIZE = 0.25
TICK_VALUE = 0.50
COMM_PER_SIDE = 0.62


@dataclass
class SimState:
    """Mutable simulation state tracked bar-by-bar."""

    hwm_price: float = 0.0
    current_sl: float = 0.0
    original_risk: float = 0.0
    tp1_hit: bool = False
    tp2_hit: bool = False
    events: list[str] = field(default_factory=list)


class Simulator:
    """Stateless bar-level trade simulator."""

    def simulate_be_trail(
        self,
        plan: TradePlan,
        context: AnalysisContext,
        *,
        be_buffer_ticks: int = 4,
        trail_mult: float = 0.25,
        trail_min_points: float = 3.0,
        max_bars: int = 40,
    ) -> TradeResult:
        bars = self._execution_bars(context, plan)
        if bars.empty:
            return self._blocked(plan, "no execution bars")

        state = SimState(
            hwm_price=plan.entry_price,
            current_sl=plan.sl_price,
            original_risk=abs(plan.entry_price - plan.sl_price),
        )

        for i in range(min(len(bars), max_bars)):
            row = bars.iloc[i]
            high = float(row.get("high", plan.entry_price))
            low = float(row.get("low", plan.entry_price))

            result = (
                self._bar_long(plan, state, high=high, low=low,
                               be_buffer_ticks=be_buffer_ticks,
                               trail_mult=trail_mult, trail_min=trail_min_points)
                if plan.direction == Direction.LONG
                else self._bar_short(plan, state, high=high, low=low,
                                     be_buffer_ticks=be_buffer_ticks,
                                     trail_mult=trail_mult, trail_min=trail_min_points)
            )
            if result is not None:
                result.bars_held = i + 1
                return result

        return self._make_result(plan, state, plan.entry_price, TradeStatus.CLOSED_TIME,
                                 state.events + ["max_bars_exit"], max_bars)

    def simulate_tp1_lock(
        self,
        plan: TradePlan,
        context: AnalysisContext,
        *,
        be_buffer_ticks: int = 4,
        trail_offset_points: float = 40.0,
        tp1_lock_buffer_points: float = 5.0,
        max_bars: int = 40,
    ) -> TradeResult:
        bars = self._execution_bars(context, plan)
        if bars.empty:
            return self._blocked(plan, "no execution bars")

        state = SimState(
            hwm_price=plan.entry_price,
            current_sl=plan.sl_price,
            original_risk=abs(plan.entry_price - plan.sl_price),
        )

        for i in range(min(len(bars), max_bars)):
            row = bars.iloc[i]
            high = float(row.get("high", plan.entry_price))
            low = float(row.get("low", plan.entry_price))

            result = (
                self._bar_long_tp1_lock(plan, state, high=high, low=low,
                                        be_buffer_ticks=be_buffer_ticks,
                                        trail_offset=trail_offset_points,
                                        tp1_lock_buffer=tp1_lock_buffer_points)
                if plan.direction == Direction.LONG
                else self._bar_short_tp1_lock(plan, state, high=high, low=low,
                                              be_buffer_ticks=be_buffer_ticks,
                                              trail_offset=trail_offset_points,
                                              tp1_lock_buffer=tp1_lock_buffer_points)
            )
            if result is not None:
                result.bars_held = i + 1
                return result

        return self._make_result(plan, state, plan.entry_price, TradeStatus.CLOSED_TIME,
                                 state.events + ["max_bars_exit"], max_bars)

    # ------------------------------------------------------------------
    # Long bar processing — BE+Trail
    # ------------------------------------------------------------------

    def _bar_long(self, plan: TradePlan, state: SimState, *,
                  high: float, low: float,
                  be_buffer_ticks: int, trail_mult: float, trail_min: float) -> TradeResult | None:
        if high > state.hwm_price:
            state.hwm_price = high

        if not state.tp1_hit and high >= plan.tp1_price:
            state.tp1_hit = True
            state.events.append(f"tp1:{plan.tp1_price:.2f}")
            state.current_sl = max(state.current_sl, plan.entry_price + be_buffer_ticks * TICK_SIZE)
            state.events.append(f"be_move:{state.current_sl:.2f}")

        if state.tp1_hit and not state.tp2_hit and high >= plan.tp2_price:
            state.tp2_hit = True
            state.events.append(f"tp2:{plan.tp2_price:.2f}")

        if high >= plan.tp3_price:
            return self._make_result(plan, state, plan.tp3_price, TradeStatus.CLOSED_TP3,
                                     state.events + [f"tp3:{plan.tp3_price:.2f}"], 0)

        if state.tp1_hit:
            trail_dist = max(state.original_risk * trail_mult, trail_min)
            new_trail = state.hwm_price - trail_dist
            if new_trail > state.current_sl:
                state.current_sl = new_trail
                state.events.append(f"trail:{state.current_sl:.2f}")

        if low <= state.current_sl:
            status = TradeStatus.CLOSED_TRAIL if state.tp1_hit and state.current_sl > plan.entry_price else TradeStatus.CLOSED_SL
            return self._make_result(plan, state, state.current_sl, status,
                                     state.events + [f"exit_sl:{state.current_sl:.2f}"], 0)
        return None

    # ------------------------------------------------------------------
    # Short bar processing — BE+Trail
    # ------------------------------------------------------------------

    def _bar_short(self, plan: TradePlan, state: SimState, *,
                   high: float, low: float,
                   be_buffer_ticks: int, trail_mult: float, trail_min: float) -> TradeResult | None:
        if state.hwm_price == plan.entry_price or low < state.hwm_price:
            state.hwm_price = low

        if not state.tp1_hit and low <= plan.tp1_price:
            state.tp1_hit = True
            state.events.append(f"tp1:{plan.tp1_price:.2f}")
            state.current_sl = min(state.current_sl, plan.entry_price - be_buffer_ticks * TICK_SIZE)
            state.events.append(f"be_move:{state.current_sl:.2f}")

        if state.tp1_hit and not state.tp2_hit and low <= plan.tp2_price:
            state.tp2_hit = True
            state.events.append(f"tp2:{plan.tp2_price:.2f}")

        if low <= plan.tp3_price:
            return self._make_result(plan, state, plan.tp3_price, TradeStatus.CLOSED_TP3,
                                     state.events + [f"tp3:{plan.tp3_price:.2f}"], 0)

        if state.tp1_hit:
            trail_dist = max(state.original_risk * trail_mult, trail_min)
            new_trail = state.hwm_price + trail_dist
            if new_trail < state.current_sl:
                state.current_sl = new_trail
                state.events.append(f"trail:{state.current_sl:.2f}")

        if high >= state.current_sl:
            status = TradeStatus.CLOSED_TRAIL if state.tp1_hit and state.current_sl < plan.entry_price else TradeStatus.CLOSED_SL
            return self._make_result(plan, state, state.current_sl, status,
                                     state.events + [f"exit_sl:{state.current_sl:.2f}"], 0)
        return None

    # ------------------------------------------------------------------
    # TP1 Lock variants
    # ------------------------------------------------------------------

    def _bar_long_tp1_lock(self, plan: TradePlan, state: SimState, *,
                           high: float, low: float,
                           be_buffer_ticks: int, trail_offset: float, tp1_lock_buffer: float) -> TradeResult | None:
        if high > state.hwm_price:
            state.hwm_price = high

        if not state.tp1_hit and high >= plan.tp1_price:
            state.tp1_hit = True
            state.events.append(f"tp1_lock:{plan.tp1_price:.2f}")
            be_price = plan.entry_price + be_buffer_ticks * TICK_SIZE
            locked = plan.tp1_price - tp1_lock_buffer
            state.current_sl = max(state.current_sl, locked, be_price)
            state.events.append(f"lock_sl:{state.current_sl:.2f}")

        if state.tp1_hit and not state.tp2_hit and high >= plan.tp2_price:
            state.tp2_hit = True
            state.events.append(f"tp2:{plan.tp2_price:.2f}")
            return self._make_result(plan, state, plan.tp2_price, TradeStatus.CLOSED_TP2, state.events, 0)

        if state.tp1_hit:
            new_trail = state.hwm_price - trail_offset
            if new_trail > state.current_sl:
                state.current_sl = new_trail
                state.events.append(f"trail:{state.current_sl:.2f}")

        if low <= state.current_sl:
            status = TradeStatus.CLOSED_TRAIL if state.tp1_hit else TradeStatus.CLOSED_SL
            return self._make_result(plan, state, state.current_sl, status,
                                     state.events + [f"exit_sl:{state.current_sl:.2f}"], 0)
        return None

    def _bar_short_tp1_lock(self, plan: TradePlan, state: SimState, *,
                            high: float, low: float,
                            be_buffer_ticks: int, trail_offset: float, tp1_lock_buffer: float) -> TradeResult | None:
        if state.hwm_price == plan.entry_price or low < state.hwm_price:
            state.hwm_price = low

        if not state.tp1_hit and low <= plan.tp1_price:
            state.tp1_hit = True
            state.events.append(f"tp1_lock:{plan.tp1_price:.2f}")
            be_price = plan.entry_price - be_buffer_ticks * TICK_SIZE
            locked = plan.tp1_price + tp1_lock_buffer
            state.current_sl = min(state.current_sl, locked, be_price)
            state.events.append(f"lock_sl:{state.current_sl:.2f}")

        if state.tp1_hit and not state.tp2_hit and low <= plan.tp2_price:
            state.tp2_hit = True
            state.events.append(f"tp2:{plan.tp2_price:.2f}")
            return self._make_result(plan, state, plan.tp2_price, TradeStatus.CLOSED_TP2, state.events, 0)

        if state.tp1_hit:
            new_trail = state.hwm_price + trail_offset
            if new_trail < state.current_sl:
                state.current_sl = new_trail
                state.events.append(f"trail:{state.current_sl:.2f}")

        if high >= state.current_sl:
            status = TradeStatus.CLOSED_TRAIL if state.tp1_hit else TradeStatus.CLOSED_SL
            return self._make_result(plan, state, state.current_sl, status,
                                     state.events + [f"exit_sl:{state.current_sl:.2f}"], 0)
        return None

    # ------------------------------------------------------------------
    # Helpers
    # ------------------------------------------------------------------

    def _execution_bars(self, context: AnalysisContext, plan: TradePlan) -> pd.DataFrame:
        bars = context.bar_data.execution_bars_df
        if bars.empty:
            bars = context.bar_data.bars_df
        if bars.empty:
            return bars
        idx = plan.metadata.get("start_bar_index")
        if idx is not None:
            return bars.iloc[int(idx):].reset_index(drop=True)
        return bars

    def _make_result(
        self,
        plan: TradePlan,
        state: SimState,
        exit_price: float,
        status: TradeStatus,
        events: list[str],
        bars_held: int,
    ) -> TradeResult:
        if plan.contracts <= 0:
            return TradeResult(
                plan_id=plan.candidate_id,
                status=TradeStatus.BLOCKED,
                events=events + ["blocked_zero_contracts"],
            )

        if plan.direction == Direction.LONG:
            gross = (exit_price - plan.entry_price) / TICK_SIZE * TICK_VALUE * plan.contracts
        else:
            gross = (plan.entry_price - exit_price) / TICK_SIZE * TICK_VALUE * plan.contracts

        commission = COMM_PER_SIDE * 2 * plan.contracts

        return TradeResult(
            plan_id=plan.candidate_id,
            status=status,
            pnl=round(gross - commission, 2),
            bars_held=bars_held,
            events=events,
            metadata={
                "exit_price": round(exit_price, 2),
                "gross_pnl": round(gross, 2),
                "commission": round(commission, 2),
                "contracts": plan.contracts,
                "tp1_hit": state.tp1_hit,
                "tp2_hit": state.tp2_hit,
                "hwm_price": round(state.hwm_price, 2),
                "final_sl": round(state.current_sl, 2),
            },
        )

    def _blocked(self, plan: TradePlan, reason: str) -> TradeResult:
        return TradeResult(
            plan_id=plan.candidate_id,
            status=TradeStatus.BLOCKED,
            events=[f"blocked:{reason}"],
        )
