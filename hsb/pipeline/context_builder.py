"""Context builder — constructs AnalysisContext from raw bar/tick data.

This is the piece that was mixed into ComparisonRunner in V1.  Now it has
its own module with a single clear responsibility.
"""

from __future__ import annotations

from datetime import datetime, timezone

import pandas as pd

from hsb.domain.context import AnalysisContext, BarData, GateConfig, PositionState, RegimeInfo
from hsb.pipeline.regime import infer_regime


class ContextBuilder:
    """Builds an :class:`AnalysisContext` from raw data."""

    def build(
        self,
        *,
        bars_df: pd.DataFrame,
        ticks_df: pd.DataFrame | None = None,
        macro_bars_df: pd.DataFrame | None = None,
        micro_bars_df: pd.DataFrame | None = None,
        session: str = "bars",
        day: str = "",
        source: str = "",
        position: PositionState | None = None,
        require_flat_position: bool = True,
        gate_mode: str = "off",
        live_mode: bool = False,
    ) -> AnalysisContext:
        if bars_df.empty:
            return AnalysisContext()

        bars_df = self._ensure_types(bars_df)
        current = bars_df.iloc[-1]
        current_ts = self._extract_timestamp(current)

        # Regime inference
        regime = self._infer_regime(bars_df)

        # ATR
        atr = float(current.get("atr", 20.0)) if "atr" in bars_df.columns else 20.0
        if atr <= 0:
            atr = 20.0

        # CVD
        cvd = float(current.get("cum_delta", 0.0)) if "cum_delta" in bars_df.columns else None

        # Bar data
        bar_data = BarData(
            bars_df=bars_df,
            macro_bars_df=macro_bars_df if macro_bars_df is not None else bars_df,
            micro_bars_df=micro_bars_df if micro_bars_df is not None else bars_df,
            ticks_df=ticks_df if ticks_df is not None else pd.DataFrame(),
            execution_bars_df=bars_df,
            execution_ticks_df=ticks_df if ticks_df is not None else pd.DataFrame(),
        )

        # Gate config
        gate = self._resolve_gate(gate_mode, session, regime)

        return AnalysisContext(
            timestamp=current_ts,
            session=session,
            day=day or current_ts.strftime("%Y%m%d"),
            source=source,
            regime=regime,
            atr=atr,
            cvd=cvd,
            move_from_open=regime.move_from_open,
            bar_data=bar_data,
            position=position or PositionState(),
            require_flat_position=require_flat_position,
            gate=gate,
            live_mode=live_mode,
            current_bar_index=len(bars_df) - 1,
        )

    def _infer_regime(self, bars_df: pd.DataFrame) -> RegimeInfo:
        if len(bars_df) < 2:
            return RegimeInfo()

        first = bars_df.iloc[0]
        last = bars_df.iloc[-1]
        open_price = float(first.get("open", 0.0))
        current_close = float(last.get("close", 0.0))
        move = current_close - open_price

        # Total path: sum of absolute bar-to-bar close changes
        if "close" in bars_df.columns:
            closes = pd.to_numeric(bars_df["close"], errors="coerce").dropna()
            total_path = float(closes.diff().abs().sum()) if len(closes) > 1 else abs(move)
        else:
            total_path = abs(move)

        # V2: pass EMA/VWAP/ATR for enhanced regime detection
        last = bars_df.iloc[-1]
        ema20 = float(last.get("ema_20", 0)) if "ema_20" in bars_df.columns else 0.0
        ema50 = float(last.get("ema_50", 0)) if "ema_50" in bars_df.columns else 0.0
        vwap = float(last.get("vwap", 0)) if "vwap" in bars_df.columns else 0.0
        atr = float(last.get("atr", 20)) if "atr" in bars_df.columns else 20.0
        # Previous EMA values (3 bars back for slope)
        ema20_prev = float(bars_df.iloc[-3].get("ema_20", 0)) if len(bars_df) >= 3 and "ema_20" in bars_df.columns else 0.0
        ema50_prev = float(bars_df.iloc[-3].get("ema_50", 0)) if len(bars_df) >= 3 and "ema_50" in bars_df.columns else 0.0

        return infer_regime(
            move_from_open=move,
            total_path=total_path,
            current_close=current_close,
            open_price=open_price,
            ema20=ema20,
            ema50=ema50,
            ema20_prev=ema20_prev,
            ema50_prev=ema50_prev,
            vwap=vwap,
            atr=atr,
        )

    def _resolve_gate(self, gate_mode: str, session: str, regime: RegimeInfo) -> GateConfig:
        if gate_mode == "off" or session == "15s":
            return GateConfig(enabled=False, profile="off")
        if gate_mode == "always":
            return GateConfig(enabled=True, profile="strict")
        # Conditional mode
        r = regime.regime
        eff = regime.directional_efficiency
        if r == "chop":
            return GateConfig(enabled=True, profile="chop")
        if r == "transition" or eff < 0.08:
            return GateConfig(enabled=True, profile="transition")
        return GateConfig(enabled=False, profile="off")

    def _ensure_types(self, bars: pd.DataFrame) -> pd.DataFrame:
        df = bars.copy()
        # Normalize column name: datetime → timestamp
        if "datetime" in df.columns and "timestamp" not in df.columns:
            df = df.rename(columns={"datetime": "timestamp"})
        for col in ("open", "high", "low", "close", "vwap", "atr", "ema_20", "ema_50", "delta", "cum_delta", "cvd"):
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors="coerce")
        if "timestamp" in df.columns:
            df["timestamp"] = pd.to_datetime(df["timestamp"], utc=True, errors="coerce")
        if "date" not in df.columns and "timestamp" in df.columns:
            df["date"] = df["timestamp"].dt.date
        return df

    def _extract_timestamp(self, row: pd.Series) -> datetime:
        for col in ("timestamp", "datetime"):
            ts = row.get(col)
            if ts is not None and hasattr(ts, "to_pydatetime"):
                return ts.to_pydatetime()
        return datetime.now(timezone.utc)

