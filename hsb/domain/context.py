"""Typed analysis context — replaces the untyped extras dict from V1.

Every field that previously lived inside ``MarketContext.extras`` now has an
explicit type and a default value.  This eliminates:

* ``KeyError`` at runtime when a module expects a key that was never set.
* Silent ``None`` returns from ``.get()`` that propagate downstream.
* Inability to discover the API surface via IDE autocomplete.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime

import pandas as pd

from hsb.domain.enums import MacroRegime


# ---------------------------------------------------------------------------
# Sub-contexts — each represents a tightly-scoped slice of information.
# ---------------------------------------------------------------------------

@dataclass(slots=True)
class BarData:
    """DataFrames available for analysis."""

    bars_df: pd.DataFrame = field(default_factory=pd.DataFrame)
    macro_bars_df: pd.DataFrame = field(default_factory=pd.DataFrame)
    micro_bars_df: pd.DataFrame = field(default_factory=pd.DataFrame)
    ticks_df: pd.DataFrame = field(default_factory=pd.DataFrame)

    # Execution-specific slices (may differ from analysis slices)
    execution_bars_df: pd.DataFrame = field(default_factory=pd.DataFrame)
    execution_ticks_df: pd.DataFrame = field(default_factory=pd.DataFrame)


@dataclass(slots=True)
class RegimeInfo:
    """Deterministic regime classification output."""

    regime: str = "transition"
    move_from_open: float = 0.0
    directional_efficiency: float = 0.0
    total_path: float = 0.0
    bias: str = "neutral"


@dataclass(slots=True)
class PositionState:
    """Current broker position state."""

    direction: str = "flat"
    qty: int = 0
    avg_price: float = 0.0

    @property
    def is_flat(self) -> bool:
        return self.qty == 0 or self.direction == "flat"

    @property
    def raw(self) -> str:
        if self.is_flat:
            return "FLAT"
        return f"{self.direction.upper()}_{self.qty}"


@dataclass(slots=True)
class GateConfig:
    """15-second micro gate settings."""

    enabled: bool = False
    profile: str = "off"  # off | strict | chop | transition


@dataclass(slots=True)
class SessionLevels:
    """Key price levels for the current session."""

    vwap: float = 0.0
    ema20: float = 0.0
    ema50: float = 0.0
    prev_day_high: float = 0.0
    prev_day_low: float = 0.0
    orb_high: float = 0.0
    orb_low: float = 0.0
    asia_high: float = 0.0
    asia_low: float = 0.0
    london_high: float = 0.0
    london_low: float = 0.0
    extras: dict[str, float] = field(default_factory=dict)


# ---------------------------------------------------------------------------
# Main context object passed through the pipeline.
# ---------------------------------------------------------------------------

@dataclass(slots=True)
class AnalysisContext:
    """Typed context replacing the V1 ``MarketContext.extras`` dict.

    This object is constructed once per cycle by the ``ContextBuilder`` and
    passed immutably through signals → filters → director → execution.
    Individual modules read the fields they need — no reaching into an
    opaque dict.
    """

    # --- Timestamps & identification ---
    timestamp: datetime = field(default_factory=lambda: datetime.min)
    session: str = "bars"
    day: str = ""
    source: str = ""

    # --- Market regime ---
    regime: RegimeInfo = field(default_factory=RegimeInfo)
    atr: float = 0.0
    cvd: float | None = None
    move_from_open: float | None = None

    # --- Bar / tick data ---
    bar_data: BarData = field(default_factory=BarData)

    # --- Position ---
    position: PositionState = field(default_factory=PositionState)
    require_flat_position: bool = True

    # --- Session levels & structure ---
    session_levels: SessionLevels = field(default_factory=SessionLevels)
    htf_inventory: dict = field(default_factory=dict)

    # --- Gate config ---
    gate: GateConfig = field(default_factory=GateConfig)

    # --- Pipeline metadata ---
    live_mode: bool = False
    current_bar_index: int = 0

    # --- Director state (set during pipeline run) ---
    macro_regime: MacroRegime = MacroRegime.TRANSITION
