"""Domain enums — the vocabulary of the trading system.

Every enum here defines a closed set of valid values. Using enums instead of
raw strings prevents typos, enables IDE autocomplete, and makes the protocol
between modules explicit.
"""

from __future__ import annotations

from enum import Enum


class Direction(str, Enum):
    LONG = "long"
    SHORT = "short"
    NEUTRAL = "neutral"


class CandidateFamily(str, Enum):
    """Which signal generator produced the candidate."""

    COMPOSITE = "composite"
    MICRO_SMC = "micro_smc"
    DELTA_ACCELERATION = "delta_acceleration"
    MTF_CONFLUENCE = "mtf_confluence"


class DirectorAction(str, Enum):
    """What the director decides for a candidate."""

    ALLOW = "allow"
    BLOCK = "block"
    REDUCE_SIZE = "reduce_size"
    RECHECK = "recheck"


class PolicyName(str, Enum):
    """Execution policy applied to a trade."""

    BASIC = "basic"
    BE_TRAIL = "be_trail"
    TP1_LOCK = "tp1_lock"
    RANGE_QUICK_EXIT = "range_quick_exit"


class MacroRegime(str, Enum):
    TREND_UP = "trend_up"
    TREND_DOWN = "trend_down"
    RANGE_DAY = "range_day"
    CHOP_DAY = "chop_day"
    TRANSITION = "transition"


class MicroRegime(str, Enum):
    LOCAL_TREND = "local_trend"
    LOCAL_CONSOLIDATION = "local_consolidation"
    RANGE = "range"
    BREAKOUT_SETUP = "breakout_setup"
    FAILED_BREAKOUT = "failed_breakout"
    NO_TRADE = "no_trade"


class StrategyName(str, Enum):
    ORB = "orb"
    IFVG_RECLAIM = "ifvg_reclaim"
    IFVG_REJECT = "ifvg_reject"
    RECLAIM_CONTINUATION = "reclaim_continuation"
    CONTINUATION_PULLBACK = "continuation_pullback"
    RANGE_REVERSAL = "range_reversal"
    SWEEP_REVERSAL = "sweep_reversal"
    BREAKOUT_RETEST = "breakout_retest"
    NO_TRADE = "no_trade"


class TriggerType(str, Enum):
    TOUCH = "touch"
    CLOSE_BACK_INSIDE = "close_back_inside"
    BREAK_AND_RETEST = "break_and_retest"
    DELTA_CONFIRM = "delta_confirm"
    SWEEP_THEN_RECLAIM = "sweep_then_reclaim"
    RETEST_AND_REJECT = "retest_and_reject"


class RiskMode(str, Enum):
    NORMAL = "normal"
    REDUCED = "reduced"
    DEFENSIVE = "defensive"


class TradeStatus(str, Enum):
    OPEN = "open"
    CLOSED_TP1 = "closed_tp1"
    CLOSED_TP2 = "closed_tp2"
    CLOSED_TP3 = "closed_tp3"
    CLOSED_TRAIL = "closed_trail"
    CLOSED_SL = "closed_sl"
    CLOSED_TIME = "closed_time"
    BLOCKED = "blocked"
