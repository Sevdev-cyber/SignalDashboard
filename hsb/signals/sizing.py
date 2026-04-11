"""Dynamic contract sizing based on signal quality tier.

Maps signal source_type → contract count using historical performance data.
Updated from 136-day backtest with DedupeFilter(6/2) across Apr 2024-Mar 2026.

Tier 1 (3 contracts): PF > 3.0, WR > 55% — highest conviction
Tier 2 (2 contracts): PF 2.0–3.0 — strong edge
Tier 3 (1 contract):  PF 1.0–2.0 — marginal edge, size conservatively
"""

from __future__ import annotations

# Signal tag → contract count
# Tags are matched against source_type or first reason via substring match
_TIER_MAP: dict[str, int] = {
    # ── Tier 1: 3 contracts (PF > 3.0) ──
    "trend_cont": 3,        # PF=6.5, 62% WR, $306/trade
    "vwap_reclaim": 3,      # PF=3.7, 48% WR, $42/trade
    "vwap_loss": 3,         # PF=3.0, 48% WR, $35/trade

    # ── Tier 2: 2 contracts (PF 2.0–3.0) ──
    "higher_low": 2,        # PF=2.5, 44% WR — UPGRADED from 1c ($7.9k PnL!)
    "lower_high": 2,        # PF=2.5, 48% WR — UPGRADED from 1c ($6.2k PnL!)
    "ema_bounce": 2,        # PF=2.4, 57% WR — UPGRADED from 1c ($4.1k PnL!)
    "pullback": 2,          # PF=2.4, 47% WR, $49/trade
    "fvg_bear": 2,          # PF=1.7, 50% WR, $41/trade
    "delta_divergence": 2,  # PF=1.5, 57% WR — DOWNGRADED from 3c

    # ── Tier 3: 1 contract (PF < 2.0 or small sample) ──
    "fvg_bull": 1,          # PF=1.6, 46% WR
    "choch": 1,             # Small sample
    "exhaustion": 1,        # Small sample

    # ── Reduced: previously oversized losers ──
    "delta_accel": 1,       # PF=0.2, 39% WR — DOWNGRADED from 3c (was -$3.7k!)
    "delta_exhaustion": 1,  # PF=0.0 — DOWNGRADED from 3c
    "vwap_bounce": 1,       # PF=0.1, 36% WR — DOWNGRADED from 2c (was -$1k!)
}


def get_contracts(source_type: str, reasons: list[str] | None = None) -> int:
    """Return the number of contracts for a given signal.

    Checks source_type first, then falls back to reasons[0] tag matching.
    Default: 1 contract for unknown signals.
    """
    # Check source_type
    for tag, contracts in _TIER_MAP.items():
        if tag in source_type:
            return contracts

    # Fallback: check first reason
    if reasons:
        first = reasons[0] if reasons else ""
        for tag, contracts in _TIER_MAP.items():
            if tag in first:
                return contracts

    return 1  # default: conservative
