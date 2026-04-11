"""
Delta Acceleration Signal — ported from Scalper V4
═══════════════════════════════════════════════════
Detects sudden acceleration in cumulative delta (2nd derivative spike).
Unlike delta_divergence which looks at price vs delta direction mismatch,
this signal fires on the RAW MOMENTUM of buying/selling pressure.

When delta accelerates sharply (rate of change exceeds 2σ of rolling window),
it indicates a sudden influx of aggressive orders — strong directional conviction.

Entry: in the direction of acceleration
SL: recent swing against direction
"""
from __future__ import annotations

import numpy as np
import pandas as pd

from hsb.domain.context import AnalysisContext
from hsb.domain.enums import CandidateFamily, Direction
from hsb.domain.models import SignalCandidate

from hsb.signals._helpers import make_candidate


class DeltaAccelerationGenerator:
    """Detect sudden acceleration in cumulative delta.

    Ported 1:1 from V4 DeltaAccelerationSignal, adapted to V2
    SignalCandidate output format. All detection logic is preserved.
    """

    def __init__(self, config: dict | None = None) -> None:
        cfg = config or {}
        self.acceleration_z_threshold = cfg.get("acceleration_z_threshold", 2.0)
        self.lookback = cfg.get("lookback", 20)
        self.min_delta_volume = cfg.get("min_delta_volume", 10)
        self.sl_atr_mult = cfg.get("sl_atr_mult", 1.5)

    def generate(self, bars: pd.DataFrame, ctx: AnalysisContext) -> list[SignalCandidate]:
        """Scan for delta acceleration events.

        Requires columns: close, high, low, delta, atr
        Optional: timestamp
        """
        candidates = []
        if len(bars) < self.lookback + 5:
            return candidates

        if "delta" not in bars.columns:
            return candidates

        closes = bars["close"].values.astype(float)
        highs = bars["high"].values.astype(float)
        lows = bars["low"].values.astype(float)
        deltas = bars["delta"].values.astype(float)
        atrs = bars["atr"].values.astype(float) if "atr" in bars.columns else np.full(len(bars), 20.0)

        # Compute delta velocity (1st derivative) and acceleration (2nd derivative)
        delta_velocity = np.diff(deltas, prepend=deltas[0])
        delta_acceleration = np.diff(delta_velocity, prepend=delta_velocity[0])

        # Rolling stats for z-score of acceleration
        for i in range(self.lookback + 2, len(bars)):
            atr = max(atrs[i], 0.5)

            # Z-score of acceleration
            window = delta_acceleration[i - self.lookback : i]
            acc_mean = np.mean(window)
            acc_std = max(np.std(window), 1.0)
            acc_z = (delta_acceleration[i] - acc_mean) / acc_std

            # Minimum delta volume filter (avoid noise)
            recent_delta_abs = abs(deltas[i])
            if recent_delta_abs < self.min_delta_volume:
                continue

            # Also check velocity confirms direction (not just noise spike)
            vel = delta_velocity[i]

            if acc_z > self.acceleration_z_threshold and vel > 0:
                # Strong bullish acceleration
                entry = closes[i]
                # SL at recent low (exact V4 logic)
                recent_lows = lows[max(0, i - 10) : i + 1]
                sl = float(np.min(recent_lows)) - 0.25
                risk = entry - sl
                if risk <= 0:
                    continue
                # Adapt risk limits for 5min bars (V4 used 6pts on 15s/30s bars)
                if risk > atr * self.sl_atr_mult:
                    continue

                score = min(1.0, 0.4 + abs(acc_z) * 0.1)
                reasons = [
                    "delta_acceleration",
                    f"acc_z={acc_z:.1f}",
                    f"vel={vel:.0f}",
                    f"delta={deltas[i]:.0f}",
                ]
                if acc_z > 3.0:
                    reasons.append("extreme_acceleration")
                if recent_delta_abs > np.mean(np.abs(deltas[max(0, i-20):i])) * 3:
                    reasons.append("delta_spike")
                    score = min(1.0, score + 0.1)

                candidates.append(make_candidate(
                    bars=bars,
                    ctx=ctx,
                    bar_index=i,
                    direction=Direction.LONG,
                    entry=entry,
                    sl=sl,
                    score=score,
                    reasons=reasons,
                    source_type="derived_delta_accel_long",
                    family=CandidateFamily.MTF_CONFLUENCE,
                    meta={"acc_z": float(acc_z), "velocity": float(vel), "delta": float(deltas[i])},
                ))

            elif acc_z < -self.acceleration_z_threshold and vel < 0:
                # Strong bearish acceleration
                entry = closes[i]
                recent_highs = highs[max(0, i - 10) : i + 1]
                sl = float(np.max(recent_highs)) + 0.25
                risk = sl - entry
                if risk <= 0:
                    continue
                if risk > atr * self.sl_atr_mult:
                    continue

                score = min(1.0, 0.4 + abs(acc_z) * 0.1)
                reasons = [
                    "delta_acceleration",
                    f"acc_z={acc_z:.1f}",
                    f"vel={vel:.0f}",
                    f"delta={deltas[i]:.0f}",
                ]
                if acc_z < -3.0:
                    reasons.append("extreme_acceleration")
                if recent_delta_abs > np.mean(np.abs(deltas[max(0, i-20):i])) * 3:
                    reasons.append("delta_spike")
                    score = min(1.0, score + 0.1)

                candidates.append(make_candidate(
                    bars=bars,
                    ctx=ctx,
                    bar_index=i,
                    direction=Direction.SHORT,
                    entry=entry,
                    sl=sl,
                    score=score,
                    reasons=reasons,
                    source_type="derived_delta_accel_short",
                    family=CandidateFamily.MTF_CONFLUENCE,
                    meta={"acc_z": float(acc_z), "velocity": float(vel), "delta": float(deltas[i])},
                ))

        return candidates
