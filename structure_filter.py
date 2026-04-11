"""
Structure Filter — CHoCH/BOS based directional gate
════════════════════════════════════════════════════

Detects market structure (swing highs/lows) and determines the
current structural bias:
  - After bullish BOS → allow LONG, block SHORT
  - After bearish BOS → allow SHORT, block LONG
  - After CHoCH       → flip bias

Uses the same swing detection logic as MicroSMCGenerator but
operates as a FILTER (blocks signals) rather than a generator.

Lookback configurable: works on 5-min bars but evaluates
structure over a longer window (e.g., last 30-50 bars ≈ 2.5-4h).
"""

import logging
import numpy as np
import pandas as pd

log = logging.getLogger("jajcus.structure")


class StructureFilter:
    """Detect BOS/CHoCH and maintain structural bias."""

    def __init__(self, swing_lookback: int = 3, structure_window: int = 40):
        """
        Args:
            swing_lookback: bars on each side to confirm a swing point (3 = standard)
            structure_window: how many bars back to scan for swings (40 = ~3.3h on 5-min)
        """
        self.swing_lookback = swing_lookback
        self.structure_window = structure_window
        self.bias: str = "neutral"  # "bullish", "bearish", "neutral"
        self.last_bos_bar: int = -1
        self.last_bos_type: str = ""
        self.last_swing_high: float = 0.0
        self.last_swing_low: float = 0.0

    def update(self, bars_df: pd.DataFrame) -> str:
        """Update structural bias from current bars. Returns: 'bullish', 'bearish', 'neutral'."""
        if len(bars_df) < 20:
            return self.bias

        highs = bars_df["high"].values.astype(float)
        lows = bars_df["low"].values.astype(float)
        closes = bars_df["close"].values.astype(float)
        n = len(bars_df)

        # Find swing points in recent window
        lb = self.swing_lookback
        start = max(lb, n - self.structure_window)

        swing_highs = []  # (index, price)
        swing_lows = []

        for i in range(start, n - lb):
            window_h = highs[i - lb: i + lb + 1]
            if highs[i] == window_h.max() and highs[i] > highs[i - 1] and highs[i] > highs[i + 1]:
                swing_highs.append((i, highs[i]))

            window_l = lows[i - lb: i + lb + 1]
            if lows[i] == window_l.min() and lows[i] < lows[i - 1] and lows[i] < lows[i + 1]:
                swing_lows.append((i, lows[i]))

        if len(swing_highs) < 2 or len(swing_lows) < 2:
            return self.bias

        # Determine structure from last 2 swing highs and lows
        sh1_idx, sh1 = swing_highs[-2]
        sh2_idx, sh2 = swing_highs[-1]
        sl1_idx, sl1 = swing_lows[-2]
        sl2_idx, sl2 = swing_lows[-1]

        self.last_swing_high = sh2
        self.last_swing_low = sl2

        # Classic structure analysis:
        # HH + HL = bullish
        # LH + LL = bearish
        higher_highs = sh2 > sh1
        higher_lows = sl2 > sl1
        lower_highs = sh2 < sh1
        lower_lows = sl2 < sl1

        old_bias = self.bias

        if higher_highs and higher_lows:
            self.bias = "bullish"
        elif lower_highs and lower_lows:
            self.bias = "bearish"
        elif lower_highs and higher_lows:
            # Compression / range — keep current bias but weaken it
            pass  # Stay with current
        elif higher_highs and lower_lows:
            # Expansion — keep current bias
            pass

        # BOS / CHoCH detection on the LAST bar
        last_close = closes[-1]
        last_bar_idx = n - 1

        # Bullish BOS: close breaks above last swing high
        if last_close > sh2 and sh2_idx < last_bar_idx:
            if old_bias == "bearish":
                log.info("  🔄 CHoCH BULL: close %.2f > swing high %.2f (was bearish)", last_close, sh2)
                self.last_bos_type = "choch_bull"
            else:
                self.last_bos_type = "bos_bull"
            self.bias = "bullish"
            self.last_bos_bar = last_bar_idx

        # Bearish BOS: close breaks below last swing low
        if last_close < sl2 and sl2_idx < last_bar_idx:
            if old_bias == "bullish":
                log.info("  🔄 CHoCH BEAR: close %.2f < swing low %.2f (was bullish)", last_close, sl2)
                self.last_bos_type = "choch_bear"
            else:
                self.last_bos_type = "bos_bear"
            self.bias = "bearish"
            self.last_bos_bar = last_bar_idx

        if self.bias != old_bias:
            log.info(
                "  📐 Structure: %s → %s | SH=%.2f SL=%.2f | type=%s",
                old_bias, self.bias, sh2, sl2, self.last_bos_type,
            )

        return self.bias

    def allows_direction(self, direction: str) -> bool:
        """Check if current structure allows this trade direction.
        
        Rules:
          - bullish bias → allow LONG, block SHORT
          - bearish bias → allow SHORT, block LONG
          - neutral → allow both
        """
        if self.bias == "neutral":
            return True
        if self.bias == "bullish" and direction == "long":
            return True
        if self.bias == "bearish" and direction == "short":
            return True
        return False

    def get_info(self) -> str:
        """Return human-readable structure state."""
        return f"bias={self.bias} SH={self.last_swing_high:.2f} SL={self.last_swing_low:.2f} last={self.last_bos_type}"
