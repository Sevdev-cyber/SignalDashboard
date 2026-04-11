"""
Session Level Tracker — computes RTH/ETH session levels from bars.

Tracks:
  - RTH (9:30-16:00 ET) open/high/low
  - Previous session high/low
  - VWAP
  - Overnight (ETH) high/low
  
These levels serve as smart TP/SL targets and trade filters.
"""

import logging
from datetime import time as dt_time

log = logging.getLogger("wicher.session")


class SessionLevels:
    """Track intraday session levels for smart execution."""

    # RTH = Regular Trading Hours in CT (Central Time = ET - 1h)
    # NT8 timestamps are in CT
    RTH_OPEN_CT = dt_time(8, 30)   # 9:30 ET = 8:30 CT
    RTH_CLOSE_CT = dt_time(15, 0)  # 16:00 ET = 15:00 CT
    
    def __init__(self):
        self.reset()
        # Previous session levels (persist across days)
        self.prev_session_high: float = 0.0
        self.prev_session_low: float = 0.0
        self.prev_session_close: float = 0.0
        self.prev_session_vwap: float = 0.0
    
    def reset(self):
        """Reset for new session."""
        # RTH levels
        self.rth_open: float = 0.0
        self.rth_high: float = 0.0
        self.rth_low: float = float('inf')
        self.rth_close: float = 0.0
        self.rth_started: bool = False
        
        # Overnight (pre-RTH) levels
        self.overnight_high: float = 0.0
        self.overnight_low: float = float('inf')
        
        # Full session
        self.session_high: float = 0.0
        self.session_low: float = float('inf')
        self.session_open: float = 0.0
        
        # VWAP tracking
        self._cum_pv: float = 0.0  # cumulative price × volume
        self._cum_vol: float = 0.0  # cumulative volume
        self.vwap: float = 0.0
        
        self._bar_count = 0
    
    def update(self, bar_time, open_p: float, high: float, low: float, 
               close: float, volume: float = 0):
        """Update levels with new bar data.
        
        Args:
            bar_time: datetime or time object with bar timestamp
        """
        self._bar_count += 1
        
        # Extract time
        if hasattr(bar_time, 'time'):
            t = bar_time.time()
        elif hasattr(bar_time, 'hour'):
            t = bar_time
        else:
            return  # Can't parse time
        
        # Full session tracking
        if self.session_open == 0:
            self.session_open = open_p
        self.session_high = max(self.session_high, high)
        self.session_low = min(self.session_low, low)
        
        # VWAP
        if volume > 0:
            typical_price = (high + low + close) / 3
            self._cum_pv += typical_price * volume
            self._cum_vol += volume
            self.vwap = self._cum_pv / self._cum_vol
        
        # Is this bar during RTH?
        in_rth = self.RTH_OPEN_CT <= t < self.RTH_CLOSE_CT
        
        if in_rth:
            if not self.rth_started:
                self.rth_started = True
                self.rth_open = open_p
                self.rth_high = high
                self.rth_low = low
                log.info("  📊 RTH OPEN @ %.2f | overnight H=%.2f L=%.2f | prev H=%.2f L=%.2f",
                         open_p, self.overnight_high, self.overnight_low,
                         self.prev_session_high, self.prev_session_low)
            else:
                self.rth_high = max(self.rth_high, high)
                self.rth_low = min(self.rth_low, low)
            self.rth_close = close
        else:
            # Overnight
            self.overnight_high = max(self.overnight_high, high)
            self.overnight_low = min(self.overnight_low, low)
    
    def end_session(self):
        """Call at end of day to save levels for next session."""
        if self.rth_started:
            self.prev_session_high = self.rth_high
            self.prev_session_low = self.rth_low
            self.prev_session_close = self.rth_close
            self.prev_session_vwap = self.vwap
            log.info("  📊 SESSION END | H=%.2f L=%.2f C=%.2f VWAP=%.2f",
                     self.rth_high, self.rth_low, self.rth_close, self.vwap)
    
    def get_targets_long(self, entry: float, risk: float) -> dict:
        """Get smart TP targets for a LONG trade.
        
        Returns dict with 'tp1', 'tp2', 'tp3' based on structure.
        Falls back to R-multiples if no structure available.
        """
        targets = []
        
        # Collect all resistance levels above entry
        for level, name in [
            (self.rth_high, "rth_high"),
            (self.session_high, "session_high"),
            (self.overnight_high, "overnight_high"),
            (self.prev_session_high, "prev_high"),
            (self.vwap, "vwap"),
        ]:
            if level > entry + risk * 0.5:  # at least 0.5R above
                targets.append((level, name))
        
        targets.sort(key=lambda x: x[0])
        
        # Assign TPs
        tp1 = targets[0][0] if len(targets) >= 1 else entry + risk * 1.5
        tp2 = targets[1][0] if len(targets) >= 2 else entry + risk * 2.5
        tp3 = entry + risk * 4.0  # keep 4R as max target
        
        return {"tp1": tp1, "tp2": tp2, "tp3": tp3,
                "tp1_source": targets[0][1] if targets else "1.5R",
                "tp2_source": targets[1][1] if len(targets) >= 2 else "2.5R"}
    
    def get_targets_short(self, entry: float, risk: float) -> dict:
        """Get smart TP targets for a SHORT trade."""
        targets = []
        
        for level, name in [
            (self.rth_low, "rth_low"),
            (self.session_low, "session_low"),
            (self.overnight_low, "overnight_low"),
            (self.prev_session_low, "prev_low"),
            (self.vwap, "vwap"),
        ]:
            if 0 < level < entry - risk * 0.5:  # at least 0.5R below
                targets.append((level, name))
        
        targets.sort(key=lambda x: x[0], reverse=True)  # highest first (nearest)
        
        tp1 = targets[0][0] if len(targets) >= 1 else entry - risk * 1.5
        tp2 = targets[1][0] if len(targets) >= 2 else entry - risk * 2.5
        tp3 = entry - risk * 4.0
        
        return {"tp1": tp1, "tp2": tp2, "tp3": tp3,
                "tp1_source": targets[0][1] if targets else "1.5R",
                "tp2_source": targets[1][1] if len(targets) >= 2 else "2.5R"}
    
    def get_info(self) -> str:
        """Human-readable summary."""
        parts = [f"bars={self._bar_count}"]
        if self.rth_started:
            parts.append(f"RTH O={self.rth_open:.2f} H={self.rth_high:.2f} L={self.rth_low:.2f}")
        if self.vwap > 0:
            parts.append(f"VWAP={self.vwap:.2f}")
        if self.prev_session_high > 0:
            parts.append(f"prevH={self.prev_session_high:.2f} prevL={self.prev_session_low:.2f}")
        return " | ".join(parts)
