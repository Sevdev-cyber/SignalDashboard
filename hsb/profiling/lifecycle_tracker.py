import uuid
from dataclasses import dataclass, field
from typing import List, Optional
import pandas as pd


@dataclass
class TrackedSignal:
    id: str
    signal_name: str
    direction: str
    confidence: float
    timeframe: str
    confluences: List[str]
    
    # Discovery
    time_detected: str
    price_at_detection: float
    
    # Levels
    entry_price: float
    tp_price: float
    sl_price: float
    real_entry_price: Optional[float] = None
    atr_at_detection: float = 0.0
    
    # State tracking
    is_active: bool = False
    is_resolved: bool = False
    resolution: str = "PENDING"  # WIN, LOSS, EXPIRED
    
    # Execution stats
    time_entered: Optional[str] = None
    time_resolved: Optional[str] = None
    mae_ticks: float = 0.0  # Max adverse excursion (in ticks)
    mfe_ticks: float = 0.0  # Max favorable excursion (in ticks)
    pnl_usd: float = 0.0    # Realized net PnL in USD (including specific commissions/slippage)
    
    def to_dict(self):
        return {
            "id": self.id,
            "signal_name": self.signal_name,
            "direction": self.direction,
            "confidence": self.confidence,
            "confluence_count": len(self.confluences),
            "confluences": "|".join(self.confluences) if self.confluences else "",
            "time_detected": self.time_detected,
            "price_at_detection": self.price_at_detection,
            "entry_price": self.entry_price,
            "tp_price": self.tp_price,
            "sl_price": self.sl_price,
            "real_entry_price": self.real_entry_price,
            "time_entered": self.time_entered,
            "time_resolved": self.time_resolved,
            "mae_ticks": self.mae_ticks,
            "mfe_ticks": self.mfe_ticks,
            "pnl_usd": self.pnl_usd,
            "atr_at_detection": self.atr_at_detection,
            "resolution": self.resolution,
        }


class LifecycleTracker:
    """
    Tracks the lifecycle of a predictive signal strictly via price ticks.
    Calculates Maximum Adverse Excursion (MAE) and Maximum Favorable
    Excursion (MFE) without any simulated broker connection.
    """
    def __init__(self, tick_size: float = 0.25):
        self.tick_size = tick_size
        self.pending_signals: List[TrackedSignal] = []
        self.active_signals: List[TrackedSignal] = []
        self.completed_signals: List[TrackedSignal] = []
        
    def add_candidate(self, ts: str, current_price: float, candidate):
        """Register a newly discovered SignalCandidate (object or dict)."""
        is_dict = isinstance(candidate, dict)
        name = candidate['name'] if is_dict else candidate.name
        direction = candidate['direction'] if is_dict else candidate.direction
        score = candidate.get('confidence_pct', 0) / 100 if is_dict else candidate.score
        entry_p = candidate.get('entry', 0) if is_dict else candidate.entry_price
        tp_p = candidate.get('tp1', 0) if is_dict else candidate.tp1_price
        sl_p = candidate.get('invalidation', 0) if is_dict else candidate.invalidation_price
        
        # safely handle reasons
        if is_dict:
            reasons = candidate.get('reasons', [])
            timeframe = candidate.get('features', {}).get('timeframe', 'unknown')
            atr = candidate.get('atr', 0.0)
        else:
            reasons = candidate.reasons.copy() if candidate.reasons else []
            timeframe = candidate.features.get("timeframe", "unknown")
            atr = candidate.atr if hasattr(candidate, 'atr') else 0.0

        for p in self.pending_signals:
            if p.signal_name == name and p.direction == direction and abs(p.entry_price - entry_p) < self.tick_size:
                if score * 100 > p.confidence:
                    p.confidence = score * 100
                return

        id_str = str(uuid.uuid4())
        s = TrackedSignal(
            id=id_str,
            signal_name=name,
            direction=direction,
            confidence=score * 100,
            timeframe=timeframe,
            confluences=reasons,
            time_detected=ts,
            price_at_detection=current_price,
            entry_price=entry_p,
            tp_price=tp_p,
            sl_price=sl_p,
            atr_at_detection=atr
        )
        self.pending_signals.append(s)
        
    def process_tick(self, ts: str, price: float):
        """Update all tracked signals based on live physical price."""
        
        # 1. Check pendings to see if entry is hit
        new_active = []
        for s in self.pending_signals:
            # Entry hit?
            hit = False
            # Allow 1-tick leniency for frontrunning
            leniency = self.tick_size
            if s.direction == "long" and price <= s.entry_price + leniency:
                hit = True
                s.real_entry_price = price + (2 * self.tick_size)
            elif s.direction == "short" and price >= s.entry_price - leniency:
                hit = True
                s.real_entry_price = price - (2 * self.tick_size)
                
            if hit:
                s.is_active = True
                s.time_entered = ts
                new_active.append(s)
                
        # Move activated from pending to active
        if new_active:
            self.pending_signals = [s for s in self.pending_signals if not s.is_active]
            self.active_signals.extend(new_active)
            
        # 2. Update active signals for MAE, MFE, and TP/SL hitting
        resolved = []
        for s in self.active_signals:
            if s.direction == "long":
                # Real entry was populated on fill
                ref_entry = s.real_entry_price if s.real_entry_price is not None else s.entry_price
                mae = (ref_entry - price) / self.tick_size
                mfe = (price - ref_entry) / self.tick_size
                s.mae_ticks = max(s.mae_ticks, mae)
                s.mfe_ticks = max(s.mfe_ticks, mfe)
                
                if price >= s.tp_price:
                    s.resolution = "WIN"
                    s.is_resolved = True
                    s.time_resolved = ts
                    s.pnl_usd = (((s.tp_price - ref_entry) / self.tick_size) * 0.50) - 2.48
                    resolved.append(s)
                elif price <= s.sl_price:
                    s.resolution = "LOSS"
                    s.is_resolved = True
                    s.time_resolved = ts
                    exit_price = price - (1 * self.tick_size)
                    s.pnl_usd = (((exit_price - ref_entry) / self.tick_size) * 0.50) - 2.48
                    resolved.append(s)
            
            elif s.direction == "short":
                ref_entry = s.real_entry_price if s.real_entry_price is not None else s.entry_price
                mae = (price - ref_entry) / self.tick_size
                mfe = (ref_entry - price) / self.tick_size
                s.mae_ticks = max(s.mae_ticks, mae)
                s.mfe_ticks = max(s.mfe_ticks, mfe)
                
                if price <= s.tp_price:
                    s.resolution = "WIN"
                    s.is_resolved = True
                    s.time_resolved = ts
                    s.pnl_usd = (((ref_entry - s.tp_price) / self.tick_size) * 0.50) - 2.48
                    resolved.append(s)
                elif price >= s.sl_price:
                    s.resolution = "LOSS"
                    s.is_resolved = True
                    s.time_resolved = ts
                    exit_price = price + (1 * self.tick_size)
                    s.pnl_usd = (((ref_entry - exit_price) / self.tick_size) * 0.50) - 2.48
                    resolved.append(s)
                    
        # Move resolved out of active list
        if resolved:
            self.active_signals = [s for s in self.active_signals if not s.is_resolved]
            self.completed_signals.extend(resolved)
            
    def export_to_dataframe(self) -> pd.DataFrame:
        """Returns all completed (resolved) signals as a dataframe."""
        records = [s.to_dict() for s in self.completed_signals]
        # Also let's capture the ones that never reached resolution
        records.extend([s.to_dict() for s in self.active_signals])
        records.extend([s.to_dict() for s in self.pending_signals])
        return pd.DataFrame(records)
