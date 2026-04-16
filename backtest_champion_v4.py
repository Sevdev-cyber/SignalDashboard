"""Champion V4 Sweep — Realistic tick-by-tick backtest (30 days).

Zero look-ahead bias:
- Processes ticks chronologically
- Signals evaluated at bar close only (no future data)
- SL/TP checked tick-by-tick (not bar OHLC)

Realistic prop firm costs:
- Spread: 1 tick (0.25 pts = $0.50/contract)
- Slippage: 1 tick + ATR-based (variable)
- Commission: $0.62/side ($1.24 round trip per contract)
- MNQ: $2/point, tick_size=0.25

Usage:
    /Users/sacredforest/Trading\ Setup/Testing\ Nautilus/venv/bin/python backtest_champion_v4.py
    /Users/sacredforest/Trading\ Setup/Testing\ Nautilus/venv/bin/python backtest_champion_v4.py --days 30
"""
import os, sys, time, json, logging
from collections import defaultdict
from dataclasses import dataclass, field

import pandas as pd
import numpy as np

sys.path.insert(0, os.path.join(os.path.dirname(__file__)))
from bar_builder import enrich_bars

logging.basicConfig(level=logging.INFO, format='%(asctime)s %(levelname)s %(message)s')
log = logging.getLogger("bt_v4")

CATALOG = "/Users/sacredforest/Trading Setup/Testing Nautilus/catalog"
TICK_SIZE = 0.25
POINT_VALUE = 2.0
COMMISSION_PER_SIDE = 0.62  # $0.62/side
SPREAD_TICKS = 1            # 1 tick spread
SLIPPAGE_BASE_TICKS = 1     # base slippage

# ── V4 Champion Config ──
SL_MAX_PTS = 6.0
SL_ATR_MULT = 1.0
TP1_R = 1.0
TP2_R = 2.0
TP3_R = 8.0
TRAIL_MULT = 0.15           # trail = HWM - 0.15 × risk
TRAIL_AFTER_TP1 = True
BE_BUFFER_TICKS = 4
PYRAMID_AFTER_TP1 = True
PYRAMID_CONTRACTS = 2
PYRAMID_MAX_ADDS = 1
DEFAULT_CONTRACTS = 4
MAX_HOLD_SEC = 600
CIRCUIT_BREAKER_SL = 5
COOLDOWN_SEC = 60
SWING_LOOKBACK = 5
FVG_MIN_GAP_TICKS = 1


@dataclass
class Trade:
    direction: str          # "long" / "short"
    entry_price: float
    sl_price: float
    tp1: float
    tp2: float
    tp3: float
    risk: float
    contracts: int
    entry_time: int         # ns timestamp
    signal_name: str
    # State
    hwm: float = 0.0       # high water mark
    be_triggered: bool = False
    tp1_hit: bool = False
    pyramid_added: bool = False
    filled_price: float = 0.0  # after slippage
    pnl: float = 0.0
    exit_reason: str = ""
    exit_price: float = 0.0


class ChampionV4Backtester:
    def __init__(self):
        self.trades: list[Trade] = []
        self.completed: list[Trade] = []
        self.active: Trade = None
        self.daily_pnl = 0.0
        self.daily_trades = 0
        self.consecutive_sl = 0
        self.cooldown_until = 0
        self.last_session_date = None
        self.stats = defaultdict(float)
        self.daily_results = []

    def process_tick(self, ts_ns: int, price: float, size: int, is_buy: bool):
        """Process a single tick against active trade."""
        if self.active is None:
            return

        t = self.active
        is_long = t.direction == "long"

        # Update HWM
        if is_long:
            t.hwm = max(t.hwm, price)
        else:
            t.hwm = min(t.hwm, price) if t.hwm > 0 else price

        # Check SL
        sl_hit = (is_long and price <= t.sl_price) or (not is_long and price >= t.sl_price)
        if sl_hit:
            self._close_trade(price, ts_ns, "SL")
            self.consecutive_sl += 1
            if self.consecutive_sl >= CIRCUIT_BREAKER_SL:
                self.cooldown_until = ts_ns + COOLDOWN_SEC * 1_000_000_000
            return

        # Check TP1
        tp1_hit = (is_long and price >= t.tp1) or (not is_long and price <= t.tp1)
        if tp1_hit and not t.tp1_hit:
            t.tp1_hit = True
            # Move SL to breakeven + buffer
            if is_long:
                t.sl_price = t.filled_price + BE_BUFFER_TICKS * TICK_SIZE
            else:
                t.sl_price = t.filled_price - BE_BUFFER_TICKS * TICK_SIZE
            t.be_triggered = True
            self.consecutive_sl = 0  # reset on win

        # Check TP3 (full exit)
        tp3_hit = (is_long and price >= t.tp3) or (not is_long and price <= t.tp3)
        if tp3_hit:
            self._close_trade(price, ts_ns, "TP3")
            return

        # Trailing stop (after TP1)
        if t.tp1_hit and TRAIL_AFTER_TP1:
            trail_dist = t.risk * TRAIL_MULT
            if is_long:
                trail_sl = t.hwm - trail_dist
                t.sl_price = max(t.sl_price, trail_sl)
            else:
                trail_sl = t.hwm + trail_dist
                t.sl_price = min(t.sl_price, trail_sl)

        # Max hold time
        age_ns = ts_ns - t.entry_time
        if age_ns > MAX_HOLD_SEC * 1_000_000_000:
            self._close_trade(price, ts_ns, "TIMEOUT")
            return

    def _close_trade(self, price: float, ts_ns: int, reason: str):
        t = self.active
        if t is None:
            return

        # Apply exit slippage
        exit_slip = SLIPPAGE_BASE_TICKS * TICK_SIZE
        if t.direction == "long":
            exit_price = price - exit_slip
            raw_pnl = (exit_price - t.filled_price) * POINT_VALUE * t.contracts
        else:
            exit_price = price + exit_slip
            raw_pnl = (t.filled_price - exit_price) * POINT_VALUE * t.contracts

        # Commission (entry + exit)
        commission = COMMISSION_PER_SIDE * 2 * t.contracts
        net_pnl = raw_pnl - commission

        t.pnl = net_pnl
        t.exit_price = exit_price
        t.exit_reason = reason

        self.daily_pnl += net_pnl
        self.daily_trades += 1
        self.completed.append(t)
        self.active = None

        self.stats["total_pnl"] += net_pnl
        self.stats["total_trades"] += 1
        self.stats[f"exits_{reason.lower()}"] += 1
        if net_pnl > 0:
            self.stats["wins"] += 1
        else:
            self.stats["losses"] += 1

    def try_enter(self, signal: dict, price: float, ts_ns: int, atr: float):
        """Try to open a trade from signal."""
        if self.active is not None:
            return  # already in trade

        if ts_ns < self.cooldown_until:
            return  # in cooldown

        direction = signal["direction"]
        is_long = direction == "long"

        # Entry with spread + slippage
        spread_cost = SPREAD_TICKS * TICK_SIZE
        slip = SLIPPAGE_BASE_TICKS * TICK_SIZE + 0.1 * atr
        if is_long:
            fill_price = price + spread_cost + slip
        else:
            fill_price = price - spread_cost - slip

        # SL calculation: micro-SMC swing or ATR, capped at 6pts
        sl_dist = min(SL_MAX_PTS, atr * SL_ATR_MULT)
        if sl_dist <= 0.5:
            sl_dist = 2.0  # minimum SL

        if is_long:
            sl = fill_price - sl_dist
            tp1 = fill_price + sl_dist * TP1_R
            tp2 = fill_price + sl_dist * TP2_R
            tp3 = fill_price + sl_dist * TP3_R
        else:
            sl = fill_price + sl_dist
            tp1 = fill_price - sl_dist * TP1_R
            tp2 = fill_price - sl_dist * TP2_R
            tp3 = fill_price - sl_dist * TP3_R

        self.active = Trade(
            direction=direction,
            entry_price=price,
            sl_price=sl,
            tp1=tp1, tp2=tp2, tp3=tp3,
            risk=sl_dist,
            contracts=DEFAULT_CONTRACTS,
            entry_time=ts_ns,
            signal_name=signal.get("name", "?"),
            filled_price=fill_price,
            hwm=fill_price,
        )

    def new_session(self, date_str: str):
        """Reset daily counters."""
        if self.daily_trades > 0:
            self.daily_results.append({
                "date": self.last_session_date or date_str,
                "trades": self.daily_trades,
                "pnl": round(self.daily_pnl, 2),
            })
        self.daily_pnl = 0.0
        self.daily_trades = 0
        self.consecutive_sl = 0
        self.cooldown_until = 0
        self.last_session_date = date_str
        if self.active:
            # Force close at EOD
            self._close_trade(self.active.filled_price, 0, "EOD")

    def report(self):
        total = int(self.stats["total_trades"])
        wins = int(self.stats["wins"])
        losses = int(self.stats["losses"])
        pnl = self.stats["total_pnl"]
        wr = wins / (wins + losses) * 100 if (wins + losses) > 0 else 0
        days = len(self.daily_results)
        per_day = pnl / days if days > 0 else 0
        per_trade = pnl / total if total > 0 else 0

        # Max drawdown
        equity = 0
        peak = 0
        max_dd = 0
        for t in self.completed:
            equity += t.pnl
            peak = max(peak, equity)
            dd = peak - equity
            max_dd = max(max_dd, dd)

        # Profit factor
        gross_win = sum(t.pnl for t in self.completed if t.pnl > 0)
        gross_loss = abs(sum(t.pnl for t in self.completed if t.pnl < 0))
        pf = gross_win / gross_loss if gross_loss > 0 else 999

        print(f"\n{'='*70}")
        print(f"CHAMPION V4 SWEEP — REALISTIC TICK-BY-TICK BACKTEST")
        print(f"{'='*70}")
        print(f"Days:      {days}")
        print(f"Trades:    {total}")
        print(f"Wins:      {wins}  ({wr:.1f}%)")
        print(f"Losses:    {losses}")
        print(f"PnL:       ${pnl:+,.0f}")
        print(f"$/day:     ${per_day:+,.0f}")
        print(f"$/trade:   ${per_trade:+,.1f}")
        print(f"PF:        {pf:.2f}")
        print(f"Max DD:    ${max_dd:,.0f}")
        print(f"{'='*70}")
        print(f"Exits: SL={int(self.stats.get('exits_sl',0))} "
              f"TP3={int(self.stats.get('exits_tp3',0))} "
              f"Timeout={int(self.stats.get('exits_timeout',0))} "
              f"EOD={int(self.stats.get('exits_eod',0))}")
        print(f"Costs: spread={SPREAD_TICKS}tick, slip={SLIPPAGE_BASE_TICKS}tick+ATR, "
              f"comm=${COMMISSION_PER_SIDE*2:.2f}/rt")
        print(f"{'='*70}")

        print(f"\nDAILY BREAKDOWN:")
        for d in self.daily_results:
            bar = "█" * max(0, int(d["pnl"] / 20)) if d["pnl"] > 0 else "▓" * max(0, int(-d["pnl"] / 20))
            print(f"  {d['date']}  {d['trades']:>3}t  ${d['pnl']:>+8,.0f}  {bar}")

        # Save results
        pd.DataFrame(self.daily_results).to_csv("champion_v4_30d_results.csv", index=False)
        log.info("Saved to champion_v4_30d_results.csv")


def simple_micro_smc(bars: pd.DataFrame, lookback: int = 5) -> list[dict]:
    """Simplified Micro-SMC signal detection (BOS/CHOCH/FVG).

    No look-ahead — only uses closed bars.
    """
    if len(bars) < lookback + 3:
        return []

    signals = []
    highs = bars["high"].values
    lows = bars["low"].values
    closes = bars["close"].values
    atr = float(bars["atr"].iloc[-1]) if "atr" in bars.columns else 5.0

    # Find swing highs/lows in last N bars (excluding current)
    n = len(bars)
    recent = slice(max(0, n - lookback - 1), n - 1)

    swing_highs = []
    swing_lows = []
    for i in range(recent.start + 1, recent.stop):
        if highs[i] > highs[i-1] and highs[i] > highs[min(i+1, n-2)]:
            swing_highs.append((i, highs[i]))
        if lows[i] < lows[i-1] and lows[i] < lows[min(i+1, n-2)]:
            swing_lows.append((i, lows[i]))

    last_close = closes[-1]
    last_bar = n - 1

    # BOS: break of structure
    if swing_highs:
        highest = max(swing_highs, key=lambda x: x[1])
        if last_close > highest[1]:
            signals.append({
                "name": "BOS_BULL", "direction": "long",
                "entry": last_close, "confidence_pct": 70,
            })

    if swing_lows:
        lowest = min(swing_lows, key=lambda x: x[1])
        if last_close < lowest[1]:
            signals.append({
                "name": "BOS_BEAR", "direction": "short",
                "entry": last_close, "confidence_pct": 70,
            })

    # FVG: gap between bar[i-2].low and bar[i].high (bullish) or vice versa
    if n >= 3:
        gap_up = lows[-1] - highs[-3]  # bullish FVG
        gap_down = lows[-3] - highs[-1]  # bearish FVG
        min_gap = FVG_MIN_GAP_TICKS * TICK_SIZE

        if gap_up > min_gap and last_close < highs[-3] + gap_up * 0.5:
            signals.append({
                "name": "FVG_FILL_LONG", "direction": "long",
                "entry": last_close, "confidence_pct": 65,
            })
        if gap_down > min_gap and last_close > lows[-3] - gap_down * 0.5:
            signals.append({
                "name": "FVG_FILL_SHORT", "direction": "short",
                "entry": last_close, "confidence_pct": 65,
            })

    return signals


def main():
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument("--days", type=int, default=30)
    args = parser.parse_args()

    t0 = time.time()

    from nautilus_trader.persistence.catalog import ParquetDataCatalog
    catalog = ParquetDataCatalog(CATALOG)

    # Load trades for date range
    all_trades = catalog.trade_ticks(instrument_ids=["MNQ.CME"])
    log.info(f"Total ticks: {len(all_trades):,}")

    # Last N days
    from datetime import datetime, timedelta
    last_dt = datetime.utcfromtimestamp(all_trades[-1].ts_event / 1e9)
    cutoff = last_dt - timedelta(days=args.days)
    cutoff_ns = int(cutoff.timestamp() * 1e9)

    ticks = [t for t in all_trades if t.ts_event >= cutoff_ns]
    del all_trades
    log.info(f"Selected {len(ticks):,} ticks for {args.days} days")

    # Build 1-min bars incrementally
    bt = ChampionV4Backtester()
    bars_data = []
    current_bar_minute = None
    bar_open = bar_high = bar_low = bar_close = 0.0
    bar_volume = 0
    bar_buy_vol = bar_sell_vol = 0

    for tick in ticks:
        price = float(tick.price)
        size = int(tick.size)
        is_buy = bool(tick.aggressor_side)
        ts = tick.ts_event
        minute = ts // 60_000_000_000  # round to minute

        # Session boundary (daily reset)
        dt = datetime.utcfromtimestamp(ts / 1e9)
        date_str = dt.strftime("%Y-%m-%d")
        hour_et = (dt.hour - 4) % 24  # rough UTC→ET
        if bt.last_session_date != date_str and hour_et >= 9:
            bt.new_session(date_str)

        # Process tick against active trade
        bt.process_tick(ts, price, size, is_buy)

        # Build bar
        if current_bar_minute is None:
            current_bar_minute = minute
            bar_open = bar_high = bar_low = bar_close = price
            bar_volume = size
            bar_buy_vol = size if is_buy else 0
            bar_sell_vol = size if not is_buy else 0
        elif minute == current_bar_minute:
            bar_high = max(bar_high, price)
            bar_low = min(bar_low, price)
            bar_close = price
            bar_volume += size
            if is_buy:
                bar_buy_vol += size
            else:
                bar_sell_vol += size
        else:
            # Bar complete — save and evaluate signals
            bar_dt = datetime.utcfromtimestamp(current_bar_minute * 60)
            bars_data.append({
                "datetime": bar_dt,
                "open": bar_open, "high": bar_high,
                "low": bar_low, "close": bar_close,
                "volume": bar_volume,
                "delta": bar_buy_vol - bar_sell_vol,
                "buy_volume": bar_buy_vol,
                "sell_volume": bar_sell_vol,
                "has_real_tick_delta": True,
            })

            # Keep last 200 bars
            if len(bars_data) > 200:
                bars_data = bars_data[-200:]

            # Evaluate signals at bar close (no look-ahead)
            if len(bars_data) >= 30 and bt.active is None:
                bars_df = pd.DataFrame(bars_data)
                bars_df = enrich_bars(bars_df)
                atr = float(bars_df["atr"].iloc[-1])

                # Only trade during RTH (9:30-15:45 ET)
                if 13 <= bar_dt.hour < 20:  # rough UTC RTH check
                    sigs = simple_micro_smc(bars_df)
                    if sigs:
                        best = max(sigs, key=lambda s: s["confidence_pct"])
                        bt.try_enter(best, bar_close, ts, atr)

            # Start new bar
            current_bar_minute = minute
            bar_open = bar_high = bar_low = bar_close = price
            bar_volume = size
            bar_buy_vol = size if is_buy else 0
            bar_sell_vol = size if not is_buy else 0

    # Final session close
    bt.new_session("END")

    elapsed = time.time() - t0
    log.info(f"Backtest done in {elapsed:.0f}s")
    bt.report()


if __name__ == "__main__":
    main()
