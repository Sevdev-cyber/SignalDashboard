"""14-day tick-by-tick backtest of SignalEngine with SL/TP tracking.

Uses Nautilus catalog to read trade ticks, builds 5-min bars with full
indicators (EMA, VWAP, Delta, ATR), runs SignalEngine, and tracks
every signal to SL/TP resolution.

Usage:
    /Users/sacredforest/Trading Setup/Testing Nautilus/venv/bin/python backtest_14d.py
"""
import os
import sys
import time
import logging
from datetime import datetime, timedelta
from collections import defaultdict

import pandas as pd
import numpy as np

# Paths
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from signal_engine import SignalEngine
from bar_builder import enrich_bars

logging.basicConfig(level=logging.INFO, format='%(asctime)s %(levelname)s %(message)s')
log = logging.getLogger("backtest")

# ── CONFIG ──
CATALOG_PATH = "/Users/sacredforest/Trading Setup/Testing Nautilus/catalog"
DAYS = 14
TICK_SIZE = 0.25
POINT_VALUE = 2.0  # MNQ $2/point


def load_ticks(n_days: int = 14) -> pd.DataFrame:
    """Load last N days of trade ticks via Nautilus catalog."""
    from nautilus_trader.persistence.catalog import ParquetDataCatalog

    log.info(f"Loading Nautilus catalog from {CATALOG_PATH}...")
    catalog = ParquetDataCatalog(CATALOG_PATH)

    ticks = catalog.trade_ticks(instrument_ids=["MNQ.CME"])
    log.info(f"Total ticks in catalog: {len(ticks):,}")

    # Convert to DataFrame
    rows = []
    for t in ticks:
        rows.append({
            "timestamp": pd.Timestamp(t.ts_event, unit="ns"),
            "price": float(t.price),
            "size": int(t.size),
            "aggressor": int(t.aggressor_side),  # 1=buyer, 2=seller
        })

    df = pd.DataFrame(rows)
    df.sort_values("timestamp", inplace=True)

    # Take last N days
    last_date = df["timestamp"].dt.date.max()
    cutoff = last_date - timedelta(days=n_days)
    df = df[df["timestamp"].dt.date > cutoff].reset_index(drop=True)

    log.info(f"Selected {len(df):,} ticks over {n_days} days "
             f"({df['timestamp'].min()} → {df['timestamp'].max()})")
    return df


def ticks_to_5min_bars(ticks: pd.DataFrame) -> pd.DataFrame:
    """Resample ticks to 5-min OHLCV bars with delta."""
    df = ticks.set_index("timestamp")

    bars = df["price"].resample("5min").agg(["first", "max", "min", "last"])
    bars.columns = ["open", "high", "low", "close"]
    bars["volume"] = df["size"].resample("5min").sum()

    # Delta from aggressor
    buy = df[df["aggressor"] == 1]["size"].resample("5min").sum()
    sell = df[df["aggressor"] == 2]["size"].resample("5min").sum()
    bars["delta"] = buy.sub(sell, fill_value=0)

    bars.dropna(subset=["open"], inplace=True)
    bars.reset_index(inplace=True)
    bars.rename(columns={"timestamp": "datetime"}, inplace=True)

    # Enrich with ATR, EMA, VWAP, CVD
    bars = enrich_bars(bars)
    log.info(f"Built {len(bars)} 5-min bars")
    return bars


class SignalTracker:
    """Track active signals tick-by-tick to SL/TP resolution."""

    def __init__(self):
        self.active = {}  # id → signal dict + tracking
        self.resolved = []  # completed signals
        self.stats = defaultdict(lambda: {"wins": 0, "losses": 0, "expired": 0, "pnl": 0.0})

    def add_signals(self, signals: list[dict], bar_time):
        """Add new signals from engine evaluation."""
        for s in signals:
            sid = s["id"]
            if sid in self.active:
                continue  # already tracking
            self.active[sid] = {
                **s,
                "track_start": bar_time,
                "price_min": s["entry"],
                "price_max": s["entry"],
                "entry_touched": False,
                "bars_alive": 0,
            }

    def process_tick(self, price: float):
        """Update all active signals with new price."""
        to_remove = []
        for sid, s in self.active.items():
            s["price_min"] = min(s["price_min"], price)
            s["price_max"] = max(s["price_max"], price)

            # Check entry touched
            if not s["entry_touched"]:
                if s["direction"] == "long" and price <= s["entry"]:
                    s["entry_touched"] = True
                elif s["direction"] == "short" and price >= s["entry"]:
                    s["entry_touched"] = True

            # Check SL/TP
            result = None
            if s["direction"] == "long":
                if s["price_min"] <= s["sl"]:
                    result = "sl_hit"
                elif s["price_max"] >= s["tp1"]:
                    result = "tp_hit"
            else:
                if s["price_max"] >= s["sl"]:
                    result = "sl_hit"
                elif s["price_min"] <= s["tp1"]:
                    result = "tp_hit"

            if result:
                s["result"] = result
                risk = abs(s["entry"] - s["sl"])
                if result == "tp_hit":
                    pnl = abs(s["tp1"] - s["entry"]) * POINT_VALUE
                    self.stats[s["name"]]["wins"] += 1
                else:
                    pnl = -risk * POINT_VALUE
                    self.stats[s["name"]]["losses"] += 1

                self.stats[s["name"]]["pnl"] += pnl
                s["pnl"] = pnl
                self.resolved.append(s)
                to_remove.append(sid)

        for sid in to_remove:
            del self.active[sid]

    def process_bar_end(self):
        """Increment bars_alive counter and expire old signals."""
        to_remove = []
        for sid, s in self.active.items():
            s["bars_alive"] += 1
            if s["bars_alive"] > 48:  # 4 hours on 5-min
                s["result"] = "expired"
                self.stats[s["name"]]["expired"] += 1
                self.resolved.append(s)
                to_remove.append(sid)
        for sid in to_remove:
            del self.active[sid]

    def report(self):
        """Print summary table."""
        print("\n" + "=" * 90)
        print(f"{'Signal':<25} {'N':>5} {'Wins':>5} {'Loss':>5} {'Exp':>4} "
              f"{'WR%':>6} {'PnL$':>8} {'$/trade':>8} {'Grade':>6}")
        print("-" * 90)

        rows = []
        for name, st in sorted(self.stats.items(), key=lambda x: -x[1]["pnl"]):
            total = st["wins"] + st["losses"] + st["expired"]
            if total == 0:
                continue
            wr = st["wins"] / (st["wins"] + st["losses"]) * 100 if (st["wins"] + st["losses"]) > 0 else 0
            per_trade = st["pnl"] / total
            grade = "A+" if wr >= 70 and per_trade > 0 else \
                    "A" if wr >= 60 and per_trade > 0 else \
                    "B" if wr >= 50 and per_trade > 0 else \
                    "C" if per_trade > 0 else "D"
            print(f"{name:<25} {total:>5} {st['wins']:>5} {st['losses']:>5} {st['expired']:>4} "
                  f"{wr:>5.1f}% {st['pnl']:>+8.0f} {per_trade:>+8.1f} {grade:>6}")
            rows.append({"signal": name, "n": total, "wins": st["wins"], "losses": st["losses"],
                         "wr": round(wr, 1), "pnl": round(st["pnl"]), "per_trade": round(per_trade, 1)})

        total_pnl = sum(s["pnl"] for s in self.stats.values())
        total_n = sum(s["wins"] + s["losses"] + s["expired"] for s in self.stats.values())
        total_w = sum(s["wins"] for s in self.stats.values())
        total_l = sum(s["losses"] for s in self.stats.values())
        total_wr = total_w / (total_w + total_l) * 100 if (total_w + total_l) > 0 else 0

        print("-" * 90)
        print(f"{'TOTAL':<25} {total_n:>5} {total_w:>5} {total_l:>5} "
              f"{'':>4} {total_wr:>5.1f}% {total_pnl:>+8.0f} "
              f"{total_pnl/total_n if total_n else 0:>+8.1f}")
        print("=" * 90)

        # Save to CSV
        pd.DataFrame(rows).to_csv("backtest_14d_results.csv", index=False)
        log.info("Results saved to backtest_14d_results.csv")


def main():
    t0 = time.time()

    # 1. Load ticks
    ticks = load_ticks(DAYS)

    # 2. Build bars
    bars = ticks_to_5min_bars(ticks)

    # 3. Run engine bar-by-bar with tick tracking
    engine = SignalEngine()
    tracker = SignalTracker()

    # Group ticks by 5-min window for tick-level tracking
    ticks["bar_idx"] = np.searchsorted(
        pd.to_datetime(bars["datetime"]).values.astype("int64"),
        ticks["timestamp"].values.astype("int64"),
        side="right"
    ) - 1
    ticks["bar_idx"] = ticks["bar_idx"].clip(0, len(bars) - 1)

    log.info(f"Starting tick-by-tick backtest: {len(bars)} bars, {len(ticks):,} ticks")

    warmup = 50  # need enough bars for indicators
    last_eval_bar = -1

    for bar_i in range(warmup, len(bars)):
        # Get ticks for this bar
        bar_ticks = ticks[ticks["bar_idx"] == bar_i]

        # Process each tick
        for _, tick in bar_ticks.iterrows():
            tracker.process_tick(tick["price"])

        # Evaluate signals at bar close
        history = bars.iloc[:bar_i + 1]
        bar_dt = bars.iloc[bar_i].get("datetime")
        now = pd.to_datetime(bar_dt) if bar_dt else datetime.now()

        signals = engine.evaluate(history, current_price=float(bars.iloc[bar_i]["close"]),
                                  now=now)

        # Add qualifying signals
        tracker.add_signals([s for s in signals if s["confidence_pct"] >= 50], bar_dt)
        tracker.process_bar_end()

        if bar_i % 100 == 0:
            elapsed = time.time() - t0
            pct = bar_i / len(bars) * 100
            active = len(tracker.active)
            resolved = len(tracker.resolved)
            log.info(f"Bar {bar_i}/{len(bars)} ({pct:.0f}%) | "
                     f"active={active} resolved={resolved} | {elapsed:.0f}s")

    # Expire remaining
    for sid, s in list(tracker.active.items()):
        s["result"] = "expired_end"
        tracker.stats[s["name"]]["expired"] += 1
        tracker.resolved.append(s)
    tracker.active.clear()

    elapsed = time.time() - t0
    log.info(f"Backtest complete in {elapsed:.0f}s ({len(ticks):,} ticks, {len(bars)} bars)")

    # 4. Report
    tracker.report()


if __name__ == "__main__":
    main()
