"""Fast 14-day backtest — reads Nautilus parquet files directly (no catalog API).

Usage:
    /Users/sacredforest/Trading Setup/Testing Nautilus/venv/bin/python backtest_fast.py
"""
import os, sys, time, struct, logging
from collections import defaultdict
from datetime import datetime

import pandas as pd
import numpy as np

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from signal_engine import SignalEngine
from bar_builder import enrich_bars

logging.basicConfig(level=logging.INFO, format='%(asctime)s %(levelname)s %(message)s')
log = logging.getLogger("bt")

DATA_DIR = "/Users/sacredforest/Trading Setup/Testing Nautilus/catalog/data/trade_tick/MNQ.CME"
POINT_VALUE = 2.0


def decode_nautilus_parquet(path: str) -> pd.DataFrame:
    """Read Nautilus binary parquet and decode decimal128 prices."""
    df = pd.read_parquet(path)
    if df.empty:
        return pd.DataFrame()

    def dec128(b):
        if isinstance(b, bytes) and len(b) == 16:
            lo = struct.unpack('<Q', b[:8])[0]
            hi = struct.unpack('<q', b[8:])[0]
            return ((hi << 64) | lo) / 1_000_000_000
        return float('nan')

    # Vectorized decode
    prices = np.array([dec128(b) for b in df['price'].values])
    sizes = np.array([dec128(b) for b in df['size'].values])

    # Prices are way too large — find correct scale
    # MNQ should be ~18000-25000
    if len(prices) > 0 and prices[0] > 1e6:
        # Try different scales
        for exp in range(1, 15):
            test = prices[0] / (10 ** exp)
            if 10000 < test < 50000:
                prices = prices / (10 ** exp)
                sizes = sizes / (10 ** exp)
                break

    return pd.DataFrame({
        "timestamp": pd.to_datetime(df["ts_event"], unit="ns"),
        "price": prices,
        "size": sizes.astype(int).clip(1),
        "aggressor": df["aggressor_side"].values,  # 0=no, 1=buyer, 2=seller
    })


def ticks_to_bars(ticks: pd.DataFrame) -> pd.DataFrame:
    """5-min OHLCV + delta from ticks."""
    df = ticks.set_index("timestamp")
    bars = df["price"].resample("5min").agg(["first", "max", "min", "last"])
    bars.columns = ["open", "high", "low", "close"]
    bars["volume"] = df["size"].resample("5min").sum()

    buy = df.loc[df["aggressor"] == 1, "size"].resample("5min").sum()
    sell = df.loc[df["aggressor"] == 2, "size"].resample("5min").sum()
    bars["delta"] = buy.sub(sell, fill_value=0)

    bars.dropna(subset=["open"], inplace=True)
    bars.reset_index(inplace=True)
    bars.rename(columns={"timestamp": "datetime"}, inplace=True)
    return bars


class Tracker:
    """Tick-level SL/TP tracker."""

    def __init__(self):
        self.active = {}
        self.stats = defaultdict(lambda: {"w": 0, "l": 0, "x": 0, "pnl": 0.0, "trades": []})

    def add(self, signals, bar_time):
        for s in signals:
            if s["id"] in self.active:
                continue
            self.active[s["id"]] = {**s, "pmin": s["entry"], "pmax": s["entry"],
                                     "filled": False, "bars": 0, "t0": bar_time}

    def tick(self, price):
        dead = []
        for sid, s in self.active.items():
            s["pmin"] = min(s["pmin"], price)
            s["pmax"] = max(s["pmax"], price)

            # Check fill
            if not s["filled"]:
                if s["direction"] == "long" and price <= s["entry"]:
                    s["filled"] = True
                elif s["direction"] == "short" and price >= s["entry"]:
                    s["filled"] = True

            # SL/TP check (even if not filled — signal is invalidated)
            result = None
            if s["direction"] == "long":
                if s["pmin"] <= s["sl"]:
                    result = "L"
                elif s["pmax"] >= s["tp1"]:
                    result = "W"
            else:
                if s["pmax"] >= s["sl"]:
                    result = "L"
                elif s["pmin"] <= s["tp1"]:
                    result = "W"

            if result:
                risk = abs(s["entry"] - s["sl"])
                reward = abs(s["tp1"] - s["entry"])
                pnl = (reward if result == "W" else -risk) * POINT_VALUE
                name = s["name"]
                self.stats[name][{"W": "w", "L": "l"}[result]] += 1
                self.stats[name]["pnl"] += pnl
                self.stats[name]["trades"].append({
                    "dir": s["direction"], "result": result, "pnl": pnl,
                    "conf": s.get("confidence_pct", 0),
                    "grade": s.get("quality_grade", "?"),
                    "tier": s.get("tier_label", ""),
                    "filled": s["filled"],
                })
                dead.append(sid)

        for sid in dead:
            del self.active[sid]

    def bar_end(self):
        dead = []
        for sid, s in self.active.items():
            s["bars"] += 1
            if s["bars"] > 48:
                self.stats[s["name"]]["x"] += 1
                dead.append(sid)
        for sid in dead:
            del self.active[sid]

    def report(self):
        print("\n" + "=" * 95)
        print(f"{'Signal':<25} {'N':>4} {'W':>4} {'L':>4} {'X':>3} "
              f"{'WR%':>6} {'PnL$':>8} {'$/tr':>7} {'Grd':>4} {'Fill%':>5}")
        print("-" * 95)

        rows = []
        for name in sorted(self.stats, key=lambda n: -self.stats[n]["pnl"]):
            st = self.stats[name]
            total = st["w"] + st["l"] + st["x"]
            if total == 0:
                continue
            wr = st["w"] / (st["w"] + st["l"]) * 100 if (st["w"] + st["l"]) > 0 else 0
            pt = st["pnl"] / total
            filled = sum(1 for t in st["trades"] if t["filled"])
            fill_pct = filled / len(st["trades"]) * 100 if st["trades"] else 0
            grade = "A+" if wr >= 70 and pt > 0 else "A" if wr >= 60 and pt > 0 else \
                    "B" if wr >= 50 and pt > 0 else "C" if pt > 0 else "D"
            print(f"{name:<25} {total:>4} {st['w']:>4} {st['l']:>4} {st['x']:>3} "
                  f"{wr:>5.1f}% {st['pnl']:>+8.0f} {pt:>+6.1f} {grade:>4} {fill_pct:>4.0f}%")
            rows.append({"signal": name, "n": total, "w": st["w"], "l": st["l"],
                         "wr": round(wr, 1), "pnl": round(st["pnl"]), "per_trade": round(pt, 1)})

        total_pnl = sum(s["pnl"] for s in self.stats.values())
        total_n = sum(s["w"] + s["l"] + s["x"] for s in self.stats.values())
        total_w = sum(s["w"] for s in self.stats.values())
        total_l = sum(s["l"] for s in self.stats.values())
        wr = total_w / (total_w + total_l) * 100 if (total_w + total_l) > 0 else 0
        print("-" * 95)
        print(f"{'TOTAL':<25} {total_n:>4} {total_w:>4} {total_l:>4} "
              f"{'':>3} {wr:>5.1f}% {total_pnl:>+8.0f} "
              f"{total_pnl / total_n if total_n else 0:>+6.1f}")
        print("=" * 95)

        pd.DataFrame(rows).to_csv("backtest_results.csv", index=False)
        log.info("Saved to backtest_results.csv")


def main():
    t0 = time.time()

    # Find last 14 RTH session files
    files = sorted([os.path.join(DATA_DIR, f) for f in os.listdir(DATA_DIR) if f.endswith(".parquet")])
    # Filter: only RTH sessions (13:30-20:00 UTC = 9:30-16:00 ET)
    rth_files = [f for f in files if "T13-30" in os.path.basename(f)]
    rth_files = rth_files[-14:]  # last 14 sessions
    log.info(f"Selected {len(rth_files)} RTH sessions for backtest")

    engine = SignalEngine()
    tracker = Tracker()
    all_bars = pd.DataFrame()

    for fi, fpath in enumerate(rth_files):
        fname = os.path.basename(fpath)
        log.info(f"[{fi + 1}/{len(rth_files)}] {fname}")

        ticks = decode_nautilus_parquet(fpath)
        if ticks.empty:
            log.warning(f"  Empty file, skipping")
            continue

        log.info(f"  {len(ticks):,} ticks, price range: {ticks['price'].min():.2f}-{ticks['price'].max():.2f}")

        # Build 5-min bars for this session
        session_bars = ticks_to_bars(ticks)
        if session_bars.empty:
            continue

        # Append to rolling history (keep last 200 bars)
        all_bars = pd.concat([all_bars, session_bars], ignore_index=True).tail(200)
        all_bars = enrich_bars(all_bars)

        # Process bar by bar
        bar_start = max(0, len(all_bars) - len(session_bars))
        for bi in range(bar_start, len(all_bars)):
            # Get ticks for this bar's time window
            bar_dt = all_bars.iloc[bi]["datetime"]
            bar_end = bar_dt
            bar_begin = bar_dt - pd.Timedelta(minutes=5)
            bar_ticks = ticks[(ticks["timestamp"] > bar_begin) & (ticks["timestamp"] <= bar_end)]

            # Process ticks
            for price in bar_ticks["price"].values:
                tracker.tick(price)

            # Evaluate at bar close
            if bi >= 20:  # need warmup
                history = all_bars.iloc[:bi + 1]
                now = pd.to_datetime(bar_dt)
                try:
                    signals = engine.evaluate(history, current_price=float(all_bars.iloc[bi]["close"]), now=now)
                    tracker.add([s for s in signals if s["confidence_pct"] >= 50], bar_dt)
                except Exception as e:
                    if bi == bar_start:
                        log.warning(f"  Engine error: {e}")

            tracker.bar_end()

        active = len(tracker.active)
        resolved = sum(s["w"] + s["l"] + s["x"] for s in tracker.stats.values())
        log.info(f"  Bars: {len(session_bars)} | Active: {active} | Resolved: {resolved}")

    elapsed = time.time() - t0
    log.info(f"Backtest complete in {elapsed:.0f}s")
    tracker.report()


if __name__ == "__main__":
    main()
