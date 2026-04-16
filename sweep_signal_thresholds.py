"""Parameter sweep for signal thresholds and per-type performance.

Runs the SignalEngine over historical Nautilus data and reports:
- per-signal win rate and PnL
- effect of confidence floor
- effect of keeping weaker signals as confirmations

Usage:
    python sweep_signal_thresholds.py
"""

from __future__ import annotations

import logging
from collections import defaultdict
from itertools import product
from datetime import timedelta

import pandas as pd

from backtest_14d import load_ticks, ticks_to_5min_bars, POINT_VALUE
from signal_engine import SignalEngine

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger("sweep")


class Tracker:
    def __init__(self):
        self.stats = defaultdict(lambda: {"w": 0, "l": 0, "x": 0, "pnl": 0.0, "n": 0})

    def add(self, signals, bar_time):
        for s in signals:
            st = self.stats[s["name"]]
            st["n"] += 1
            risk = abs(s["entry"] - s["sl"])
            reward = abs(s["tp1"] - s["entry"])
            if risk <= 0 or reward <= 0:
                continue
            if s["direction"] == "long":
                if s.get("pmin", s["entry"]) <= s["sl"]:
                    st["l"] += 1
                    st["pnl"] -= risk * POINT_VALUE
                elif s.get("pmax", s["entry"]) >= s["tp1"]:
                    st["w"] += 1
                    st["pnl"] += reward * POINT_VALUE
            else:
                if s.get("pmax", s["entry"]) >= s["sl"]:
                    st["l"] += 1
                    st["pnl"] -= risk * POINT_VALUE
                elif s.get("pmin", s["entry"]) <= s["tp1"]:
                    st["w"] += 1
                    st["pnl"] += reward * POINT_VALUE


def run_once(conf_floor: int, keep_confirms: bool):
    ticks = load_ticks(30)
    bars = ticks_to_5min_bars(ticks)
    engine = SignalEngine()
    tracker = Tracker()

    ticks["bar_idx"] = pd.Series(pd.cut(ticks["timestamp"], bins=bars["datetime"].tolist() + [bars["datetime"].iloc[-1] + pd.Timedelta(minutes=5)], labels=False, right=True))
    ticks["bar_idx"] = ticks["bar_idx"].fillna(0).astype(int).clip(0, len(bars) - 1)

    for bar_i in range(50, len(bars)):
        history = bars.iloc[: bar_i + 1]
        now = pd.to_datetime(bars.iloc[bar_i]["datetime"])
        signals = engine.evaluate(history, current_price=float(bars.iloc[bar_i]["close"]), now=now)
        if not keep_confirms:
            signals = [s for s in signals if s["confidence_pct"] >= conf_floor]
        else:
            strong = [s for s in signals if s["confidence_pct"] >= conf_floor]
            weak = [s for s in signals if conf_floor - 15 <= s["confidence_pct"] < conf_floor]
            signals = strong + weak[:3]

        bar_ticks = ticks[ticks["bar_idx"] == bar_i]
        pmin = float(bar_ticks["price"].min()) if not bar_ticks.empty else float(bars.iloc[bar_i]["close"])
        pmax = float(bar_ticks["price"].max()) if not bar_ticks.empty else float(bars.iloc[bar_i]["close"])
        for s in signals:
            s["pmin"] = pmin
            s["pmax"] = pmax
        tracker.add(signals, now)

    total = sum(v["w"] + v["l"] for v in tracker.stats.values())
    wins = sum(v["w"] for v in tracker.stats.values())
    losses = sum(v["l"] for v in tracker.stats.values())
    pnl = sum(v["pnl"] for v in tracker.stats.values())
    wr = wins / (wins + losses) * 100 if wins + losses else 0
    return {"conf_floor": conf_floor, "keep_confirms": keep_confirms, "n": total, "wr": wr, "pnl": pnl}


def main():
    rows = []
    for conf_floor, keep_confirms in product([35, 40, 45, 50], [False, True]):
        try:
            rows.append(run_once(conf_floor, keep_confirms))
        except Exception as e:
            log.warning("sweep failed conf=%s keep=%s: %s", conf_floor, keep_confirms, e)

    out = pd.DataFrame(rows).sort_values(["pnl", "wr"], ascending=False)
    print(out.to_string(index=False))
    out.to_csv("signal_threshold_sweep.csv", index=False)
    log.info("Saved signal_threshold_sweep.csv")


if __name__ == "__main__":
    main()
