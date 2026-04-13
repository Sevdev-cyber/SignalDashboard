"""Backtest the execution bot on top of live dashboard signals.

This runner uses the same pieces as live mode:
  - ``SignalEngine`` in ``final_mtf_v3`` mode
  - ``MarketSnapshotBot`` for context / bias / scenario
  - ``SignalExecutionBot`` for trade selection and management

It is intentionally bar-driven for signal generation and tick-driven for
fill / hard SL / hard TP handling.
"""

from __future__ import annotations

import argparse
import json
import os
import struct
import sys
import time
from collections import Counter
from dataclasses import asdict
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd

THIS_DIR = Path(__file__).resolve().parent
sys.path.insert(0, str(THIS_DIR))

from bar_builder import enrich_bars  # noqa: E402
from market_snapshot_bot import MarketSnapshotBot  # noqa: E402
from signal_engine import SignalEngine  # noqa: E402
from signal_execution_bot import ExecutionConfig, SignalExecutionBot  # noqa: E402

DATA_DIR = Path("/Users/sacredforest/Trading Setup/Testing Nautilus/catalog/data/trade_tick/MNQ.CME")
OUTPUT_DIR = Path("/Users/sacredforest/Trading Setup/NewSignal")
POINT_VALUE = 2.0


def decode_nautilus_parquet(path: Path) -> pd.DataFrame:
    df = pd.read_parquet(path)
    if df.empty:
        return pd.DataFrame()

    def dec128(buf: Any) -> float:
        if isinstance(buf, bytes) and len(buf) == 16:
            lo = struct.unpack("<Q", buf[:8])[0]
            hi = struct.unpack("<q", buf[8:])[0]
            return ((hi << 64) | lo) / 1_000_000_000
        return float("nan")

    prices = np.array([dec128(x) for x in df["price"].values], dtype=np.float64)
    sizes = np.array([dec128(x) for x in df["size"].values], dtype=np.float64)
    if len(prices) > 0 and prices[0] > 1e6:
        for exp in range(1, 15):
            test = prices[0] / (10 ** exp)
            if 10000 < test < 50000:
                prices = prices / (10 ** exp)
                sizes = sizes / (10 ** exp)
                break

    out = pd.DataFrame(
        {
            "timestamp": pd.to_datetime(df["ts_event"], unit="ns"),
            "price": prices,
            "size": np.clip(np.rint(sizes).astype(int), 1, None),
            "aggressor": df["aggressor_side"].astype(int).values,
        }
    )
    out = out.dropna(subset=["timestamp", "price"]).reset_index(drop=True)
    return out


def ticks_to_bars(ticks: pd.DataFrame, timeframe_min: int) -> pd.DataFrame:
    frame = ticks.set_index("timestamp").sort_index()
    rule = f"{timeframe_min}min"

    bars = frame["price"].resample(rule).agg(["first", "max", "min", "last"])
    bars.columns = ["open", "high", "low", "close"]
    bars["volume"] = frame["size"].resample(rule).sum()

    buy = frame.loc[frame["aggressor"] == 1, "size"].resample(rule).sum()
    sell = frame.loc[frame["aggressor"] == 2, "size"].resample(rule).sum()
    bars["buy_volume"] = buy.fillna(0.0)
    bars["sell_volume"] = sell.fillna(0.0)
    bars["delta"] = bars["buy_volume"] - bars["sell_volume"]
    bars["trade_value"] = (frame["price"] * frame["size"]).resample(rule).sum()
    bars["has_real_tick_delta"] = True

    bars = bars.dropna(subset=["open"]).reset_index()
    bars.rename(columns={"timestamp": "datetime"}, inplace=True)
    return bars


def list_rth_files(days: int, *, start_date: str | None = None, end_date: str | None = None) -> list[Path]:
    files = sorted(p for p in DATA_DIR.iterdir() if p.suffix == ".parquet")
    rth = []
    for path in files:
        if "T13-30" not in path.name:
            continue
        session_date = path.name.split("T", 1)[0]
        if start_date and session_date < start_date:
            continue
        if end_date and session_date > end_date:
            continue
        rth.append(path)
    return rth[-days:]


def summarize_trades(df: pd.DataFrame) -> dict[str, Any]:
    if df.empty:
        return {
            "total_trades": 0,
            "wins": 0,
            "losses": 0,
            "scratches": 0,
            "win_rate": 0.0,
            "profit_factor": 0.0,
            "gross_points": 0.0,
            "gross_dollars": 0.0,
            "avg_points": 0.0,
            "avg_r": 0.0,
            "median_hold_bars": 0.0,
        }

    wins = int((df["gross_points"] > 0).sum())
    losses = int((df["gross_points"] < 0).sum())
    scratches = int((df["gross_points"] == 0).sum())
    gross_points = float(df["gross_points"].sum())
    gross_dollars = gross_points * POINT_VALUE
    pos_sum = float(df.loc[df["gross_points"] > 0, "gross_points"].sum())
    neg_sum = float(-df.loc[df["gross_points"] < 0, "gross_points"].sum())
    return {
        "total_trades": int(len(df)),
        "wins": wins,
        "losses": losses,
        "scratches": scratches,
        "win_rate": round((wins / max(wins + losses, 1)) * 100.0, 1),
        "profit_factor": round(pos_sum / neg_sum, 2) if neg_sum > 0 else round(pos_sum, 2),
        "gross_points": round(gross_points, 2),
        "gross_dollars": round(gross_dollars, 2),
        "avg_points": round(float(df["gross_points"].mean()), 2),
        "avg_r": round(float(df["gross_r"].mean()), 2),
        "median_hold_bars": round(float(df["bars_held"].median()), 1),
    }


def save_outputs(
    *,
    trades_df: pd.DataFrame,
    events_df: pd.DataFrame,
    summary: dict[str, Any],
    per_signal: list[dict[str, Any]],
    out_prefix: str,
) -> dict[str, Path]:
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    trades_path = OUTPUT_DIR / f"{out_prefix}_trades.csv"
    events_path = OUTPUT_DIR / f"{out_prefix}_events.csv"
    summary_path = OUTPUT_DIR / f"{out_prefix}_summary.json"
    report_path = OUTPUT_DIR / f"{out_prefix}_report.txt"

    trades_df.to_csv(trades_path, index=False)
    events_df.to_csv(events_path, index=False)
    summary_path.write_text(json.dumps(summary, ensure_ascii=True, indent=2), encoding="utf-8")

    lines = [
        f"Execution bot backtest: {summary.get('days', 0)} sessions",
        f"TF: {summary.get('timeframe_min', 0)}m | Engine: {summary.get('engine_mode', '')}",
        "",
        f"Trades: {summary.get('total_trades', 0)}",
        f"Wins/Losses/Scratch: {summary.get('wins', 0)}/{summary.get('losses', 0)}/{summary.get('scratches', 0)}",
        f"Win rate: {summary.get('win_rate', 0.0)}%",
        f"Gross points: {summary.get('gross_points', 0.0)}",
        f"Gross dollars: ${summary.get('gross_dollars', 0.0)}",
        f"Avg points: {summary.get('avg_points', 0.0)}",
        f"Avg R: {summary.get('avg_r', 0.0)}",
        f"Profit factor: {summary.get('profit_factor', 0.0)}",
        f"Median hold bars: {summary.get('median_hold_bars', 0.0)}",
        "",
        "Exit reasons:",
    ]
    for key, value in (summary.get("exit_reason_counts") or {}).items():
        lines.append(f"  {key}: {value}")
    lines.append("")
    lines.append("Per signal:")
    for row in per_signal:
        lines.append(
            f"  {row['signal_name']}: n={row['trades']} wr={row['win_rate']}% "
            f"pts={row['gross_points']} avgR={row['avg_r']} exits={row['top_exit_reason']}"
        )
    report_path.write_text("\n".join(lines) + "\n", encoding="utf-8")
    return {
        "trades": trades_path,
        "events": events_path,
        "summary": summary_path,
        "report": report_path,
    }


def run_backtest(
    days: int,
    timeframe_min: int,
    config: ExecutionConfig,
    out_prefix: str,
    *,
    start_date: str | None = None,
    end_date: str | None = None,
    min_bars: int = 350,
) -> dict[str, Any]:
    os.environ.setdefault("SIGNAL_ENGINE_MODE", "final_mtf_v3")
    selected = list_rth_files(days, start_date=start_date, end_date=end_date)
    if not selected:
        raise FileNotFoundError(f"No RTH parquet files found in {DATA_DIR}")

    engine = SignalEngine()
    snapshot_bot = MarketSnapshotBot()
    execution_bot = SignalExecutionBot(config)
    all_bars = pd.DataFrame()
    used_files: list[str] = []
    skipped_files: list[tuple[str, int]] = []
    warmup = max(60, 60 * max(timeframe_min, 1))
    t0 = time.time()

    for path in selected:
        ticks = decode_nautilus_parquet(path)
        if ticks.empty:
            continue
        session_bars = ticks_to_bars(ticks, timeframe_min)
        if session_bars.empty or len(session_bars) < min_bars:
            skipped_files.append((path.name, len(session_bars)))
            continue

        execution_bot.start_session(path.name)
        used_files.append(path.name)
        all_bars = pd.concat([all_bars, session_bars], ignore_index=True).tail(800)
        all_bars = enrich_bars(all_bars)
        bar_start = max(0, len(all_bars) - len(session_bars))

        for bar_index in range(bar_start, len(all_bars)):
            row = all_bars.iloc[bar_index]
            bar_dt = pd.to_datetime(row["datetime"])
            bar_begin = bar_dt - pd.Timedelta(minutes=timeframe_min)
            bar_ticks = ticks[(ticks["timestamp"] > bar_begin) & (ticks["timestamp"] <= bar_dt)]
            for tick in bar_ticks.itertuples(index=False):
                execution_bot.on_tick(float(tick.price), pd.Timestamp(tick.timestamp).isoformat(), bar_index)

            if bar_index < warmup:
                continue

            history = all_bars.iloc[: bar_index + 1].copy()
            price = float(row["close"])
            volume = float(row.get("volume", 0.0) or 0.0)
            atr = float(row.get("atr", 0.0) or 0.0)
            bar_delta_pct = (float(row.get("delta", 0.0)) / volume * 100.0) if volume > 0 else 0.0

            signals = engine.evaluate(
                history,
                bar_delta_pct=bar_delta_pct,
                current_price=price,
                now=bar_dt.to_pydatetime(),
            )
            zones = engine.compute_weighted_zones(signals)
            state = engine.get_market_state(
                history,
                current_price=price,
                bar_delta_pct=bar_delta_pct,
                now=bar_dt.to_pydatetime(),
            )
            payload = {
                "state": state,
                "signals": signals,
                "ghost_signals": [],
                "zones": zones,
            }
            market_decision = snapshot_bot.analyze(payload).to_dict()
            execution_bot.on_bar_close(
                bar_index=bar_index,
                timestamp=bar_dt.isoformat(),
                price=price,
                atr=atr,
                signals=signals,
                market_decision=market_decision,
                state=state,
            )

        execution_bot.flatten(
            price=float(all_bars.iloc[-1]["close"]),
            timestamp=pd.to_datetime(all_bars.iloc[-1]["datetime"]).isoformat(),
            reason="session_flat",
        )

    trades_df = pd.DataFrame([asdict(t) for t in execution_bot.closed_trades])
    if not trades_df.empty:
        trades_df["gross_dollars"] = trades_df["gross_points"] * POINT_VALUE
    events_df = pd.DataFrame(execution_bot.event_log)

    summary = summarize_trades(trades_df)
    exit_counts = Counter()
    if not trades_df.empty:
        exit_counts.update(trades_df["exit_reason"].astype(str).tolist())
    summary.update(
        {
            "days": len(used_files),
            "timeframe_min": timeframe_min,
            "engine_mode": os.getenv("SIGNAL_ENGINE_MODE", "final_mtf_v3"),
            "runtime_sec": round(time.time() - t0, 1),
            "files": used_files,
            "skipped_files": [name for name, _ in skipped_files],
            "exit_reason_counts": dict(exit_counts),
        }
    )

    per_signal: list[dict[str, Any]] = []
    if not trades_df.empty:
        for signal_name, grp in trades_df.groupby("signal_name"):
            wins = int((grp["gross_points"] > 0).sum())
            losses = int((grp["gross_points"] < 0).sum())
            win_rate = round((wins / max(wins + losses, 1)) * 100.0, 1)
            exit_counter = Counter(grp["exit_reason"].astype(str).tolist())
            per_signal.append(
                {
                    "signal_name": signal_name,
                    "trades": int(len(grp)),
                    "win_rate": win_rate,
                    "gross_points": round(float(grp["gross_points"].sum()), 2),
                    "avg_r": round(float(grp["gross_r"].mean()), 2),
                    "top_exit_reason": exit_counter.most_common(1)[0][0] if exit_counter else "",
                }
            )
        per_signal.sort(key=lambda x: (x["gross_points"], x["win_rate"]), reverse=True)

    paths = save_outputs(
        trades_df=trades_df,
        events_df=events_df,
        summary=summary,
        per_signal=per_signal,
        out_prefix=out_prefix,
    )
    return {
        "summary": summary,
        "paths": {k: str(v) for k, v in paths.items()},
        "per_signal": per_signal,
    }


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Backtest execution bot on SignalDashboard outputs.")
    parser.add_argument("--days", type=int, default=15, help="Number of recent RTH sessions.")
    parser.add_argument("--tf", type=int, default=1, help="Bar timeframe in minutes.")
    parser.add_argument("--start-date", default=None, help="Inclusive date filter YYYY-MM-DD.")
    parser.add_argument("--end-date", default=None, help="Inclusive date filter YYYY-MM-DD.")
    parser.add_argument("--min-bars", type=int, default=350, help="Minimum non-empty bars per session.")
    parser.add_argument("--out-prefix", default="execution_bot_backtest_15d", help="Output filename prefix.")
    args = parser.parse_args(argv)

    result = run_backtest(
        days=args.days,
        timeframe_min=args.tf,
        config=ExecutionConfig(),
        out_prefix=args.out_prefix,
        start_date=args.start_date,
        end_date=args.end_date,
        min_bars=args.min_bars,
    )
    print(json.dumps(result["summary"], ensure_ascii=True, indent=2))
    print(json.dumps(result["paths"], ensure_ascii=True, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
