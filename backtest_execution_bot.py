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
from zoneinfo import ZoneInfo

THIS_DIR = Path(__file__).resolve().parent
sys.path.insert(0, str(THIS_DIR))
ROOT_DIR = THIS_DIR.parent
NEWSIGNAL_DIR = ROOT_DIR / "NewSignal"
if str(NEWSIGNAL_DIR) not in sys.path:
    sys.path.insert(0, str(NEWSIGNAL_DIR))

from bar_builder import enrich_bars  # noqa: E402
from daily_htf_context import DailyHTFContextService  # noqa: E402
from env_bootstrap import load_project_env  # noqa: E402
from intraday_llm_context import IntradayLLMContextService  # noqa: E402
from market_snapshot_bot import MarketSnapshotBot  # noqa: E402
from signal_engine import SignalEngine  # noqa: E402
from signal_execution_bot import ExecutionConfig, SignalExecutionBot  # noqa: E402
from newsignal_core import NewSignalGenerator  # noqa: E402

load_project_env()

DATA_DIR = Path(os.environ.get("SIGNAL_BACKTEST_DATA_DIR", "/Users/sacredforest/Trading Setup/Testing Nautilus/catalog/data/trade_tick/MNQ.CME"))
OUTPUT_DIR = Path(os.environ.get("SIGNAL_BACKTEST_OUTPUT_DIR", "/Users/sacredforest/Trading Setup/NewSignal"))
POINT_VALUE = 2.0
UTC = ZoneInfo("UTC")
ET = ZoneInfo("America/New_York")


def _norm_bias(value: Any) -> str:
    text = str(value or "").strip().lower()
    return text if text else "neutral"


def _llm_direction(value: Any) -> str:
    text = _norm_bias(value)
    if text.endswith("long"):
        return "long"
    if text.endswith("short"):
        return "short"
    return "neutral"


def _execution_view(signals: list[dict[str, Any]]) -> dict[str, Any]:
    shortlist = [dict(sig) for sig in signals[:4] if isinstance(sig, dict)]
    primary = dict(shortlist[0]) if shortlist else {}
    return {"primary": primary, "shortlist": shortlist}


def _inject_daily_context(
    state: dict[str, Any],
    *,
    bars_df: pd.DataFrame,
    price: float,
    now,
    service: DailyHTFContextService | None,
) -> dict[str, Any] | None:
    if service is None:
        return None
    trader_guide = dict(state.get("trader_guide") or {})
    context = service.maybe_refresh(
        bars_df=bars_df,
        trader_guide=trader_guide,
        current_price=price,
        now=now,
    )
    if context:
        trader_guide["daily_context"] = context
        state["daily_context"] = context
        state["trader_guide"] = trader_guide
    return context


def _inject_intraday_llm_context(
    state: dict[str, Any],
    *,
    signals: list[dict[str, Any]],
    market_decision: dict[str, Any],
    zones: dict[str, Any],
    now,
    service: IntradayLLMContextService | None,
) -> dict[str, Any] | None:
    if service is None:
        return None
    payload = {
        "state": state,
        "signals": signals,
        "ghost_signals": [],
        "market_decision": market_decision,
        "execution": _execution_view(signals),
        "decision_ledger": [],
        "zones": zones,
    }
    context = service.maybe_refresh(payload, now=now)
    if context:
        trader_guide = dict(state.get("trader_guide") or {})
        trader_guide["llm_context"] = context
        state["llm_context"] = context
        chart_annotations = [x for x in list(context.get("chart_annotations") or []) if isinstance(x, dict)]
        if chart_annotations:
            trader_guide["zones"] = (chart_annotations + list(trader_guide.get("zones") or []))[:6]
        state["trader_guide"] = trader_guide
    return context


def _merge_market_decision_with_llm(
    market_decision: dict[str, Any],
    llm_context: dict[str, Any] | None,
) -> tuple[dict[str, Any], dict[str, Any]]:
    merged = dict(market_decision or {})
    meta = {
        "assist_state": "disabled" if not llm_context else "no_change",
        "base_bias": _norm_bias(merged.get("bias")),
        "base_confidence": int(float(merged.get("confidence") or 0)),
        "llm_bias": _norm_bias((llm_context or {}).get("bias")),
        "llm_confidence": int(float((llm_context or {}).get("confidence") or 0)),
    }
    if not llm_context:
        return merged, meta

    base_bias = meta["base_bias"]
    base_conf = meta["base_confidence"]
    llm_bias = meta["llm_bias"]
    llm_conf = meta["llm_confidence"]
    llm_dir = _llm_direction(llm_bias)
    base_dir = _llm_direction(base_bias)
    llm_summary = str(llm_context.get("summary") or "").strip()

    if llm_summary:
        merged["summary"] = f"{merged.get('summary', '')} | LLM: {llm_summary}".strip(" |")

    if llm_dir == "neutral":
        if base_conf < 72:
            merged["action"] = "wait"
            meta["assist_state"] = "neutral_gate"
        return merged, meta

    if base_dir == "neutral":
        meta["assist_state"] = "watch_only"
        merged["llm_watch_bias"] = llm_bias
        merged["llm_watch_confidence"] = llm_conf
        return merged, meta

    if llm_dir == base_dir:
        merged["confidence"] = max(base_conf, min(95, int(round(base_conf * 0.70 + llm_conf * 0.30))))
        meta["assist_state"] = "aligned_boost"
        return merged, meta

    if llm_conf >= 80:
        merged["bias"] = llm_dir
        merged["confidence"] = max(base_conf, llm_conf)
        merged["action"] = f"watch_{llm_dir}"
        meta["assist_state"] = "hard_override"
        return merged, meta

    if llm_conf >= 68:
        merged["action"] = "wait"
        merged["confidence"] = min(base_conf, max(55, llm_conf))
        meta["assist_state"] = "soft_gate"
        return merged, meta

    return merged, meta


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


def list_rth_files(
    days: int,
    *,
    data_dir: Path | None = None,
    start_date: str | None = None,
    end_date: str | None = None,
    explicit_dates: set[str] | None = None,
) -> list[Path]:
    root = data_dir or DATA_DIR
    files = sorted(p for p in root.iterdir() if p.suffix == ".parquet")
    rth = []
    for path in files:
        if "T13-30" not in path.name:
            continue
        session_date = path.name.split("T", 1)[0]
        if explicit_dates is not None and session_date not in explicit_dates:
            continue
        if start_date and session_date < start_date:
            continue
        if end_date and session_date > end_date:
            continue
        rth.append(path)
    if explicit_dates is not None:
        return rth
    return rth[-days:]


def load_explicit_dates(path: str | None) -> set[str] | None:
    if not path:
        return None
    raw = Path(path).read_text(encoding="utf-8")
    dates = {
        line.strip()
        for line in raw.splitlines()
        if line.strip() and not line.strip().startswith("#")
    }
    return dates or None


def parse_playbooks(raw: str | None) -> tuple[str, ...]:
    if not raw:
        return ()
    items = [item.strip().upper() for item in str(raw).split(",")]
    return tuple(item for item in items if item)


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
    llm_df: pd.DataFrame | None,
    summary: dict[str, Any],
    per_signal: list[dict[str, Any]],
    out_prefix: str,
    output_dir: Path,
) -> dict[str, Path]:
    output_dir.mkdir(parents=True, exist_ok=True)
    trades_path = output_dir / f"{out_prefix}_trades.csv"
    events_path = output_dir / f"{out_prefix}_events.csv"
    llm_path = output_dir / f"{out_prefix}_llm.csv"
    summary_path = output_dir / f"{out_prefix}_summary.json"
    report_path = output_dir / f"{out_prefix}_report.txt"

    trades_df.to_csv(trades_path, index=False)
    events_df.to_csv(events_path, index=False)
    if llm_df is not None and not llm_df.empty:
        llm_df.to_csv(llm_path, index=False)
    summary_path.write_text(json.dumps(summary, ensure_ascii=True, indent=2), encoding="utf-8")

    lines = [
        f"Execution bot backtest: {summary.get('days', 0)} sessions",
        f"TF: {summary.get('timeframe_min', 0)}m | Engine: {summary.get('engine_mode', '')} | Signal source: {summary.get('signal_source', '')}",
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
        f"LLM intraday refreshes: {summary.get('llm_intraday_refreshes', 0)}",
        f"LLM daily refreshes: {summary.get('llm_daily_refreshes', 0)}",
        f"LLM assist changes: {summary.get('llm_assist_changes', 0)}",
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
        "llm": llm_path if llm_df is not None and not llm_df.empty else llm_path,
        "summary": summary_path,
        "report": report_path,
    }


def run_backtest(
    days: int,
    timeframe_min: int,
    config: ExecutionConfig,
    out_prefix: str,
    *,
    data_dir: Path | None = None,
    output_dir: Path | None = None,
    start_date: str | None = None,
    end_date: str | None = None,
    min_bars: int = 350,
    explicit_dates: set[str] | None = None,
    use_daily_llm: bool = False,
    use_intraday_llm: bool = False,
    llm_assist: bool = False,
    llm_base_min: int = 30,
    llm_event_cooldown_min: int = 15,
    llm_max_events_per_hour: int = 1,
    signal_source: str = "signal_dashboard",
) -> dict[str, Any]:
    os.environ.setdefault("SIGNAL_ENGINE_MODE", "final_mtf_v3")
    if use_daily_llm:
        os.environ["SIGNAL_DAILY_CONTEXT_USE_LLM"] = "1"
    if use_intraday_llm:
        os.environ["SIGNAL_INTRADAY_LLM_USE_API"] = "1"
    os.environ["SIGNAL_INTRADAY_LLM_BASE_MIN"] = str(llm_base_min)
    os.environ["SIGNAL_INTRADAY_LLM_EVENT_COOLDOWN_MIN"] = str(llm_event_cooldown_min)
    os.environ["SIGNAL_INTRADAY_LLM_MAX_EVENTS_PER_HOUR"] = str(llm_max_events_per_hour)

    output_root = output_dir or OUTPUT_DIR

    selected = list_rth_files(days, data_dir=data_dir, start_date=start_date, end_date=end_date, explicit_dates=explicit_dates)
    if not selected:
        raise FileNotFoundError(f"No RTH parquet files found in {data_dir or DATA_DIR}")

    engine = SignalEngine()
    new_signal_generator = NewSignalGenerator() if signal_source == "newsignal_core" else None
    snapshot_bot = MarketSnapshotBot()
    execution_bot = SignalExecutionBot(config)
    daily_context_service = (
        DailyHTFContextService(cache_path=output_root / f"{out_prefix}_daily_context_cache.json")
        if use_daily_llm
        else None
    )
    intraday_llm_service = (
        IntradayLLMContextService(cache_path=output_root / f"{out_prefix}_intraday_context_cache.json")
        if use_intraday_llm
        else None
    )
    all_bars = pd.DataFrame()
    used_files: list[str] = []
    skipped_files: list[tuple[str, int]] = []
    llm_rows: list[dict[str, Any]] = []
    llm_assist_changes = 0
    last_daily_generated_at = ""
    last_intraday_generated_at = ""
    last_assist_signature: tuple[Any, ...] | None = None
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
            bar_dt_utc = bar_dt.tz_localize(UTC) if bar_dt.tzinfo is None else bar_dt.tz_convert(UTC)
            bar_dt_et = bar_dt_utc.tz_convert(ET)
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

            if new_signal_generator is not None:
                signals = new_signal_generator.evaluate(
                    history,
                    current_price=price,
                    now=bar_dt.to_pydatetime(),
                )
            else:
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
            daily_context = _inject_daily_context(
                state,
                bars_df=history,
                price=price,
                now=bar_dt.to_pydatetime(),
                service=daily_context_service,
            )
            if daily_context and str(daily_context.get("generated_at") or "") != last_daily_generated_at:
                last_daily_generated_at = str(daily_context.get("generated_at") or "")
                llm_rows.append(
                    {
                        "type": "daily",
                        "session": path.name,
                        "bar_index": int(bar_index),
                        "timestamp": bar_dt.isoformat(),
                        "source": str(daily_context.get("source") or ""),
                        "trigger_type": "daily_bucket",
                        "bias": str(daily_context.get("daily_bias") or ""),
                        "confidence": 0,
                        "summary": str(daily_context.get("summary") or ""),
                    }
                )
            payload = {
                "state": state,
                "signals": signals,
                "ghost_signals": [],
                "zones": zones,
            }
            market_decision = snapshot_bot.analyze(payload).to_dict()
            intraday_context = _inject_intraday_llm_context(
                state,
                signals=signals,
                market_decision=market_decision,
                zones=zones,
                now=bar_dt.to_pydatetime(),
                service=intraday_llm_service,
            )
            if intraday_context and str(intraday_context.get("generated_at") or "") != last_intraday_generated_at:
                last_intraday_generated_at = str(intraday_context.get("generated_at") or "")
                llm_rows.append(
                    {
                        "type": "intraday",
                        "session": path.name,
                        "bar_index": int(bar_index),
                        "timestamp": bar_dt.isoformat(),
                        "source": str(intraday_context.get("source") or ""),
                        "trigger_type": str(intraday_context.get("trigger_type") or ""),
                        "bias": str(intraday_context.get("bias") or ""),
                        "confidence": int(float(intraday_context.get("confidence") or 0)),
                        "summary": str(intraday_context.get("summary") or ""),
                    }
                )
            if llm_assist:
                market_decision, assist_meta = _merge_market_decision_with_llm(market_decision, intraday_context)
                if assist_meta.get("assist_state") not in {"disabled", "no_change"}:
                    llm_assist_changes += 1
                    assist_signature = (
                        assist_meta.get("assist_state"),
                        assist_meta.get("base_bias"),
                        assist_meta.get("base_confidence"),
                        assist_meta.get("llm_bias"),
                        assist_meta.get("llm_confidence"),
                    )
                    if assist_signature != last_assist_signature:
                        execution_bot.event_log.append(
                            {
                                "kind": "llm_assist",
                                "bar_index": int(bar_index),
                                "timestamp": bar_dt.isoformat(),
                                "assist_state": assist_meta.get("assist_state"),
                                "base_bias": assist_meta.get("base_bias"),
                                "base_confidence": assist_meta.get("base_confidence"),
                                "llm_bias": assist_meta.get("llm_bias"),
                                "llm_confidence": assist_meta.get("llm_confidence"),
                            }
                        )
                        last_assist_signature = assist_signature
            execution_bot.on_bar_close(
                bar_index=bar_index,
                timestamp=bar_dt.isoformat(),
                timestamp_et=bar_dt_et.isoformat(),
                price=price,
                atr=atr,
                signals=signals,
                market_decision=market_decision,
                state=state,
                recent_bars=history.tail(16).copy(),
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
    llm_df = pd.DataFrame(llm_rows)

    summary = summarize_trades(trades_df)
    exit_counts = Counter()
    if not trades_df.empty:
        exit_counts.update(trades_df["exit_reason"].astype(str).tolist())
    llm_intraday = llm_df[llm_df["type"] == "intraday"] if not llm_df.empty else pd.DataFrame()
    llm_daily = llm_df[llm_df["type"] == "daily"] if not llm_df.empty else pd.DataFrame()
    summary.update(
        {
            "days": len(used_files),
            "timeframe_min": timeframe_min,
            "engine_mode": os.getenv("SIGNAL_ENGINE_MODE", "final_mtf_v3"),
            "signal_source": signal_source,
            "allowed_playbooks": list(config.allowed_playbooks),
            "data_dir": str(data_dir or DATA_DIR),
            "runtime_sec": round(time.time() - t0, 1),
            "files": used_files,
            "skipped_files": [name for name, _ in skipped_files],
            "exit_reason_counts": dict(exit_counts),
            "llm_assist_enabled": bool(llm_assist),
            "llm_daily_enabled": bool(use_daily_llm),
            "llm_intraday_enabled": bool(use_intraday_llm),
            "llm_base_min": int(llm_base_min),
            "llm_event_cooldown_min": int(llm_event_cooldown_min),
            "llm_max_events_per_hour": int(llm_max_events_per_hour),
            "llm_daily_refreshes": int(len(llm_daily)),
            "llm_intraday_refreshes": int(len(llm_intraday)),
            "llm_intraday_api_refreshes": int((llm_intraday["source"] == "llm").sum()) if not llm_intraday.empty else 0,
            "llm_intraday_fallback_refreshes": int((llm_intraday["source"] != "llm").sum()) if not llm_intraday.empty else 0,
            "llm_assist_changes": int(llm_assist_changes),
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
        llm_df=llm_df,
        summary=summary,
        per_signal=per_signal,
        out_prefix=out_prefix,
        output_dir=output_root,
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
    parser.add_argument("--data-dir", default=str(DATA_DIR), help="Directory with MNQ trade_tick parquet files.")
    parser.add_argument("--output-dir", default=str(OUTPUT_DIR), help="Directory for backtest outputs and caches.")
    parser.add_argument("--start-date", default=None, help="Inclusive date filter YYYY-MM-DD.")
    parser.add_argument("--end-date", default=None, help="Inclusive date filter YYYY-MM-DD.")
    parser.add_argument("--dates-file", default=None, help="Optional file with explicit YYYY-MM-DD dates, one per line.")
    parser.add_argument("--min-bars", type=int, default=350, help="Minimum non-empty bars per session.")
    parser.add_argument("--out-prefix", default="execution_bot_backtest_15d", help="Output filename prefix.")
    parser.add_argument("--playbooks", default="", help="Optional comma-separated playbook ids, e.g. PB01,PB02.")
    parser.add_argument("--signal-source", choices=["signal_dashboard", "newsignal_core"], default="signal_dashboard")
    parser.add_argument("--use-daily-llm", action="store_true", help="Enable daily HTF LLM layer during backtest.")
    parser.add_argument("--use-intraday-llm", action="store_true", help="Enable intraday LLM layer during backtest.")
    parser.add_argument("--llm-assist", action="store_true", help="Allow intraday LLM to gate/override market_decision conservatively.")
    parser.add_argument("--llm-base-min", type=int, default=30, help="Base refresh cadence for intraday LLM.")
    parser.add_argument("--llm-event-cooldown-min", type=int, default=15, help="Cooldown between non-hard LLM event refreshes.")
    parser.add_argument("--llm-max-events-per-hour", type=int, default=1, help="Hard cap on event-triggered LLM refreshes per hour.")
    args = parser.parse_args(argv)
    explicit_dates = load_explicit_dates(args.dates_file)
    allowed_playbooks = parse_playbooks(args.playbooks)
    config = ExecutionConfig(allowed_playbooks=allowed_playbooks)

    result = run_backtest(
        days=args.days,
        timeframe_min=args.tf,
        config=config,
        out_prefix=args.out_prefix,
        data_dir=Path(args.data_dir),
        output_dir=Path(args.output_dir),
        start_date=args.start_date,
        end_date=args.end_date,
        min_bars=args.min_bars,
        explicit_dates=explicit_dates,
        use_daily_llm=args.use_daily_llm,
        use_intraday_llm=args.use_intraday_llm,
        llm_assist=args.llm_assist,
        llm_base_min=args.llm_base_min,
        llm_event_cooldown_min=args.llm_event_cooldown_min,
        llm_max_events_per_hour=args.llm_max_events_per_hour,
        signal_source=args.signal_source,
    )
    print(json.dumps(result["summary"], ensure_ascii=True, indent=2))
    print(json.dumps(result["paths"], ensure_ascii=True, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
