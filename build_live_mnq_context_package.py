from __future__ import annotations

import argparse
import asyncio
import json
import os
from datetime import datetime
from pathlib import Path
from typing import Any

import pandas as pd
import websockets

from daily_htf_context import DailyHTFContextService
from env_bootstrap import load_project_env
from intraday_llm_context import IntradayLLMContextService
from signal_engine import SignalEngine

load_project_env()


ROOT = Path(__file__).resolve().parent
WORKSPACE = ROOT.parent
NEW_SIGNAL_DIR = WORKSPACE / "NewSignal"
RUNTIME_DIR = ROOT / "runtime"
DEFAULT_WS_URL = os.environ.get("SIGNAL_LIVE_WS_URL", "wss://web-production-3ff3f.up.railway.app/ws")
RELAY_BARS_LIMIT = max(200, int(os.environ.get("SIGNAL_RELAY_BARS_LIMIT", "200") or "200"))


async def fetch_live_payload(ws_url: str, *, max_messages: int = 6) -> dict[str, Any]:
    async with websockets.connect(ws_url, max_size=10_000_000) as ws:
        for _ in range(max_messages):
            raw = await ws.recv()
            msg = json.loads(raw)
            if isinstance(msg, dict) and msg.get("state") and msg.get("bars"):
                return msg
    raise RuntimeError(f"No usable state+bars payload received from {ws_url}")


def _rolling_rsi(close: pd.Series, period: int = 14) -> pd.Series:
    delta = close.diff()
    gain = delta.clip(lower=0).rolling(period).mean()
    loss = (-delta.clip(upper=0)).rolling(period).mean().replace(0, pd.NA)
    rs = gain / loss
    return (100 - (100 / (1 + rs))).fillna(50)


def build_snapshot_frame(bars: list[dict[str, Any]]) -> pd.DataFrame:
    if not bars:
        raise ValueError("No bars in live payload")

    df = pd.DataFrame(bars).copy()
    for col in ("open", "high", "low", "close", "cum_delta"):
        if col not in df.columns:
            df[col] = 0.0
        df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0.0)

    if "time" not in df.columns:
        raise ValueError("Live payload bars are missing 'time'")

    # Chart payload stores open-time seconds. The engine expects close timestamps.
    close_dt = pd.to_datetime(df["time"], unit="s", errors="coerce") + pd.Timedelta(minutes=1)
    df["datetime"] = close_dt
    df["timestamp"] = close_dt
    df = df.dropna(subset=["timestamp"]).sort_values("timestamp").drop_duplicates("timestamp").reset_index(drop=True)

    if "cum_delta" in df.columns:
        df["delta"] = df["cum_delta"].diff().fillna(0.0)
    else:
        df["delta"] = 0.0

    # Relay bars do not carry true volume. Use a stable proxy so HTF structure can be computed.
    df["volume"] = 1.0

    prev_close = df["close"].shift(1)
    tr = pd.concat(
        [
            df["high"] - df["low"],
            (df["high"] - prev_close).abs(),
            (df["low"] - prev_close).abs(),
        ],
        axis=1,
    ).max(axis=1)
    df["atr"] = tr.rolling(14).mean().fillna(tr.expanding().mean()).fillna(0.0)
    df["ema_20"] = df["close"].ewm(span=20, adjust=False).mean()
    df["ema_50"] = df["close"].ewm(span=50, adjust=False).mean()
    df["ema_100"] = df["close"].ewm(span=100, adjust=False).mean()
    df["rsi"] = _rolling_rsi(df["close"])
    typical = (df["high"] + df["low"] + df["close"]) / 3.0
    # This is a proxy VWAP/TWAP because relay chart bars do not include real volume.
    df["vwap"] = typical.expanding().mean()
    df["has_real_tick_delta"] = True
    return df


def build_enriched_state(raw_payload: dict[str, Any], bars_df: pd.DataFrame) -> tuple[dict[str, Any], dict[str, Any]]:
    raw_state = dict(raw_payload.get("state") or {})
    current_price = float(raw_state.get("price") or bars_df.iloc[-1]["close"])
    delta_pct = float(raw_state.get("delta_pct") or 0.0)

    engine = SignalEngine()
    derived_state = engine.get_market_state(
        bars_df,
        current_price=current_price,
        bar_delta_pct=delta_pct,
    )
    final_state = dict(raw_state)
    final_state["trader_guide"] = dict(derived_state.get("trader_guide") or {})
    final_state["htf_build_meta"] = {
        "source": "relay_200bars_local_rebuild",
        "bars_loaded": int(len(bars_df)),
        "volume_mode": "proxy_twap_volume",
        "note": (
            "HTF rebuilt locally from 200 relay bars. 1H is usable, but 4H/1D/1W/1M remain partial "
            "until the feed sends more history."
        ),
    }
    return final_state, derived_state


def build_daily_context(
    *,
    bars_df: pd.DataFrame,
    trader_guide: dict[str, Any],
    current_price: float,
    now: datetime,
    use_llm: bool,
) -> dict[str, Any]:
    temp_cache = RUNTIME_DIR / "live_mnq_daily_htf_context.json"
    service = DailyHTFContextService(cache_path=temp_cache)
    service.use_llm = bool(use_llm)
    context = service.maybe_refresh(
        bars_df=bars_df,
        trader_guide=trader_guide,
        current_price=current_price,
        now=now,
    ) or {}
    (RUNTIME_DIR / "daily_htf_context.json").write_text(
        json.dumps(context, ensure_ascii=True, indent=2),
        encoding="utf-8",
    )
    return context


def build_intraday_context(
    *,
    state: dict[str, Any],
    raw_payload: dict[str, Any],
    now: datetime,
    use_llm: bool,
) -> dict[str, Any]:
    temp_cache = RUNTIME_DIR / "live_mnq_intraday_llm_context.json"
    service = IntradayLLMContextService(cache_path=temp_cache)
    service.use_llm = bool(use_llm)
    state_market_decision = dict(state.get("market_decision") or {})
    state_execution = dict(state.get("execution_view") or {})
    state_decision_ledger = list(state.get("decision_ledger") or [])
    payload = {
        "state": state,
        "signals": list(raw_payload.get("signals") or []),
        "ghost_signals": list(raw_payload.get("ghost_signals") or []),
        "market_decision": dict(raw_payload.get("market_decision") or state_market_decision),
        "execution": dict(raw_payload.get("execution") or state_execution),
        "decision_ledger": list(raw_payload.get("decision_ledger") or state_decision_ledger),
        "zones": dict(raw_payload.get("zones") or {}),
    }
    context = service.maybe_refresh(payload, now=now) or {}
    (RUNTIME_DIR / "intraday_llm_context.json").write_text(
        json.dumps(context, ensure_ascii=True, indent=2),
        encoding="utf-8",
    )
    return context


def build_package(
    raw_payload: dict[str, Any],
    *,
    ws_url: str,
    now: datetime,
    use_daily_llm: bool = False,
    use_intraday_llm: bool = False,
) -> dict[str, Any]:
    bars_df = build_snapshot_frame(list(raw_payload.get("bars") or []))
    final_state, derived_state = build_enriched_state(raw_payload, bars_df)

    trader_guide = dict(final_state.get("trader_guide") or {})
    current_price = float(final_state.get("price") or bars_df.iloc[-1]["close"])

    daily_context = build_daily_context(
        bars_df=bars_df,
        trader_guide=trader_guide,
        current_price=current_price,
        now=now,
        use_llm=use_daily_llm,
    )
    trader_guide["daily_context"] = daily_context
    final_state["daily_context"] = daily_context
    final_state["trader_guide"] = trader_guide

    intraday_context = build_intraday_context(
        state=final_state,
        raw_payload=raw_payload,
        now=now,
        use_llm=use_intraday_llm,
    )
    if intraday_context:
        trader_guide["llm_context"] = intraday_context
        final_state["llm_context"] = intraday_context
    final_state["trader_guide"] = trader_guide

    htf_audit = dict(trader_guide.get("htf_audit") or {})
    package = {
        "meta": {
            "generated_at": now.isoformat(),
            "source_ws": ws_url,
            "symbol": "MNQ",
            "purpose": "live_mnq_htf_test_package",
            "limitations": [
                f"Only {RELAY_BARS_LIMIT} relay bars were available from the public dashboard websocket.",
                "4H/1D/1W/1M are partial until more history is streamed.",
                "Relay bars do not include true volume, so local HTF rebuild uses a stable proxy volume and TWAP-style value line.",
            ],
        },
        "summary": {
            "price": round(current_price, 2),
            "overall_bias": trader_guide.get("overall_bias"),
            "overall_confidence": trader_guide.get("confidence"),
            "guide_summary": trader_guide.get("summary"),
            "htf_pre_signal_bias": htf_audit.get("pre_signal_bias"),
            "htf_pre_signal_confidence": htf_audit.get("pre_signal_confidence"),
            "htf_summary": htf_audit.get("summary"),
            "history_note": htf_audit.get("history_note"),
            "daily_summary": daily_context.get("summary"),
            "intraday_summary": intraday_context.get("summary"),
            "daily_source": daily_context.get("source"),
            "intraday_source": intraday_context.get("source"),
            "signals_live_count": len(raw_payload.get("signals") or []),
        },
        "raw_payload": raw_payload,
        "rebuilt_state": final_state,
        "derived_state": derived_state,
        "daily_context": daily_context,
        "intraday_context": intraday_context,
        "bars_frame_tail": json.loads(
            bars_df.tail(50).to_json(orient="records", date_format="iso")
        ),
    }
    return package


def write_outputs(package: dict[str, Any]) -> tuple[Path, Path]:
    NEW_SIGNAL_DIR.mkdir(parents=True, exist_ok=True)
    RUNTIME_DIR.mkdir(parents=True, exist_ok=True)

    stamp = datetime.fromisoformat(package["meta"]["generated_at"]).strftime("%Y-%m-%d_%H-%M-%S")
    json_path = NEW_SIGNAL_DIR / f"live_mnq_htf_package_{stamp}.json"
    txt_path = NEW_SIGNAL_DIR / f"live_mnq_htf_package_{stamp}.txt"

    json_path.write_text(json.dumps(package, ensure_ascii=True, indent=2), encoding="utf-8")

    summary = package["summary"]
    rebuilt = package["rebuilt_state"]
    guide = dict(rebuilt.get("trader_guide") or {})
    tf_1h = dict(guide.get("tf_1h") or {})
    tf_4h = dict(guide.get("tf_4h") or {})
    tf_1d = dict(guide.get("tf_1d") or {})
    lines = [
        f"Generated: {package['meta']['generated_at']}",
        f"Source WS: {package['meta']['source_ws']}",
        "",
        "CURRENT MNQ TEST PACKAGE",
        f"Price: {summary.get('price')}",
        f"Overall bias: {summary.get('overall_bias')} ({summary.get('overall_confidence')}%)",
        f"HTF bias: {summary.get('htf_pre_signal_bias')} ({summary.get('htf_pre_signal_confidence')}%)",
        f"Guide summary: {summary.get('guide_summary')}",
        f"HTF summary: {summary.get('htf_summary')}",
        f"Daily summary: {summary.get('daily_summary')}",
        f"Intraday context: {summary.get('intraday_summary')}",
        f"Live signals in payload: {summary.get('signals_live_count')}",
        "",
        "TIMEFRAMES",
        f"1H: bias={tf_1h.get('bias')} strength={tf_1h.get('strength')} trigger={tf_1h.get('trigger_level')} invalidation={tf_1h.get('invalidation_level')} history_ok={tf_1h.get('history_ok')}",
        f"4H: bias={tf_4h.get('bias')} strength={tf_4h.get('strength')} trigger={tf_4h.get('trigger_level')} invalidation={tf_4h.get('invalidation_level')} history_ok={tf_4h.get('history_ok')}",
        f"1D: bias={tf_1d.get('bias')} strength={tf_1d.get('strength')} trigger={tf_1d.get('trigger_level')} invalidation={tf_1d.get('invalidation_level')} history_ok={tf_1d.get('history_ok')}",
        "",
        "LIMITATIONS",
        *[f"- {item}" for item in package["meta"]["limitations"]],
        "",
        f"History note: {summary.get('history_note')}",
    ]
    txt_path.write_text("\n".join(lines) + "\n", encoding="utf-8")
    return json_path, txt_path


def write_latest_runtime(package: dict[str, Any]) -> Path:
    RUNTIME_DIR.mkdir(parents=True, exist_ok=True)
    latest_path = RUNTIME_DIR / "live_mnq_context_latest.json"
    latest_path.write_text(json.dumps(package, ensure_ascii=True, indent=2), encoding="utf-8")
    return latest_path


def main() -> None:
    parser = argparse.ArgumentParser(description="Fetch live MNQ snapshot and rebuild HTF context locally.")
    parser.add_argument("--ws-url", default=DEFAULT_WS_URL, help="WebSocket source for the live dashboard payload")
    parser.add_argument("--daily-llm", action="store_true", help="Allow daily HTF LLM call when API key is configured")
    parser.add_argument("--intraday-llm", action="store_true", help="Allow intraday LLM call when API key is configured")
    parser.add_argument("--latest-only", action="store_true", help="Write runtime latest snapshot only, without timestamped NewSignal artifacts")
    args = parser.parse_args()

    now = datetime.now()
    raw_payload = asyncio.run(fetch_live_payload(args.ws_url))
    package = build_package(
        raw_payload,
        ws_url=args.ws_url,
        now=now,
        use_daily_llm=args.daily_llm,
        use_intraday_llm=args.intraday_llm,
    )
    latest_path = write_latest_runtime(package)
    json_path = txt_path = None
    if not args.latest_only:
        json_path, txt_path = write_outputs(package)

    summary = package["summary"]
    if json_path and txt_path:
        print(f"Saved JSON: {json_path}")
        print(f"Saved TXT:  {txt_path}")
    print(f"Saved latest runtime package: {latest_path}")
    print(
        json.dumps(
            {
                "price": summary.get("price"),
                "overall_bias": summary.get("overall_bias"),
                "overall_confidence": summary.get("overall_confidence"),
                "htf_bias": summary.get("htf_pre_signal_bias"),
                "htf_confidence": summary.get("htf_pre_signal_confidence"),
                "htf_summary": summary.get("htf_summary"),
                "daily_summary": summary.get("daily_summary"),
                "daily_source": summary.get("daily_source"),
                "intraday_summary": summary.get("intraday_summary"),
                "intraday_source": summary.get("intraday_source"),
            },
            ensure_ascii=True,
            indent=2,
        )
    )


if __name__ == "__main__":
    main()
