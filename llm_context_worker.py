from __future__ import annotations

import argparse
import json
import time
from datetime import datetime
from pathlib import Path

from env_bootstrap import load_project_env
from build_live_mnq_context_package import (
    DEFAULT_WS_URL,
    build_package,
    fetch_live_payload,
    write_latest_runtime,
    write_outputs,
)

load_project_env()


def _summary_blob(package: dict) -> dict:
    summary = dict(package.get("summary") or {})
    return {
        "generated_at": package.get("meta", {}).get("generated_at"),
        "price": summary.get("price"),
        "overall_bias": summary.get("overall_bias"),
        "overall_confidence": summary.get("overall_confidence"),
        "htf_bias": summary.get("htf_pre_signal_bias"),
        "htf_confidence": summary.get("htf_pre_signal_confidence"),
        "daily_source": summary.get("daily_source"),
        "intraday_source": summary.get("intraday_source"),
        "daily_summary": summary.get("daily_summary"),
        "intraday_summary": summary.get("intraday_summary"),
    }


def run_loop(
    *,
    ws_url: str,
    interval_sec: float,
    daily_llm: bool,
    intraday_llm: bool,
    archive: bool,
) -> None:
    last_archive_bucket = None
    while True:
        now = datetime.now()
        raw_payload = None
        try:
            import asyncio

            raw_payload = asyncio.run(fetch_live_payload(ws_url))
            package = build_package(
                raw_payload,
                ws_url=ws_url,
                now=now,
                use_daily_llm=daily_llm,
                use_intraday_llm=intraday_llm,
            )
            latest_path = write_latest_runtime(package)

            archive_bucket = package.get("intraday_context", {}).get("generated_at") or package.get("daily_context", {}).get("generated_at")
            if archive and archive_bucket and archive_bucket != last_archive_bucket:
                write_outputs(package)
                last_archive_bucket = archive_bucket

            print(json.dumps({"status": "ok", "latest_path": str(latest_path), **_summary_blob(package)}, ensure_ascii=True), flush=True)
        except KeyboardInterrupt:
            raise
        except Exception as e:
            debug_path = Path(__file__).resolve().parent / "runtime" / "llm_context_worker_error.json"
            debug_payload = {
                "generated_at": now.isoformat(),
                "error": str(e),
                "ws_url": ws_url,
                "had_payload": bool(raw_payload),
            }
            debug_path.write_text(json.dumps(debug_payload, ensure_ascii=True, indent=2), encoding="utf-8")
            print(json.dumps({"status": "error", **debug_payload}, ensure_ascii=True), flush=True)

        # HSB v11.5: Event-Driven Sleep (react to Macro Boss flags)
        waited = 0.0
        flag_path = Path(__file__).resolve().parent / "runtime" / "macro_event_trigger.json"
        
        while waited < max(2.0, interval_sec):
            if flag_path.exists():
                try:
                    flag_data = json.loads(flag_path.read_text(encoding="utf-8"))
                    print(f"\n[EVENT] 🚨 MACRO BOSS AWAKENED BY {flag_data.get('event_type')} 🚨", flush=True)
                    flag_path.unlink(missing_ok=True)
                    break # Interrupt sleep and run LLM immediately!
                except Exception:
                    pass
            time.sleep(1.0)
            waited += 1.0


def main() -> int:
    parser = argparse.ArgumentParser(description="Headless live MNQ LLM/HTF context worker.")
    parser.add_argument("--ws-url", default=DEFAULT_WS_URL, help="WebSocket source for the live dashboard payload")
    parser.add_argument("--interval", type=float, default=10.0, help="Polling interval in seconds")
    parser.add_argument("--daily-llm", action="store_true", help="Enable daily HTF LLM enrichment")
    parser.add_argument("--intraday-llm", action="store_true", help="Enable intraday LLM enrichment")
    parser.add_argument("--archive", action="store_true", help="Also save timestamped packages to NewSignal when a new context is generated")
    args = parser.parse_args()

    run_loop(
        ws_url=args.ws_url,
        interval_sec=args.interval,
        daily_llm=args.daily_llm,
        intraday_llm=args.intraday_llm,
        archive=args.archive,
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
