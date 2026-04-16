"""CLI runner for market snapshot decisions.

Usage examples:
  python market_snapshot_runner.py --snapshot snapshot.json --pretty
  cat snapshot.json | python market_snapshot_runner.py --pretty
  python market_snapshot_runner.py --watch latest_snapshot.json --llm
"""

from __future__ import annotations

import argparse
import json
import os
import sys
import time
from pathlib import Path

from env_bootstrap import load_project_env
from market_snapshot_bot import MarketSnapshotBot
from market_snapshot_llm import MarketSnapshotLLMClient

load_project_env()


def _load_snapshot(path: str | None) -> dict:
    if path:
        return json.loads(Path(path).read_text(encoding="utf-8"))
    raw = sys.stdin.read()
    if not raw.strip():
        raise SystemExit("No JSON snapshot provided on stdin or via --snapshot.")
    return json.loads(raw)


def _print_decision(bot: MarketSnapshotBot, payload: dict, *, pretty: bool, with_prompt: bool, llm: bool) -> None:
    decision = bot.analyze(payload)
    if llm:
        client = MarketSnapshotLLMClient()
        llm_resp = client.call(
            "Return only JSON. Follow the structured snapshot and do not use screenshots.",
            decision.prompt,
        )
        out = {
            "decision": decision.to_dict(),
            "llm": {
                "success": llm_resp.success,
                "error": llm_resp.error,
                "model": llm_resp.model,
                "elapsed_sec": llm_resp.elapsed_sec,
                "parsed_json": llm_resp.parsed_json,
                "raw_text": llm_resp.raw_text,
            },
        }
    elif with_prompt:
        out = {"decision": decision.to_dict(), "prompt": decision.prompt}
    else:
        out = decision.to_dict()

    if pretty:
        print(json.dumps(out, ensure_ascii=True, indent=2))
    else:
        print(json.dumps(out, ensure_ascii=True))


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Run the MNQ market snapshot bot.")
    parser.add_argument("--snapshot", help="Path to a JSON snapshot file.")
    parser.add_argument("--pretty", action="store_true", help="Pretty-print JSON output.")
    parser.add_argument("--prompt", action="store_true", help="Include the LLM prompt in output.")
    parser.add_argument("--llm", action="store_true", help="Call the optional LLM wrapper after the deterministic decision.")
    parser.add_argument("--watch", help="Watch a JSON file and re-run when it changes.")
    parser.add_argument("--interval", type=float, default=1.0, help="Polling interval for --watch.")
    args = parser.parse_args(argv)

    bot = MarketSnapshotBot()

    if args.watch:
        path = Path(args.watch)
        last_sig = None
        while True:
            if not path.exists():
                print(json.dumps({"error": "watch file missing", "path": str(path)}, ensure_ascii=True))
                time.sleep(args.interval)
                continue
            stat = path.stat()
            sig = (stat.st_mtime_ns, stat.st_size)
            if sig != last_sig:
                last_sig = sig
                payload = json.loads(path.read_text(encoding="utf-8"))
                _print_decision(bot, payload, pretty=True, with_prompt=args.prompt, llm=args.llm)
            time.sleep(args.interval)

    payload = _load_snapshot(args.snapshot)
    _print_decision(bot, payload, pretty=args.pretty, with_prompt=args.prompt, llm=args.llm)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
