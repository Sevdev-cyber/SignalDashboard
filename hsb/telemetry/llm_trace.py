"""LLM trace logger — dedicated audit trail for all LLM calls."""

from __future__ import annotations

import json
from pathlib import Path


class LLMTrace:
    """Append-only JSONL logger for LLM API calls."""

    def __init__(self, path: Path) -> None:
        self.path = path

    def log(self, payload: dict) -> None:
        self.path.parent.mkdir(parents=True, exist_ok=True)
        with self.path.open("a", encoding="utf-8") as f:
            f.write(json.dumps(payload, default=str) + "\n")
