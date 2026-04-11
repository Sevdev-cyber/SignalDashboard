"""JSONL event logger."""

from __future__ import annotations

import json
from pathlib import Path


class EventLog:
    """Append-only JSONL logger."""

    def __init__(self, path: Path) -> None:
        self.path = path

    def append(self, payload: dict) -> None:
        self.path.parent.mkdir(parents=True, exist_ok=True)
        with self.path.open("a", encoding="utf-8") as f:
            f.write(json.dumps(payload, default=str) + "\n")
