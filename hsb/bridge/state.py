"""Submission state — cursor and deduplication state management."""

from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path


def load_cursor(path: Path) -> datetime | None:
    """Load the last submitted timestamp from disk."""
    if not path.exists():
        return None
    raw = path.read_text(encoding="utf-8").strip()
    if not raw:
        return None
    return datetime.fromisoformat(raw).replace(tzinfo=timezone.utc)


def save_cursor(path: Path, ts: datetime) -> None:
    """Persist the submission cursor."""
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(ts.isoformat(), encoding="utf-8")


def load_submitted_ids(path: Path) -> set[str]:
    """Load submitted candidate IDs from JSON."""
    if not path.exists():
        return set()
    return set(json.loads(path.read_text(encoding="utf-8")))


def save_submitted_ids(path: Path, ids: set[str]) -> None:
    """Persist submitted candidate IDs."""
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(sorted(ids), indent=2), encoding="utf-8")


def bootstrap_cursor(path: Path, ts: datetime) -> None:
    """Create cursor only if it does not exist."""
    if path.exists():
        return
    save_cursor(path, ts)
