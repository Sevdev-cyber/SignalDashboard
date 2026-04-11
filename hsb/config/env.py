"""Environment file loader."""

from __future__ import annotations

import os
from pathlib import Path


def load_env_file(path: Path) -> dict[str, str]:
    """Load a .env file and return key=value pairs (does not set os.environ)."""
    values: dict[str, str] = {}
    if not path.exists():
        return values
    for line in path.read_text(encoding="utf-8").splitlines():
        line = line.strip()
        if not line or line.startswith("#"):
            continue
        if "=" not in line:
            continue
        key, _, value = line.partition("=")
        values[key.strip()] = value.strip()
    return values


def apply_env_file(path: Path) -> None:
    """Load .env and set values as os.environ defaults."""
    for key, value in load_env_file(path).items():
        os.environ.setdefault(key, value)
