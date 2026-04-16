"""Project-local .env bootstrap for SignalDashboard scripts.

Loads `.env` and `.env.local` from the project root as environment defaults.
Existing environment variables always win.
"""

from __future__ import annotations

from pathlib import Path

from hsb.config.env import apply_env_file

_LOADED = False


def load_project_env() -> None:
    global _LOADED
    if _LOADED:
        return

    root = Path(__file__).resolve().parent
    for candidate in (root / ".env", root / ".env.local"):
        if candidate.exists():
            apply_env_file(candidate)

    _LOADED = True
