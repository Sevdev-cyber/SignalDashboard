"""Parquet bar loader — configurable paths (no hardcodes!).

The bar cache path is resolved from:
1. Constructor ``bar_cache`` argument, or
2. ``HSB_BAR_CACHE`` environment variable, or
3. A sensible default relative to the project root.
"""

from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path

import pandas as pd

# Default for Mac development — overridden by env var on Windows VPS
_DEFAULT_BAR_CACHE = Path(
    os.environ.get(
        "HSB_BAR_CACHE",
        os.path.expanduser("~/Trading Setup/backtest/v2/bar_cache"),
    )
)


@dataclass(slots=True)
class ReplayDataset:
    day: str
    session: str
    path: Path
    bars: pd.DataFrame


class ParquetBarLoader:
    """Loads bar data from parquet files."""

    def __init__(self, bar_cache: Path | None = None) -> None:
        self.bar_cache = Path(bar_cache or _DEFAULT_BAR_CACHE)

    def list_available_days(self, session: str = "bars") -> list[str]:
        suffix = self._session_suffix(session)
        days: list[str] = []
        if not self.bar_cache.exists():
            return days
        for path in sorted(self.bar_cache.iterdir()):
            if not path.name.endswith(suffix):
                continue
            parsed = self._parse_day(path.name)
            if parsed:
                days.append(parsed)
        return sorted(set(days))

    def load_day(self, day: str, session: str = "bars") -> ReplayDataset:
        path = self._resolve_path(day, session)
        bars = pd.read_parquet(path)
        return ReplayDataset(day=day, session=session, path=path, bars=bars)

    def _resolve_path(self, day: str, session: str) -> Path:
        normalized = self._normalize_day(day)
        month = int(normalized[4:6])
        day_num = int(normalized[6:8])
        suffix = self._session_suffix(session)
        path = self.bar_cache / f"mnq_2026_{month:02d}_{day_num}_{suffix}"
        if not path.exists():
            raise FileNotFoundError(f"Replay parquet not found: {path}")
        return path

    def _normalize_day(self, day: str) -> str:
        raw = day.strip()
        if len(raw) == 8 and raw.isdigit():
            return raw
        month_map = {
            "Jan": "01", "Feb": "02", "Mar": "03", "Apr": "04",
            "May": "05", "Jun": "06", "Jul": "07", "Aug": "08",
            "Sep": "09", "Oct": "10", "Nov": "11", "Dec": "12",
        }
        prefix = raw[:3].title()
        if prefix not in month_map:
            raise ValueError(f"Unsupported day format: {day}")
        return f"2026{month_map[prefix]}{int(raw[3:]):02d}"

    def _session_suffix(self, session: str) -> str:
        return {
            "bars": "bars.parquet",
            "full": "full_bars.parquet",
            "rth": "rth_bars.parquet",
            "15s": "ticks.parquet",
        }.get(session, "bars.parquet")

    def _parse_day(self, name: str) -> str | None:
        parts = name.split("_")
        if len(parts) < 4:
            return None
        try:
            month = int(parts[2])
            day = int(parts[3])
            return f"2026{month:02d}{day:02d}"
        except (ValueError, IndexError):
            return None
