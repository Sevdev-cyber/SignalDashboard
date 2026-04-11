"""Intraday deduplication filter.

Prevents the same setup from firing multiple times within a short window.
"""

from __future__ import annotations

from collections import defaultdict

from hsb.domain.context import AnalysisContext
from hsb.domain.models import SignalCandidate


class DedupeFilter:
    """Suppresses duplicate signals within price-band and time windows."""

    def __init__(
        self,
        *,
        family_cooldown_bars: int = 6,
        direction_cooldown_bars: int = 2,
        price_band_min: float = 4.0,
        price_band_cap: float = 12.0,
    ) -> None:
        self.family_cooldown_bars = family_cooldown_bars
        self.direction_cooldown_bars = direction_cooldown_bars
        self.price_band_min = price_band_min
        self.price_band_cap = price_band_cap
        # Internal state tracked across calls within a session
        self._family_last_bar: dict[str, int] = {}
        self._direction_last_bar: dict[str, int] = {}
        self._price_bands: dict[str, list[float]] = defaultdict(list)

    def filter(
        self,
        candidates: list[SignalCandidate],
        context: AnalysisContext,
    ) -> list[SignalCandidate]:
        passed: list[SignalCandidate] = []
        for c in candidates:
            bar_idx = int(c.features.get("bar_index", 0) or 0)
            risk = abs(c.entry_price - c.sl_price)
            atr = context.atr if context.atr > 0 else 20.0

            # Family cooldown
            family_key = f"{c.family.value}_{c.direction.value}"
            last_bar = self._family_last_bar.get(family_key)
            if last_bar is not None and bar_idx - last_bar < self.family_cooldown_bars:
                continue

            # Direction cooldown
            dir_key = c.direction.value
            last_dir_bar = self._direction_last_bar.get(dir_key)
            if last_dir_bar is not None and bar_idx - last_dir_bar < self.direction_cooldown_bars:
                continue

            # Price band check
            band = min(max(risk * 1.25, atr * 0.2, self.price_band_min), self.price_band_cap)
            band_key = f"{c.direction.value}_{c.family.value}"
            recent_prices = self._price_bands.get(band_key, [])
            too_close = any(abs(c.entry_price - p) < band for p in recent_prices)
            if too_close:
                continue

            # Passed — update state
            self._family_last_bar[family_key] = bar_idx
            self._direction_last_bar[dir_key] = bar_idx
            self._price_bands[band_key].append(c.entry_price)
            passed.append(c)

        return passed

    def reset(self) -> None:
        """Clear all dedup state (call at session start)."""
        self._family_last_bar.clear()
        self._direction_last_bar.clear()
        self._price_bands.clear()
