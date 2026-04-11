"""Protocol definition for candidate filters."""

from __future__ import annotations

from typing import Protocol

from hsb.domain.context import AnalysisContext
from hsb.domain.models import SignalCandidate


class CandidateFilter(Protocol):
    """Filters a list of candidates, returning only those that pass."""

    def filter(
        self,
        candidates: list[SignalCandidate],
        context: AnalysisContext,
    ) -> list[SignalCandidate]:
        ...
