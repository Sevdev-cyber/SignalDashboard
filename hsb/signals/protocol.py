"""Protocol definition for candidate generators.

Any generator that implements this protocol can be plugged into the pipeline
without any code changes.
"""

from __future__ import annotations

from typing import Protocol

from hsb.domain.context import AnalysisContext
from hsb.domain.models import SignalCandidate


class CandidateGenerator(Protocol):
    """Generates trade candidates from market context."""

    def generate(self, context: AnalysisContext) -> list[SignalCandidate]:
        """Produce zero or more candidates for the current bar."""
        ...


class NullGenerator:
    """No-op generator — useful as a default or for testing."""

    def generate(self, context: AnalysisContext) -> list[SignalCandidate]:
        return []
