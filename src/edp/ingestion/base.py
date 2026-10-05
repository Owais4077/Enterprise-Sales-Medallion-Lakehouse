"""The contract every source extractor implements."""

from __future__ import annotations

from abc import ABC, abstractmethod
from dataclasses import dataclass

from edp.ingestion.landing import LandingBatch
from edp.ingestion.models import ExtractionStats
from edp.ingestion.state import StateStore


@dataclass
class ExtractionContext:
    """What the runner hands an extractor for one run."""

    since: str | None  # stored watermark, or None for the first run / a full refresh
    full_refresh: bool
    sink: LandingBatch
    state: StateStore


class Extractor(ABC):
    """Reads from one dataset of one source system and writes it into ``ctx.sink``.

    Extractors only *read and write to the batch*. Watermarks, audit rows and atomic
    publishing are the runner's job, so every source gets identical guarantees.
    """

    source_system: str
    dataset: str

    @abstractmethod
    def extract(self, ctx: ExtractionContext) -> ExtractionStats:
        """Read new data, write it to ``ctx.sink`` and report what happened."""

    def close(self) -> None:  # noqa: B027  (optional hook, intentionally not abstract)
        """Release connections/clients. Called by the runner in a ``finally`` block."""
