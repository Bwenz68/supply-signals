# alert_engine/sinks/base.py
from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Dict, Optional, Protocol

Alert = Dict[str, Any]


@dataclass
class SinkMetrics:
    """Per-sink counters for one process run."""
    attempted: int = 0
    sent: int = 0
    skipped: int = 0
    errors: int = 0


class AlertSink(Protocol):
    """Protocol for all sinks. Concrete sinks should update self.metrics."""
    name: str
    dry_run: bool
    metrics: SinkMetrics

    def emit(self, alert: Alert) -> bool: ...
    def flush(self) -> None: ...


class BaseSink:
    """Helper base with common bookkeeping; not strictly required."""
    name: str = "base"

    def __init__(self, *, dry_run: bool = True) -> None:
        self.dry_run = dry_run
        self.metrics = SinkMetrics()

    def emit(self, alert: Alert) -> bool:  # pragma: no cover (interface)
        raise NotImplementedError

    def flush(self) -> None:
        # Most sinks will be fire-and-forget; override if batching.
        pass

    # Utilities for subclasses
    def _on_attempt(self) -> None:
        self.metrics.attempted += 1

    def _on_sent(self) -> None:
        self.metrics.sent += 1

    def _on_skip(self) -> None:
        self.metrics.skipped += 1

    def _on_error(self) -> None:
        self.metrics.errors += 1
