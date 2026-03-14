"""
Thread-safe metrics aggregation for the transfer engine.

Design
------
Counters (completed, failed, queued, active) use a threading.Lock-guarded
integer — simple and safe for N writers.

Speed meter uses a sliding-window deque of (timestamp, bytes) samples.
Speed is computed as total_bytes_in_window / window_duration_seconds.
This avoids the naive total_bytes / total_elapsed distortion at the start
and end of a run.
"""

from __future__ import annotations

import threading
import time
from collections import deque
from dataclasses import dataclass, field


_SPEED_WINDOW_SECONDS = 5.0   # rolling window for MB/s calculation
_SPEED_SAMPLE_INTERVAL = 0.25  # min seconds between samples


@dataclass
class EngineStats:
    """Snapshot of engine metrics at a point in time. Immutable."""
    completed: int = 0
    failed: int = 0
    queued: int = 0
    active: int = 0
    bytes_total: int = 0
    bytes_done: int = 0
    speed_mb_per_sec: float = 0.0


class MetricsCollector:
    """
    Collects and exposes live engine metrics.

    All methods are thread-safe.
    Workers call record_bytes() as they write chunks.
    Engine calls inc/dec counters on job state transitions.
    """

    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._completed = 0
        self._failed = 0
        self._queued = 0
        self._active = 0
        self._bytes_total = 0
        self._bytes_done = 0
        # sliding window: deque of (monotonic_time, bytes_written)
        self._speed_samples: deque[tuple[float, int]] = deque()

    def job_queued(self, expected_size: int | None = None) -> None:
        # TODO: increment _queued, add expected_size to _bytes_total if known
        raise NotImplementedError

    def job_started(self) -> None:
        # TODO: decrement _queued, increment _active
        raise NotImplementedError

    def job_completed(self) -> None:
        # TODO: decrement _active, increment _completed
        raise NotImplementedError

    def job_failed(self) -> None:
        # TODO: decrement _active, increment _failed
        raise NotImplementedError

    def record_bytes(self, n: int) -> None:
        """Called by workers as each chunk is written to disk."""
        # TODO:
        #   - add n to _bytes_done
        #   - append (time.monotonic(), n) to _speed_samples
        #   - prune samples older than _SPEED_WINDOW_SECONDS
        raise NotImplementedError

    def snapshot(self) -> EngineStats:
        """Return a consistent point-in-time snapshot of all metrics."""
        # TODO:
        #   - acquire lock
        #   - compute speed from _speed_samples window
        #   - return EngineStats(...)
        raise NotImplementedError
