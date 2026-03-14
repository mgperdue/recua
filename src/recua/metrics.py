"""
Thread-safe metrics aggregation for the transfer engine.

Design
------
Counters (completed, failed, queued, active) use a single threading.Lock
shared across all state. Simple, correct, and low-overhead for the call
frequency expected here (job transitions, not per-byte).

Speed meter
-----------
Uses a sliding-window deque of (timestamp, bytes) samples.

    speed = sum(bytes in window) / window_duration_seconds

This avoids the naive total_bytes / total_elapsed distortion that makes
speed read near-zero at the start of a run and artificially low at the end.

Sample debouncing
-----------------
_SPEED_SAMPLE_INTERVAL (0.25s) limits how often a new sample is appended.
Without it, N workers writing small chunks at high throughput would produce
thousands of deque entries per second. The debounce means at most 4 samples/s
per worker, bounded by _SPEED_WINDOW_SECONDS * 4 = 20 samples total — trivial
memory and negligible speed error.

The last-sample timestamp is tracked per-thread via threading.local() so
workers don't contend on a shared "last sample time" variable under the lock.
"""

from __future__ import annotations

import threading
import time
from collections import deque
from dataclasses import dataclass

_SPEED_WINDOW_SECONDS = 5.0  # rolling window for speed calculation
_SPEED_SAMPLE_INTERVAL = 0.25  # min seconds between deque appends per thread
_BYTES_PER_MB = 1_048_576.0


@dataclass(frozen=True)
class EngineStats:
    """
    Immutable snapshot of engine metrics at a point in time.

    Returned by MetricsCollector.snapshot(). All fields are consistent
    with each other — captured atomically under the collector's lock.
    """

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

    All public methods are thread-safe.

    Lifecycle hooks (called by TransferEngine / Worker):
        job_queued()     — job enters the queue
        job_started()    — worker picks up the job
        job_completed()  — transfer succeeded
        job_failed()     — transfer exhausted retries

    Data hooks (called by Worker per chunk):
        record_bytes(n)  — n bytes written to disk

    Observability:
        snapshot()       — consistent point-in-time EngineStats
    """

    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._completed = 0
        self._failed = 0
        self._queued = 0
        self._active = 0
        self._bytes_total = 0
        self._bytes_done = 0
        # Sliding window: deque of (monotonic_timestamp, bytes) pairs.
        # Oldest samples are at the left; newest at the right.
        self._speed_samples: deque[tuple[float, int]] = deque()
        # Per-thread last-sample timestamp for debouncing record_bytes().
        self._thread_local = threading.local()

    # ------------------------------------------------------------------
    # Job lifecycle hooks
    # ------------------------------------------------------------------

    def job_queued(self, expected_size: int | None = None) -> None:
        """
        Called when a job enters the queue.

        If expected_size is known, adds it to bytes_total so the progress
        percentage is meaningful from the start of the run.
        """
        with self._lock:
            self._queued += 1
            if expected_size is not None:
                self._bytes_total += expected_size

    def job_started(self) -> None:
        """Called when a worker dequeues and begins executing a job."""
        with self._lock:
            self._queued = max(0, self._queued - 1)
            self._active += 1

    def job_completed(self) -> None:
        """Called when a job finishes successfully."""
        with self._lock:
            self._active = max(0, self._active - 1)
            self._completed += 1

    def job_failed(self) -> None:
        """Called when a job exhausts all retries and is marked failed."""
        with self._lock:
            self._active = max(0, self._active - 1)
            self._failed += 1

    # ------------------------------------------------------------------
    # Data hook
    # ------------------------------------------------------------------

    def record_bytes(self, n: int) -> None:
        """
        Called by workers each time n bytes are written to disk.

        Updates bytes_done and appends to the speed window, subject to
        per-thread debouncing (_SPEED_SAMPLE_INTERVAL).
        """
        if n <= 0:
            return

        now = time.monotonic()
        last = getattr(self._thread_local, "last_sample_time", 0.0)
        should_sample = (now - last) >= _SPEED_SAMPLE_INTERVAL

        with self._lock:
            self._bytes_done += n
            if should_sample:
                self._speed_samples.append((now, n))
                self._prune_speed_window(now)

        if should_sample:
            self._thread_local.last_sample_time = now

    # ------------------------------------------------------------------
    # Observability
    # ------------------------------------------------------------------

    def snapshot(self) -> EngineStats:
        """
        Return a consistent point-in-time snapshot of all metrics.

        All fields are captured atomically under the lock — callers get
        a coherent view even when N workers are writing concurrently.
        """
        now = time.monotonic()
        with self._lock:
            self._prune_speed_window(now)
            speed = self._compute_speed()
            return EngineStats(
                completed=self._completed,
                failed=self._failed,
                queued=self._queued,
                active=self._active,
                bytes_total=self._bytes_total,
                bytes_done=self._bytes_done,
                speed_mb_per_sec=speed,
            )

    # ------------------------------------------------------------------
    # Internal
    # ------------------------------------------------------------------

    def _prune_speed_window(self, now: float) -> None:
        """
        Remove samples older than _SPEED_WINDOW_SECONDS from the left.

        Must be called under self._lock.
        """
        cutoff = now - _SPEED_WINDOW_SECONDS
        while self._speed_samples and self._speed_samples[0][0] < cutoff:
            self._speed_samples.popleft()

    def _compute_speed(self) -> float:
        """
        Compute MB/s from the current speed window.

        Must be called under self._lock.

        Returns 0.0 if:
          - the window is empty (no data yet or idle for > window duration)
          - all samples share the same timestamp (divide-by-zero guard)
        """
        if not self._speed_samples:
            return 0.0

        total_bytes = sum(s[1] for s in self._speed_samples)
        oldest_ts = self._speed_samples[0][0]
        newest_ts = self._speed_samples[-1][0]
        duration = newest_ts - oldest_ts

        if duration <= 0.0:
            # Single sample or instantaneous burst — not enough time elapsed
            # to compute a meaningful rate. Return 0 rather than inf.
            return 0.0

        return (total_bytes / duration) / _BYTES_PER_MB
