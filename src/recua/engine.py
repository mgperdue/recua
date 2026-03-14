"""
TransferEngine — public entry point and lifecycle manager.

Typical usage (context manager, recommended):

    opts = TransferOptions(max_workers=4, state_path=Path("state.db"))

    with TransferEngine(opts) as engine:
        for url, dest in my_urls:
            engine.submit(TransferJob(source=url, dest=dest))

Explicit lifecycle (for programmatic control):

    engine = TransferEngine(opts)
    engine.start()
    engine.submit(job)
    engine.close()
    engine.join()

Thread safety: submit() is safe to call from any thread.
"""

from __future__ import annotations

import logging
import threading
from typing import Iterable

from recua.adapters.http import HTTPAdapter
from recua.exceptions import EngineNotStartedError, EngineShutdownError
from recua.job import TransferJob
from recua.metrics import EngineStats, MetricsCollector
from recua.options import TransferOptions
from recua.rate_limit import RateLimiter
from recua.scheduler import Scheduler
from recua.state import NullStateStore, SQLiteStateStore
from recua.workers import Worker

logger = logging.getLogger(__name__)


class TransferEngine:
    """
    Concurrent job engine for moving many large files reliably.

    See module docstring for usage examples.
    """

    def __init__(self, options: TransferOptions | None = None) -> None:
        self._options = options or TransferOptions()
        self._scheduler = Scheduler(maxsize=self._options.queue_size)
        self._metrics = MetricsCollector()
        self._rate_limiter = RateLimiter(self._options.max_mb_per_sec)
        self._state = (
            SQLiteStateStore(self._options.state_path)
            if self._options.state_path
            else NullStateStore()
        )
        self._adapters = [HTTPAdapter()]
        self._workers: list[Worker] = []
        self._shutdown_event = threading.Event()
        self._started = False
        self._closed = False

    # ------------------------------------------------------------------
    # Lifecycle
    # ------------------------------------------------------------------

    def start(self) -> None:
        """Spin up worker threads. Must be called before submit()."""
        # TODO:
        #   - guard against double-start
        #   - create and start max_workers Worker instances
        #   - set self._started = True
        raise NotImplementedError

    def close(self) -> None:
        """
        Signal that no more jobs will be submitted.

        Workers will finish all queued jobs, then exit.
        Non-blocking — call join() to wait for completion.
        """
        # TODO: set self._closed = True, set shutdown_event (after queue drains)
        raise NotImplementedError

    def join(self) -> None:
        """Block until all submitted jobs are finished and workers have exited."""
        # TODO: scheduler.join() then thread.join() for each worker
        raise NotImplementedError

    def cancel(self) -> None:
        """
        Immediately stop scheduling new jobs.

        Workers finish their current chunk, then exit.
        Partial downloads remain resumable via StateStore.
        """
        # TODO: set shutdown_event immediately (don't drain queue)
        raise NotImplementedError

    # ------------------------------------------------------------------
    # Submission
    # ------------------------------------------------------------------

    def submit(self, job: TransferJob, block: bool = True, timeout: float | None = None) -> None:
        """
        Add a job to the transfer queue.

        Args:
            job:     The TransferJob to enqueue.
            block:   If True, wait when queue is full (backpressure).
            timeout: Max seconds to wait when block=True.

        Raises:
            EngineNotStartedError: if start() has not been called.
            EngineShutdownError:   if close() or cancel() has been called.
            QueueFullError:        if block=False and queue is at capacity.
        """
        if not self._started:
            raise EngineNotStartedError("Call engine.start() before submitting jobs.")
        if self._closed:
            raise EngineShutdownError("Cannot submit after close() or cancel().")
        # TODO: self._scheduler.put(job, block=block, timeout=timeout)
        #       self._metrics.job_queued(job.expected_size)
        raise NotImplementedError

    def submit_many(self, jobs: Iterable[TransferJob]) -> None:
        """Convenience wrapper — submits each job with default blocking behaviour."""
        for job in jobs:
            self.submit(job)

    # ------------------------------------------------------------------
    # Observability
    # ------------------------------------------------------------------

    def stats(self) -> EngineStats:
        """Return a consistent snapshot of current engine metrics."""
        return self._metrics.snapshot()

    # ------------------------------------------------------------------
    # Context manager
    # ------------------------------------------------------------------

    def __enter__(self) -> "TransferEngine":
        self.start()
        return self

    def __exit__(self, *_) -> None:
        self.close()
        self.join()
