"""
TransferEngine — public entry point and lifecycle manager.

Typical usage (context manager, recommended):

    opts = TransferOptions(max_workers=4, state_path=Path("state.db"))

    with TransferEngine(opts) as engine:
        for url, dest in my_urls:
            engine.submit(TransferJob(source=url, dest=dest))

    # __exit__ calls close() then join() — blocks until all jobs finish.

Explicit lifecycle (for programmatic control):

    engine = TransferEngine(opts)
    engine.start()

    engine.submit(job)          # from any thread, any time
    engine.submit_many(jobs)    # convenience bulk submission

    engine.close()              # signal: no more submissions
    engine.join()               # block until all jobs complete

    stats = engine.stats()      # live metrics snapshot at any point

Cancellation:

    engine.cancel()             # stop immediately; partial downloads resumable
    engine.join()               # wait for workers to finish current chunks

Thread safety
-------------
submit() is safe to call from any thread at any time after start().
stats() is safe to call from any thread at any time.
start() / close() / join() / cancel() are NOT thread-safe — call from
the owning thread only.

Lifecycle state machine
-----------------------
CREATED → start() → RUNNING → close() → CLOSING → join() → DONE
                                       → cancel()         → DONE
"""

from __future__ import annotations

import logging
import threading
from collections.abc import Iterable

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

    See module docstring for usage patterns.

    Parameters
    ----------
    options:
        Engine configuration. Defaults to TransferOptions() if not provided.
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
        self._display = None  # set in start() if progress=True

    # ------------------------------------------------------------------
    # Lifecycle
    # ------------------------------------------------------------------

    def start(self) -> None:
        """
        Spin up worker threads and begin processing submitted jobs.

        Must be called exactly once before any submit() call.
        If TransferOptions.progress=True and rich is installed, a live
        progress display is started in a background thread.

        Raises
        ------
        RuntimeError
            If called more than once on the same engine instance.
        """
        if self._started:
            raise RuntimeError(
                "TransferEngine.start() called more than once. "
                "Create a new TransferEngine instance for a new run."
            )

        # Initialise progress display if requested.
        if self._options.progress:
            from recua.progress import make_display
            self._display = make_display(self._metrics)
            self._display.start()

        logger.info(
            "Starting TransferEngine with %d workers", self._options.max_workers
        )

        for i in range(self._options.max_workers):
            worker = Worker(
                worker_id=i,
                scheduler=self._scheduler,
                adapters=self._adapters,
                state=self._state,
                metrics=self._metrics,
                rate_limiter=self._rate_limiter,
                options=self._options,
                shutdown_event=self._shutdown_event,
                display=self._display,
            )
            worker.start()
            self._workers.append(worker)

        self._started = True
        logger.debug("TransferEngine started — %d workers running", len(self._workers))

    def close(self) -> None:
        """
        Signal that no more jobs will be submitted.

        Non-blocking. Workers continue processing all queued jobs until
        the queue is drained, then exit. Call join() to wait for completion.

        Safe to call multiple times — subsequent calls are no-ops.
        """
        if self._closed:
            return
        self._closed = True
        logger.debug("TransferEngine closed — no more submissions accepted")

    def join(self) -> None:
        """
        Block until all submitted jobs are finished and all workers have exited.

        For a graceful shutdown (after close()):
          1. Waits for the scheduler queue to drain completely.
          2. Sets the shutdown event, signalling workers to exit.
          3. Joins each worker thread.
          4. Stops the progress display.

        For a cancelled shutdown (after cancel()):
          Skips the queue drain and waits directly for workers to exit.

        Safe to call before close() — will drain and shut down.
        """
        if not self._started:
            return

        if not self._shutdown_event.is_set():
            logger.debug("TransferEngine joining — draining queue")
            self._scheduler.join()
            self._shutdown_event.set()

        logger.debug("TransferEngine waiting for workers to exit")
        for worker in self._workers:
            worker.join()

        if self._display is not None:
            self._display.stop()

        logger.info("TransferEngine shut down cleanly")

    def cancel(self) -> None:
        """
        Immediately stop scheduling new jobs.

        Workers finish their current in-progress chunk, then exit.
        Queued jobs that have not yet started are abandoned — partial
        downloads remain resumable via StateStore on the next run.

        Call join() after cancel() to wait for workers to finish.

        Safe to call multiple times — subsequent calls are no-ops.
        """
        if self._shutdown_event.is_set():
            return
        self._closed = True
        self._shutdown_event.set()
        logger.info("TransferEngine cancelled — workers will exit after current chunk")

    # ------------------------------------------------------------------
    # Submission
    # ------------------------------------------------------------------

    def submit(
        self,
        job: TransferJob,
        block: bool = True,
        timeout: float | None = None,
    ) -> None:
        """
        Add a job to the transfer queue.

        Parameters
        ----------
        job:
            The TransferJob to enqueue.
        block:
            If True (default), block until queue space is available.
        timeout:
            When block=True, maximum seconds to wait for queue space.

        Raises
        ------
        EngineNotStartedError:  start() has not been called.
        EngineShutdownError:    close() or cancel() has been called.
        QueueFullError:         queue is full and block=False or timeout expired.
        """
        if not self._started:
            raise EngineNotStartedError(
                "Call engine.start() before submitting jobs."
            )
        if self._closed:
            raise EngineShutdownError(
                "Cannot submit jobs after close() or cancel() has been called."
            )

        self._scheduler.put(job, block=block, timeout=timeout)
        self._metrics.job_queued(job.expected_size)

    def submit_many(self, jobs: Iterable[TransferJob]) -> None:
        """
        Submit an iterable of jobs with default blocking behaviour.

        The queue applies backpressure automatically — the producer is
        only as fast as workers can drain.

        Example
        -------
        engine.submit_many(
            TransferJob(source=url, dest=out_dir / Path(url).name)
            for url in discover_urls()
        )
        """
        for job in jobs:
            self.submit(job)

    # ------------------------------------------------------------------
    # Observability
    # ------------------------------------------------------------------

    def stats(self) -> EngineStats:
        """
        Return a consistent snapshot of current engine metrics.

        Safe to call from any thread at any time.
        """
        return self._metrics.snapshot()

    # ------------------------------------------------------------------
    # Context manager
    # ------------------------------------------------------------------

    def __enter__(self) -> "TransferEngine":
        self.start()
        return self

    def __exit__(self, *_: object) -> None:
        self.close()
        self.join()
