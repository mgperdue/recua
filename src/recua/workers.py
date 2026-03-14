"""
Worker threads — each pulls jobs from the Scheduler and executes them.

Worker lifecycle
----------------
1.  get() a job from Scheduler (short timeout keeps workers cancellable)
2.  Check StateStore — skip immediately if already complete
3.  Enter retry loop (up to options.retries + 1 total attempts)
4.  Resolve adapter for job.source scheme
5.  Get resume offset from StateStore
6.  Ensure dest.parent directory exists
7.  Open dest file ('wb' for fresh start, 'r+b'+seek for resume)
8.  Stream chunks from adapter.fetch():
      - RateLimiter.consume() before each write
      - write chunk to file
      - MetricsCollector.record_bytes() after each write
      - StateStore.set_offset() periodically to checkpoint progress
9.  On clean completion:
      - StateStore.mark_complete()
      - MetricsCollector.job_completed()
      - Fire options.on_complete callback
10. On RetriableError (includes RateLimitError):
      - Sleep retry_after (RateLimitError) or backoff (RetriableError)
      - If offset was reset to 0 (server doesn't support Range), truncate file
      - Increment attempt counter; if exhausted → treat as fatal
11. On FatalTransferError:
      - StateStore.mark_failed()
      - MetricsCollector.job_failed()
      - Fire options.on_error callback
12. Repeat from step 1 until shutdown_event is set

Retry / backoff policy
-----------------------
Attempt 0: immediate
Attempt n: sleep backoff_base ** n seconds  (default: 1.5^n)
RateLimitError: sleep retry_after if provided, else fall back to backoff

200-for-206 reset
-----------------
When the HTTP adapter raises RetriableError because the server returned
200 instead of 206 for a Range request, the offset in StateStore is
non-zero but the file must be restarted from byte 0. We detect this via
the "200 instead of 206" message in the exception, reset the StateStore
offset to 0, and truncate the partial file before retrying.
"""

from __future__ import annotations

import logging
import threading
import time
from typing import TYPE_CHECKING

from recua.exceptions import FatalTransferError, RateLimitError, RetriableError
from recua.job import TransferJob

if TYPE_CHECKING:
    from recua.metrics import MetricsCollector
    from recua.options import TransferOptions
    from recua.protocols import StateStore, TransferAdapter
    from recua.rate_limit import RateLimiter
    from recua.scheduler import Scheduler

logger = logging.getLogger(__name__)

# Bytes written between StateStore checkpoints.
# Smaller = more resume granularity, more SQLite writes.
# Larger = fewer writes, coarser resume point on crash.
_CHECKPOINT_INTERVAL = 16 * 1_048_576  # 16 MiB


class Worker(threading.Thread):
    """
    A single download worker thread.

    Instantiated and started by TransferEngine. Not user-facing.
    Workers are daemon threads — they do not prevent process exit.
    """

    def __init__(
        self,
        *,
        worker_id: int,
        scheduler: "Scheduler",
        adapters: list["TransferAdapter"],
        state: "StateStore",
        metrics: "MetricsCollector",
        rate_limiter: "RateLimiter",
        options: "TransferOptions",
        shutdown_event: threading.Event,
    ) -> None:
        super().__init__(name=f"recua-worker-{worker_id}", daemon=True)
        self._id = worker_id
        self._scheduler = scheduler
        self._adapters = adapters
        self._state = state
        self._metrics = metrics
        self._rate_limiter = rate_limiter
        self._options = options
        self._shutdown = shutdown_event

    # ------------------------------------------------------------------
    # Thread entry point
    # ------------------------------------------------------------------

    def run(self) -> None:
        """
        Main worker loop. Runs until shutdown_event is set.

        Drains the scheduler continuously. On shutdown, finishes the
        current job (if any) then exits cleanly.
        """
        logger.debug("Worker %d started", self._id)
        while not self._shutdown.is_set():
            job = self._scheduler.get()
            if job is None:
                continue
            try:
                self._execute(job)
            finally:
                self._scheduler.task_done()
        logger.debug("Worker %d exiting", self._id)

    # ------------------------------------------------------------------
    # Job execution
    # ------------------------------------------------------------------

    def _execute(self, job: TransferJob) -> None:
        """
        Attempt to transfer job, retrying on transient errors.

        Uses an iterative retry loop to avoid recursion stack growth
        on high retry counts.
        """
        if self._state.is_complete(job.resume_key):
            logger.debug(
                "Worker %d skipping already-complete: %s", self._id, job.display_name
            )
            # Count as completed so metrics stay consistent.
            self._metrics.job_completed()
            return

        self._metrics.job_started()
        max_attempts = self._options.retries + 1  # retries=5 → 6 total attempts

        for attempt in range(max_attempts):
            try:
                self._transfer(job)
                return  # success — _transfer updated state and metrics

            except RateLimitError as exc:
                # Catch before RetriableError since it's a subclass.
                if attempt + 1 >= max_attempts:
                    self._fail(job, exc)
                    return
                wait = (
                    exc.retry_after
                    if exc.retry_after is not None
                    else self._backoff_delay(attempt)
                )
                logger.warning(
                    "Worker %d rate-limited on %s — waiting %.1fs (attempt %d/%d)",
                    self._id, job.display_name, wait, attempt + 1, max_attempts,
                )
                time.sleep(wait)

            except RetriableError as exc:
                if attempt + 1 >= max_attempts:
                    self._fail(job, exc)
                    return
                # Server ignored Range header — reset to restart from byte 0.
                if "200 instead of 206" in str(exc):
                    logger.debug(
                        "Worker %d resetting offset for %s (no Range support)",
                        self._id, job.display_name,
                    )
                    self._state.set_offset(job.resume_key, 0)
                    self._truncate(job)

                delay = self._backoff_delay(attempt)
                logger.warning(
                    "Worker %d retrying %s in %.1fs: %s (attempt %d/%d)",
                    self._id, job.display_name, delay, exc, attempt + 1, max_attempts,
                )
                time.sleep(delay)

            except FatalTransferError as exc:
                self._fail(job, exc)
                return

            except Exception as exc:
                # Unexpected errors (disk full, permission denied, etc.)
                # are treated as fatal — no point retrying a broken environment.
                logger.error(
                    "Worker %d unexpected error on %s: %s",
                    self._id, job.display_name, exc, exc_info=True,
                )
                self._fail(job, FatalTransferError(str(exc)))
                return

        # Safety net — should be unreachable because the last iteration
        # always calls return via success, _fail(), or the except clauses.
        self._fail(job, RetriableError(f"Exhausted {max_attempts} attempts for {job.display_name}"))

    def _transfer(self, job: TransferJob) -> None:
        """
        Execute a single transfer attempt — no retry logic here.

        Streams bytes from the adapter to disk, checkpointing progress
        to StateStore every _CHECKPOINT_INTERVAL bytes.

        Raises RetriableError, RateLimitError, or FatalTransferError —
        _execute() handles the retry/fail decision.
        """
        adapter = self._resolve_adapter(job.source)
        offset = self._state.get_offset(job.resume_key)

        job.dest.parent.mkdir(parents=True, exist_ok=True)

        mode = "r+b" if offset > 0 else "wb"
        bytes_since_checkpoint = 0

        logger.debug(
            "Worker %d transferring %s (offset=%d, mode=%s)",
            self._id, job.display_name, offset, mode,
        )

        with open(job.dest, mode) as fh:
            if offset > 0:
                fh.seek(offset)

            for chunk in adapter.fetch(
                job,
                offset=offset,
                chunk_size=self._options.chunk_size,
            ):
                self._rate_limiter.consume(len(chunk))
                fh.write(chunk)
                self._metrics.record_bytes(len(chunk))

                bytes_since_checkpoint += len(chunk)
                if bytes_since_checkpoint >= _CHECKPOINT_INTERVAL:
                    self._state.set_offset(job.resume_key, fh.tell())
                    bytes_since_checkpoint = 0

                if self._options.on_progress is not None:
                    try:
                        self._options.on_progress(job, fh.tell(), job.expected_size)
                    except Exception as exc:
                        logger.debug(
                            "Worker %d on_progress callback raised: %s", self._id, exc
                        )

        self._state.mark_complete(job.resume_key)
        self._metrics.job_completed()
        logger.info("Worker %d completed: %s", self._id, job.display_name)

        if self._options.on_complete is not None:
            try:
                self._options.on_complete(job)
            except Exception as exc:
                logger.warning(
                    "Worker %d on_complete callback raised for %s: %s",
                    self._id, job.display_name, exc,
                )

    # ------------------------------------------------------------------
    # Helpers
    # ------------------------------------------------------------------

    def _fail(self, job: TransferJob, exc: Exception) -> None:
        """Record a permanent failure and fire the on_error callback."""
        reason = str(exc)
        logger.error("Worker %d failed: %s — %s", self._id, job.display_name, reason)
        self._state.mark_failed(job.resume_key, reason)
        self._metrics.job_failed()

        if self._options.on_error is not None:
            try:
                self._options.on_error(job, exc)
            except Exception as cb_exc:
                logger.warning(
                    "Worker %d on_error callback raised for %s: %s",
                    self._id, job.display_name, cb_exc,
                )

    def _resolve_adapter(self, source: str) -> "TransferAdapter":
        """Return the first adapter that supports source, or raise FatalTransferError."""
        for adapter in self._adapters:
            if adapter.supports(source):
                return adapter
        raise FatalTransferError(f"No adapter found for source: {source!r}")

    def _backoff_delay(self, attempt: int) -> float:
        """Return exponential backoff delay in seconds for the given attempt."""
        return float(self._options.backoff_base ** attempt)

    def _truncate(self, job: TransferJob) -> None:
        """Truncate dest file to zero bytes for a clean restart."""
        try:
            if job.dest.exists():
                job.dest.write_bytes(b"")
        except OSError as exc:
            logger.warning(
                "Worker %d could not truncate %s: %s", self._id, job.dest, exc
            )
