"""
Scheduler — owns the internal job queue and dispatches to workers.

Queue type: PriorityQueue
  Items: (priority, sequence_number, TransferJob)

  sequence_number is a monotonically increasing tie-breaker that preserves
  FIFO ordering among jobs with equal priority. This is critical because
  PriorityQueue compares tuple elements left-to-right, and if priority is
  equal it would fall through to compare TransferJob instances — which are
  frozen dataclasses and not orderable by default, causing a TypeError.

  The counter is global across all put() calls, so submission order is
  always preserved within a priority level regardless of which thread
  submitted the job.

Backpressure
  put(block=True) blocks the producer when the queue is full, naturally
  slowing fast producers to the rate workers can drain. This is the
  recommended usage pattern. put(block=False) raises QueueFullError
  immediately, giving callers the option to apply their own backpressure.

Shutdown
  Workers call get() in a loop with a short timeout. On each None return
  they check their shutdown_event and exit if set. This means the engine
  can signal shutdown without needing sentinel values in the queue, and
  partial draining is safe — jobs still in the queue when cancel() is
  called remain resumable via StateStore.

The Scheduler is internal. Callers interact with TransferEngine only.
"""

from __future__ import annotations

import itertools
import queue

from recua.exceptions import QueueFullError
from recua.job import TransferJob


class Scheduler:
    """
    Priority-ordered job queue with backpressure and worker-friendly get().

    Items are dequeued in (priority ASC, submission_order ASC) order.
    Lower priority value = higher urgency (min-heap convention).
    Default priority is 0; all jobs at the same priority are FIFO.

    Parameters
    ----------
    maxsize:
        Maximum number of jobs held in the queue before put() blocks.
        0 means unlimited (not recommended — no backpressure).
    """

    def __init__(self, maxsize: int) -> None:
        self._queue: queue.PriorityQueue[tuple[int, int, TransferJob]] = queue.PriorityQueue(
            maxsize=maxsize
        )
        self._counter = itertools.count()

    # ------------------------------------------------------------------
    # Producer API
    # ------------------------------------------------------------------

    def put(
        self,
        job: TransferJob,
        block: bool = True,
        timeout: float | None = None,
    ) -> None:
        """
        Enqueue a job.

        Parameters
        ----------
        job:
            The TransferJob to enqueue.
        block:
            If True (default), block until space is available.
            If False, raise QueueFullError immediately if the queue is full.
        timeout:
            When block=True, maximum seconds to wait for space.
            None means wait indefinitely.
            Raises QueueFullError if the timeout expires.

        Raises
        ------
        QueueFullError:
            If block=False and queue is at capacity, or if block=True and
            the timeout expires before space becomes available.
        """
        seq = next(self._counter)
        item = (job.priority, seq, job)
        try:
            self._queue.put(item, block=block, timeout=timeout)
        except queue.Full as exc:
            raise QueueFullError(
                f"Transfer queue is full (maxsize={self._queue.maxsize}). "
                "Use block=True or increase TransferOptions.queue_size."
            ) from exc

    # ------------------------------------------------------------------
    # Consumer API
    # ------------------------------------------------------------------

    def get(self, timeout: float = 0.1) -> TransferJob | None:
        """
        Dequeue and return the highest-priority job.

        Blocks up to timeout seconds waiting for a job. Returns None if
        no job arrives within the timeout, allowing the caller (a worker
        thread) to check its shutdown_event and loop back.

        Parameters
        ----------
        timeout:
            Seconds to wait before returning None. Keep short (0.1s) so
            workers stay responsive to shutdown signals.

        Returns
        -------
        TransferJob | None
            The next job, or None on timeout.
        """
        try:
            _priority, _seq, job = self._queue.get(block=True, timeout=timeout)
            return job
        except queue.Empty:
            return None

    # ------------------------------------------------------------------
    # Coordination
    # ------------------------------------------------------------------

    def task_done(self) -> None:
        """
        Signal that a previously dequeued job has been processed.

        Must be called by the worker after each get() that returned a job
        (not on None returns). Required for join() to unblock correctly.
        """
        self._queue.task_done()

    def join(self) -> None:
        """
        Block until every enqueued job has had task_done() called.

        Used by TransferEngine.join() to wait for full queue drain before
        declaring the run complete.
        """
        self._queue.join()

    # ------------------------------------------------------------------
    # Observability
    # ------------------------------------------------------------------

    @property
    def qsize(self) -> int:
        """Approximate number of jobs currently in the queue."""
        return self._queue.qsize()

    @property
    def maxsize(self) -> int:
        """Configured maximum queue depth."""
        return self._queue.maxsize
