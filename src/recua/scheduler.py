"""
Scheduler — owns the internal job queue and dispatches to workers.

Queue type: PriorityQueue
  Items: (priority, sequence_number, TransferJob)
  sequence_number is a monotonically increasing tie-breaker that preserves
  FIFO ordering among jobs with equal priority (avoids tuple comparison
  falling through to TransferJob, which is not orderable).

The Scheduler is internal. Callers interact with TransferEngine only.
"""

from __future__ import annotations

import itertools
import queue
from typing import TYPE_CHECKING

from recua.job import TransferJob

if TYPE_CHECKING:
    pass


class Scheduler:
    """
    Wraps a PriorityQueue and exposes put/get semantics with
    backpressure and graceful shutdown support.
    """

    def __init__(self, maxsize: int) -> None:
        self._queue: queue.PriorityQueue = queue.PriorityQueue(maxsize=maxsize)
        self._counter = itertools.count()  # tie-breaker sequence

    def put(self, job: TransferJob, block: bool = True, timeout: float | None = None) -> None:
        """
        Enqueue a job.

        Raises QueueFullError if block=False and queue is at capacity.
        """
        # TODO:
        #   - build (priority, seq, job) tuple
        #   - call _queue.put(), catch queue.Full and raise QueueFullError
        raise NotImplementedError

    def get(self, timeout: float = 0.1) -> TransferJob | None:
        """
        Dequeue the next job.

        Returns None on timeout (allows workers to check shutdown flag).
        """
        # TODO: _queue.get(timeout=timeout), return None on queue.Empty
        raise NotImplementedError

    def task_done(self) -> None:
        self._queue.task_done()

    def join(self) -> None:
        self._queue.join()

    @property
    def qsize(self) -> int:
        return self._queue.qsize()
