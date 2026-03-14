"""
Worker threads — each pulls jobs from the Scheduler and executes them.

Worker lifecycle
----------------
1. get() a job from Scheduler (blocks with timeout to stay cancellable)
2. Check StateStore — skip if already complete
3. Get resume offset from StateStore
4. Resolve adapter from AdapterRegistry
5. Call adapter.fetch(job, offset=offset)
6. Stream chunks → disk, calling RateLimiter.consume() and MetricsCollector.record_bytes()
7. On success: StateStore.mark_complete(), fire on_complete callback
8. On RetriableError: exponential backoff, re-enqueue if retries remain
9. On FatalTransferError: StateStore.mark_failed(), fire on_error callback
10. Repeat until shutdown_event is set and queue is drained
"""

from __future__ import annotations

import logging
import threading
import time
from pathlib import Path
from typing import TYPE_CHECKING

from recua.exceptions import FatalTransferError, RetriableError, RateLimitError
from recua.job import TransferJob

if TYPE_CHECKING:
    from recua.metrics import MetricsCollector
    from recua.options import TransferOptions
    from recua.protocols import StateStore, TransferAdapter
    from recua.scheduler import Scheduler

logger = logging.getLogger(__name__)


class Worker(threading.Thread):
    """
    A single download worker thread.

    Instantiated and started by TransferEngine. Not user-facing.
    """

    def __init__(
        self,
        *,
        worker_id: int,
        scheduler: "Scheduler",
        adapters: list["TransferAdapter"],
        state: "StateStore",
        metrics: "MetricsCollector",
        options: "TransferOptions",
        shutdown_event: threading.Event,
    ) -> None:
        super().__init__(name=f"te-worker-{worker_id}", daemon=True)
        self._id = worker_id
        self._scheduler = scheduler
        self._adapters = adapters
        self._state = state
        self._metrics = metrics
        self._options = options
        self._shutdown = shutdown_event

    def run(self) -> None:
        while not self._shutdown.is_set():
            job = self._scheduler.get()
            if job is None:
                continue
            try:
                self._execute(job)
            finally:
                self._scheduler.task_done()

    def _execute(self, job: TransferJob, attempt: int = 0) -> None:
        """Execute one transfer attempt. Handles retry logic recursively."""
        # TODO:
        #   1. Check state.is_complete(job.resume_key) → skip
        #   2. Resolve adapter (raise FatalTransferError if none match)
        #   3. offset = state.get_offset(job.resume_key)
        #   4. Ensure job.dest.parent exists
        #   5. Open dest in 'ab' mode if offset > 0, else 'wb'
        #   6. for chunk in adapter.fetch(job, offset): write + consume rate + record bytes
        #   7. On success → mark_complete, on_complete callback
        #   8. On RetriableError → backoff, retry if attempt < options.retries
        #   9. On RateLimitError → sleep retry_after, retry
        #  10. On FatalTransferError → mark_failed, on_error callback
        raise NotImplementedError

    def _backoff(self, attempt: int) -> None:
        delay = self._options.backoff_base ** attempt
        logger.debug("Worker %d backing off %.1fs (attempt %d)", self._id, delay, attempt)
        time.sleep(delay)

    def _resolve_adapter(self, source: str) -> "TransferAdapter":
        for adapter in self._adapters:
            if adapter.supports(source):
                return adapter
        raise FatalTransferError(f"No adapter found for source: {source!r}")
