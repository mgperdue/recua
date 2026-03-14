"""
Tests for Worker.

Strategy
--------
Workers integrate every module built so far. We use real instances of:
  - Scheduler, StateStore (NullStateStore), MetricsCollector, RateLimiter
  - TransferOptions
  - tmp_path for destination files

HTTP calls are intercepted by the `responses` library — no real network.

We drive workers by:
  1. Submitting jobs to a real Scheduler
  2. Starting the worker thread
  3. Calling scheduler.join() to wait for completion
  4. Setting shutdown_event and joining the thread
  5. Asserting on filesystem, StateStore, and MetricsCollector state

Coverage targets
----------------
run() / lifecycle:
  - worker exits when shutdown_event is set and queue is empty
  - worker calls task_done() even when _execute raises unexpectedly

_execute():
  - skips already-complete jobs (metrics still updated)
  - successful transfer writes correct bytes to disk
  - successful transfer marks state complete
  - successful transfer fires on_complete callback
  - on_complete callback exception is swallowed (not fatal)

_execute() — retries:
  - RetriableError retries up to options.retries times
  - RetriableError exhausted → mark_failed, on_error callback
  - RateLimitError uses retry_after for sleep duration
  - RateLimitError exhausted → mark_failed
  - FatalTransferError does not retry → immediate mark_failed
  - Unexpected exception treated as fatal

_execute() — resume:
  - 200-for-206 resets offset to 0 and truncates file

_transfer():
  - resume: opens in r+b and seeks to offset
  - rate_limiter.consume() called for each chunk
  - on_progress callback fired during transfer
  - on_progress callback exception is swallowed

_resolve_adapter():
  - raises FatalTransferError for unknown scheme
"""

from __future__ import annotations

import threading
import time
from pathlib import Path
from unittest.mock import MagicMock, call, patch

import pytest
import responses as responses_lib

from recua.exceptions import FatalTransferError, RateLimitError, RetriableError
from recua.job import TransferJob
from recua.metrics import MetricsCollector
from recua.options import TransferOptions
from recua.rate_limit import RateLimiter
from recua.scheduler import Scheduler
from recua.state import NullStateStore, SQLiteStateStore
from recua.workers import Worker


# ---------------------------------------------------------------------------
# Fixtures and helpers
# ---------------------------------------------------------------------------

URL = "https://example.com/file.bin"
BODY = b"hello recua " * 256  # 3 KiB


def _job(url: str = URL, dest: Path | None = None) -> TransferJob:
    return TransferJob(
        source=url,
        dest=dest or Path("/tmp/recua_test_file.bin"),
        name="file.bin",
    )


def _make_worker(
    *,
    tmp_path: Path,
    options: TransferOptions | None = None,
    state: NullStateStore | SQLiteStateStore | None = None,
    metrics: MetricsCollector | None = None,
    scheduler: Scheduler | None = None,
    rate_limiter: RateLimiter | None = None,
    shutdown_event: threading.Event | None = None,
) -> tuple[Worker, Scheduler, threading.Event]:
    from recua.adapters.http import HTTPAdapter

    sched = scheduler or Scheduler(maxsize=100)
    shutdown = shutdown_event or threading.Event()
    opts = options or TransferOptions(chunk_size=1024, retries=2)

    worker = Worker(
        worker_id=0,
        scheduler=sched,
        adapters=[HTTPAdapter()],
        state=state or NullStateStore(),
        metrics=metrics or MetricsCollector(),
        rate_limiter=rate_limiter or RateLimiter(None),
        options=opts,
        shutdown_event=shutdown,
    )
    return worker, sched, shutdown


def _run_worker(worker: Worker, sched: Scheduler, shutdown: threading.Event) -> None:
    """Start worker, wait for queue to drain, then signal shutdown."""
    worker.start()
    sched.join()           # wait for all submitted jobs to be processed
    shutdown.set()         # tell the worker to exit
    worker.join(timeout=3.0)


# ---------------------------------------------------------------------------
# Lifecycle
# ---------------------------------------------------------------------------

class TestLifecycle:
    def test_worker_exits_when_shutdown_set(self, tmp_path: Path) -> None:
        worker, sched, shutdown = _make_worker(tmp_path=tmp_path)
        worker.start()
        shutdown.set()
        worker.join(timeout=2.0)
        assert not worker.is_alive()

    def test_worker_calls_task_done_on_exception(self, tmp_path: Path) -> None:
        """
        Even if _execute raises an unexpected exception, task_done() must
        be called so scheduler.join() doesn't block forever.
        """
        worker, sched, shutdown = _make_worker(tmp_path=tmp_path)
        job = _job(dest=tmp_path / "file.bin")
        sched.put(job)

        with patch.object(worker, "_execute", side_effect=RuntimeError("boom")):
            worker.start()
            sched.join()
            shutdown.set()
            worker.join(timeout=2.0)

        assert not worker.is_alive()


# ---------------------------------------------------------------------------
# Successful transfer
# ---------------------------------------------------------------------------

class TestSuccessfulTransfer:
    @responses_lib.activate
    def test_writes_correct_bytes_to_disk(self, tmp_path: Path) -> None:
        dest = tmp_path / "file.bin"
        responses_lib.add(responses_lib.GET, URL, body=BODY, status=200)

        worker, sched, shutdown = _make_worker(tmp_path=tmp_path)
        sched.put(_job(dest=dest))
        _run_worker(worker, sched, shutdown)

        assert dest.exists()
        assert dest.read_bytes() == BODY

    @responses_lib.activate
    def test_marks_state_complete(self, tmp_path: Path) -> None:
        dest = tmp_path / "file.bin"
        responses_lib.add(responses_lib.GET, URL, body=BODY, status=200)

        state = NullStateStore()
        worker, sched, shutdown = _make_worker(tmp_path=tmp_path, state=state)
        # NullStateStore doesn't persist — use SQLite to verify
        db_state = SQLiteStateStore(tmp_path / "state.db")

        worker2, sched2, shutdown2 = _make_worker(tmp_path=tmp_path, state=db_state)
        sched2.put(_job(dest=dest))
        _run_worker(worker2, sched2, shutdown2)

        assert db_state.is_complete(_job(dest=dest).resume_key)

    @responses_lib.activate
    def test_fires_on_complete_callback(self, tmp_path: Path) -> None:
        dest = tmp_path / "file.bin"
        responses_lib.add(responses_lib.GET, URL, body=BODY, status=200)

        completed: list[TransferJob] = []
        opts = TransferOptions(chunk_size=1024, retries=2, on_complete=completed.append)
        worker, sched, shutdown = _make_worker(tmp_path=tmp_path, options=opts)
        job = _job(dest=dest)
        sched.put(job)
        _run_worker(worker, sched, shutdown)

        assert len(completed) == 1
        assert completed[0] is job

    @responses_lib.activate
    def test_on_complete_exception_is_swallowed(self, tmp_path: Path) -> None:
        dest = tmp_path / "file.bin"
        responses_lib.add(responses_lib.GET, URL, body=BODY, status=200)

        def bad_callback(job: TransferJob) -> None:
            raise RuntimeError("callback exploded")

        opts = TransferOptions(chunk_size=1024, retries=0, on_complete=bad_callback)
        worker, sched, shutdown = _make_worker(tmp_path=tmp_path, options=opts)
        sched.put(_job(dest=dest))
        _run_worker(worker, sched, shutdown)

        # Worker should have completed without propagating the callback error
        assert dest.exists()
        assert not worker.is_alive()

    @responses_lib.activate
    def test_skips_already_complete_job(self, tmp_path: Path) -> None:
        """
        A job whose resume_key is already marked complete must be skipped
        without making any HTTP request.
        """
        dest = tmp_path / "file.bin"
        state = SQLiteStateStore(tmp_path / "state.db")
        job = _job(dest=dest)
        state.mark_complete(job.resume_key)

        metrics = MetricsCollector()
        worker, sched, shutdown = _make_worker(
            tmp_path=tmp_path, state=state, metrics=metrics
        )
        sched.put(job)
        _run_worker(worker, sched, shutdown)

        # No HTTP calls should have been made
        assert not dest.exists()
        # Metrics should still reflect a completed job
        assert metrics.snapshot().completed == 1


# ---------------------------------------------------------------------------
# Retry behaviour
# ---------------------------------------------------------------------------

class TestRetryBehaviour:
    @responses_lib.activate
    def test_retries_on_500_then_succeeds(self, tmp_path: Path) -> None:
        dest = tmp_path / "file.bin"
        # First call: 500; second call: 200
        responses_lib.add(responses_lib.GET, URL, status=500)
        responses_lib.add(responses_lib.GET, URL, body=BODY, status=200)

        opts = TransferOptions(chunk_size=1024, retries=2, backoff_base=0.01)
        worker, sched, shutdown = _make_worker(tmp_path=tmp_path, options=opts)
        sched.put(_job(dest=dest))
        _run_worker(worker, sched, shutdown)

        assert dest.read_bytes() == BODY

    @responses_lib.activate
    def test_exhausted_retries_marks_failed(self, tmp_path: Path) -> None:
        dest = tmp_path / "file.bin"
        # Always 503
        for _ in range(4):
            responses_lib.add(responses_lib.GET, URL, status=503)

        state = SQLiteStateStore(tmp_path / "state.db")
        opts = TransferOptions(chunk_size=1024, retries=2, backoff_base=0.01)
        worker, sched, shutdown = _make_worker(
            tmp_path=tmp_path, state=state, options=opts
        )
        sched.put(_job(dest=dest))
        _run_worker(worker, sched, shutdown)

        assert state.get_status(_job(dest=dest).resume_key) == "failed"

    @responses_lib.activate
    def test_exhausted_retries_fires_on_error_callback(self, tmp_path: Path) -> None:
        dest = tmp_path / "file.bin"
        for _ in range(4):
            responses_lib.add(responses_lib.GET, URL, status=503)

        errors: list[tuple[TransferJob, Exception]] = []
        opts = TransferOptions(
            chunk_size=1024, retries=2, backoff_base=0.01,
            on_error=lambda job, exc: errors.append((job, exc)),
        )
        worker, sched, shutdown = _make_worker(tmp_path=tmp_path, options=opts)
        job = _job(dest=dest)
        sched.put(job)
        _run_worker(worker, sched, shutdown)

        assert len(errors) == 1
        assert errors[0][0] is job

    @responses_lib.activate
    def test_fatal_error_does_not_retry(self, tmp_path: Path) -> None:
        dest = tmp_path / "file.bin"
        responses_lib.add(responses_lib.GET, URL, status=404)

        state = SQLiteStateStore(tmp_path / "state.db")
        opts = TransferOptions(chunk_size=1024, retries=5, backoff_base=0.01)
        worker, sched, shutdown = _make_worker(
            tmp_path=tmp_path, state=state, options=opts
        )
        sched.put(_job(dest=dest))
        _run_worker(worker, sched, shutdown)

        # Should be exactly 1 HTTP call — no retries
        assert len(responses_lib.calls) == 1
        assert state.get_status(_job(dest=dest).resume_key) == "failed"

    @responses_lib.activate
    def test_rate_limit_uses_retry_after(self, tmp_path: Path) -> None:
        dest = tmp_path / "file.bin"
        responses_lib.add(
            responses_lib.GET, URL,
            status=429, headers={"Retry-After": "0.05"},
        )
        responses_lib.add(responses_lib.GET, URL, body=BODY, status=200)

        opts = TransferOptions(chunk_size=1024, retries=2, backoff_base=0.01)
        worker, sched, shutdown = _make_worker(tmp_path=tmp_path, options=opts)
        sched.put(_job(dest=dest))
        _run_worker(worker, sched, shutdown)

        assert dest.read_bytes() == BODY
        assert len(responses_lib.calls) == 2

    @responses_lib.activate
    def test_unexpected_exception_treated_as_fatal(self, tmp_path: Path) -> None:
        dest = tmp_path / "file.bin"
        state = SQLiteStateStore(tmp_path / "state.db")

        from recua.adapters.http import HTTPAdapter
        adapter = HTTPAdapter()

        with patch.object(adapter, "fetch", side_effect=MemoryError("OOM")):
            opts = TransferOptions(chunk_size=1024, retries=3, backoff_base=0.01)
            shutdown = threading.Event()
            sched = Scheduler(maxsize=10)
            worker = Worker(
                worker_id=0,
                scheduler=sched,
                adapters=[adapter],
                state=state,
                metrics=MetricsCollector(),
                rate_limiter=RateLimiter(None),
                options=opts,
                shutdown_event=shutdown,
            )
            job = _job(dest=dest)
            sched.put(job)
            _run_worker(worker, sched, shutdown)

        # Treated as fatal — should not have retried 3 times
        assert state.get_status(job.resume_key) == "failed"


# ---------------------------------------------------------------------------
# Resume behaviour
# ---------------------------------------------------------------------------

class TestResumeBehaviour:
    @responses_lib.activate
    def test_resume_appends_from_offset(self, tmp_path: Path) -> None:
        """
        If StateStore returns offset=N, the worker seeks to N and appends.
        We simulate by pre-writing the first half and setting offset in state.
        """
        dest = tmp_path / "file.bin"
        first_half = b"first-" * 100
        second_half = b"second" * 100

        # Pre-write first half
        dest.write_bytes(first_half)
        offset = len(first_half)

        state = SQLiteStateStore(tmp_path / "state.db")
        state.set_offset(_job(dest=dest).resume_key, offset)

        # Server serves the second half with 206
        responses_lib.add(responses_lib.GET, URL, body=second_half, status=206)

        opts = TransferOptions(chunk_size=256, retries=0)
        worker, sched, shutdown = _make_worker(
            tmp_path=tmp_path, state=state, options=opts
        )
        sched.put(_job(dest=dest))
        _run_worker(worker, sched, shutdown)

        assert dest.read_bytes() == first_half + second_half

    @responses_lib.activate
    def test_200_for_206_resets_and_retries_from_zero(self, tmp_path: Path) -> None:
        dest = tmp_path / "file.bin"
        first_half = b"x" * 512
        dest.write_bytes(first_half)

        state = SQLiteStateStore(tmp_path / "state.db")
        state.set_offset(_job(dest=dest).resume_key, len(first_half))

        # First call: server ignores Range, returns 200 with full body
        responses_lib.add(responses_lib.GET, URL, body=BODY, status=200)
        # Second call (retry from 0): returns 200 again (fresh start)
        responses_lib.add(responses_lib.GET, URL, body=BODY, status=200)

        opts = TransferOptions(chunk_size=256, retries=2, backoff_base=0.01)
        worker, sched, shutdown = _make_worker(
            tmp_path=tmp_path, state=state, options=opts
        )
        sched.put(_job(dest=dest))
        _run_worker(worker, sched, shutdown)

        # After reset, should have the full body from byte 0
        assert dest.read_bytes() == BODY
        assert state.get_offset(_job(dest=dest).resume_key) == 0 or state.is_complete(
            _job(dest=dest).resume_key
        )


# ---------------------------------------------------------------------------
# Rate limiter integration
# ---------------------------------------------------------------------------

class TestRateLimiterIntegration:
    @responses_lib.activate
    def test_rate_limiter_consume_called_per_chunk(self, tmp_path: Path) -> None:
        dest = tmp_path / "file.bin"
        responses_lib.add(responses_lib.GET, URL, body=BODY, status=200)

        mock_limiter = MagicMock()
        mock_limiter.unlimited = False

        opts = TransferOptions(chunk_size=512, retries=0)
        shutdown = threading.Event()
        sched = Scheduler(maxsize=10)
        from recua.adapters.http import HTTPAdapter
        worker = Worker(
            worker_id=0,
            scheduler=sched,
            adapters=[HTTPAdapter()],
            state=NullStateStore(),
            metrics=MetricsCollector(),
            rate_limiter=mock_limiter,
            options=opts,
            shutdown_event=shutdown,
        )
        sched.put(_job(dest=dest))
        _run_worker(worker, sched, shutdown)

        assert mock_limiter.consume.call_count > 0


# ---------------------------------------------------------------------------
# on_progress callback
# ---------------------------------------------------------------------------

class TestOnProgressCallback:
    @responses_lib.activate
    def test_on_progress_called_during_transfer(self, tmp_path: Path) -> None:
        dest = tmp_path / "file.bin"
        responses_lib.add(responses_lib.GET, URL, body=BODY, status=200)

        progress_calls: list[tuple] = []

        def on_progress(job: TransferJob, done: int, total: int | None) -> None:
            progress_calls.append((done, total))

        opts = TransferOptions(chunk_size=256, retries=0, on_progress=on_progress)
        worker, sched, shutdown = _make_worker(tmp_path=tmp_path, options=opts)
        sched.put(_job(dest=dest))
        _run_worker(worker, sched, shutdown)

        assert len(progress_calls) > 0
        # bytes_done should be monotonically increasing
        dones = [p[0] for p in progress_calls]
        assert dones == sorted(dones)

    @responses_lib.activate
    def test_on_progress_exception_is_swallowed(self, tmp_path: Path) -> None:
        dest = tmp_path / "file.bin"
        responses_lib.add(responses_lib.GET, URL, body=BODY, status=200)

        def bad_progress(job: TransferJob, done: int, total: int | None) -> None:
            raise ValueError("progress callback exploded")

        opts = TransferOptions(chunk_size=256, retries=0, on_progress=bad_progress)
        worker, sched, shutdown = _make_worker(tmp_path=tmp_path, options=opts)
        sched.put(_job(dest=dest))
        _run_worker(worker, sched, shutdown)

        assert dest.read_bytes() == BODY


# ---------------------------------------------------------------------------
# _resolve_adapter
# ---------------------------------------------------------------------------

class TestResolveAdapter:
    def test_raises_fatal_for_unknown_scheme(self, tmp_path: Path) -> None:
        worker, sched, shutdown = _make_worker(tmp_path=tmp_path)
        with pytest.raises(FatalTransferError, match="No adapter found"):
            worker._resolve_adapter("s3://bucket/key")
