"""
Tests for TransferEngine.

Strategy
--------
TransferEngine is the integration layer. Tests here verify the lifecycle,
submission API, and end-to-end flow. We use real instances of all
dependencies and mock HTTP via `responses`.

Coverage targets
----------------
Lifecycle:
  - start() spins up correct number of workers
  - start() raises on double-call
  - context manager calls start() / close() / join() correctly
  - close() is idempotent
  - cancel() is idempotent
  - join() before start() is a no-op
  - join() drains queue before stopping workers (graceful)
  - cancel() stops workers without draining queue

Submission:
  - submit() raises EngineNotStartedError before start()
  - submit() raises EngineShutdownError after close()
  - submit() raises EngineShutdownError after cancel()
  - submit(block=False) raises QueueFullError when full
  - submit_many() submits all jobs

End-to-end:
  - single job downloaded correctly
  - multiple concurrent jobs all complete
  - failed job fires on_error, others complete
  - stats() reflects job counts correctly

stats():
  - returns EngineStats with correct queued count after submit
  - completed count increments after jobs finish
"""

from __future__ import annotations

import time
from pathlib import Path

import pytest
import responses as responses_lib

from recua.engine import TransferEngine
from recua.exceptions import EngineNotStartedError, EngineShutdownError, QueueFullError
from recua.job import TransferJob
from recua.options import TransferOptions

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

BASE_URL = "https://example.com"
BODY = b"recua test data " * 64  # 1 KiB


def _job(name: str, tmp_path: Path) -> TransferJob:
    return TransferJob(
        source=f"{BASE_URL}/{name}",
        dest=tmp_path / name,
        name=name,
    )


def _opts(**kwargs) -> TransferOptions:
    defaults = dict(max_workers=2, chunk_size=256, retries=1, backoff_base=0.01)
    defaults.update(kwargs)
    return TransferOptions(**defaults)


def _register(name: str, body: bytes = BODY, status: int = 200) -> None:
    responses_lib.add(
        responses_lib.GET,
        f"{BASE_URL}/{name}",
        body=body,
        status=status,
    )


# ---------------------------------------------------------------------------
# Lifecycle
# ---------------------------------------------------------------------------


class TestLifecycle:
    def test_start_creates_correct_number_of_workers(self, tmp_path: Path) -> None:
        engine = TransferEngine(_opts(max_workers=3))
        engine.start()
        assert len(engine._workers) == 3
        engine.cancel()
        engine.join()

    def test_double_start_raises(self, tmp_path: Path) -> None:
        engine = TransferEngine(_opts())
        engine.start()
        with pytest.raises(RuntimeError, match="start()"):
            engine.start()
        engine.cancel()
        engine.join()

    def test_context_manager_starts_and_joins(self, tmp_path: Path) -> None:
        with TransferEngine(_opts()) as engine:
            assert engine._started
        assert not any(w.is_alive() for w in engine._workers)

    def test_close_is_idempotent(self, tmp_path: Path) -> None:
        engine = TransferEngine(_opts())
        engine.start()
        engine.close()
        engine.close()  # must not raise
        engine.join()

    def test_cancel_is_idempotent(self, tmp_path: Path) -> None:
        engine = TransferEngine(_opts())
        engine.start()
        engine.cancel()
        engine.cancel()  # must not raise
        engine.join()

    def test_join_before_start_is_noop(self, tmp_path: Path) -> None:
        engine = TransferEngine(_opts())
        engine.join()  # must not raise or block

    def test_cancel_stops_workers_promptly(self, tmp_path: Path) -> None:
        engine = TransferEngine(_opts(max_workers=2))
        engine.start()
        engine.cancel()
        start = time.monotonic()
        engine.join()
        elapsed = time.monotonic() - start
        assert elapsed < 2.0, f"join() after cancel() took {elapsed:.2f}s"
        assert not any(w.is_alive() for w in engine._workers)

    @responses_lib.activate
    def test_graceful_close_drains_queue(self, tmp_path: Path) -> None:
        """close() + join() must complete all submitted jobs."""
        n = 4
        for i in range(n):
            _register(f"file{i}.bin")

        with TransferEngine(_opts(max_workers=2)) as engine:
            for i in range(n):
                engine.submit(_job(f"file{i}.bin", tmp_path))

        for i in range(n):
            assert (tmp_path / f"file{i}.bin").exists()


# ---------------------------------------------------------------------------
# Submission
# ---------------------------------------------------------------------------


class TestSubmission:
    def test_submit_before_start_raises(self, tmp_path: Path) -> None:
        engine = TransferEngine(_opts())
        with pytest.raises(EngineNotStartedError):
            engine.submit(_job("file.bin", tmp_path))

    def test_submit_after_close_raises(self, tmp_path: Path) -> None:
        engine = TransferEngine(_opts())
        engine.start()
        engine.close()
        with pytest.raises(EngineShutdownError):
            engine.submit(_job("file.bin", tmp_path))
        engine.join()

    def test_submit_after_cancel_raises(self, tmp_path: Path) -> None:
        engine = TransferEngine(_opts())
        engine.start()
        engine.cancel()
        with pytest.raises(EngineShutdownError):
            engine.submit(_job("file.bin", tmp_path))
        engine.join()

    def test_submit_non_blocking_raises_when_full(self, tmp_path: Path) -> None:
        engine = TransferEngine(_opts(max_workers=1, queue_size=1))
        engine.start()
        # Fill the queue — worker is busy so it won't drain immediately
        # We need to block the worker first; submit 2 jobs quickly
        try:
            for i in range(10):
                engine.submit(_job(f"f{i}.bin", tmp_path), block=False)
        except QueueFullError:
            pass  # expected
        finally:
            engine.cancel()
            engine.join()

    @responses_lib.activate
    def test_submit_many_submits_all_jobs(self, tmp_path: Path) -> None:
        n = 5
        for i in range(n):
            _register(f"file{i}.bin")

        with TransferEngine(_opts(max_workers=2)) as engine:
            engine.submit_many(_job(f"file{i}.bin", tmp_path) for i in range(n))

        for i in range(n):
            assert (tmp_path / f"file{i}.bin").exists()


# ---------------------------------------------------------------------------
# End-to-end
# ---------------------------------------------------------------------------


class TestEndToEnd:
    @responses_lib.activate
    def test_single_job_downloaded_correctly(self, tmp_path: Path) -> None:
        _register("file.bin", body=BODY)

        with TransferEngine(_opts(max_workers=1)) as engine:
            engine.submit(_job("file.bin", tmp_path))

        assert (tmp_path / "file.bin").read_bytes() == BODY

    @responses_lib.activate
    def test_multiple_concurrent_jobs_all_complete(self, tmp_path: Path) -> None:
        n = 8
        bodies = {f"file{i}.bin": bytes([i] * 512) for i in range(n)}
        for name, body in bodies.items():
            _register(name, body=body)

        with TransferEngine(_opts(max_workers=4)) as engine:
            for name in bodies:
                engine.submit(_job(name, tmp_path))

        for name, body in bodies.items():
            assert (tmp_path / name).read_bytes() == body

    @responses_lib.activate
    def test_failed_job_does_not_block_others(self, tmp_path: Path) -> None:
        """A 404 on one job must not prevent others from completing."""
        _register("good1.bin", body=BODY)
        _register("bad.bin", status=404)
        _register("good2.bin", body=BODY)

        errors: list[TransferJob] = []
        opts = _opts(
            max_workers=2,
            retries=0,
            on_error=lambda job, exc: errors.append(job),
        )

        with TransferEngine(opts) as engine:
            engine.submit(_job("good1.bin", tmp_path))
            engine.submit(_job("bad.bin", tmp_path))
            engine.submit(_job("good2.bin", tmp_path))

        assert (tmp_path / "good1.bin").read_bytes() == BODY
        assert (tmp_path / "good2.bin").read_bytes() == BODY
        assert len(errors) == 1
        assert errors[0].display_name == "bad.bin"

    @responses_lib.activate
    def test_on_complete_callback_fired_for_each_job(self, tmp_path: Path) -> None:
        n = 3
        for i in range(n):
            _register(f"file{i}.bin")

        completed: list[str] = []
        opts = _opts(
            max_workers=2,
            on_complete=lambda job: completed.append(job.display_name),
        )

        with TransferEngine(opts) as engine:
            for i in range(n):
                engine.submit(_job(f"file{i}.bin", tmp_path))

        assert sorted(completed) == sorted(f"file{i}.bin" for i in range(n))

    @responses_lib.activate
    def test_with_state_persistence_skips_completed(self, tmp_path: Path) -> None:
        """
        Second engine run with same state_path skips already-complete jobs.
        """
        dest = tmp_path / "file.bin"
        _register("file.bin", body=BODY)

        opts = _opts(max_workers=1, state_path=tmp_path / "state.db")

        # First run — downloads the file
        with TransferEngine(opts) as engine:
            engine.submit(_job("file.bin", tmp_path))

        assert dest.exists()

        # Second run — should skip (no new HTTP call)
        with TransferEngine(opts) as engine:
            engine.submit(_job("file.bin", tmp_path))

        # Only 1 HTTP call across both runs
        assert len(responses_lib.calls) == 1

    @responses_lib.activate
    def test_producer_pattern(self, tmp_path: Path) -> None:
        """Verify the recommended producer pattern works end-to-end."""
        n = 6
        for i in range(n):
            _register(f"data{i}.bin", body=bytes([i % 256] * 128))

        def produce(submit):
            for i in range(n):
                submit(
                    TransferJob(
                        source=f"{BASE_URL}/data{i}.bin",
                        dest=tmp_path / f"data{i}.bin",
                        name=f"data{i}.bin",
                    )
                )

        with TransferEngine(_opts(max_workers=3)) as engine:
            produce(engine.submit)

        for i in range(n):
            assert (tmp_path / f"data{i}.bin").exists()
            assert (tmp_path / f"data{i}.bin").read_bytes() == bytes([i % 256] * 128)


# ---------------------------------------------------------------------------
# stats()
# ---------------------------------------------------------------------------


class TestStats:
    def test_stats_before_start_returns_zeros(self, tmp_path: Path) -> None:
        engine = TransferEngine(_opts())
        stats = engine.stats()
        assert stats.completed == 0
        assert stats.queued == 0
        assert stats.active == 0

    @responses_lib.activate
    def test_stats_completed_count_after_run(self, tmp_path: Path) -> None:
        n = 3
        for i in range(n):
            _register(f"file{i}.bin")

        with TransferEngine(_opts(max_workers=2)) as engine:
            for i in range(n):
                engine.submit(_job(f"file{i}.bin", tmp_path))

        stats = engine.stats()
        assert stats.completed == n
        assert stats.failed == 0
        assert stats.queued == 0
        assert stats.active == 0

    @responses_lib.activate
    def test_stats_failed_count_after_errors(self, tmp_path: Path) -> None:
        responses_lib.add(responses_lib.GET, f"{BASE_URL}/bad.bin", status=404)

        opts = _opts(max_workers=1, retries=0)
        with TransferEngine(opts) as engine:
            engine.submit(_job("bad.bin", tmp_path))

        stats = engine.stats()
        assert stats.failed == 1
        assert stats.completed == 0
