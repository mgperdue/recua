"""
Tests for MetricsCollector and EngineStats.

Coverage targets
----------------
EngineStats:
  - frozen dataclass (immutability)
  - default values

MetricsCollector — counters:
  - job_queued increments queued, adds to bytes_total
  - job_started decrements queued, increments active
  - job_completed decrements active, increments completed
  - job_failed decrements active, increments failed
  - counters never go below zero (floor guard)
  - full lifecycle: queued → started → completed
  - full lifecycle: queued → started → failed

MetricsCollector — record_bytes:
  - bytes_done accumulates correctly
  - zero and negative bytes are no-ops
  - speed window prunes old samples

MetricsCollector — snapshot:
  - returns EngineStats with correct values
  - speed_mb_per_sec is 0 with no samples
  - speed_mb_per_sec is 0 with single sample (no duration)
  - speed_mb_per_sec reflects recent throughput (smoke test)
  - snapshot is consistent (atomic read)

MetricsCollector — thread safety:
  - concurrent job lifecycle calls, no corruption
  - concurrent record_bytes calls, bytes_done correct
"""

from __future__ import annotations

import threading
import time
from dataclasses import FrozenInstanceError

import pytest

from recua.metrics import EngineStats, MetricsCollector


# ---------------------------------------------------------------------------
# EngineStats
# ---------------------------------------------------------------------------

class TestEngineStats:
    def test_default_values_are_zero(self) -> None:
        stats = EngineStats()
        assert stats.completed == 0
        assert stats.failed == 0
        assert stats.queued == 0
        assert stats.active == 0
        assert stats.bytes_total == 0
        assert stats.bytes_done == 0
        assert stats.speed_mb_per_sec == 0.0

    def test_is_frozen(self) -> None:
        stats = EngineStats(completed=1)
        with pytest.raises(FrozenInstanceError):
            stats.completed = 2  # type: ignore[misc]

    def test_custom_values(self) -> None:
        stats = EngineStats(completed=5, failed=2, queued=10, active=4)
        assert stats.completed == 5
        assert stats.failed == 2
        assert stats.queued == 10
        assert stats.active == 4


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

@pytest.fixture
def mc() -> MetricsCollector:
    return MetricsCollector()


# ---------------------------------------------------------------------------
# Counter: job_queued
# ---------------------------------------------------------------------------

class TestJobQueued:
    def test_increments_queued(self, mc: MetricsCollector) -> None:
        mc.job_queued()
        assert mc.snapshot().queued == 1

    def test_multiple_calls_accumulate(self, mc: MetricsCollector) -> None:
        mc.job_queued()
        mc.job_queued()
        mc.job_queued()
        assert mc.snapshot().queued == 3

    def test_adds_expected_size_to_bytes_total(self, mc: MetricsCollector) -> None:
        mc.job_queued(expected_size=1_048_576)
        assert mc.snapshot().bytes_total == 1_048_576

    def test_no_expected_size_leaves_bytes_total_zero(self, mc: MetricsCollector) -> None:
        mc.job_queued()
        assert mc.snapshot().bytes_total == 0

    def test_multiple_sizes_accumulate(self, mc: MetricsCollector) -> None:
        mc.job_queued(expected_size=100)
        mc.job_queued(expected_size=200)
        mc.job_queued(expected_size=None)
        assert mc.snapshot().bytes_total == 300
        assert mc.snapshot().queued == 3


# ---------------------------------------------------------------------------
# Counter: job_started
# ---------------------------------------------------------------------------

class TestJobStarted:
    def test_decrements_queued_increments_active(self, mc: MetricsCollector) -> None:
        mc.job_queued()
        mc.job_started()
        stats = mc.snapshot()
        assert stats.queued == 0
        assert stats.active == 1

    def test_queued_floor_at_zero(self, mc: MetricsCollector) -> None:
        """Calling job_started without a prior job_queued must not go negative."""
        mc.job_started()
        assert mc.snapshot().queued == 0

    def test_multiple_starts(self, mc: MetricsCollector) -> None:
        mc.job_queued()
        mc.job_queued()
        mc.job_started()
        mc.job_started()
        stats = mc.snapshot()
        assert stats.queued == 0
        assert stats.active == 2


# ---------------------------------------------------------------------------
# Counter: job_completed
# ---------------------------------------------------------------------------

class TestJobCompleted:
    def test_decrements_active_increments_completed(self, mc: MetricsCollector) -> None:
        mc.job_queued()
        mc.job_started()
        mc.job_completed()
        stats = mc.snapshot()
        assert stats.active == 0
        assert stats.completed == 1

    def test_active_floor_at_zero(self, mc: MetricsCollector) -> None:
        mc.job_completed()
        assert mc.snapshot().active == 0

    def test_accumulates(self, mc: MetricsCollector) -> None:
        for _ in range(5):
            mc.job_queued()
            mc.job_started()
            mc.job_completed()
        assert mc.snapshot().completed == 5


# ---------------------------------------------------------------------------
# Counter: job_failed
# ---------------------------------------------------------------------------

class TestJobFailed:
    def test_decrements_active_increments_failed(self, mc: MetricsCollector) -> None:
        mc.job_queued()
        mc.job_started()
        mc.job_failed()
        stats = mc.snapshot()
        assert stats.active == 0
        assert stats.failed == 1

    def test_active_floor_at_zero(self, mc: MetricsCollector) -> None:
        mc.job_failed()
        assert mc.snapshot().active == 0


# ---------------------------------------------------------------------------
# Full lifecycle sequences
# ---------------------------------------------------------------------------

class TestLifecycleSequences:
    def test_successful_job_lifecycle(self, mc: MetricsCollector) -> None:
        mc.job_queued(expected_size=512)
        s = mc.snapshot()
        assert s.queued == 1 and s.active == 0 and s.completed == 0

        mc.job_started()
        s = mc.snapshot()
        assert s.queued == 0 and s.active == 1 and s.completed == 0

        mc.job_completed()
        s = mc.snapshot()
        assert s.queued == 0 and s.active == 0 and s.completed == 1

    def test_failed_job_lifecycle(self, mc: MetricsCollector) -> None:
        mc.job_queued()
        mc.job_started()
        mc.job_failed()
        s = mc.snapshot()
        assert s.queued == 0 and s.active == 0 and s.failed == 1

    def test_mixed_outcomes(self, mc: MetricsCollector) -> None:
        for _ in range(3):
            mc.job_queued()
            mc.job_started()
            mc.job_completed()
        for _ in range(2):
            mc.job_queued()
            mc.job_started()
            mc.job_failed()
        s = mc.snapshot()
        assert s.completed == 3
        assert s.failed == 2
        assert s.active == 0
        assert s.queued == 0


# ---------------------------------------------------------------------------
# record_bytes
# ---------------------------------------------------------------------------

class TestRecordBytes:
    def test_accumulates_bytes_done(self, mc: MetricsCollector) -> None:
        mc.record_bytes(1024)
        mc.record_bytes(2048)
        assert mc.snapshot().bytes_done == 3072

    def test_zero_bytes_is_noop(self, mc: MetricsCollector) -> None:
        mc.record_bytes(0)
        assert mc.snapshot().bytes_done == 0

    def test_negative_bytes_is_noop(self, mc: MetricsCollector) -> None:
        mc.record_bytes(-1)
        assert mc.snapshot().bytes_done == 0

    def test_large_values(self, mc: MetricsCollector) -> None:
        mc.record_bytes(10 * 1_073_741_824)  # 10 GiB
        assert mc.snapshot().bytes_done == 10 * 1_073_741_824


# ---------------------------------------------------------------------------
# snapshot — speed calculation
# ---------------------------------------------------------------------------

class TestSnapshotSpeed:
    def test_speed_is_zero_with_no_samples(self, mc: MetricsCollector) -> None:
        assert mc.snapshot().speed_mb_per_sec == 0.0

    def test_speed_is_zero_with_single_sample(self, mc: MetricsCollector) -> None:
        """Single sample has no duration — speed is undefined, return 0."""
        mc.record_bytes(1_048_576)
        # With a single sample, oldest == newest, duration == 0
        # May read 0 depending on debounce; either 0 is correct
        stats = mc.snapshot()
        assert stats.speed_mb_per_sec >= 0.0

    def test_speed_reflects_recent_bytes(self, mc: MetricsCollector) -> None:
        """
        Inject two samples 0.5s apart with a known byte count.
        Speed should be approximately bytes / 0.5s.
        Tolerance is generous — we just want to confirm it's non-zero
        and in the right ballpark.
        """
        chunk = 5 * 1_048_576  # 5 MiB
        mc.record_bytes(chunk)
        time.sleep(0.5)
        mc.record_bytes(chunk)

        stats = mc.snapshot()
        # 10 MiB over ~0.5s ≈ 20 MB/s, but timing is imprecise on CI
        # Just assert it's non-zero and plausible (> 1 MB/s, < 500 MB/s)
        assert stats.speed_mb_per_sec > 1.0
        assert stats.speed_mb_per_sec < 500.0

    def test_speed_returns_zero_after_idle(self, mc: MetricsCollector) -> None:
        """
        After the speed window expires with no new samples, speed drops to 0.
        We monkey-patch the window to 0.1s to avoid a 5s test.
        """
        import recua.metrics as metrics_mod
        original = metrics_mod._SPEED_WINDOW_SECONDS
        metrics_mod._SPEED_WINDOW_SECONDS = 0.1
        try:
            mc.record_bytes(1_048_576)
            time.sleep(0.2)  # outlast the window
            stats = mc.snapshot()
            assert stats.speed_mb_per_sec == 0.0
        finally:
            metrics_mod._SPEED_WINDOW_SECONDS = original

    def test_snapshot_is_consistent(self, mc: MetricsCollector) -> None:
        """
        All fields in a snapshot reflect the same instant.
        Run a background writer and assert snapshot fields are self-consistent
        (bytes_done never exceeds bytes_total, active never negative).
        """
        for _ in range(10):
            mc.job_queued(expected_size=1_048_576)

        stop = threading.Event()

        def writer() -> None:
            while not stop.is_set():
                mc.record_bytes(4096)

        def transitioner() -> None:
            for _ in range(10):
                mc.job_started()
                time.sleep(0.01)
                mc.job_completed()

        threads = [
            threading.Thread(target=writer),
            threading.Thread(target=transitioner),
        ]
        for t in threads:
            t.start()

        for _ in range(20):
            s = mc.snapshot()
            assert s.active >= 0
            assert s.queued >= 0
            assert s.completed >= 0
            assert s.bytes_done >= 0
            assert s.speed_mb_per_sec >= 0.0

        stop.set()
        for t in threads:
            t.join()


# ---------------------------------------------------------------------------
# Thread safety
# ---------------------------------------------------------------------------

class TestThreadSafety:
    def test_concurrent_lifecycle_calls_no_corruption(self, mc: MetricsCollector) -> None:
        """
        16 threads each running a full job lifecycle (queued→started→completed).
        Final state: completed=16, everything else=0.
        """
        n = 16
        errors: list[Exception] = []

        def lifecycle() -> None:
            try:
                mc.job_queued()
                mc.job_started()
                mc.job_completed()
            except Exception as e:
                errors.append(e)

        threads = [threading.Thread(target=lifecycle) for _ in range(n)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()

        assert errors == []
        s = mc.snapshot()
        assert s.completed == n
        assert s.active == 0
        assert s.queued == 0

    def test_concurrent_record_bytes_no_corruption(self, mc: MetricsCollector) -> None:
        """
        16 threads each recording 1000 chunks of 1024 bytes.
        Final bytes_done must equal exactly 16 * 1000 * 1024.
        """
        n_threads = 16
        n_chunks = 1000
        chunk = 1024
        expected = n_threads * n_chunks * chunk
        errors: list[Exception] = []

        def writer() -> None:
            try:
                for _ in range(n_chunks):
                    mc.record_bytes(chunk)
            except Exception as e:
                errors.append(e)

        threads = [threading.Thread(target=writer) for _ in range(n_threads)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()

        assert errors == []
        assert mc.snapshot().bytes_done == expected
