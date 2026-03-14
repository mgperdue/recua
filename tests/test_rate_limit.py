"""
Tests for RateLimiter.

Coverage targets
----------------
- Unlimited mode: consume() is a no-op, no attributes initialised
- Construction: MB/s → bytes/s conversion, full bucket on init
- consume() basic: tokens decremented correctly
- consume() blocking: throughput bounded under cap
- consume() burst ceiling: idle period doesn't allow unbounded burst
- Thread safety: N workers, aggregate throughput stays within cap
- Properties: unlimited, rate_bytes_per_sec

Testing approach for timing-sensitive tests
--------------------------------------------
We avoid asserting exact throughput — that's fragile on loaded CI runners.
Instead we assert:
  - actual_rate < cap * tolerance_factor   (cap is respected)
  - actual_rate > cap * lower_floor        (limiter isn't absurdly slow)

Tolerance is deliberately generous (2x) so tests don't flap.
"""

from __future__ import annotations

import threading
import time

import pytest

from recua.rate_limit import RateLimiter


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _measure_throughput(
    limiter: RateLimiter,
    chunk_size: int,
    n_chunks: int,
) -> float:
    """Return bytes/sec achieved by consuming n_chunks of chunk_size."""
    start = time.monotonic()
    for _ in range(n_chunks):
        limiter.consume(chunk_size)
    elapsed = time.monotonic() - start
    return (chunk_size * n_chunks) / elapsed


# ---------------------------------------------------------------------------
# Unlimited mode
# ---------------------------------------------------------------------------

class TestUnlimitedMode:
    def test_unlimited_flag_is_true(self) -> None:
        limiter = RateLimiter(None)
        assert limiter.unlimited is True

    def test_rate_bytes_per_sec_is_none(self) -> None:
        limiter = RateLimiter(None)
        assert limiter.rate_bytes_per_sec is None

    def test_consume_returns_immediately(self) -> None:
        limiter = RateLimiter(None)
        start = time.monotonic()
        for _ in range(1000):
            limiter.consume(1_048_576)  # 1 MiB per call
        elapsed = time.monotonic() - start
        # 1000 calls should complete in well under 1 second — no sleeping
        assert elapsed < 0.5

    def test_consume_large_chunk_returns_immediately(self) -> None:
        limiter = RateLimiter(None)
        start = time.monotonic()
        limiter.consume(10 * 1_073_741_824)  # 10 GiB nominal
        elapsed = time.monotonic() - start
        assert elapsed < 0.01


# ---------------------------------------------------------------------------
# Construction
# ---------------------------------------------------------------------------

class TestConstruction:
    def test_unlimited_flag_is_false_when_capped(self) -> None:
        limiter = RateLimiter(1.0)
        assert limiter.unlimited is False

    def test_rate_bytes_per_sec_conversion(self) -> None:
        limiter = RateLimiter(10.0)
        # 10 MB/s = 10 * 1_048_576 bytes/s
        assert limiter.rate_bytes_per_sec == pytest.approx(10 * 1_048_576)

    def test_fractional_mb_per_sec(self) -> None:
        limiter = RateLimiter(0.5)
        assert limiter.rate_bytes_per_sec == pytest.approx(0.5 * 1_048_576)


# ---------------------------------------------------------------------------
# consume() — token accounting (no timing dependency)
# ---------------------------------------------------------------------------

class TestConsumeTokenAccounting:
    def test_consume_zero_bytes_is_noop(self) -> None:
        """consume(0) should not raise or block."""
        limiter = RateLimiter(1.0)
        limiter.consume(0)  # must return immediately

    def test_consume_within_bucket_does_not_sleep(self) -> None:
        """
        After the bucket fills (1s idle), consuming less than the full
        bucket should return without sleeping.
        The bucket starts empty, so we wait 1s for it to fill first.
        """
        limiter = RateLimiter(10.0)  # 10 MB/s → 10 MiB bucket
        time.sleep(1.1)              # let the bucket fill to its 1s ceiling
        chunk = 1 * 1_048_576        # 1 MiB — well within the 10 MiB bucket
        start = time.monotonic()
        limiter.consume(chunk)
        elapsed = time.monotonic() - start
        assert elapsed < 0.05        # no sleep should occur

    def test_full_bucket_consumed_then_refills(self) -> None:
        """
        Exhaust the bucket, wait 0.5s, then consume again.
        The second consume should succeed without a full 1s wait.
        """
        rate_mb = 2.0
        limiter = RateLimiter(rate_mb)
        rate_bytes = rate_mb * 1_048_576

        # Drain the bucket completely
        limiter.consume(int(rate_bytes))

        # Wait for half a bucket to refill
        time.sleep(0.5)

        # Should be able to consume half a bucket's worth immediately
        half_bucket = int(rate_bytes * 0.4)  # conservative: 40% of bucket
        start = time.monotonic()
        limiter.consume(half_bucket)
        elapsed = time.monotonic() - start
        assert elapsed < 0.2  # should not need to sleep long


# ---------------------------------------------------------------------------
# consume() — throughput cap (timing-based, generous tolerance)
# ---------------------------------------------------------------------------

class TestThroughputCap:
    def test_throughput_bounded_by_cap(self) -> None:
        """
        Actual throughput must not significantly exceed the cap.
        We use a 2x tolerance to avoid CI flakiness.
        """
        cap_mb = 5.0
        cap_bytes = cap_mb * 1_048_576
        limiter = RateLimiter(cap_mb)

        chunk = 256 * 1024       # 256 KiB chunks
        n_chunks = 30            # 30 * 256 KiB = 7.5 MiB total
        # At 5 MB/s this should take ~1.5s; well measurable

        actual = _measure_throughput(limiter, chunk, n_chunks)
        assert actual < cap_bytes * 2.0, (
            f"Throughput {actual/1e6:.2f} MB/s exceeded 2x cap of {cap_mb} MB/s"
        )

    def test_throughput_not_absurdly_slow(self) -> None:
        """
        The limiter must not throttle to well below the cap.
        Floor: actual >= cap * 0.3 (generous — we just want to catch broken logic).
        """
        cap_mb = 5.0
        cap_bytes = cap_mb * 1_048_576
        limiter = RateLimiter(cap_mb)

        chunk = 256 * 1024
        n_chunks = 20

        actual = _measure_throughput(limiter, chunk, n_chunks)
        assert actual > cap_bytes * 0.3, (
            f"Throughput {actual/1e6:.2f} MB/s is far below cap of {cap_mb} MB/s"
        )


# ---------------------------------------------------------------------------
# Burst ceiling
# ---------------------------------------------------------------------------

class TestBurstCeiling:
    def test_idle_period_does_not_allow_unbounded_burst(self) -> None:
        """
        After a long idle, the bucket should be capped at 1 second of tokens
        — not the full idle duration. We verify by:
          1. Waiting 1.5s so the bucket fills to its 1s ceiling.
          2. Consuming 1 full bucket — should return immediately.
          3. Consuming another full bucket — must wait ~1s for refill.

        The bucket starts empty, so we must wait for it to fill before
        the first consume can proceed without sleeping.
        """
        cap_mb = 2.0
        cap_bytes = cap_mb * 1_048_576
        limiter = RateLimiter(cap_mb)

        # Wait long enough that the bucket would overflow if uncapped (1.5s > 1s ceiling).
        # With the ceiling, bucket tops out at exactly 1s worth of tokens.
        time.sleep(1.5)

        # First consume: bucket is full (1s ceiling), should return quickly.
        start = time.monotonic()
        limiter.consume(int(cap_bytes))
        elapsed = time.monotonic() - start
        assert elapsed < 0.3, f"First consume took {elapsed:.2f}s — bucket should have been full"

        # Second consume: bucket just emptied, must refill ~1s worth.
        start = time.monotonic()
        limiter.consume(int(cap_bytes))
        elapsed = time.monotonic() - start
        assert elapsed > 0.5, f"Expected ~1s wait for empty bucket, got {elapsed:.2f}s"


# ---------------------------------------------------------------------------
# Thread safety
# ---------------------------------------------------------------------------

class TestThreadSafety:
    def test_concurrent_consumers_no_errors(self) -> None:
        """
        4 threads each calling consume() 20 times must not raise.
        We don't assert timing here — just absence of errors/corruption.
        """
        limiter = RateLimiter(20.0)  # generous cap so test runs quickly
        errors: list[Exception] = []
        chunk = 128 * 1024  # 128 KiB

        def worker() -> None:
            try:
                for _ in range(20):
                    limiter.consume(chunk)
            except Exception as e:
                errors.append(e)

        threads = [threading.Thread(target=worker) for _ in range(4)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()

        assert errors == [], f"Thread errors: {errors}"

    def test_aggregate_throughput_bounded_across_threads(self) -> None:
        """
        4 concurrent workers sharing one limiter — aggregate throughput
        must not exceed 2x the cap (generous CI tolerance).
        """
        cap_mb = 4.0
        cap_bytes = cap_mb * 1_048_576
        limiter = RateLimiter(cap_mb)

        chunk = 128 * 1024  # 128 KiB
        n_chunks_per_thread = 16
        total_bytes = chunk * n_chunks_per_thread * 4

        start = time.monotonic()
        threads = [
            threading.Thread(
                target=lambda: [limiter.consume(chunk) for _ in range(n_chunks_per_thread)]
            )
            for _ in range(4)
        ]
        for t in threads:
            t.start()
        for t in threads:
            t.join()
        elapsed = time.monotonic() - start

        actual = total_bytes / elapsed
        assert actual < cap_bytes * 2.0, (
            f"Aggregate throughput {actual/1e6:.2f} MB/s exceeded 2x cap {cap_mb} MB/s"
        )
