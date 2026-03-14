"""
Token-bucket rate limiter for aggregate bandwidth control.

Thread-safe. All workers share a single RateLimiter instance.
When max_mb_per_sec is None the limiter is a no-op.

Algorithm
---------
Classic token bucket:
  - Bucket holds up to `rate` tokens (bytes), refilled continuously at
    `rate` tokens/second using monotonic wall time.
  - consume(n) waits until n tokens are available, then removes them.

Critical threading detail
-------------------------
We do NOT sleep while holding the lock. The pattern is:

    1. Acquire lock
    2. Refill tokens from elapsed time
    3. Calculate sleep_for = max(0, (deficit / rate))
    4. Release lock
    5. Sleep (other workers run freely during this time)
    6. Re-acquire lock, consume tokens, release

Sleeping inside the lock would serialize all workers behind a single
sleep — effectively making the engine single-threaded under any cap.

Burst behaviour
---------------
Bucket capacity is capped at `rate` (1-second burst ceiling). This
prevents a thundering-herd effect after idle periods where a full bucket
would let workers send a large burst before throttling kicks in.
"""

from __future__ import annotations

import threading
import time


class RateLimiter:
    """
    Token-bucket bandwidth limiter.

    Tokens represent bytes. Workers call consume(n) before writing n bytes
    to disk. If the bucket is empty, consume() sleeps until enough tokens
    have refilled, then proceeds.

    All workers share one RateLimiter instance. The aggregate throughput
    across all workers is bounded by max_mb_per_sec.

    When max_mb_per_sec is None, consume() is a no-op — zero overhead.

    Parameters
    ----------
    max_mb_per_sec:
        Aggregate cap in megabytes per second (MB/s, not Mb/s).
        None means unlimited.

    Example
    -------
    limiter = RateLimiter(max_mb_per_sec=10.0)  # 10 MB/s cap
    for chunk in adapter.fetch(job):
        limiter.consume(len(chunk))
        file.write(chunk)
    """

    def __init__(self, max_mb_per_sec: float | None) -> None:
        self._unlimited = max_mb_per_sec is None
        if not self._unlimited:
            self._rate: float = max_mb_per_sec * 1_048_576  # MB/s → bytes/s
            self._tokens: float = 0.0                       # start empty — no burst on first call
            self._last_refill: float = time.monotonic()
            self._lock = threading.Lock()

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def consume(self, n_bytes: int) -> None:
        """
        Block until n_bytes tokens are available, then consume them.

        Safe to call from multiple threads simultaneously.
        Returns immediately when the limiter is unlimited.
        """
        if self._unlimited:
            return

        while True:
            with self._lock:
                self._refill()

                if self._tokens >= n_bytes:
                    # Enough tokens — consume and return immediately.
                    self._tokens -= n_bytes
                    return

                # Insufficient tokens: calculate how long to wait, then
                # release the lock before sleeping so other workers aren't
                # blocked during our sleep.
                deficit = n_bytes - self._tokens
                sleep_for = deficit / self._rate

            # Sleep outside the lock. Other workers can consume tokens
            # or refill concurrently during this window.
            time.sleep(sleep_for)

            # Loop back to re-check — another worker may have consumed
            # tokens during our sleep, so we verify before consuming.

    @property
    def unlimited(self) -> bool:
        """True if no rate cap is configured."""
        return self._unlimited

    @property
    def rate_bytes_per_sec(self) -> float | None:
        """Current rate cap in bytes/sec, or None if unlimited."""
        return None if self._unlimited else self._rate

    # ------------------------------------------------------------------
    # Internal
    # ------------------------------------------------------------------

    def _refill(self) -> None:
        """
        Add tokens earned since the last refill call.

        Must be called under self._lock.
        Bucket is capped at self._rate (1-second burst ceiling).
        """
        now = time.monotonic()
        elapsed = now - self._last_refill
        self._tokens = min(self._rate, self._tokens + elapsed * self._rate)
        self._last_refill = now
