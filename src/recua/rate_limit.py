"""
Token-bucket rate limiter for aggregate bandwidth control.

Thread-safe. All workers share a single RateLimiter instance.
When max_mb_per_sec is None the limiter is a no-op.
"""

from __future__ import annotations

import threading
import time


class RateLimiter:
    """
    Token-bucket implementation.

    Tokens represent bytes. Workers call consume(n) before writing n bytes.
    If the bucket is empty, consume() sleeps until tokens refill.

    Refill rate: max_bytes_per_sec tokens per second.
    Bucket capacity: max_bytes_per_sec (1-second burst ceiling).
    """

    def __init__(self, max_mb_per_sec: float | None) -> None:
        self._unlimited = max_mb_per_sec is None
        if not self._unlimited:
            self._rate = max_mb_per_sec * 1_048_576  # MB/s → bytes/s
            self._tokens = self._rate
            self._last_refill = time.monotonic()
            self._lock = threading.Lock()

    def consume(self, n_bytes: int) -> None:
        """Block until n_bytes tokens are available, then consume them."""
        if self._unlimited:
            return
        # TODO:
        #   - acquire lock
        #   - refill tokens based on elapsed time since last refill
        #   - if insufficient tokens, sleep for deficit / rate seconds
        #   - consume tokens
        raise NotImplementedError

    @property
    def unlimited(self) -> bool:
        return self._unlimited
