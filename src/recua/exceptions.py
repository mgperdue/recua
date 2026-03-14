"""
Public exception hierarchy for recua.

All exceptions raised across the public API are subclasses of
TransferEngineError, giving callers a single catch-all if needed,
while still allowing fine-grained handling.
"""

from __future__ import annotations


class TransferEngineError(Exception):
    """Base class for all recua exceptions."""


# --- Lifecycle errors -------------------------------------------------------


class EngineNotStartedError(TransferEngineError):
    """Raised when submit() is called before start()."""


class EngineShutdownError(TransferEngineError):
    """Raised when submit() is called after close() or cancel()."""


# --- Queue errors ------------------------------------------------------------


class QueueFullError(TransferEngineError):
    """
    Raised by submit(block=False) when the internal queue is at capacity.

    Callers should either retry, apply backpressure to their producer,
    or switch to submit(block=True).
    """


# --- Transfer errors ---------------------------------------------------------


class TransferError(TransferEngineError):
    """Base class for errors that occur during a file transfer."""


class RetriableError(TransferError):
    """
    Transient error — the worker will retry with exponential backoff.

    Examples: 5xx responses, connection resets, timeouts, incomplete reads.
    """


class FatalTransferError(TransferError):
    """
    Permanent error — the worker will not retry.

    Examples: 4xx responses (except 429), unsupported scheme.
    The job will be recorded as permanently failed in StateStore.
    """


class RateLimitError(RetriableError):
    """
    HTTP 429 Too Many Requests.

    Carries retry_after (seconds) parsed from the Retry-After header.
    Worker should sleep retry_after before rescheduling.
    """

    def __init__(self, message: str, retry_after: float | None = None) -> None:
        super().__init__(message)
        self.retry_after = retry_after
