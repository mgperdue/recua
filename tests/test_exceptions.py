"""Tests for the exception hierarchy."""
from recua.exceptions import (
    EngineNotStartedError,
    EngineShutdownError,
    FatalTransferError,
    QueueFullError,
    RateLimitError,
    RetriableError,
    TransferEngineError,
)


def test_all_errors_inherit_from_base():
    for exc_class in [
        QueueFullError,
        EngineNotStartedError,
        EngineShutdownError,
        RetriableError,
        FatalTransferError,
        RateLimitError,
    ]:
        assert issubclass(exc_class, TransferEngineError)


def test_rate_limit_error_carries_retry_after():
    err = RateLimitError("too many requests", retry_after=30.0)
    assert err.retry_after == 30.0


def test_rate_limit_error_retry_after_optional():
    err = RateLimitError("too many requests")
    assert err.retry_after is None
