"""
recua — embeddable high-performance file transfer library.

Quick start:

    from recua import TransferEngine, TransferJob, TransferOptions

    with TransferEngine(TransferOptions(max_workers=4)) as engine:
        engine.submit_many(
            TransferJob(source=url, dest=out / Path(url).name)
            for url in urls
        )

Public surface
--------------
Core:
    TransferEngine      lifecycle manager and job queue
    TransferJob         immutable job descriptor
    TransferOptions     configuration dataclass
    EngineStats         metrics snapshot returned by engine.stats()

Exceptions (also importable from recua.exceptions):
    TransferEngineError     base class for all recua exceptions
    EngineNotStartedError   submit() before start()
    EngineShutdownError     submit() after close() / cancel()
    QueueFullError          queue at capacity on non-blocking submit
    TransferError           base class for transfer-level errors
    RetriableError          transient failure — worker will retry
    FatalTransferError      permanent failure — worker will not retry
    RateLimitError          HTTP 429 — carries retry_after in seconds

Everything else is internal.
"""

from recua.engine import TransferEngine
from recua.exceptions import (
    EngineNotStartedError,
    EngineShutdownError,
    FatalTransferError,
    QueueFullError,
    RateLimitError,
    RetriableError,
    TransferEngineError,
    TransferError,
)
from recua.job import TransferJob
from recua.metrics import EngineStats
from recua.options import TransferOptions

__all__ = [
    # Core
    "TransferEngine",
    "TransferJob",
    "TransferOptions",
    "EngineStats",
    # Exceptions
    "TransferEngineError",
    "EngineNotStartedError",
    "EngineShutdownError",
    "QueueFullError",
    "TransferError",
    "RetriableError",
    "FatalTransferError",
    "RateLimitError",
]
