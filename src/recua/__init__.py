"""
recua — embeddable high-performance file transfer library.

Public surface:

    from recua import TransferEngine, TransferJob, TransferOptions
    from recua.exceptions import (
        TransferEngineError,
        QueueFullError,
        EngineNotStartedError,
        EngineShutdownError,
        RetriableError,
        FatalTransferError,
        RateLimitError,
    )

Everything else is internal.
"""

from recua.engine import TransferEngine
from recua.job import TransferJob
from recua.options import TransferOptions

__all__ = [
    "TransferEngine",
    "TransferJob",
    "TransferOptions",
]
