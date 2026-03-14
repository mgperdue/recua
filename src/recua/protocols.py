"""
Structural protocols (PEP 544) for recua extension points.

Defines the interfaces that all pluggable components must satisfy.
No concrete implementations live here.
"""

from __future__ import annotations

from collections.abc import Iterator
from typing import Protocol, runtime_checkable

from recua.job import TransferJob

_DEFAULT_CHUNK_SIZE = 1_048_576  # 1 MiB — matches TransferOptions default


@runtime_checkable
class TransferAdapter(Protocol):
    """
    Protocol for pluggable transfer backends.

    Implementations must be thread-safe — multiple workers call
    fetch() concurrently on the same adapter instance.

    Provided adapters (v1):   HTTPAdapter
    Planned:                  S3Adapter, FileAdapter
    """

    def supports(self, source: str) -> bool:
        """Return True if this adapter can handle the given source URI."""
        ...

    def get_size(self, source: str) -> int | None:
        """
        Return the expected byte size of source, or None if unavailable.

        Should not raise — return None on any uncertainty.
        """
        ...

    def fetch(
        self,
        job: TransferJob,
        offset: int = 0,
        chunk_size: int = _DEFAULT_CHUNK_SIZE,
    ) -> Iterator[bytes]:
        """
        Yield chunks of bytes for the given job, starting at offset.

        Parameters
        ----------
        job:
            The job to fetch.
        offset:
            Byte offset to resume from. Adapter must honour it (e.g. via
            HTTP Range header) or raise FatalTransferError if unsupported.
        chunk_size:
            Read buffer size in bytes. Passed from TransferOptions.chunk_size.

        Yields
        ------
        bytes
            Raw content chunks. Never yields empty bytes.

        Raises
        ------
        RetriableError:     transient failure, worker will retry
        FatalTransferError: permanent failure, worker will not retry
        RateLimitError:     429-style, carries retry_after in seconds
        """
        ...


@runtime_checkable
class StateStore(Protocol):
    """
    Protocol for persistent transfer state backends.

    The default implementation is SQLiteStateStore.
    All methods must be thread-safe.
    """

    def get_offset(self, resume_key: tuple[str, str]) -> int:
        """Return bytes already written for this job. 0 if unknown."""
        ...

    def set_offset(self, resume_key: tuple[str, str], offset: int) -> None:
        """Update the byte offset for an in-progress transfer."""
        ...

    def mark_complete(self, resume_key: tuple[str, str]) -> None:
        """Record a job as successfully completed."""
        ...

    def mark_failed(self, resume_key: tuple[str, str], reason: str) -> None:
        """Record a job as permanently failed."""
        ...

    def is_complete(self, resume_key: tuple[str, str]) -> bool:
        """Return True if this job was previously completed successfully."""
        ...
