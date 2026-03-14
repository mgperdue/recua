"""
TransferJob — immutable descriptor for a single unit of transfer work.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path
from typing import Any


@dataclass(frozen=True)
class TransferJob:
    """
    Describes one file to be transferred.

    Attributes
    ----------
    source:
        URL or URI of the remote resource.
        Supported schemes (v1): https://, http://
        Planned: s3://, file://
    dest:
        Absolute or relative path for the local output file.
        Parent directories will be created if absent.
    name:
        Human-readable label used in progress display and logs.
        Defaults to the basename of dest if not provided.
    expected_size:
        Content-Length in bytes if known ahead of time.
        Used by the progress display to show a completion percentage
        without issuing a HEAD request.
    expected_checksum:
        Hex-encoded expected digest of the completed file.
        Verified after download if TransferOptions.checksum_algorithm
        is set. If verification fails the job is retried (the partial
        file may be corrupt) and eventually marked failed.
        Example: "a3f5..." for sha256.
    priority:
        Lower value = higher priority (min-heap convention).
        Default 0. Jobs with equal priority are FIFO-ordered.
    meta:
        Arbitrary caller-supplied key/value data.
        Passed through to callbacks unchanged.
        Treat as read-only after submission — the dataclass is frozen
        but dict contents are not enforced immutable.

    Notes on resume_key
    -------------------
    The StateStore identifies a resumable transfer by the composite key
    (source, str(dest.resolve())) — NOT source alone, since the same URL
    may legally be written to multiple destinations.
    """

    source: str
    dest: Path
    name: str | None = None
    expected_size: int | None = None
    expected_checksum: str | None = None
    priority: int = 0
    meta: dict[str, Any] = field(default_factory=dict)

    @property
    def resume_key(self) -> tuple[str, str]:
        """Stable identity key used by StateStore for resumption tracking."""
        return (self.source, str(self.dest.resolve()))

    @property
    def display_name(self) -> str:
        """Label for progress bars and log lines."""
        return self.name or self.dest.name
