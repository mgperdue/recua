"""
TransferOptions — global behavioral configuration for the engine.
"""

from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Literal

# Supported checksum algorithms — subset of hashlib guaranteed available
# across all Python 3.11+ platforms.
ChecksumAlgorithm = Literal["md5", "sha1", "sha256", "sha512"]


@dataclass
class TransferOptions:
    """
    Knobs that control engine-wide behavior.

    Concurrency
    -----------
    max_workers:    Number of parallel download threads.
    queue_size:     Max jobs held in the internal queue before submit() blocks.
                    Acts as backpressure against fast producers.

    Performance
    -----------
    max_mb_per_sec: Aggregate bandwidth cap in MB/s (megabytes, not megabits).
                    None = unlimited. Enforced via token-bucket RateLimiter.
    chunk_size:     Streaming read size in bytes per iteration.

    Reliability
    -----------
    retries:        Max retry attempts per job before marking as failed.
    backoff_base:   Multiplier for exponential backoff.
                    Delay after attempt n = backoff_base ** n seconds.

    Persistence
    -----------
    state_path:     Path to SQLite state database.
                    None disables persistence (no resume across restarts).

    Integrity
    ---------
    checksum_algorithm:
                    Hash algorithm used for post-download verification.
                    None disables checksum verification.
                    TransferJob.expected_checksum must be set for verification
                    to occur — jobs without expected_checksum are skipped.
                    Supported: "md5", "sha1", "sha256", "sha512".

    Callbacks
    ---------
    on_complete:    Called in the worker thread when a job finishes successfully.
    on_error:       Called in the worker thread when a job exhausts all retries.
    on_progress:    Called periodically during transfer.
                    Args: (job, bytes_done, bytes_total_or_None)

    UX
    --
    progress:       Show a rich progress display in the terminal.
                    Requires the `rich` extra: pip install recua[progress].
                    Set False for daemon/embedded use where there is no TTY.
    """

    # concurrency
    max_workers: int = 4
    queue_size: int = 1000

    # performance
    max_mb_per_sec: float | None = None
    chunk_size: int = 1_048_576  # 1 MiB

    # reliability
    retries: int = 5
    backoff_base: float = 1.5

    # persistence
    state_path: Path | None = None

    # integrity
    checksum_algorithm: ChecksumAlgorithm | None = None

    # callbacks
    on_complete: Callable[[Any], None] | None = None
    on_error: Callable[[Any, Exception], None] | None = None
    on_progress: Callable[[Any, int, int | None], None] | None = None

    # ux
    progress: bool = True
