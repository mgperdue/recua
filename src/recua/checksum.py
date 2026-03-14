"""
Post-download checksum verification.

Computes a hash of a completed file and compares it against the
expected digest stored in TransferJob.expected_checksum.

Used by workers after a successful transfer when:
  - TransferOptions.checksum_algorithm is set
  - TransferJob.expected_checksum is not None

On mismatch, raises RetriableError so the worker retries from scratch.
The partial/corrupt file is truncated before retry.

Design notes
------------
Hashing is done in a streaming fashion (chunk by chunk) to avoid loading
multi-GB files into memory. We reuse options.chunk_size as the read buffer
so memory usage is bounded and consistent with the download path.

The hex digest comparison is case-insensitive to accept both uppercase
(common in .sha256 sidecar files) and lowercase (Python's default).
"""

from __future__ import annotations

import hashlib
import logging
from pathlib import Path

from recua.exceptions import FatalTransferError, RetriableError

logger = logging.getLogger(__name__)

# Read buffer for hashing — 4 MiB balances throughput vs memory
_HASH_CHUNK_SIZE = 4 * 1_048_576


def compute_checksum(path: Path, algorithm: str, chunk_size: int = _HASH_CHUNK_SIZE) -> str:
    """
    Compute the hex digest of a file using the given algorithm.

    Parameters
    ----------
    path:
        Path to the file to hash.
    algorithm:
        Hash algorithm name. Must be supported by hashlib
        (e.g. "sha256", "sha512", "md5").
    chunk_size:
        Read buffer size in bytes.

    Returns
    -------
    str
        Lowercase hex digest string.

    Raises
    ------
    FatalTransferError
        If the algorithm is not supported by hashlib.
    OSError
        If the file cannot be read (propagated to caller).
    """
    try:
        h = hashlib.new(algorithm)
    except ValueError as exc:
        raise FatalTransferError(f"Unsupported checksum algorithm {algorithm!r}: {exc}") from exc

    with open(path, "rb") as fh:
        while True:
            chunk = fh.read(chunk_size)
            if not chunk:
                break
            h.update(chunk)

    return h.hexdigest()


def verify_checksum(
    path: Path,
    expected: str,
    algorithm: str,
    display_name: str = "",
) -> None:
    """
    Verify that a file matches its expected checksum.

    Parameters
    ----------
    path:
        Path to the completed download file.
    expected:
        Expected hex digest (case-insensitive).
    algorithm:
        Hash algorithm name.
    display_name:
        Human-readable job name for log messages.

    Raises
    ------
    RetriableError
        If the computed digest does not match expected.
        The worker should truncate the file and retry from byte 0.
    FatalTransferError
        If the algorithm is unsupported.
    """
    actual = compute_checksum(path, algorithm)
    if actual.lower() != expected.lower():
        raise RetriableError(
            f"Checksum mismatch for {display_name or path.name}: "
            f"expected {expected.lower()[:16]}… "
            f"got {actual[:16]}… "
            f"(algorithm={algorithm})"
        )
    logger.debug("Checksum OK for %s (%s)", display_name or path.name, algorithm)
