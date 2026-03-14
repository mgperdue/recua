"""
HTTP/HTTPS transfer adapter.

Implements the TransferAdapter protocol using requests with:
  - Range-based resumption
  - Chunked streaming
  - Retry-aware status code handling
"""

from __future__ import annotations

from collections.abc import Iterator

import requests

from recua.job import TransferJob
from recua.protocols import TransferAdapter


class HTTPAdapter(TransferAdapter):
    """Handles https:// and http:// sources."""

    RETRIABLE_STATUS = frozenset({500, 502, 503, 504})
    NON_RETRIABLE_STATUS = frozenset({400, 401, 403, 404, 405, 410})

    def __init__(self, session: requests.Session | None = None) -> None:
        self._session = session or requests.Session()

    def supports(self, source: str) -> bool:
        return source.startswith(("https://", "http://"))

    def get_size(self, source: str) -> int | None:
        """HEAD request to resolve content-length. Returns None if unavailable."""
        # TODO: implement HEAD request, parse Content-Length header
        raise NotImplementedError

    def fetch(self, job: TransferJob, offset: int = 0) -> Iterator[bytes]:
        """
        Stream bytes from source, resuming from offset if non-zero.

        Yields chunks of options.chunk_size bytes.
        Raises RetriableError or FatalTransferError depending on HTTP status.
        """
        # TODO:
        #   - Set Range header if offset > 0
        #   - Stream response in chunks
        #   - Raise appropriate errors for non-2xx status codes
        #   - Handle 429 with Retry-After header respect
        raise NotImplementedError
