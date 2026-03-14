"""
HTTP/HTTPS transfer adapter.

Implements the TransferAdapter protocol using requests with:
  - Range-based resumption (RFC 7233)
  - Chunked streaming
  - Retry-aware status code classification
  - Retry-After header parsing for 429 responses
  - Per-thread sessions for thread safety

Thread safety
-------------
requests.Session is not thread-safe for concurrent use. Since multiple
workers share one HTTPAdapter instance, each thread gets its own Session
via threading.local(). Sessions are created lazily on first use per thread
and reused across calls (connection pooling benefit retained).

Resume semantics
----------------
When offset > 0 we send `Range: bytes=<offset>-`. A correctly behaving
server returns 206 Partial Content and the body starts at the offset.
Some servers ignore the Range header and return 200 OK with the full body.
We cannot safely append to a partially written file in this case, so we
raise RetriableError to trigger a clean retry from offset 0. The worker
is responsible for truncating the destination file on retry.

Status code policy
------------------
Retriable (transient — worker will retry with backoff):
    500, 502, 503, 504 — server-side transient errors
    requests.ConnectionError, requests.Timeout — network errors
    ChunkedEncodingError — stream interrupted mid-transfer

Rate-limited (special retriable):
    429 — respect Retry-After header if present

Fatal (permanent — worker will not retry):
    400, 401, 403, 404, 405, 410 — permanent client/resource errors
    Any other non-2xx not listed above defaults to fatal.
"""

from __future__ import annotations

import threading
from collections.abc import Iterator

import requests
import requests.adapters

from recua.exceptions import FatalTransferError, RateLimitError, RetriableError
from recua.job import TransferJob

_DEFAULT_CHUNK_SIZE = 1_048_576  # 1 MiB — matches TransferOptions default

# Status codes that indicate a transient server problem worth retrying.
_RETRIABLE_STATUS = frozenset({500, 502, 503, 504})

# Status codes that indicate a permanent problem — no point retrying.
_FATAL_STATUS = frozenset({400, 401, 403, 404, 405, 410})

# Connection pool config per thread.
_POOL_CONNECTIONS = 4
_POOL_MAXSIZE = 8


class HTTPAdapter:
    """
    Transfer adapter for http:// and https:// sources.

    Instantiate once and share across all workers — thread-safe via
    per-thread Session objects.

    Parameters
    ----------
    connect_timeout:
        Seconds to wait for the TCP connection to be established.
    read_timeout:
        Seconds to wait for the server to send the first byte of the
        response body. Does not cap total transfer time.
    """

    def __init__(
        self,
        connect_timeout: float = 10.0,
        read_timeout: float = 30.0,
    ) -> None:
        self._connect_timeout = connect_timeout
        self._read_timeout = read_timeout
        self._local = threading.local()

    # ------------------------------------------------------------------
    # TransferAdapter protocol
    # ------------------------------------------------------------------

    def supports(self, source: str) -> bool:
        """Return True for http:// and https:// URIs."""
        return source.startswith(("https://", "http://"))

    def get_size(self, source: str) -> int | None:
        """
        Issue a HEAD request and return Content-Length in bytes.

        Returns None if:
          - the server does not include a Content-Length header
          - the request fails for any reason
          - the header value is not a valid integer

        Never raises — callers treat None as "size unknown".
        """
        try:
            response = self._session.head(
                source,
                timeout=(self._connect_timeout, self._read_timeout),
                allow_redirects=True,
            )
            response.raise_for_status()
            raw = response.headers.get("Content-Length")
            if raw is not None:
                return int(raw)
        except Exception:
            pass
        return None

    def fetch(
        self,
        job: TransferJob,
        offset: int = 0,
        chunk_size: int = _DEFAULT_CHUNK_SIZE,
    ) -> Iterator[bytes]:
        """
        Stream bytes from job.source, starting at offset.

        Yields non-empty bytes chunks of up to chunk_size bytes each.

        Parameters
        ----------
        job:
            The job to fetch. job.source must be an http/https URL.
        offset:
            Byte offset to resume from. 0 means start from the beginning.
        chunk_size:
            Read buffer size in bytes. Pass TransferOptions.chunk_size here.

        Yields
        ------
        bytes
            Raw content chunks. Never yields empty bytes.

        Raises
        ------
        RetriableError:
            5xx responses, connection errors, timeouts, stream interruptions,
            or a server that returned 200 OK despite a Range request.
        RateLimitError:
            HTTP 429. Carries retry_after parsed from Retry-After header.
        FatalTransferError:
            4xx responses (except 429), or any unexpected non-2xx status.
        """
        headers: dict[str, str] = {}
        if offset > 0:
            headers["Range"] = f"bytes={offset}-"

        try:
            response = self._session.get(
                job.source,
                headers=headers,
                stream=True,
                timeout=(self._connect_timeout, self._read_timeout),
                allow_redirects=True,
            )
        except requests.Timeout as exc:
            raise RetriableError(
                f"Request timed out for {job.source!r}"
            ) from exc
        except (requests.ConnectionError, requests.exceptions.ChunkedEncodingError) as exc:
            # ConnectionError covers network-level failures.
            # ChunkedEncodingError can fire at request time (before iter_content)
            # when the server sends a malformed chunked response on first contact.
            raise RetriableError(
                f"Connection error for {job.source!r}: {exc}"
            ) from exc

        self._raise_for_status(response, job)

        # If we requested a Range but the server returned 200 (ignored Range),
        # the body starts at byte 0. We cannot safely append to a partial file,
        # so raise RetriableError — the worker will retry from offset 0.
        if offset > 0 and response.status_code == 200:
            response.close()
            raise RetriableError(
                f"Server returned 200 instead of 206 for Range request on "
                f"{job.source!r} — server does not support resume. "
                f"Retrying from byte 0."
            )

        try:
            for chunk in response.iter_content(chunk_size=chunk_size):
                if chunk:  # filter keep-alive empty chunks
                    yield chunk
        except requests.ChunkedEncodingError as exc:
            raise RetriableError(
                f"Stream interrupted for {job.source!r}: {exc}"
            ) from exc
        except requests.ConnectionError as exc:
            raise RetriableError(
                f"Connection lost during download of {job.source!r}: {exc}"
            ) from exc
        finally:
            response.close()

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    @property
    def _session(self) -> requests.Session:
        """
        Return the Session for the current thread, creating it lazily.

        Each thread gets its own Session with a configured connection pool.
        Sessions are never shared across threads.
        """
        session = getattr(self._local, "session", None)
        if session is None:
            session = requests.Session()
            http_adapter = requests.adapters.HTTPAdapter(
                pool_connections=_POOL_CONNECTIONS,
                pool_maxsize=_POOL_MAXSIZE,
                max_retries=0,  # recua handles retries, not requests
            )
            session.mount("https://", http_adapter)
            session.mount("http://", http_adapter)
            self._local.session = session
        return session

    def _raise_for_status(
        self,
        response: requests.Response,
        job: TransferJob,
    ) -> None:
        """
        Translate HTTP status codes into recua exceptions.

        2xx — no-op, caller proceeds to stream body.
        429 — RateLimitError with parsed retry_after.
        5xx in _RETRIABLE_STATUS — RetriableError.
        4xx in _FATAL_STATUS — FatalTransferError.
        Anything else non-2xx — FatalTransferError.
        """
        code = response.status_code

        if response.ok:  # 2xx
            return

        if code == 429:
            retry_after = self._parse_retry_after(response)
            raise RateLimitError(
                f"HTTP 429 Too Many Requests for {job.source!r}",
                retry_after=retry_after,
            )

        if code in _RETRIABLE_STATUS:
            raise RetriableError(
                f"HTTP {code} from {job.source!r} — transient server error"
            )

        if code in _FATAL_STATUS:
            raise FatalTransferError(
                f"HTTP {code} from {job.source!r} — permanent error, will not retry"
            )

        # Any other non-2xx (redirects not followed, 418, etc.)
        raise FatalTransferError(
            f"HTTP {code} from {job.source!r} — unexpected status"
        )

    @staticmethod
    def _parse_retry_after(response: requests.Response) -> float | None:
        """
        Parse the Retry-After header into seconds as a float.

        Handles the integer-seconds form only (e.g. "Retry-After: 30").
        HTTP-date form is not parsed — returns None, and the caller falls
        back to default exponential backoff.

        Returns None if the header is absent or unparseable.
        """
        raw = response.headers.get("Retry-After")
        if raw is None:
            return None
        try:
            return float(raw)
        except ValueError:
            return None
