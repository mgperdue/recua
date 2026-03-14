"""
Tests for HTTPAdapter.

Uses the `responses` library to mock HTTP interactions without real network
calls. All tests are fully offline.

Coverage targets
----------------
supports():
  - http:// and https:// return True
  - other schemes return False

get_size():
  - returns Content-Length as int on 200
  - returns None when header absent
  - returns None on non-2xx response
  - returns None on connection error
  - returns None on invalid header value

fetch() — happy path:
  - yields all chunks from a 200 response
  - yields correct bytes in order
  - empty keep-alive chunks are filtered

fetch() — resume (Range):
  - sends Range header when offset > 0
  - no Range header when offset == 0
  - yields chunks from 206 response
  - raises RetriableError when 200 returned for Range request

fetch() — status code handling:
  - 429 raises RateLimitError
  - 429 with Retry-After header populates retry_after
  - 429 with HTTP-date Retry-After returns None retry_after
  - 500 raises RetriableError
  - 502 raises RetriableError
  - 503 raises RetriableError
  - 504 raises RetriableError
  - 400 raises FatalTransferError
  - 401 raises FatalTransferError
  - 403 raises FatalTransferError
  - 404 raises FatalTransferError
  - 418 raises FatalTransferError (unlisted non-2xx)

fetch() — network errors:
  - requests.Timeout raises RetriableError
  - requests.ConnectionError raises RetriableError
  - ChunkedEncodingError mid-stream raises RetriableError

fetch() — chunk_size:
  - custom chunk_size is respected

Thread safety:
  - each thread gets an independent session
"""

from __future__ import annotations

import threading
from pathlib import Path

import pytest
import requests
import responses as responses_lib

from recua.adapters.http import HTTPAdapter
from recua.exceptions import FatalTransferError, RateLimitError, RetriableError
from recua.job import TransferJob

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _job(url: str = "https://example.com/file.bin") -> TransferJob:
    return TransferJob(source=url, dest=Path("/tmp/file.bin"), name="file.bin")


def _adapter() -> HTTPAdapter:
    return HTTPAdapter(connect_timeout=5.0, read_timeout=10.0)


# ---------------------------------------------------------------------------
# supports()
# ---------------------------------------------------------------------------


class TestSupports:
    def test_https_is_supported(self) -> None:
        assert _adapter().supports("https://example.com/file.bin")

    def test_http_is_supported(self) -> None:
        assert _adapter().supports("http://example.com/file.bin")

    def test_s3_is_not_supported(self) -> None:
        assert not _adapter().supports("s3://bucket/key")

    def test_file_is_not_supported(self) -> None:
        assert not _adapter().supports("file:///tmp/file.bin")

    def test_empty_string_is_not_supported(self) -> None:
        assert not _adapter().supports("")


# ---------------------------------------------------------------------------
# get_size()
# ---------------------------------------------------------------------------


class TestGetSize:
    @responses_lib.activate
    def test_returns_content_length(self) -> None:
        responses_lib.add(
            responses_lib.HEAD,
            "https://example.com/file.bin",
            headers={"Content-Length": "1048576"},
            status=200,
        )
        assert _adapter().get_size("https://example.com/file.bin") == 1_048_576

    @responses_lib.activate
    def test_returns_none_when_header_absent(self) -> None:
        responses_lib.add(
            responses_lib.HEAD,
            "https://example.com/file.bin",
            status=200,
        )
        assert _adapter().get_size("https://example.com/file.bin") is None

    @responses_lib.activate
    def test_returns_none_on_non_2xx(self) -> None:
        responses_lib.add(
            responses_lib.HEAD,
            "https://example.com/file.bin",
            status=404,
        )
        assert _adapter().get_size("https://example.com/file.bin") is None

    @responses_lib.activate
    def test_returns_none_on_connection_error(self) -> None:
        responses_lib.add(
            responses_lib.HEAD,
            "https://example.com/file.bin",
            body=requests.ConnectionError("network down"),
        )
        assert _adapter().get_size("https://example.com/file.bin") is None

    @responses_lib.activate
    def test_returns_none_on_invalid_header_value(self) -> None:
        responses_lib.add(
            responses_lib.HEAD,
            "https://example.com/file.bin",
            headers={"Content-Length": "not-a-number"},
            status=200,
        )
        assert _adapter().get_size("https://example.com/file.bin") is None


# ---------------------------------------------------------------------------
# fetch() — happy path
# ---------------------------------------------------------------------------


class TestFetchHappyPath:
    @responses_lib.activate
    def test_yields_all_chunks(self) -> None:
        body = b"hello world" * 100
        responses_lib.add(
            responses_lib.GET,
            "https://example.com/file.bin",
            body=body,
            status=200,
        )
        adapter = _adapter()
        job = _job()
        received = b"".join(adapter.fetch(job, chunk_size=64))
        assert received == body

    @responses_lib.activate
    def test_yields_bytes_in_order(self) -> None:
        body = bytes(range(256))
        responses_lib.add(
            responses_lib.GET,
            "https://example.com/file.bin",
            body=body,
            status=200,
        )
        received = b"".join(_adapter().fetch(_job(), chunk_size=32))
        assert received == body

    @responses_lib.activate
    def test_no_range_header_on_offset_zero(self) -> None:
        responses_lib.add(
            responses_lib.GET,
            "https://example.com/file.bin",
            body=b"data",
            status=200,
        )
        list(_adapter().fetch(_job(), offset=0))
        assert "Range" not in responses_lib.calls[0].request.headers


# ---------------------------------------------------------------------------
# fetch() — resume (Range header)
# ---------------------------------------------------------------------------


class TestFetchResume:
    @responses_lib.activate
    def test_sends_range_header_on_nonzero_offset(self) -> None:
        responses_lib.add(
            responses_lib.GET,
            "https://example.com/file.bin",
            body=b"partial data",
            status=206,
        )
        list(_adapter().fetch(_job(), offset=1024))
        assert responses_lib.calls[0].request.headers["Range"] == "bytes=1024-"

    @responses_lib.activate
    def test_yields_chunks_from_206(self) -> None:
        body = b"resumed content"
        responses_lib.add(
            responses_lib.GET,
            "https://example.com/file.bin",
            body=body,
            status=206,
        )
        received = b"".join(_adapter().fetch(_job(), offset=512))
        assert received == body

    @responses_lib.activate
    def test_raises_retriable_when_200_returned_for_range_request(self) -> None:
        """Server ignored Range header — cannot safely resume, must retry."""
        responses_lib.add(
            responses_lib.GET,
            "https://example.com/file.bin",
            body=b"full file from start",
            status=200,
        )
        with pytest.raises(RetriableError, match="200 instead of 206"):
            list(_adapter().fetch(_job(), offset=512))


# ---------------------------------------------------------------------------
# fetch() — status code handling
# ---------------------------------------------------------------------------


class TestFetchStatusCodes:
    @responses_lib.activate
    def test_429_raises_rate_limit_error(self) -> None:
        responses_lib.add(
            responses_lib.GET,
            "https://example.com/file.bin",
            status=429,
        )
        with pytest.raises(RateLimitError):
            list(_adapter().fetch(_job()))

    @responses_lib.activate
    def test_429_parses_retry_after_seconds(self) -> None:
        responses_lib.add(
            responses_lib.GET,
            "https://example.com/file.bin",
            headers={"Retry-After": "42"},
            status=429,
        )
        with pytest.raises(RateLimitError) as exc_info:
            list(_adapter().fetch(_job()))
        assert exc_info.value.retry_after == 42.0

    @responses_lib.activate
    def test_429_http_date_retry_after_returns_none(self) -> None:
        responses_lib.add(
            responses_lib.GET,
            "https://example.com/file.bin",
            headers={"Retry-After": "Fri, 31 Dec 1999 23:59:59 GMT"},
            status=429,
        )
        with pytest.raises(RateLimitError) as exc_info:
            list(_adapter().fetch(_job()))
        assert exc_info.value.retry_after is None

    @pytest.mark.parametrize("status", [500, 502, 503, 504])
    @responses_lib.activate
    def test_5xx_raises_retriable_error(self, status: int) -> None:
        responses_lib.add(
            responses_lib.GET,
            "https://example.com/file.bin",
            status=status,
        )
        with pytest.raises(RetriableError):
            list(_adapter().fetch(_job()))

    @pytest.mark.parametrize("status", [400, 401, 403, 404, 405, 410])
    @responses_lib.activate
    def test_4xx_raises_fatal_error(self, status: int) -> None:
        responses_lib.add(
            responses_lib.GET,
            "https://example.com/file.bin",
            status=status,
        )
        with pytest.raises(FatalTransferError):
            list(_adapter().fetch(_job()))

    @responses_lib.activate
    def test_unlisted_non_2xx_raises_fatal_error(self) -> None:
        """418 I'm a Teapot — not in either set, should be fatal."""
        responses_lib.add(
            responses_lib.GET,
            "https://example.com/file.bin",
            status=418,
        )
        with pytest.raises(FatalTransferError):
            list(_adapter().fetch(_job()))


# ---------------------------------------------------------------------------
# fetch() — network errors
# ---------------------------------------------------------------------------


class TestFetchNetworkErrors:
    @responses_lib.activate
    def test_timeout_raises_retriable_error(self) -> None:
        responses_lib.add(
            responses_lib.GET,
            "https://example.com/file.bin",
            body=requests.Timeout("timed out"),
        )
        with pytest.raises(RetriableError, match="timed out"):
            list(_adapter().fetch(_job()))

    @responses_lib.activate
    def test_connection_error_raises_retriable_error(self) -> None:
        responses_lib.add(
            responses_lib.GET,
            "https://example.com/file.bin",
            body=requests.ConnectionError("connection refused"),
        )
        with pytest.raises(RetriableError):
            list(_adapter().fetch(_job()))

    @responses_lib.activate
    def test_chunked_encoding_error_mid_stream_raises_retriable(self) -> None:
        responses_lib.add(
            responses_lib.GET,
            "https://example.com/file.bin",
            body=requests.exceptions.ChunkedEncodingError("stream broken"),
        )
        with pytest.raises(RetriableError):
            list(_adapter().fetch(_job()))


# ---------------------------------------------------------------------------
# fetch() — chunk_size
# ---------------------------------------------------------------------------


class TestFetchChunkSize:
    @responses_lib.activate
    def test_custom_chunk_size_returns_correct_bytes(self) -> None:
        body = b"x" * 4096
        responses_lib.add(
            responses_lib.GET,
            "https://example.com/file.bin",
            body=body,
            status=200,
        )
        received = b"".join(_adapter().fetch(_job(), chunk_size=512))
        assert received == body


# ---------------------------------------------------------------------------
# Thread safety — per-thread sessions
# ---------------------------------------------------------------------------


class TestThreadSafety:
    def test_each_thread_gets_independent_session(self) -> None:
        """
        Collect the id() of the session from N threads concurrently.
        Every thread must see a different session object.
        """
        adapter = _adapter()
        sessions: list[int] = []
        lock = threading.Lock()

        def capture_session() -> None:
            sid = id(adapter._session)
            with lock:
                sessions.append(sid)

        n = 8
        threads = [threading.Thread(target=capture_session) for _ in range(n)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()

        # All session IDs should be unique (one per thread)
        assert len(set(sessions)) == n, (
            f"Expected {n} unique sessions, got {len(set(sessions))}: {sessions}"
        )
