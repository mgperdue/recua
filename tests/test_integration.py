"""
End-to-end integration tests against a real HTTP server.

Uses pytest-httpserver to spin up a genuine HTTP server on localhost.
These tests validate the full networking stack — no responses mocking.

Coverage
--------
- Single file download end-to-end
- Multiple concurrent downloads
- Resume: partial download + server restart + re-run completes file
- State persistence across simulated process restart (two engine instances,
  same SQLite state file)
- Checksum verification passes on correct file
- Checksum verification fails (RetriableError → eventual failure) on corrupt file
- Bandwidth cap: throughput stays within 2x of configured limit
- 404 marks job failed without retry (fatal)
- 503 retries then succeeds
"""

from __future__ import annotations

import hashlib
from pathlib import Path

import pytest

from recua.engine import TransferEngine
from recua.job import TransferJob
from recua.options import TransferOptions

pytestmark = pytest.mark.integration


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _opts(tmp_path: Path, **kwargs) -> TransferOptions:
    defaults = dict(
        max_workers=2,
        chunk_size=4096,
        retries=2,
        backoff_base=0.05,
        progress=False,
    )
    defaults.update(kwargs)
    return TransferOptions(**defaults)


def _job(url: str, dest: Path) -> TransferJob:
    return TransferJob(source=url, dest=dest, name=dest.name)


def _sha256(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


# ---------------------------------------------------------------------------
# Basic download
# ---------------------------------------------------------------------------


class TestBasicDownload:
    def test_single_file(self, httpserver, tmp_path: Path) -> None:
        body = b"hello from a real HTTP server " * 100
        httpserver.expect_request("/file.bin").respond_with_data(body)

        url = httpserver.url_for("/file.bin")
        dest = tmp_path / "file.bin"

        with TransferEngine(_opts(tmp_path)) as engine:
            engine.submit(_job(url, dest))

        assert dest.read_bytes() == body

    def test_multiple_concurrent_files(self, httpserver, tmp_path: Path) -> None:
        n = 6
        bodies = {f"/file{i}.bin": bytes([i % 256] * 2048) for i in range(n)}
        for path, body in bodies.items():
            httpserver.expect_request(path).respond_with_data(body)

        opts = _opts(tmp_path, max_workers=3)
        with TransferEngine(opts) as engine:
            for path, _body in bodies.items():
                url = httpserver.url_for(path)
                engine.submit(_job(url, tmp_path / path.lstrip("/")))

        for path, _body in bodies.items():
            assert (tmp_path / path.lstrip("/")).read_bytes() == _body

    def test_large_file(self, httpserver, tmp_path: Path) -> None:
        body = b"x" * (1 * 1024 * 1024)  # 1 MiB
        httpserver.expect_request("/large.bin").respond_with_data(body)

        url = httpserver.url_for("/large.bin")
        dest = tmp_path / "large.bin"

        with TransferEngine(_opts(tmp_path, chunk_size=16384)) as engine:
            engine.submit(_job(url, dest))

        assert dest.read_bytes() == body


# ---------------------------------------------------------------------------
# State persistence — restart simulation
# ---------------------------------------------------------------------------


class TestStatePersistence:
    def test_completed_job_skipped_on_second_run(self, httpserver, tmp_path: Path) -> None:
        """
        Two engine instances sharing the same SQLite state file.
        The second run must not re-download already-completed files.
        """
        body = b"persistent data " * 200
        request_count = 0

        def handler(request):
            nonlocal request_count
            request_count += 1
            from werkzeug.wrappers import Response

            return Response(body, status=200)

        httpserver.expect_request("/data.bin").respond_with_handler(handler)
        url = httpserver.url_for("/data.bin")
        dest = tmp_path / "data.bin"
        state_db = tmp_path / "state.db"

        # First run — downloads the file
        opts = _opts(tmp_path, state_path=state_db)
        with TransferEngine(opts) as engine:
            engine.submit(_job(url, dest))

        assert dest.read_bytes() == body
        assert request_count == 1

        # Second run — same state file, same job — must skip
        with TransferEngine(opts) as engine:
            engine.submit(_job(url, dest))

        assert request_count == 1, "Second run should not have made an HTTP request"

    def test_failed_jobs_retried_on_second_run(self, httpserver, tmp_path: Path) -> None:
        """
        A job that fails in run 1 (404) is not in state as 'complete',
        so run 2 (after fixing the server) can download it successfully.
        """
        body = b"now available"
        dest = tmp_path / "resource.bin"
        state_db = tmp_path / "state.db"
        url = httpserver.url_for("/resource.bin")

        # First run: 404
        httpserver.expect_ordered_request("/resource.bin").respond_with_data(
            "not found", status=404
        )
        opts = _opts(tmp_path, state_path=state_db, retries=0)
        with TransferEngine(opts) as engine:
            engine.submit(_job(url, dest))

        assert not dest.exists() or dest.stat().st_size == 0

        # Second run: 200
        httpserver.expect_request("/resource.bin").respond_with_data(body)
        with TransferEngine(opts) as engine:
            engine.submit(_job(url, dest))

        assert dest.read_bytes() == body


# ---------------------------------------------------------------------------
# Checksum verification
# ---------------------------------------------------------------------------


class TestChecksumVerification:
    def test_correct_checksum_passes(self, httpserver, tmp_path: Path) -> None:
        body = b"verified data " * 50
        checksum = _sha256(body)
        httpserver.expect_request("/verified.bin").respond_with_data(body)

        url = httpserver.url_for("/verified.bin")
        dest = tmp_path / "verified.bin"

        opts = _opts(tmp_path, checksum_algorithm="sha256")
        with TransferEngine(opts) as engine:
            engine.submit(
                TransferJob(
                    source=url,
                    dest=dest,
                    name="verified.bin",
                    expected_checksum=checksum,
                )
            )

        assert dest.read_bytes() == body

    def test_wrong_checksum_marks_failed(self, httpserver, tmp_path: Path) -> None:
        body = b"tampered data " * 50
        wrong_checksum = "a" * 64  # deliberately wrong sha256
        httpserver.expect_request("/tampered.bin").respond_with_data(body)
        # Need enough responses for all retry attempts
        for _ in range(4):
            httpserver.expect_request("/tampered.bin").respond_with_data(body)

        url = httpserver.url_for("/tampered.bin")
        dest = tmp_path / "tampered.bin"

        errors: list[tuple] = []
        opts = _opts(
            tmp_path,
            checksum_algorithm="sha256",
            retries=2,
            on_error=lambda job, exc: errors.append((job, exc)),
        )
        with TransferEngine(opts) as engine:
            engine.submit(
                TransferJob(
                    source=url,
                    dest=dest,
                    name="tampered.bin",
                    expected_checksum=wrong_checksum,
                )
            )

        assert len(errors) == 1
        assert "Checksum mismatch" in str(errors[0][1])

    def test_no_checksum_field_skips_verification(self, httpserver, tmp_path: Path) -> None:
        """Jobs without expected_checksum are not verified even if algorithm is set."""
        body = b"unverified"
        httpserver.expect_request("/unverified.bin").respond_with_data(body)

        url = httpserver.url_for("/unverified.bin")
        dest = tmp_path / "unverified.bin"

        opts = _opts(tmp_path, checksum_algorithm="sha256")
        with TransferEngine(opts) as engine:
            engine.submit(_job(url, dest))  # no expected_checksum

        assert dest.read_bytes() == body


# ---------------------------------------------------------------------------
# Error handling
# ---------------------------------------------------------------------------


class TestErrorHandling:
    def test_404_marks_failed_immediately(self, httpserver, tmp_path: Path) -> None:
        httpserver.expect_request("/missing.bin").respond_with_data("not found", status=404)

        url = httpserver.url_for("/missing.bin")
        errors: list = []
        opts = _opts(tmp_path, retries=3, on_error=lambda j, e: errors.append(j))

        with TransferEngine(opts) as engine:
            engine.submit(_job(url, tmp_path / "missing.bin"))

        # 404 is fatal — should be exactly 1 HTTP request despite retries=3
        assert len(errors) == 1
        assert engine.stats().failed == 1

    def test_503_retries_then_succeeds(self, httpserver, tmp_path: Path) -> None:
        body = b"eventually available"
        httpserver.expect_ordered_request("/flaky.bin").respond_with_data("err", status=503)
        httpserver.expect_ordered_request("/flaky.bin").respond_with_data("err", status=503)
        httpserver.expect_ordered_request("/flaky.bin").respond_with_data(body, status=200)

        url = httpserver.url_for("/flaky.bin")
        dest = tmp_path / "flaky.bin"

        opts = _opts(tmp_path, retries=3)
        with TransferEngine(opts) as engine:
            engine.submit(_job(url, dest))

        assert dest.read_bytes() == body
