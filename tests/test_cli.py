"""
Tests for cli.py.

Uses pytest-httpserver for commands that need real HTTP.
All tests call main() directly with argv lists — no subprocess spawning.
"""

from __future__ import annotations

from pathlib import Path

import pytest

from recua.cli import main, _read_url_file


# ---------------------------------------------------------------------------
# Argument parsing
# ---------------------------------------------------------------------------

class TestArgParsing:
    def test_no_command_exits_nonzero(self) -> None:
        with pytest.raises(SystemExit) as exc:
            main([])
        assert exc.value.code != 0

    def test_get_requires_url(self) -> None:
        with pytest.raises(SystemExit):
            main(["get"])

    def test_batch_requires_file(self) -> None:
        with pytest.raises(SystemExit):
            main(["batch"])

    def test_help_exits_zero(self) -> None:
        with pytest.raises(SystemExit) as exc:
            main(["--help"])
        assert exc.value.code == 0

    def test_get_help_exits_zero(self) -> None:
        with pytest.raises(SystemExit) as exc:
            main(["get", "--help"])
        assert exc.value.code == 0

    def test_batch_help_exits_zero(self) -> None:
        with pytest.raises(SystemExit) as exc:
            main(["batch", "--help"])
        assert exc.value.code == 0


# ---------------------------------------------------------------------------
# _read_url_file
# ---------------------------------------------------------------------------

class TestReadUrlFile:
    def test_reads_urls(self, tmp_path: Path) -> None:
        f = tmp_path / "urls.txt"
        f.write_text("https://example.com/a.bin\nhttps://example.com/b.bin\n")
        assert _read_url_file(f) == [
            "https://example.com/a.bin",
            "https://example.com/b.bin",
        ]

    def test_skips_blank_lines(self, tmp_path: Path) -> None:
        f = tmp_path / "urls.txt"
        f.write_text("\nhttps://example.com/a.bin\n\n")
        assert _read_url_file(f) == ["https://example.com/a.bin"]

    def test_skips_comments(self, tmp_path: Path) -> None:
        f = tmp_path / "urls.txt"
        f.write_text("# this is a comment\nhttps://example.com/a.bin\n")
        assert _read_url_file(f) == ["https://example.com/a.bin"]

    def test_empty_file_returns_empty_list(self, tmp_path: Path) -> None:
        f = tmp_path / "urls.txt"
        f.write_text("")
        assert _read_url_file(f) == []


# ---------------------------------------------------------------------------
# get command (real HTTP)
# ---------------------------------------------------------------------------

class TestGetCommand:
    def test_downloads_single_file(self, httpserver, tmp_path: Path) -> None:
        body = b"cli get test"
        httpserver.expect_request("/file.bin").respond_with_data(body)
        url = httpserver.url_for("/file.bin")

        rc = main([
            "--no-progress",
            "get", url,
            "--dest", str(tmp_path),
            "--workers", "1",
        ])

        assert rc == 0
        assert (tmp_path / "file.bin").read_bytes() == body

    def test_returns_1_on_404(self, httpserver, tmp_path: Path) -> None:
        httpserver.expect_request("/missing.bin").respond_with_data("nope", status=404)
        url = httpserver.url_for("/missing.bin")

        rc = main([
            "--no-progress",
            "get", url,
            "--dest", str(tmp_path),
            "--retries", "0",
        ])

        assert rc == 1

    def test_checksum_pass(self, httpserver, tmp_path: Path) -> None:
        import hashlib
        body = b"checksum cli test"
        sha256 = hashlib.sha256(body).hexdigest()
        httpserver.expect_request("/chk.bin").respond_with_data(body)
        url = httpserver.url_for("/chk.bin")

        rc = main([
            "--no-progress",
            "get", url,
            "--dest", str(tmp_path),
            "--sha256", sha256,
        ])

        assert rc == 0

    def test_multiple_urls_with_checksum_flag_errors(self, httpserver, tmp_path: Path) -> None:
        body = b"data"
        httpserver.expect_request("/a.bin").respond_with_data(body)
        httpserver.expect_request("/b.bin").respond_with_data(body)
        url_a = httpserver.url_for("/a.bin")
        url_b = httpserver.url_for("/b.bin")

        rc = main([
            "--no-progress",
            "get", url_a, url_b,
            "--dest", str(tmp_path),
            "--sha256", "abc123",
        ])

        assert rc == 1  # multiple URLs + checksum flag is an error


# ---------------------------------------------------------------------------
# batch command (real HTTP)
# ---------------------------------------------------------------------------

class TestBatchCommand:
    def test_downloads_all_urls_in_file(self, httpserver, tmp_path: Path) -> None:
        bodies = {"/f1.bin": b"first", "/f2.bin": b"second", "/f3.bin": b"third"}
        for path, body in bodies.items():
            httpserver.expect_request(path).respond_with_data(body)

        url_file = tmp_path / "urls.txt"
        url_file.write_text(
            "\n".join(httpserver.url_for(p) for p in bodies) + "\n"
        )

        rc = main([
            "--no-progress",
            "batch", str(url_file),
            "--dest", str(tmp_path / "out"),
            "--workers", "2",
        ])

        assert rc == 0
        for path, body in bodies.items():
            assert (tmp_path / "out" / path.lstrip("/")).read_bytes() == body

    def test_missing_url_file_returns_1(self, tmp_path: Path) -> None:
        rc = main([
            "--no-progress",
            "batch", str(tmp_path / "nonexistent.txt"),
            "--dest", str(tmp_path),
        ])
        assert rc == 1

    def test_empty_url_file_returns_1(self, tmp_path: Path) -> None:
        url_file = tmp_path / "empty.txt"
        url_file.write_text("# just a comment\n")

        rc = main([
            "--no-progress",
            "batch", str(url_file),
            "--dest", str(tmp_path),
        ])
        assert rc == 1
