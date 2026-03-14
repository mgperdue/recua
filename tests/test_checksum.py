"""
Tests for checksum.py — compute_checksum and verify_checksum.
"""

from __future__ import annotations

import hashlib
from pathlib import Path

import pytest

from recua.checksum import compute_checksum, verify_checksum
from recua.exceptions import FatalTransferError, RetriableError


def _write(tmp_path: Path, data: bytes, name: str = "file.bin") -> Path:
    p = tmp_path / name
    p.write_bytes(data)
    return p


class TestComputeChecksum:
    def test_sha256_matches_hashlib(self, tmp_path: Path) -> None:
        data = b"recua checksum test " * 100
        p = _write(tmp_path, data)
        expected = hashlib.sha256(data).hexdigest()
        assert compute_checksum(p, "sha256") == expected

    def test_sha512_matches_hashlib(self, tmp_path: Path) -> None:
        data = b"sha512 test"
        p = _write(tmp_path, data)
        assert compute_checksum(p, "sha512") == hashlib.sha512(data).hexdigest()

    def test_md5_matches_hashlib(self, tmp_path: Path) -> None:
        data = b"md5 test"
        p = _write(tmp_path, data)
        assert compute_checksum(p, "md5") == hashlib.md5(data).hexdigest()

    def test_empty_file(self, tmp_path: Path) -> None:
        p = _write(tmp_path, b"")
        assert compute_checksum(p, "sha256") == hashlib.sha256(b"").hexdigest()

    def test_unsupported_algorithm_raises_fatal(self, tmp_path: Path) -> None:
        p = _write(tmp_path, b"data")
        with pytest.raises(FatalTransferError, match="Unsupported"):
            compute_checksum(p, "rot13")

    def test_result_is_lowercase_hex(self, tmp_path: Path) -> None:
        p = _write(tmp_path, b"case test")
        result = compute_checksum(p, "sha256")
        assert result == result.lower()
        assert all(c in "0123456789abcdef" for c in result)


class TestVerifyChecksum:
    def test_correct_checksum_does_not_raise(self, tmp_path: Path) -> None:
        data = b"correct data"
        p = _write(tmp_path, data)
        expected = hashlib.sha256(data).hexdigest()
        verify_checksum(p, expected, "sha256")  # must not raise

    def test_wrong_checksum_raises_retriable(self, tmp_path: Path) -> None:
        p = _write(tmp_path, b"some data")
        with pytest.raises(RetriableError, match="Checksum mismatch"):
            verify_checksum(p, "a" * 64, "sha256", display_name="some data")

    def test_case_insensitive_comparison(self, tmp_path: Path) -> None:
        data = b"case insensitive"
        p = _write(tmp_path, data)
        upper = hashlib.sha256(data).hexdigest().upper()
        verify_checksum(p, upper, "sha256")  # must not raise

    def test_display_name_appears_in_error(self, tmp_path: Path) -> None:
        p = _write(tmp_path, b"data")
        with pytest.raises(RetriableError, match="my_file.bin"):
            verify_checksum(p, "wrong" * 12, "sha256", display_name="my_file.bin")

    def test_unsupported_algorithm_raises_fatal(self, tmp_path: Path) -> None:
        p = _write(tmp_path, b"data")
        with pytest.raises(FatalTransferError):
            verify_checksum(p, "abc", "rot13")
