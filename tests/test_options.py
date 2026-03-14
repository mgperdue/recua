"""
Tests for TransferOptions.

TransferOptions is a plain dataclass — tests focus on:
  - Default values are correct and safe
  - Field types are accepted without error
  - checksum_algorithm accepts valid literals, rejects invalid values
    at the type level (runtime accepts any str since Literal is not
    enforced at runtime, but we document valid values)
  - Mutation (it is not frozen) works as expected
  - Callback fields accept callables and None
"""

from __future__ import annotations

from pathlib import Path

import pytest

from recua.options import TransferOptions


class TestDefaults:
    def test_max_workers_default(self) -> None:
        assert TransferOptions().max_workers == 4

    def test_queue_size_default(self) -> None:
        assert TransferOptions().queue_size == 1000

    def test_max_mb_per_sec_default_is_none(self) -> None:
        assert TransferOptions().max_mb_per_sec is None

    def test_chunk_size_default(self) -> None:
        assert TransferOptions().chunk_size == 1_048_576

    def test_retries_default(self) -> None:
        assert TransferOptions().retries == 5

    def test_backoff_base_default(self) -> None:
        assert TransferOptions().backoff_base == 1.5

    def test_state_path_default_is_none(self) -> None:
        assert TransferOptions().state_path is None

    def test_checksum_algorithm_default_is_none(self) -> None:
        assert TransferOptions().checksum_algorithm is None

    def test_on_complete_default_is_none(self) -> None:
        assert TransferOptions().on_complete is None

    def test_on_error_default_is_none(self) -> None:
        assert TransferOptions().on_error is None

    def test_on_progress_default_is_none(self) -> None:
        assert TransferOptions().on_progress is None

    def test_progress_default_is_true(self) -> None:
        assert TransferOptions().progress is True


class TestFieldAssignment:
    def test_max_workers(self) -> None:
        opts = TransferOptions(max_workers=8)
        assert opts.max_workers == 8

    def test_queue_size(self) -> None:
        opts = TransferOptions(queue_size=500)
        assert opts.queue_size == 500

    def test_max_mb_per_sec(self) -> None:
        opts = TransferOptions(max_mb_per_sec=10.5)
        assert opts.max_mb_per_sec == 10.5

    def test_chunk_size(self) -> None:
        opts = TransferOptions(chunk_size=65536)
        assert opts.chunk_size == 65536

    def test_retries(self) -> None:
        opts = TransferOptions(retries=3)
        assert opts.retries == 3

    def test_backoff_base(self) -> None:
        opts = TransferOptions(backoff_base=2.0)
        assert opts.backoff_base == 2.0

    def test_state_path(self, tmp_path: Path) -> None:
        p = tmp_path / "state.db"
        opts = TransferOptions(state_path=p)
        assert opts.state_path == p

    def test_progress_false(self) -> None:
        opts = TransferOptions(progress=False)
        assert opts.progress is False

    def test_checksum_algorithm_sha256(self) -> None:
        opts = TransferOptions(checksum_algorithm="sha256")
        assert opts.checksum_algorithm == "sha256"

    def test_checksum_algorithm_sha512(self) -> None:
        opts = TransferOptions(checksum_algorithm="sha512")
        assert opts.checksum_algorithm == "sha512"

    def test_checksum_algorithm_md5(self) -> None:
        opts = TransferOptions(checksum_algorithm="md5")
        assert opts.checksum_algorithm == "md5"

    def test_checksum_algorithm_sha1(self) -> None:
        opts = TransferOptions(checksum_algorithm="sha1")
        assert opts.checksum_algorithm == "sha1"


class TestCallbackFields:
    def test_on_complete_accepts_callable(self) -> None:
        def handler(job): pass
        opts = TransferOptions(on_complete=handler)
        assert opts.on_complete is handler

    def test_on_error_accepts_callable(self) -> None:
        def handler(job, exc): pass
        opts = TransferOptions(on_error=handler)
        assert opts.on_error is handler

    def test_on_progress_accepts_callable(self) -> None:
        def handler(job, done, total): pass
        opts = TransferOptions(on_progress=handler)
        assert opts.on_progress is handler

    def test_callbacks_are_callable(self) -> None:
        called = []
        opts = TransferOptions(on_complete=called.append)
        # Verify it can actually be called
        opts.on_complete("job")  # type: ignore[misc]
        assert called == ["job"]


class TestMutability:
    def test_options_is_mutable(self) -> None:
        opts = TransferOptions()
        opts.max_workers = 16
        assert opts.max_workers == 16

    def test_can_swap_callback(self) -> None:
        opts = TransferOptions()
        opts.on_complete = lambda job: None
        assert opts.on_complete is not None
        opts.on_complete = None
        assert opts.on_complete is None
