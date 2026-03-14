"""
Tests for SQLiteStateStore and NullStateStore.

Coverage targets
----------------
NullStateStore:     full (already passing)
SQLiteStateStore:
  - schema creation / idempotency
  - get_offset returns 0 for unknown key
  - set_offset insert + update
  - mark_complete
  - mark_failed with reason
  - is_complete truth table
  - get_status all values
  - get_failed filtering
  - resume semantics: offset survives process restart (new instance, same db)
  - thread safety: N threads writing concurrent offsets, no corruption
  - close() is idempotent
"""

from __future__ import annotations

import threading
from pathlib import Path

import pytest

from recua.state import NullStateStore, SQLiteStateStore

# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

KEY_A = ("https://example.com/a.bin", "/tmp/a.bin")
KEY_B = ("https://example.com/b.bin", "/tmp/b.bin")


@pytest.fixture
def store(tmp_path: Path) -> SQLiteStateStore:
    return SQLiteStateStore(tmp_path / "state.db")


@pytest.fixture
def db_path(tmp_path: Path) -> Path:
    return tmp_path / "state.db"


# ---------------------------------------------------------------------------
# NullStateStore (already passing — kept as regression guard)
# ---------------------------------------------------------------------------


class TestNullStateStore:
    def test_offset_is_always_zero(self) -> None:
        store = NullStateStore()
        assert store.get_offset(KEY_A) == 0

    def test_is_never_complete(self) -> None:
        store = NullStateStore()
        assert not store.is_complete(KEY_A)

    def test_accepts_all_writes_without_error(self) -> None:
        store = NullStateStore()
        store.set_offset(KEY_A, 1024)
        store.mark_complete(KEY_A)
        store.mark_failed(KEY_A, "some error")


# ---------------------------------------------------------------------------
# SQLiteStateStore — schema
# ---------------------------------------------------------------------------


class TestSQLiteStateStoreSchema:
    def test_creates_db_file(self, db_path: Path) -> None:
        SQLiteStateStore(db_path)
        assert db_path.exists()

    def test_schema_creation_is_idempotent(self, db_path: Path) -> None:
        """Opening the same path twice should not raise."""
        SQLiteStateStore(db_path)
        SQLiteStateStore(db_path)  # must not raise or corrupt


# ---------------------------------------------------------------------------
# SQLiteStateStore — get_offset
# ---------------------------------------------------------------------------


class TestGetOffset:
    def test_returns_zero_for_unknown_key(self, store: SQLiteStateStore) -> None:
        assert store.get_offset(KEY_A) == 0

    def test_returns_stored_offset(self, store: SQLiteStateStore) -> None:
        store.set_offset(KEY_A, 4096)
        assert store.get_offset(KEY_A) == 4096

    def test_keys_are_independent(self, store: SQLiteStateStore) -> None:
        store.set_offset(KEY_A, 100)
        store.set_offset(KEY_B, 200)
        assert store.get_offset(KEY_A) == 100
        assert store.get_offset(KEY_B) == 200


# ---------------------------------------------------------------------------
# SQLiteStateStore — set_offset
# ---------------------------------------------------------------------------


class TestSetOffset:
    def test_insert_on_first_call(self, store: SQLiteStateStore) -> None:
        store.set_offset(KEY_A, 512)
        assert store.get_offset(KEY_A) == 512

    def test_update_on_subsequent_call(self, store: SQLiteStateStore) -> None:
        store.set_offset(KEY_A, 512)
        store.set_offset(KEY_A, 1024)
        assert store.get_offset(KEY_A) == 1024

    def test_zero_offset_is_valid(self, store: SQLiteStateStore) -> None:
        store.set_offset(KEY_A, 0)
        assert store.get_offset(KEY_A) == 0

    def test_does_not_overwrite_complete_status(self, store: SQLiteStateStore) -> None:
        """
        A completed job's status must not be degraded back to in_progress
        if set_offset is somehow called after mark_complete.
        """
        store.mark_complete(KEY_A)
        store.set_offset(KEY_A, 999)
        assert store.is_complete(KEY_A)


# ---------------------------------------------------------------------------
# SQLiteStateStore — mark_complete
# ---------------------------------------------------------------------------


class TestMarkComplete:
    def test_marks_unseen_key_complete(self, store: SQLiteStateStore) -> None:
        store.mark_complete(KEY_A)
        assert store.is_complete(KEY_A)

    def test_marks_in_progress_key_complete(self, store: SQLiteStateStore) -> None:
        store.set_offset(KEY_A, 2048)
        store.mark_complete(KEY_A)
        assert store.is_complete(KEY_A)

    def test_idempotent(self, store: SQLiteStateStore) -> None:
        store.mark_complete(KEY_A)
        store.mark_complete(KEY_A)
        assert store.is_complete(KEY_A)

    def test_does_not_affect_other_keys(self, store: SQLiteStateStore) -> None:
        store.mark_complete(KEY_A)
        assert not store.is_complete(KEY_B)


# ---------------------------------------------------------------------------
# SQLiteStateStore — mark_failed
# ---------------------------------------------------------------------------


class TestMarkFailed:
    def test_marks_unseen_key_failed(self, store: SQLiteStateStore) -> None:
        store.mark_failed(KEY_A, "404 Not Found")
        assert store.get_status(KEY_A) == "failed"

    def test_stores_reason(self, store: SQLiteStateStore) -> None:
        store.mark_failed(KEY_A, "connection reset")
        failed = store.get_failed()
        assert len(failed) == 1
        assert failed[0]["reason"] == "connection reset"
        assert failed[0]["source"] == KEY_A[0]

    def test_idempotent_with_updated_reason(self, store: SQLiteStateStore) -> None:
        store.mark_failed(KEY_A, "first error")
        store.mark_failed(KEY_A, "second error")
        failed = store.get_failed()
        assert len(failed) == 1
        assert failed[0]["reason"] == "second error"

    def test_failed_is_not_complete(self, store: SQLiteStateStore) -> None:
        store.mark_failed(KEY_A, "boom")
        assert not store.is_complete(KEY_A)


# ---------------------------------------------------------------------------
# SQLiteStateStore — is_complete / get_status truth table
# ---------------------------------------------------------------------------


class TestStatusTruthTable:
    def test_unseen_key_status_is_none(self, store: SQLiteStateStore) -> None:
        assert store.get_status(KEY_A) is None

    def test_in_progress_status(self, store: SQLiteStateStore) -> None:
        store.set_offset(KEY_A, 1)
        assert store.get_status(KEY_A) == "in_progress"

    def test_complete_status(self, store: SQLiteStateStore) -> None:
        store.mark_complete(KEY_A)
        assert store.get_status(KEY_A) == "complete"
        assert store.is_complete(KEY_A)

    def test_failed_status(self, store: SQLiteStateStore) -> None:
        store.mark_failed(KEY_A, "err")
        assert store.get_status(KEY_A) == "failed"
        assert not store.is_complete(KEY_A)


# ---------------------------------------------------------------------------
# SQLiteStateStore — get_failed
# ---------------------------------------------------------------------------


class TestGetFailed:
    def test_empty_when_no_failures(self, store: SQLiteStateStore) -> None:
        store.mark_complete(KEY_A)
        assert store.get_failed() == []

    def test_returns_only_failed_rows(self, store: SQLiteStateStore) -> None:
        store.mark_complete(KEY_A)
        store.mark_failed(KEY_B, "timeout")
        failed = store.get_failed()
        assert len(failed) == 1
        assert failed[0]["source"] == KEY_B[0]


# ---------------------------------------------------------------------------
# SQLiteStateStore — resume semantics (persistence across instances)
# ---------------------------------------------------------------------------


class TestResumeSemantics:
    def test_offset_survives_new_instance(self, db_path: Path) -> None:
        """Simulate process restart: new SQLiteStateStore, same db file."""
        store1 = SQLiteStateStore(db_path)
        store1.set_offset(KEY_A, 65536)
        store1.close()

        store2 = SQLiteStateStore(db_path)
        assert store2.get_offset(KEY_A) == 65536

    def test_complete_survives_new_instance(self, db_path: Path) -> None:
        store1 = SQLiteStateStore(db_path)
        store1.mark_complete(KEY_A)
        store1.close()

        store2 = SQLiteStateStore(db_path)
        assert store2.is_complete(KEY_A)

    def test_failed_survives_new_instance(self, db_path: Path) -> None:
        store1 = SQLiteStateStore(db_path)
        store1.mark_failed(KEY_A, "disk full")
        store1.close()

        store2 = SQLiteStateStore(db_path)
        assert store2.get_status(KEY_A) == "failed"


# ---------------------------------------------------------------------------
# SQLiteStateStore — thread safety
# ---------------------------------------------------------------------------


class TestThreadSafety:
    def test_concurrent_set_offset_no_corruption(self, store: SQLiteStateStore) -> None:
        """
        16 threads each writing a unique key concurrently.
        All offsets must be retrievable without error or corruption.
        """
        n = 16
        keys = [(f"https://example.com/{i}.bin", f"/tmp/{i}.bin") for i in range(n)]
        errors: list[Exception] = []

        def worker(key: tuple[str, str], offset: int) -> None:
            try:
                for _ in range(10):
                    store.set_offset(key, offset)
                    store.get_offset(key)
            except Exception as e:
                errors.append(e)

        threads = [threading.Thread(target=worker, args=(k, i * 1024)) for i, k in enumerate(keys)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()

        assert errors == [], f"Thread errors: {errors}"
        for i, key in enumerate(keys):
            assert store.get_offset(key) == i * 1024

    def test_concurrent_mark_complete_no_corruption(self, store: SQLiteStateStore) -> None:
        """Multiple threads marking different keys complete must not interfere."""
        n = 16
        keys = [(f"https://example.com/{i}.bin", f"/tmp/{i}.bin") for i in range(n)]
        errors: list[Exception] = []

        def worker(key: tuple[str, str]) -> None:
            try:
                store.mark_complete(key)
            except Exception as e:
                errors.append(e)

        threads = [threading.Thread(target=worker, args=(k,)) for k in keys]
        for t in threads:
            t.start()
        for t in threads:
            t.join()

        assert errors == []
        for key in keys:
            assert store.is_complete(key)


# ---------------------------------------------------------------------------
# SQLiteStateStore — close()
# ---------------------------------------------------------------------------


class TestClose:
    def test_close_is_idempotent(self, store: SQLiteStateStore) -> None:
        store.close()
        store.close()  # must not raise
