"""Tests for SQLiteStateStore and NullStateStore."""
import pytest
from pathlib import Path

from recua.state import NullStateStore, SQLiteStateStore

KEY = ("https://example.com/file.bin", "/tmp/file.bin")


def test_null_store_offset_is_zero():
    store = NullStateStore()
    assert store.get_offset(KEY) == 0


def test_null_store_is_never_complete():
    store = NullStateStore()
    assert not store.is_complete(KEY)


def test_null_store_accepts_all_writes():
    store = NullStateStore()
    store.set_offset(KEY, 1024)
    store.mark_complete(KEY)
    store.mark_failed(KEY, "some error")


@pytest.mark.skip(reason="SQLiteStateStore not yet implemented")
def test_sqlite_store_roundtrip(tmp_path):
    store = SQLiteStateStore(tmp_path / "state.db")
    assert store.get_offset(KEY) == 0
    store.set_offset(KEY, 512)
    assert store.get_offset(KEY) == 512
    store.mark_complete(KEY)
    assert store.is_complete(KEY)
