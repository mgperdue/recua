"""
Tests for protocols.py.

TransferAdapter and StateStore are structural protocols with
runtime_checkable. Tests verify:
  - Concrete implementations satisfy isinstance() checks
  - Objects missing required methods do NOT satisfy isinstance()
  - Protocol method signatures are documented correctly
  - _DEFAULT_CHUNK_SIZE matches TransferOptions.chunk_size default
"""

from __future__ import annotations

from pathlib import Path

from recua.options import TransferOptions
from recua.protocols import _DEFAULT_CHUNK_SIZE, StateStore, TransferAdapter

# ---------------------------------------------------------------------------
# TransferAdapter protocol
# ---------------------------------------------------------------------------


class TestTransferAdapterProtocol:
    def test_http_adapter_satisfies_protocol(self) -> None:
        from recua.adapters.http import HTTPAdapter

        adapter = HTTPAdapter()
        assert isinstance(adapter, TransferAdapter)

    def test_object_missing_methods_does_not_satisfy(self) -> None:
        assert not isinstance(object(), TransferAdapter)

    def test_partial_implementation_does_not_satisfy(self) -> None:
        class Partial:
            def supports(self, source: str) -> bool:
                return True

            # missing get_size and fetch

        assert not isinstance(Partial(), TransferAdapter)

    def test_full_duck_type_satisfies(self) -> None:
        """Any object with the right method names satisfies the protocol."""

        class MockAdapter:
            def supports(self, source: str) -> bool:
                return True

            def get_size(self, source: str) -> int | None:
                return None

            def fetch(self, job, offset=0, chunk_size=1_048_576):
                return iter([])

        assert isinstance(MockAdapter(), TransferAdapter)

    def test_default_chunk_size_matches_options_default(self) -> None:
        """
        Protocol default and TransferOptions default must stay in sync.
        If one changes, this test catches the drift.
        """
        assert TransferOptions().chunk_size == _DEFAULT_CHUNK_SIZE


# ---------------------------------------------------------------------------
# StateStore protocol
# ---------------------------------------------------------------------------


class TestStateStoreProtocol:
    def test_sqlite_state_store_satisfies_protocol(self, tmp_path: Path) -> None:
        from recua.state import SQLiteStateStore

        store = SQLiteStateStore(tmp_path / "state.db")
        assert isinstance(store, StateStore)

    def test_null_state_store_satisfies_protocol(self) -> None:
        from recua.state import NullStateStore

        store = NullStateStore()
        assert isinstance(store, StateStore)

    def test_object_missing_methods_does_not_satisfy(self) -> None:
        assert not isinstance(object(), StateStore)

    def test_partial_implementation_does_not_satisfy(self) -> None:
        class Partial:
            def get_offset(self, key):
                return 0

            def set_offset(self, key, offset):
                pass

            # missing mark_complete, mark_failed, is_complete

        assert not isinstance(Partial(), StateStore)

    def test_full_duck_type_satisfies(self) -> None:
        class MockStore:
            def get_offset(self, key):
                return 0

            def set_offset(self, key, offset):
                pass

            def mark_complete(self, key):
                pass

            def mark_failed(self, key, reason):
                pass

            def is_complete(self, key):
                return False

        assert isinstance(MockStore(), StateStore)


# ---------------------------------------------------------------------------
# Protocol consistency checks
# ---------------------------------------------------------------------------


class TestProtocolConsistency:
    def test_transfer_adapter_has_all_required_methods(self) -> None:
        """Verify the protocol declares the three methods we depend on."""
        import inspect

        members = {name for name, _ in inspect.getmembers(TransferAdapter)}
        assert "supports" in members
        assert "get_size" in members
        assert "fetch" in members

    def test_state_store_has_all_required_methods(self) -> None:
        import inspect

        members = {name for name, _ in inspect.getmembers(StateStore)}
        assert "get_offset" in members
        assert "set_offset" in members
        assert "mark_complete" in members
        assert "mark_failed" in members
        assert "is_complete" in members
