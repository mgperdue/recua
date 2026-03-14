"""
SQLite-backed StateStore for persistent transfer state.

Schema
------
transfers (
    source      TEXT,
    dest        TEXT,
    status      TEXT,    -- 'in_progress' | 'complete' | 'failed'
    offset      INTEGER, -- bytes confirmed written
    reason      TEXT,    -- failure reason if status = 'failed'
    updated_at  TEXT,    -- ISO-8601 timestamp
    PRIMARY KEY (source, dest)
)

Resume key is (source, dest) — same URL to two destinations is tracked
independently. See TransferJob.resume_key.

Thread safety: each method opens its own short-lived connection with
WAL mode enabled, or a shared connection with a threading.Lock.
"""

from __future__ import annotations

import sqlite3
import threading
from pathlib import Path


_SCHEMA = """
CREATE TABLE IF NOT EXISTS transfers (
    source      TEXT    NOT NULL,
    dest        TEXT    NOT NULL,
    status      TEXT    NOT NULL DEFAULT 'in_progress',
    offset      INTEGER NOT NULL DEFAULT 0,
    reason      TEXT,
    updated_at  TEXT    NOT NULL DEFAULT (datetime('now')),
    PRIMARY KEY (source, dest)
);
"""


class SQLiteStateStore:
    """
    Implements the StateStore protocol backed by a local SQLite database.

    Pass state_path to TransferOptions to enable persistence.
    All public methods are thread-safe.
    """

    def __init__(self, path: Path) -> None:
        self._path = path
        self._lock = threading.Lock()
        self._init_db()

    def _init_db(self) -> None:
        # TODO: open connection, execute _SCHEMA, enable WAL mode
        raise NotImplementedError

    def _connect(self) -> sqlite3.Connection:
        # TODO: return a connection with row_factory and WAL pragma set
        raise NotImplementedError

    def get_offset(self, resume_key: tuple[str, str]) -> int:
        # TODO: SELECT offset WHERE source=? AND dest=?; return 0 if missing
        raise NotImplementedError

    def set_offset(self, resume_key: tuple[str, str], offset: int) -> None:
        # TODO: UPSERT offset, set updated_at
        raise NotImplementedError

    def mark_complete(self, resume_key: tuple[str, str]) -> None:
        # TODO: UPDATE status='complete', offset=<final>
        raise NotImplementedError

    def mark_failed(self, resume_key: tuple[str, str], reason: str) -> None:
        # TODO: UPDATE status='failed', reason=reason
        raise NotImplementedError

    def is_complete(self, resume_key: tuple[str, str]) -> bool:
        # TODO: SELECT status='complete'
        raise NotImplementedError


class NullStateStore:
    """
    No-op StateStore used when state_path is None.

    All transfers start from zero and no state survives process exit.
    """

    def get_offset(self, resume_key: tuple[str, str]) -> int:
        return 0

    def set_offset(self, resume_key: tuple[str, str], offset: int) -> None:
        pass

    def mark_complete(self, resume_key: tuple[str, str]) -> None:
        pass

    def mark_failed(self, resume_key: tuple[str, str], reason: str) -> None:
        pass

    def is_complete(self, resume_key: tuple[str, str]) -> bool:
        return False
