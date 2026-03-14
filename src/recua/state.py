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

Thread safety
-------------
A single sqlite3.Connection is held open for the lifetime of the store.
All access is serialized through a threading.Lock, which is cheaper than
opening/closing a connection per call and avoids "database is locked" races
under high write concurrency from N worker threads.

WAL mode is enabled so that concurrent readers never block writers — useful
for external inspection tools (DB Browser, DuckDB) during long runs.
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

# Valid status values — kept as constants to avoid magic strings in queries.
_STATUS_IN_PROGRESS = "in_progress"
_STATUS_COMPLETE = "complete"
_STATUS_FAILED = "failed"


class SQLiteStateStore:
    """
    Implements the StateStore protocol backed by a local SQLite database.

    Pass state_path to TransferOptions to enable persistence.
    All public methods are thread-safe.

    Example
    -------
    store = SQLiteStateStore(Path("transfers.db"))
    store.set_offset(job.resume_key, 1_048_576)
    store.mark_complete(job.resume_key)
    """

    def __init__(self, path: Path) -> None:
        self._path = path
        self._lock = threading.Lock()
        self._conn = self._open_connection()
        self._init_db()

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    def _open_connection(self) -> sqlite3.Connection:
        """
        Open a persistent connection with sensible defaults.

        check_same_thread=False is safe here because all access is
        serialized through self._lock.
        """
        conn = sqlite3.connect(
            self._path,
            check_same_thread=False,
            isolation_level=None,   # autocommit; we manage transactions explicitly
        )
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA journal_mode=WAL;")
        conn.execute("PRAGMA synchronous=NORMAL;")  # safe with WAL, faster than FULL
        conn.execute("PRAGMA foreign_keys=ON;")
        return conn

    def _init_db(self) -> None:
        """Create schema if not already present."""
        with self._lock:
            self._conn.executescript(_SCHEMA)

    # ------------------------------------------------------------------
    # StateStore protocol
    # ------------------------------------------------------------------

    def get_offset(self, resume_key: tuple[str, str]) -> int:
        """
        Return bytes already confirmed written for this job.

        Returns 0 if the job has never been seen or was never partially
        written (i.e. safe to pass directly as the Range: start offset).
        """
        source, dest = resume_key
        with self._lock:
            row = self._conn.execute(
                "SELECT offset FROM transfers WHERE source = ? AND dest = ?",
                (source, dest),
            ).fetchone()
        return int(row["offset"]) if row else 0

    def set_offset(self, resume_key: tuple[str, str], offset: int) -> None:
        """
        Upsert the byte offset for an in-progress transfer.

        Creates the row if absent (first chunk written), updates it on
        subsequent calls. Status is set/kept as 'in_progress'.
        """
        source, dest = resume_key
        with self._lock:
            self._conn.execute(
                """
                INSERT INTO transfers (source, dest, status, offset, updated_at)
                VALUES (?, ?, ?, ?, datetime('now'))
                ON CONFLICT (source, dest) DO UPDATE SET
                    offset     = excluded.offset,
                    status     = CASE
                                     WHEN status = ? THEN status  -- preserve 'complete'/'failed'
                                     ELSE ?
                                 END,
                    updated_at = datetime('now')
                """,
                (source, dest, _STATUS_IN_PROGRESS, offset, _STATUS_COMPLETE, _STATUS_IN_PROGRESS),
            )

    def mark_complete(self, resume_key: tuple[str, str]) -> None:
        """Record a job as successfully completed."""
        source, dest = resume_key
        with self._lock:
            self._conn.execute(
                """
                INSERT INTO transfers (source, dest, status, updated_at)
                VALUES (?, ?, ?, datetime('now'))
                ON CONFLICT (source, dest) DO UPDATE SET
                    status     = ?,
                    updated_at = datetime('now')
                """,
                (source, dest, _STATUS_COMPLETE, _STATUS_COMPLETE),
            )

    def mark_failed(self, resume_key: tuple[str, str], reason: str) -> None:
        """Record a job as permanently failed with a human-readable reason."""
        source, dest = resume_key
        with self._lock:
            self._conn.execute(
                """
                INSERT INTO transfers (source, dest, status, reason, updated_at)
                VALUES (?, ?, ?, ?, datetime('now'))
                ON CONFLICT (source, dest) DO UPDATE SET
                    status     = ?,
                    reason     = ?,
                    updated_at = datetime('now')
                """,
                (source, dest, _STATUS_FAILED, reason, _STATUS_FAILED, reason),
            )

    def is_complete(self, resume_key: tuple[str, str]) -> bool:
        """Return True if this job was previously completed successfully."""
        source, dest = resume_key
        with self._lock:
            row = self._conn.execute(
                "SELECT status FROM transfers WHERE source = ? AND dest = ?",
                (source, dest),
            ).fetchone()
        return bool(row and row["status"] == _STATUS_COMPLETE)

    # ------------------------------------------------------------------
    # Extras — useful for monitoring / debugging, not in protocol
    # ------------------------------------------------------------------

    def get_status(self, resume_key: tuple[str, str]) -> str | None:
        """
        Return the raw status string for a job, or None if not seen.

        Values: 'in_progress' | 'complete' | 'failed' | None
        """
        source, dest = resume_key
        with self._lock:
            row = self._conn.execute(
                "SELECT status FROM transfers WHERE source = ? AND dest = ?",
                (source, dest),
            ).fetchone()
        return str(row["status"]) if row else None

    def get_failed(self) -> list[dict[str, object]]:
        """
        Return all permanently failed transfers.

        Useful for post-run diagnostics:
            for row in store.get_failed():
                print(row["source"], row["reason"])
        """
        with self._lock:
            rows = self._conn.execute(
                "SELECT source, dest, reason, updated_at FROM transfers WHERE status = ?",
                (_STATUS_FAILED,),
            ).fetchall()
        return [dict(row) for row in rows]

    def close(self) -> None:
        """Close the database connection. Safe to call multiple times."""
        with self._lock:
            try:
                self._conn.close()
            except Exception:
                pass


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
