"""SQLite persistence for payload-free request correlation state."""
from __future__ import annotations

import sqlite3
import os
import stat
from datetime import datetime, timezone
from pathlib import Path

import fcntl

from ..application.ports import RequestLedgerUnavailable, RequestStatusRecord


class SQLiteRequestLedger:
    """A small local ledger that stores no request, context, or model material."""

    def __init__(self, path: str | Path) -> None:
        self._path = Path(path)
        self._lock_file = None
        try:
            self._acquire_owner_lock()
            with self._connect() as connection:
                connection.execute(
                    """
                    CREATE TABLE IF NOT EXISTS request_ledger (
                        request_id TEXT PRIMARY KEY,
                        session_id TEXT NOT NULL,
                        trace_id TEXT NOT NULL,
                        context_generation INTEGER NOT NULL,
                        state TEXT NOT NULL CHECK(state IN ('in_progress', 'terminal')),
                        outcome TEXT,
                        admitted_at TEXT NOT NULL,
                        expires_at TEXT NOT NULL
                    )
                    """
                )
                connection.execute("CREATE INDEX IF NOT EXISTS request_ledger_expiry ON request_ledger(expires_at)")
            os.chmod(self._path, stat.S_IRUSR | stat.S_IWUSR)
        except (OSError, sqlite3.Error):
            self.close()
            raise RequestLedgerUnavailable() from None

    def reserve(self, record: RequestStatusRecord) -> None:
        try:
            _canonical_utc(record.admitted_at)
            _canonical_utc(record.expires_at)
            with self._connect() as connection:
                connection.execute("BEGIN IMMEDIATE")
                connection.execute(
                    "INSERT INTO request_ledger (request_id, session_id, trace_id, context_generation, state, outcome, admitted_at, expires_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
                    (record.request_id, record.session_id, record.trace_id, record.context_generation, "in_progress", None, record.admitted_at, record.expires_at),
                )
        except (OSError, sqlite3.Error, ValueError):
            raise RequestLedgerUnavailable() from None

    def mark_terminal(self, request_id: str, outcome: str) -> str:
        try:
            with self._connect() as connection:
                connection.execute("BEGIN IMMEDIATE")
                connection.execute(
                    "UPDATE request_ledger SET state = 'terminal', outcome = ? WHERE request_id = ? AND state = 'in_progress'",
                    (outcome, request_id),
                )
                row = connection.execute("SELECT outcome FROM request_ledger WHERE request_id = ?", (request_id,)).fetchone()
                if row is None:
                    raise sqlite3.DatabaseError("missing request")
                return row[0]
        except (OSError, sqlite3.Error):
            raise RequestLedgerUnavailable() from None

    def lookup(self, request_id: str, now: str) -> RequestStatusRecord | None:
        try:
            now = _canonical_utc(now)
            with self._connect() as connection:
                connection.execute("BEGIN IMMEDIATE")
                row = connection.execute(
                    "SELECT request_id, session_id, trace_id, context_generation, state, outcome, admitted_at, expires_at FROM request_ledger WHERE request_id = ?",
                    (request_id,),
                ).fetchone()
                if row is not None and row[7] <= now:
                    if row[4] == "terminal":
                        connection.execute("DELETE FROM request_ledger WHERE request_id = ?", (request_id,))
                    return None
        except (OSError, sqlite3.Error, ValueError):
            raise RequestLedgerUnavailable() from None
        return RequestStatusRecord(*row) if row is not None else None

    def recover_interrupted(self, committed_request_ids: tuple[str, ...] = ()) -> None:
        try:
            with self._connect() as connection:
                connection.execute("BEGIN IMMEDIATE")
                for request_id in committed_request_ids:
                    connection.execute("UPDATE request_ledger SET state = 'terminal', outcome = 'outcome_unknown' WHERE request_id = ? AND state = 'in_progress'", (request_id,))
                connection.execute("UPDATE request_ledger SET state = 'terminal', outcome = 'failed' WHERE state = 'in_progress'")
        except (OSError, sqlite3.Error):
            raise RequestLedgerUnavailable() from None

    def _connect(self) -> sqlite3.Connection:
        connection = sqlite3.connect(self._path, timeout=5, isolation_level=None)
        connection.execute("PRAGMA journal_mode=WAL")
        return connection

    def _acquire_owner_lock(self) -> None:
        lock_path = self._path.with_name(f"{self._path.name}.lock")
        descriptor = os.open(lock_path, os.O_CREAT | os.O_RDWR, stat.S_IRUSR | stat.S_IWUSR)
        try:
            os.chmod(lock_path, stat.S_IRUSR | stat.S_IWUSR)
            fcntl.flock(descriptor, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except OSError:
            os.close(descriptor)
            raise
        self._lock_file = descriptor

    def close(self) -> None:
        if self._lock_file is not None:
            fcntl.flock(self._lock_file, fcntl.LOCK_UN)
            os.close(self._lock_file)
            self._lock_file = None


def _canonical_utc(value: str) -> str:
    if not isinstance(value, str) or not value.endswith("Z"):
        raise ValueError("timestamp is not canonical UTC")
    parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    if parsed.tzinfo is None or parsed.utcoffset() != timezone.utc.utcoffset(parsed):
        raise ValueError("timestamp is not canonical UTC")
    canonical = parsed.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    if value != canonical:
        raise ValueError("timestamp is not canonical UTC")
    return canonical
