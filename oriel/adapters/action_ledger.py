"""Durable payload-free action reservation and audit storage."""
from __future__ import annotations

import fcntl
import json
import os
import sqlite3
import stat
import re
from datetime import datetime, timezone
from pathlib import Path

from ..application.ports import ActionExecutionResult, ActionLedgerUnavailable, ActionReadiness, ActionRecord, ActionReservation
from ..domain.ha_manifest import CAPABILITY_ID, MANIFEST_REVISION


class SQLiteActionLedger:
    """Owns action identity, pre-dispatch audit, and bounded recovery state.

    Request lifecycle state intentionally remains in ``SQLiteRequestLedger``.
    """

    def __init__(self, path: str | Path) -> None:
        self._path = Path(path)
        self._lock_file: int | None = None
        self._degraded = False
        try:
            self._acquire_owner_lock()
            with self._connect() as connection:
                connection.execute("BEGIN IMMEDIATE")
                self._migrate(connection)
                connection.execute(
                    """
                    CREATE TABLE IF NOT EXISTS action_ledger (
                        action_id TEXT PRIMARY KEY,
                        request_id TEXT NOT NULL UNIQUE,
                        trace_id TEXT NOT NULL,
                        manifest_revision TEXT NOT NULL,
                        capability_id TEXT NOT NULL,
                        operation_fingerprint TEXT NOT NULL,
                        state TEXT NOT NULL CHECK(state IN ('reserved', 'fake_attempted', 'outcome_unknown', 'confirmed', 'denied', 'failed')),
                        reserved_at TEXT NOT NULL,
                        updated_at TEXT NOT NULL,
                        result_json TEXT
                    )
                    """
                )
                connection.execute(
                    """
                    CREATE TABLE IF NOT EXISTS action_audit (
                        audit_id INTEGER PRIMARY KEY,
                        action_id TEXT NOT NULL,
                        event TEXT NOT NULL CHECK(event IN ('reserved', 'fake_attempted', 'outcome_unknown', 'confirmed', 'denied', 'failed')),
                        occurred_at TEXT NOT NULL,
                        FOREIGN KEY(action_id) REFERENCES action_ledger(action_id)
                    )
                    """
                )
                connection.execute("CREATE INDEX IF NOT EXISTS action_audit_action ON action_audit(action_id, audit_id)")
                connection.execute("CREATE TABLE IF NOT EXISTS action_readiness (singleton INTEGER PRIMARY KEY CHECK(singleton = 1), state TEXT NOT NULL CHECK(state IN ('ready', 'degraded')))")
                connection.execute("INSERT OR IGNORE INTO action_readiness (singleton, state) VALUES (1, 'ready')")
            os.chmod(self._path, stat.S_IRUSR | stat.S_IWUSR)
        except (OSError, sqlite3.Error):
            self.close()
            raise ActionLedgerUnavailable() from None

    def reserve_and_audit(self, record: ActionRecord) -> ActionReservation:
        try:
            _validate_record(record)
            with self._connect() as connection:
                connection.execute("BEGIN IMMEDIATE")
                existing = connection.execute(
                    "SELECT action_id, request_id, trace_id, manifest_revision, capability_id, operation_fingerprint, state, reserved_at, updated_at, result_json "
                    "FROM action_ledger WHERE request_id = ?",
                    (record.request_id,),
                ).fetchone()
                if existing is not None:
                    existing_record = _record(existing)
                    status = "duplicate" if existing_record.operation_fingerprint == record.operation_fingerprint else "conflict"
                    return ActionReservation(status, existing_record)
                readiness = connection.execute("SELECT state FROM action_readiness WHERE singleton = 1").fetchone()
                if self._degraded or readiness is None or readiness[0] != "ready":
                    raise sqlite3.DatabaseError("action readiness degraded")
                connection.execute(
                    "INSERT INTO action_ledger (action_id, request_id, trace_id, manifest_revision, capability_id, operation_fingerprint, state, reserved_at, updated_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
                    (
                        record.action_id, record.request_id, record.trace_id,
                        record.manifest_revision, record.capability_id,
                        record.operation_fingerprint, "reserved", record.reserved_at,
                        record.updated_at,
                    ),
                )
                connection.execute(
                    "INSERT INTO action_audit (action_id, event, occurred_at) VALUES (?, 'reserved', ?)",
                    (record.action_id, record.reserved_at),
                )
            return ActionReservation("reserved", record)
        except (OSError, sqlite3.Error, ValueError):
            self.degrade()
            raise ActionLedgerUnavailable() from None

    def mark_execution_result(self, action_id: str, occurred_at: str, result: ActionExecutionResult) -> ActionRecord:
        if type(result) is not ActionExecutionResult:
            raise ActionLedgerUnavailable()
        return self._transition(action_id, occurred_at, result.status, result)

    def mark_fake_attempt(self, action_id: str, occurred_at: str) -> ActionRecord:
        return self._transition(action_id, occurred_at, "fake_attempted")

    def mark_outcome_unknown(self, action_id: str, occurred_at: str) -> ActionRecord:
        return self._transition(action_id, occurred_at, "outcome_unknown")

    def readiness(self) -> ActionReadiness:
        try:
            with self._connect() as connection:
                row = connection.execute("SELECT state FROM action_readiness WHERE singleton = 1").fetchone()
            return ActionReadiness("degraded" if self._degraded or row is None or row[0] == "degraded" else "ready")
        except (OSError, sqlite3.Error):
            return ActionReadiness("degraded")

    def degrade(self) -> None:
        self._degraded = True
        try:
            with self._connect() as connection:
                connection.execute("UPDATE action_readiness SET state = 'degraded' WHERE singleton = 1")
        except (OSError, sqlite3.Error):
            pass

    def recover_unresolved(self, occurred_at: str) -> None:
        """Conservatively classify an interrupted reservation without retrying it."""
        try:
            _canonical_utc(occurred_at)
            with self._connect() as connection:
                connection.execute("BEGIN IMMEDIATE")
                rows = connection.execute("SELECT action_id FROM action_ledger WHERE state = 'reserved'").fetchall()
                for (action_id,) in rows:
                    connection.execute("UPDATE action_ledger SET state = 'outcome_unknown', updated_at = ? WHERE action_id = ?", (occurred_at, action_id))
                    connection.execute("INSERT INTO action_audit (action_id, event, occurred_at) VALUES (?, 'outcome_unknown', ?)", (action_id, occurred_at))
        except (OSError, sqlite3.Error, ValueError):
            self.degrade()
            raise ActionLedgerUnavailable() from None

    def committed_request_ids(self) -> tuple[str, ...]:
        try:
            with self._connect() as connection:
                return tuple(row[0] for row in connection.execute("SELECT request_id FROM action_ledger ORDER BY request_id"))
        except (OSError, sqlite3.Error):
            self.degrade()
            raise ActionLedgerUnavailable() from None

    def audit_events(self, action_id: str) -> tuple[tuple[str, str], ...]:
        """Internal test seam; no application/API audit-query route exists."""
        try:
            with self._connect() as connection:
                return tuple(connection.execute("SELECT event, occurred_at FROM action_audit WHERE action_id = ? ORDER BY audit_id", (action_id,)))
        except (OSError, sqlite3.Error):
            raise ActionLedgerUnavailable() from None

    def _transition(self, action_id: str, occurred_at: str, state: str, result: ActionExecutionResult | None = None) -> ActionRecord:
        try:
            _canonical_utc(occurred_at)
            with self._connect() as connection:
                connection.execute("BEGIN IMMEDIATE")
                existing = connection.execute("SELECT action_id, request_id, trace_id, manifest_revision, capability_id, operation_fingerprint, state, reserved_at, updated_at, result_json FROM action_ledger WHERE action_id = ?", (action_id,)).fetchone()
                existing_record = _record(existing) if existing is not None else None
                if existing_record is not None and (existing_record.state in {state, "outcome_unknown"}
                        or state == "outcome_unknown" and existing_record.state in {"confirmed", "denied", "failed"}):
                    return existing_record
                expected = "reserved', 'fake_attempted" if state == "outcome_unknown" else "reserved"
                cursor = connection.execute(
                    f"UPDATE action_ledger SET state = ?, updated_at = ?, result_json = ? WHERE action_id = ? AND state IN ('{expected}')",
                    (state, occurred_at, json.dumps(result.payload(), separators=(",", ":")) if result else None, action_id),
                )
                if cursor.rowcount != 1:
                    raise sqlite3.DatabaseError("missing action")
                connection.execute("INSERT INTO action_audit (action_id, event, occurred_at) VALUES (?, ?, ?)", (action_id, state, occurred_at))
                row = connection.execute(
                    "SELECT action_id, request_id, trace_id, manifest_revision, capability_id, operation_fingerprint, state, reserved_at, updated_at, result_json FROM action_ledger WHERE action_id = ?",
                    (action_id,),
                ).fetchone()
            return _record(row)
        except (OSError, sqlite3.Error, ValueError):
            self.degrade()
            raise ActionLedgerUnavailable() from None

    @staticmethod
    def _migrate(connection: sqlite3.Connection) -> None:
        """Replace v1 CHECK constraints and copy both tables in one transaction."""
        columns = connection.execute("PRAGMA table_info(action_ledger)").fetchall()
        if not columns or "result_json" in {row[1] for row in columns}:
            return
        connection.execute("CREATE TABLE action_ledger_v2 (action_id TEXT PRIMARY KEY, request_id TEXT NOT NULL UNIQUE, trace_id TEXT NOT NULL, manifest_revision TEXT NOT NULL, capability_id TEXT NOT NULL, operation_fingerprint TEXT NOT NULL, state TEXT NOT NULL CHECK(state IN ('reserved','fake_attempted','outcome_unknown','confirmed','denied','failed')), reserved_at TEXT NOT NULL, updated_at TEXT NOT NULL, result_json TEXT)")
        connection.execute("INSERT INTO action_ledger_v2 SELECT *, NULL FROM action_ledger")
        connection.execute("CREATE TABLE action_audit_v2 (audit_id INTEGER PRIMARY KEY, action_id TEXT NOT NULL, event TEXT NOT NULL CHECK(event IN ('reserved','fake_attempted','outcome_unknown','confirmed','denied','failed')), occurred_at TEXT NOT NULL, FOREIGN KEY(action_id) REFERENCES action_ledger(action_id))")
        connection.execute("INSERT INTO action_audit_v2 SELECT * FROM action_audit")
        connection.execute("DROP TABLE action_audit")
        connection.execute("DROP TABLE action_ledger")
        connection.execute("ALTER TABLE action_ledger_v2 RENAME TO action_ledger")
        connection.execute("ALTER TABLE action_audit_v2 RENAME TO action_audit")

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


def _validate_record(record: ActionRecord) -> None:
    if record.state != "reserved" or record.reserved_at != record.updated_at:
        raise ValueError("invalid new action reservation")
    _canonical_utc(record.reserved_at)
    if record.manifest_revision != MANIFEST_REVISION or record.capability_id != CAPABILITY_ID:
        raise ValueError("unreviewed action capability")
    if any(re.fullmatch(r"[A-Za-z0-9._~-]{1,128}", value) is None for value in (record.action_id, record.request_id, record.trace_id)):
        raise ValueError("invalid action identifiers")
    if len(record.operation_fingerprint) != 64 or any(character not in "0123456789abcdef" for character in record.operation_fingerprint):
        raise ValueError("invalid action fingerprint")


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


def _record(row: tuple) -> ActionRecord:
    try:
        result = None
        if row[9] is not None:
            material = json.loads(row[9])
            if type(material) is not dict or set(material) != {"status", "reason", "evidence", "power_state", "observed_at"}:
                raise ValueError("invalid durable evidence")
            result = ActionExecutionResult(**material)
            if result.status != row[6]:
                raise ValueError("invalid durable evidence")
        elif row[6] in {"confirmed", "denied", "failed"}:
            raise ValueError("missing durable evidence")
        return ActionRecord(*row[:9], result=result)
    except (TypeError, ValueError, OverflowError, RecursionError):
        raise ValueError("invalid durable evidence") from None
