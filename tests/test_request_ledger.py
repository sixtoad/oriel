from __future__ import annotations

from pathlib import Path
import stat
import tempfile
import unittest

from oriel.adapters.request_ledger import SQLiteRequestLedger
from oriel.application.ports import RequestLedgerUnavailable, RequestStatusRecord


def record(request_id: str = "request-1", admitted_at: str = "2026-09-26T10:00:00Z") -> RequestStatusRecord:
    return RequestStatusRecord(request_id, "session-1", "trace-1", 2, "in_progress", None, admitted_at, "2026-09-27T10:00:00Z")


class SQLiteRequestLedgerTests(unittest.TestCase):
    def test_reserves_only_correlation_fields_and_persists_terminal_transition(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "ledger.sqlite3"
            ledger = SQLiteRequestLedger(path)
            ledger.reserve(record())
            ledger.mark_terminal("request-1", "completed")
            ledger.close()
            reopened = SQLiteRequestLedger(path)
            self.addCleanup(reopened.close)
            status = reopened.lookup("request-1", "2026-09-26T11:00:00Z")
            self.assertEqual(status, RequestStatusRecord("request-1", "session-1", "trace-1", 2, "terminal", "completed", "2026-09-26T10:00:00Z", "2026-09-27T10:00:00Z"))
            with reopened._connect() as connection:
                self.assertNotIn("prompt", {row[1] for row in connection.execute("PRAGMA table_info(request_ledger)")})
            self.assertEqual(stat.S_IMODE(path.stat().st_mode), stat.S_IRUSR | stat.S_IWUSR)

    def test_recovery_fails_unfinished_entries_without_creating_work(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "ledger.sqlite3"
            first = SQLiteRequestLedger(path)
            first.reserve(record())
            first.close()
            recovered = SQLiteRequestLedger(path)
            self.addCleanup(recovered.close)
            recovered.recover_interrupted()
            status = recovered.lookup("request-1", "2026-09-26T11:00:00Z")
            self.assertIsNotNone(status)
            self.assertEqual((status.state, status.outcome), ("terminal", "failed"))

    def test_expired_record_is_unavailable_but_active_record_is_retained_for_terminal_transition(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            ledger = SQLiteRequestLedger(Path(directory) / "ledger.sqlite3")
            self.addCleanup(ledger.close)
            ledger.reserve(record())
            self.assertIsNone(ledger.lookup("request-1", "2026-09-27T10:00:00Z"))
            ledger.mark_terminal("request-1", "failed")
            self.assertIsNone(ledger.lookup("request-1", "2026-09-27T10:00:01Z"))

    def test_one_active_owner_and_canonical_utc_timestamps_are_required(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "ledger.sqlite3"
            ledger = SQLiteRequestLedger(path)
            self.addCleanup(ledger.close)
            with self.assertRaises(RequestLedgerUnavailable):
                SQLiteRequestLedger(path)
            with self.assertRaises(RequestLedgerUnavailable):
                ledger.reserve(RequestStatusRecord("request-2", "session-1", "trace-1", 0, "in_progress", None, "2026-09-26T11:00:00+01:00", "2026-09-27T10:00:00Z"))


if __name__ == "__main__":
    unittest.main()
