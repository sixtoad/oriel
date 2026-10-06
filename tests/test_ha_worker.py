"""Focused checks for the private, availability-only HA worker boundary."""
from __future__ import annotations

import os
from pathlib import Path
import socket
import subprocess
import sys
import tempfile
from threading import Thread
import time
import unittest

from oriel.adapters.ha_worker import MAX_FRAME_BYTES, UnixHaWorkerClient, availability_request, unavailable_response
from oriel.adapters.ha_worker_process import CONNECTION_INPUT_ENV, serve_once
from oriel.adapters.configuration import synthetic_ha_fact_reader
from oriel.__main__ import _compose_startup
from oriel.domain.ha_manifest import validate_ha_fact_request, canonical_ha_fact_request


ROOT = Path(__file__).resolve().parents[1]


class HaWorkerTests(unittest.TestCase):
    def worker(self, channel: Path, connection_input: str | None) -> subprocess.Popen[str]:
        environ = {} if connection_input is None else {CONNECTION_INPUT_ENV: connection_input}
        process = subprocess.Popen(
            [sys.executable, "-m", "oriel.adapters.ha_worker_process", "--channel", str(channel)],
            cwd=ROOT,
            env=environ,
            text=True,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.PIPE,
        )
        self.addCleanup(self.stop_worker, process, channel)
        deadline = time.monotonic() + 2
        while not channel.exists() and process.poll() is None and time.monotonic() < deadline:
            time.sleep(0.01)
        if not channel.exists():
            detail = process.stderr.read() if process.stderr is not None else "worker did not start"
            self.fail(detail or "worker did not start")
        return process

    def stop_worker(self, process: subprocess.Popen[str], channel: Path) -> None:
        if process.poll() is None:
            process.terminate()
            process.wait(timeout=2)
        if process.stderr is not None:
            process.stderr.close()
        channel.unlink(missing_ok=True)

    def server_once(self, channel: Path, response: bytes | None) -> Thread:
        def serve() -> None:
            with socket.socket(socket.AF_UNIX, socket.SOCK_STREAM) as listener:
                listener.bind(str(channel))
                os.chmod(channel, 0o600)
                listener.listen(1)
                with listener.accept()[0] as connection:
                    connection.recv(MAX_FRAME_BYTES)
                    if response is None:
                        time.sleep(0.1)
                    else:
                        connection.sendall(response)

        thread = Thread(target=serve, daemon=True)
        thread.start()
        deadline = time.monotonic() + 2
        while not channel.exists() and time.monotonic() < deadline:
            time.sleep(0.01)
        self.assertTrue(channel.exists())
        self.addCleanup(channel.unlink, missing_ok=True)
        return thread

    def test_separate_worker_owns_canary_and_gateway_observes_only_availability(self) -> None:
        canary = "CANARY_MUST_NOT_CROSS_HA_WORKER_BOUNDARY"
        with tempfile.TemporaryDirectory() as directory:
            channel = Path(directory) / "ha-worker.sock"
            process = self.worker(channel, canary)
            result = UnixHaWorkerClient(channel).availability()
            startup, _profile = _compose_startup(environ={"ORIEL_HA_WORKER_CHANNEL": str(channel)})

            self.assertIsNone(process.poll())
            self.assertEqual(result.state, "ready")
            self.assertTrue(startup.ready)
            self.assertEqual(startup.components["ha"], {"state": "ready"})
            self.assertEqual(os.stat(channel).st_mode & 0o777, 0o600)
            self.assertNotIn(canary, repr(result))
            self.assertNotIn(canary, str(startup.components))
            self.assertNotIn(CONNECTION_INPUT_ENV, {"ORIEL_HA_WORKER_CHANNEL": str(channel)})
            self.assertNotIn(canary, " ".join(map(str, process.args)))

    def test_missing_worker_input_and_stopped_worker_degrade_only_ha(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            channel = Path(directory) / "ha-worker.sock"
            self.worker(channel, None)
            unavailable = UnixHaWorkerClient(channel).availability()
            missing_startup, _profile = _compose_startup(environ={"ORIEL_HA_WORKER_CHANNEL": str(channel)})
            stopped_startup, _profile = _compose_startup(environ={"ORIEL_HA_WORKER_CHANNEL": str(Path(directory) / "stopped.sock")})

            self.assertEqual(unavailable.state, "unavailable")
            self.assertTrue(missing_startup.ready)
            self.assertEqual(missing_startup.components["ha"], {"state": "degraded"})
            self.assertTrue(stopped_startup.ready)
            self.assertEqual(stopped_startup.components["ha"], {"state": "degraded"})

    def test_invalid_present_worker_input_degrades_only_ha(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            channel = Path(directory) / "ha-worker.sock"
            self.worker(channel, "")
            unavailable = UnixHaWorkerClient(channel).availability()
            startup, _profile = _compose_startup(environ={"ORIEL_HA_WORKER_CHANNEL": str(channel)})

            self.assertEqual(unavailable.state, "unavailable")
            self.assertTrue(startup.ready)
            self.assertEqual(startup.components["ha"], {"state": "degraded"})

    def test_malformed_oversized_and_canary_bearing_responses_are_sanitized(self) -> None:
        canary = "CANARY_MUST_NOT_APPEAR_IN_GATEWAY_OUTPUT"
        cases = (
            b"not-json\n",
            (b"x" * MAX_FRAME_BYTES) + b"\n",
            ('{"version":"1","state":"ready","detail":"' + canary + '"}\n').encode("utf-8"),
        )
        for index, response in enumerate(cases):
            with self.subTest(response=index), tempfile.TemporaryDirectory() as directory:
                channel = Path(directory) / "ha-worker.sock"
                thread = self.server_once(channel, response)
                result = UnixHaWorkerClient(channel).availability()
                thread.join(timeout=2)

                self.assertEqual(result.state, "unavailable")
                self.assertNotIn(canary, repr(result))

    def test_nonresponsive_worker_times_out_to_the_bounded_unavailable_state(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            channel = Path(directory) / "ha-worker.sock"
            thread = self.server_once(channel, None)
            started = time.monotonic()
            result = UnixHaWorkerClient(channel, timeout_seconds=0.01).availability()
            elapsed = time.monotonic() - started
            thread.join(timeout=2)

            self.assertEqual(result.state, "unavailable")
            self.assertLess(elapsed, 0.2)

    def test_dripping_worker_cannot_extend_the_client_deadline(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            channel = Path(directory) / "ha-worker.sock"

            def drip() -> None:
                with socket.socket(socket.AF_UNIX, socket.SOCK_STREAM) as listener:
                    listener.bind(str(channel))
                    os.chmod(channel, 0o600)
                    listener.listen(1)
                    with listener.accept()[0] as connection:
                        connection.recv(MAX_FRAME_BYTES)
                        for byte in b'{"version":"1"':
                            try:
                                connection.sendall(bytes((byte,)))
                            except BrokenPipeError:
                                return
                            time.sleep(0.01)

            thread = Thread(target=drip, daemon=True)
            thread.start()
            deadline = time.monotonic() + 2
            while not channel.exists() and time.monotonic() < deadline:
                time.sleep(0.01)
            started = time.monotonic()
            result = UnixHaWorkerClient(channel, timeout_seconds=0.03).availability()
            elapsed = time.monotonic() - started
            thread.join(timeout=2)

            self.assertEqual(result.state, "unavailable")
            self.assertLess(elapsed, 0.12)

    def test_worker_replacement_requires_restart_and_keeps_input_out_of_the_channel(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            channel = Path(directory) / "ha-worker.sock"
            first = self.worker(channel, "FIRST_CANARY")
            self.assertEqual(UnixHaWorkerClient(channel).availability().state, "ready")
            self.stop_worker(first, channel)
            second = self.worker(channel, "SECOND_CANARY")
            self.assertEqual(UnixHaWorkerClient(channel).availability().state, "ready")
            self.assertNotIn("SECOND_CANARY", repr(UnixHaWorkerClient(channel).availability()))
            self.assertIsNone(second.poll())

    def test_worker_termination_removes_the_channel_before_replacement(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            channel = Path(directory) / "ha-worker.sock"
            first = self.worker(channel, "FIRST_CANARY")
            first.terminate()
            self.assertEqual(first.wait(timeout=2), 0)
            self.assertFalse(channel.exists())
            replacement = self.worker(channel, "SECOND_CANARY")
            self.assertEqual(UnixHaWorkerClient(channel).availability().state, "ready")
            self.assertIsNone(replacement.poll())

    def test_one_shot_worker_removes_its_channel_after_a_normal_request(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            channel = Path(directory) / "ha-worker.sock"
            thread = Thread(target=serve_once, args=(channel, {CONNECTION_INPUT_ENV: "CANARY"}), daemon=True)
            thread.start()
            deadline = time.monotonic() + 2
            while not channel.exists() and time.monotonic() < deadline:
                time.sleep(0.01)
            self.assertEqual(UnixHaWorkerClient(channel).availability().state, "ready")
            thread.join(timeout=2)
            self.assertFalse(channel.exists())

    def test_client_rejects_a_channel_in_a_directory_writable_by_other_users(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            channel = root / "ha-worker.sock"
            self.worker(channel, "CANARY")
            os.chmod(root, 0o777)
            try:
                self.assertEqual(UnixHaWorkerClient(channel).availability().state, "unavailable")
            finally:
                os.chmod(root, 0o700)

    def test_closed_protocol_rejects_unexpected_request_without_echoing_it(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            channel = Path(directory) / "ha-worker.sock"
            self.worker(channel, "CANARY")
            with socket.socket(socket.AF_UNIX, socket.SOCK_STREAM) as connection:
                connection.connect(str(channel))
                connection.sendall(b'{"version":"1","type":"unexpected","detail":"CANARY"}\n')
                response = connection.recv(MAX_FRAME_BYTES)

            self.assertEqual(response, unavailable_response())
            self.assertNotIn(b"CANARY", response)
            self.assertNotEqual(response, availability_request())

    def test_worker_unavailable_fact_reader_returns_only_the_bounded_limitation(self) -> None:
        request = validate_ha_fact_request(canonical_ha_fact_request()).request
        self.assertIsNotNone(request)
        fact = synthetic_ha_fact_reader("degraded").read(request)

        self.assertEqual(dict(fact.payload()), {"power_state": None, "observed_at": None, "freshness": "unavailable"})
        self.assertNotIn("CANARY", repr(fact))


if __name__ == "__main__":
    unittest.main()
