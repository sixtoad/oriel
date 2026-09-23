from __future__ import annotations

from http.client import HTTPConnection
import json
import tempfile
from pathlib import Path
import unittest

from oriel.config import load_startup
from oriel.http import HealthServer


VALID_CONFIG = '{"api_version":"1.0","provider":{"connection_ref":"fake"},"skills":{}}'


class HttpTests(unittest.TestCase):
    def request(self, server: HealthServer, path: str, method: str = "GET"):
        host, port = server.address
        connection = HTTPConnection(host, port, timeout=2)
        try:
            connection.request(method, path)
            response = connection.getresponse()
            return response.status, dict(response.getheaders()), response.read()
        finally:
            connection.close()

    def with_server(self, startup):
        server = HealthServer(startup)
        server.start()
        self.addCleanup(server.close)
        return server

    def test_ready_health_exposes_only_schema_fields(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "valid.json"
            path.write_text(VALID_CONFIG, encoding="utf-8")
            server = self.with_server(load_startup(path))
            live_status, _headers, live_body = self.request(server, "/live")
            ready_status, headers, ready_body = self.request(server, "/ready")
        self.assertEqual(live_status, 200)
        self.assertEqual(json.loads(live_body), {"api_version": "1.0", "state": "live"})
        self.assertEqual(ready_status, 200)
        self.assertEqual(json.loads(ready_body), {"api_version": "1.0", "state": "ready"})
        self.assertEqual(headers["Content-Type"], "application/json; charset=utf-8")

    def test_bad_config_leaves_live_up_and_ready_sanitized(self):
        with tempfile.TemporaryDirectory() as directory:
            private_path = Path(directory) / "PRIVATE_PATH_MUST_NOT_LEAK.json"
            private_path.write_text(
                '{"api_version":"1.0","provider":{"connection_ref":"PRIVATE_REFERENCE_MUST_NOT_LEAK","secret":"PRIVATE_PAYLOAD_MUST_NOT_LEAK"},"skills":{}}',
                encoding="utf-8",
            )
            server = self.with_server(load_startup(private_path))
            live_status, _live_headers, live_body = self.request(server, "/live")
            ready_status, _ready_headers, ready_body = self.request(server, "/ready")
        self.assertEqual(live_status, 200)
        self.assertEqual(json.loads(live_body)["state"], "live")
        self.assertEqual(ready_status, 503)
        self.assertEqual(json.loads(ready_body), {"api_version": "1.0", "state": "unready", "code": "config_unavailable"})
        for private_value in ("PRIVATE_PATH_MUST_NOT_LEAK", "PRIVATE_REFERENCE_MUST_NOT_LEAK", "PRIVATE_PAYLOAD_MUST_NOT_LEAK"):
            self.assertNotIn(private_value, ready_body.decode("utf-8"))

    def test_only_health_paths_are_exposed(self):
        server = self.with_server(load_startup("missing.json"))
        status, _headers, body = self.request(server, "/v1/sessions")
        self.assertEqual(status, 404)
        self.assertEqual(body, b"")
        post_status, _headers, post_body = self.request(server, "/v1/sessions", "POST")
        self.assertEqual(post_status, 404)
        self.assertEqual(post_body, b"")


if __name__ == "__main__":
    unittest.main()
