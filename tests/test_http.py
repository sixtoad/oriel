from __future__ import annotations

from http.client import HTTPConnection
import json
import tempfile
from pathlib import Path
import unittest

from oriel.adapters.bootstrap import ThreadSafeSynchronization
from oriel.adapters.configuration import ResolvedProviderProfile, StaticProfileResolver, activate_startup
from oriel.adapters.http import HealthServer, ready_payload
from oriel.application.startup import StartupState
from oriel.domain.configuration import parse_core_config
from oriel.application.configuration import ConfigurationService


VALID_CONFIG = '{"api_version":"1.0","provider":{"connection_ref":"fake"},"skills":{}}'


def canonical_startup(path: str | Path):
    state, _profile = activate_startup(
        ConfigurationService(ThreadSafeSynchronization()),
        StaticProfileResolver({"fake": ResolvedProviderProfile("test")}),
        explicit_path=path,
    )
    return state


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
            server = self.with_server(canonical_startup(path))
            live_status, _headers, live_body = self.request(server, "/live")
            ready_status, headers, ready_body = self.request(server, "/ready")
        self.assertEqual(live_status, 200)
        self.assertEqual(json.loads(live_body), {"api_version": "1.0", "state": "live"})
        self.assertEqual(ready_status, 200)
        self.assertEqual(json.loads(ready_body), {"api_version": "1.0", "state": "ready", "components": {"core": {"state": "ready"}, "model": {"state": "ready"}, "ha": {"state": "disabled"}}})
        self.assertEqual(headers["Content-Type"], "application/json; charset=utf-8")

    def test_bad_config_leaves_live_up_and_ready_sanitized(self):
        with tempfile.TemporaryDirectory() as directory:
            private_path = Path(directory) / "PRIVATE_PATH_MUST_NOT_LEAK.json"
            private_path.write_text(
                '{"api_version":"1.0","provider":{"connection_ref":"PRIVATE_REFERENCE_MUST_NOT_LEAK","secret":"PRIVATE_PAYLOAD_MUST_NOT_LEAK"},"skills":{}}',
                encoding="utf-8",
            )
            server = self.with_server(canonical_startup(private_path))
            live_status, _live_headers, live_body = self.request(server, "/live")
            ready_status, _ready_headers, ready_body = self.request(server, "/ready")
        self.assertEqual(live_status, 200)
        self.assertEqual(json.loads(live_body)["state"], "live")
        self.assertEqual(ready_status, 503)
        self.assertEqual(json.loads(ready_body), {"api_version": "1.0", "state": "unready", "code": "config_unavailable", "components": {"core": {"state": "unready", "code": "config_unavailable"}, "model": {"state": "unready", "code": "model_unavailable"}, "ha": {"state": "disabled"}}})
        for private_value in ("PRIVATE_PATH_MUST_NOT_LEAK", "PRIVATE_REFERENCE_MUST_NOT_LEAK", "PRIVATE_PAYLOAD_MUST_NOT_LEAK"):
            self.assertNotIn(private_value, ready_body.decode("utf-8"))

    def test_only_health_paths_are_exposed(self):
        server = self.with_server(canonical_startup("missing.json"))
        status, _headers, body = self.request(server, "/v1/sessions")
        self.assertEqual(status, 404)
        self.assertEqual(body, b"")
        post_status, _headers, post_body = self.request(server, "/v1/sessions", "POST")
        self.assertEqual(post_status, 404)
        self.assertEqual(post_body, b"")

    def test_ready_health_keeps_optional_ha_degradation_out_of_chat_readiness(self):
        config = parse_core_config({"api_version": "1.0", "provider": {"connection_ref": "safe"}, "skills": {}})
        ready = ready_payload(StartupState(config, None, optional_ha_state="degraded"))
        model_unready = ready_payload(StartupState(config, "model_unavailable", model_ready=False, model_code="model_unavailable"))
        self.assertEqual(ready, {"api_version": "1.0", "state": "ready", "components": {"core": {"state": "ready"}, "model": {"state": "ready"}, "ha": {"state": "degraded"}}})
        self.assertEqual(model_unready["state"], "unready")
        self.assertEqual(model_unready["code"], "model_unavailable")
        self.assertEqual(model_unready["components"]["ha"], {"state": "disabled"})

    def test_model_unready_health_is_a_sanitized_503(self):
        config = parse_core_config({"api_version": "1.0", "provider": {"connection_ref": "safe"}, "skills": {}})
        server = self.with_server(StartupState(config, "model_unavailable", model_ready=False, model_code="model_unavailable"))
        status, _headers, body = self.request(server, "/ready")
        self.assertEqual(status, 503)
        self.assertEqual(json.loads(body)["components"]["model"], {"state": "unready", "code": "model_unavailable"})


if __name__ == "__main__":
    unittest.main()
