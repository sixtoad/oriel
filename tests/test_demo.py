from __future__ import annotations

import json
from pathlib import Path
import subprocess
import sys
import unittest


ROOT = Path(__file__).resolve().parents[1]
DEMO = ROOT / "scripts" / "demo_text_gateway.py"


class DemoTests(unittest.TestCase):
    def test_clean_checkout_demo_is_stable_and_local(self):
        result = subprocess.run(
            [sys.executable, str(DEMO), "--self-test"],
            cwd=ROOT,
            text=True,
            capture_output=True,
            timeout=10,
        )
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertEqual(result.stderr, "")
        self.assertEqual(
            result.stdout,
            '{"api_version":"1.0","fake_turn":{"text":"The fake model is ready."},"live":{"api_version":"1.0","state":"live"},"ready":{"api_version":"1.0","state":"ready"}}\n',
        )
        self.assertEqual(json.loads(result.stdout)["ready"]["state"], "ready")

    def test_demo_blocks_non_loopback_transports(self):
        guard = """
import runpy
import socket
import sys
original = socket.create_connection
def loopback_only(address, *args, **kwargs):
    if address[0] != '127.0.0.1':
        raise AssertionError('non-loopback transport attempted')
    return original(address, *args, **kwargs)
socket.create_connection = loopback_only
sys.argv = [sys.argv[1], '--self-test']
runpy.run_path(sys.argv[0], run_name='__main__')
"""
        result = subprocess.run([sys.executable, "-c", guard, str(DEMO)], cwd=ROOT, text=True, capture_output=True, timeout=10)
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertEqual(json.loads(result.stdout)["fake_turn"]["text"], "The fake model is ready.")

    def test_demo_has_no_external_provider_or_home_assistant_dependency(self):
        source = DEMO.read_text(encoding="utf-8") + (ROOT / "oriel" / "adapters.py").read_text(encoding="utf-8")
        self.assertNotIn("requests", source)
        self.assertNotIn("homeassistant", source.lower())


if __name__ == "__main__":
    unittest.main()
