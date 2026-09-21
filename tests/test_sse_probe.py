"""Validate the local direct SSE probe without a gateway dependency."""
import json
from pathlib import Path
import subprocess
import sys
import unittest

ROOT = Path(__file__).resolve().parents[1]
PROBE = ROOT / "scripts" / "sse_probe.py"


class SseProbeTests(unittest.TestCase):
    def test_self_test_delivers_first_event_and_stops_on_disconnect(self):
        result = subprocess.run([sys.executable, str(PROBE), "--self-test"], cwd=ROOT,
                                text=True, capture_output=True, timeout=10)
        self.assertEqual(result.returncode, 0, result.stdout + result.stderr)
        self.assertEqual(result.stderr, "")
        evidence = json.loads(result.stdout)
        self.assertEqual(evidence["disconnect_eof_observed"], True)
        self.assertEqual(evidence["first_before_delayed_second"], True)
        self.assertEqual(evidence["disconnect_stopped"], True)
        self.assertGreaterEqual(evidence["first_event_ms"], 0)


if __name__ == "__main__":
    unittest.main()
