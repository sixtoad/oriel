#!/usr/bin/env python3
"""Disposable local SSE delivery and disconnect probe; never a gateway."""
from __future__ import annotations

import argparse
import json
import select
import socket
import threading
import time
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer


class ProbeState:
    def __init__(self):
        self.first_sent = None
        self.second_sent = None
        self.disconnect_eof = threading.Event()
        self.producer_done = threading.Event()


def make_handler(state):
    class Handler(BaseHTTPRequestHandler):
        def log_message(self, _format, *_args):
            pass

        def do_GET(self):
            if self.path != "/probe":
                self.send_error(404)
                return
            self.send_response(200)
            self.send_header("Content-Type", "text/event-stream")
            self.send_header("Cache-Control", "no-cache")
            self.end_headers()
            try:
                self.wfile.write(b"event: first\ndata: {\"seq\":1}\n\n")
                self.wfile.flush()
                state.first_sent = time.monotonic()
                deadline = time.monotonic() + 0.25
                while time.monotonic() < deadline:
                    readable, _, _ = select.select([self.connection], [], [], 0.02)
                    if readable:
                        data = self.connection.recv(1, socket.MSG_PEEK)
                        if not data:
                            state.disconnect_eof.set()
                            return
                self.wfile.write(b"event: second\ndata: {\"seq\":2}\n\n")
                self.wfile.flush()
                state.second_sent = time.monotonic()
            except (BrokenPipeError, ConnectionResetError, OSError):
                pass
            finally:
                state.producer_done.set()
    return Handler


def self_test():
    state = ProbeState()
    server = ThreadingHTTPServer(("127.0.0.1", 0), make_handler(state))
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    try:
        connection = socket.create_connection(("127.0.0.1", server.server_port), timeout=2)
        connection.sendall(b"GET /probe HTTP/1.1\r\nHost: localhost\r\nAccept: text/event-stream\r\nConnection: close\r\n\r\n")
        started = time.monotonic()
        first = b""
        while b"event: first\n" not in first:
            chunk = connection.recv(1024)
            if not chunk:
                break
            first += chunk
        arrived = time.monotonic()
        connection.shutdown(socket.SHUT_WR)
        observed_eof = state.disconnect_eof.wait(2)
        connection.close()
        stopped = state.producer_done.wait(2)
        if b"200" not in first.split(b"\r\n", 1)[0] or b"event: first" not in first:
            raise RuntimeError("first SSE event was not received")
        if not observed_eof:
            raise RuntimeError("server did not observe client EOF")
        if not stopped:
            raise RuntimeError("disconnect did not stop the producer")
        if state.second_sent is not None:
            raise RuntimeError("producer emitted delayed event after disconnect")
        return {"disconnect_eof_observed": True, "disconnect_stopped": True, "first_before_delayed_second": True,
                "first_event_ms": round((arrived - started) * 1000, 2)}
    finally:
        server.shutdown()
        server.server_close()
        thread.join(timeout=2)


def main(argv=None):
    parser = argparse.ArgumentParser()
    parser.add_argument("--self-test", action="store_true")
    args = parser.parse_args(argv)
    if not args.self_test:
        parser.error("only --self-test is supported; this is a disposable local probe")
    print(json.dumps(self_test(), sort_keys=True, separators=(",", ":")))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
