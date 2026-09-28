#!/usr/bin/env python3
"""A small, dependency-free reference client for Oriel's public text API."""
from __future__ import annotations

import argparse
from http.client import HTTPConnection, HTTPSConnection, HTTPResponse
import ipaddress
import json
import re
import sys
from typing import Any, Mapping, TextIO
from urllib.parse import quote, urlsplit


_OUTCOMES = {"completed", "denied", "failed", "cancelled", "outcome_unknown"}
_HOSTNAME = re.compile(r"[A-Za-z0-9](?:[A-Za-z0-9.-]*[A-Za-z0-9])?$")


class ClientFailure(Exception):
    """A transport or protocol failure that has no safe structured outcome."""


class TextClient:
    """Render Oriel HTTP/SSE responses without owning turn lifecycle decisions."""

    def __init__(self, endpoint: str, stdout: TextIO, stderr: TextIO, timeout: float = 30.0) -> None:
        try:
            parsed = urlsplit(endpoint)
            hostname = parsed.hostname
            port = parsed.port
        except ValueError as failure:
            raise ValueError("endpoint must contain a valid hostname and port") from failure
        if parsed.scheme not in {"http", "https"} or not parsed.netloc or parsed.query or parsed.fragment or parsed.username or parsed.password or hostname is None:
            raise ValueError("endpoint must be an http(s) URL without query or fragment")
        try:
            ipaddress.ip_address(hostname)
        except ValueError:
            if not _HOSTNAME.fullmatch(hostname):
                raise ValueError("endpoint must contain a valid hostname") from None
        self._parsed = parsed
        self._hostname = hostname
        self._port = port
        self._prefix = parsed.path.rstrip("/")
        self._stdout = stdout
        self._stderr = stderr
        self._timeout = timeout
        self._content_open = False
        self._active_request_id: str | None = None

    def _connection(self) -> HTTPConnection:
        connection_type = HTTPSConnection if self._parsed.scheme == "https" else HTTPConnection
        return connection_type(self._hostname, self._port, timeout=self._timeout)

    def _path(self, path: str) -> str:
        return f"{self._prefix}{path}" or "/"

    @staticmethod
    def _segment(value: str) -> str:
        return quote(value, safe="")

    @staticmethod
    def _display(value: object) -> str:
        text = str(value)
        escaped: list[str] = []
        for character in text:
            codepoint = ord(character)
            if character == "\n":
                escaped.append("\\n")
            elif character == "\r":
                escaped.append("\\r")
            elif character == "\t":
                escaped.append("\\t")
            elif codepoint < 32 or codepoint == 127:
                escaped.append(f"\\x{codepoint:02x}")
            else:
                escaped.append(character)
        return "".join(escaped)

    @staticmethod
    def _is_sse(response: HTTPResponse) -> bool:
        content_type = response.getheader("Content-Type", "")
        return content_type.split(";", 1)[0].strip().lower() == "text/event-stream"

    def _line(self, text: str) -> None:
        if self._content_open:
            self._stdout.write("\n")
            self._content_open = False
        self._stdout.write(f"{text}\n")
        self._stdout.flush()

    def _json_request(self, method: str, path: str, body: Mapping[str, object] | None = None) -> tuple[int, object | None]:
        connection = self._connection()
        try:
            encoded = None if body is None else json.dumps(body, ensure_ascii=False, separators=(",", ":")).encode("utf-8")
            headers = {} if encoded is None else {"Content-Type": "application/json", "Content-Length": str(len(encoded))}
            connection.request(method, self._path(path), body=encoded, headers=headers)
            response = connection.getresponse()
            raw = response.read()
        except (OSError, ValueError) as failure:
            raise ClientFailure(str(failure)) from failure
        finally:
            connection.close()
        if not raw:
            return response.status, None
        try:
            return response.status, json.loads(raw.decode("utf-8"))
        except (UnicodeDecodeError, json.JSONDecodeError):
            return response.status, None

    def _render_http_error(self, status: int, payload: object | None) -> None:
        error = payload.get("error") if isinstance(payload, Mapping) else None
        if isinstance(error, Mapping):
            fields = [f"status={status}"]
            for name in ("code", "category", "retryable", "message"):
                if name in error:
                    fields.append(f"{name}={self._display(error[name])}")
            self._line("http_error: " + " ".join(fields))
            return
        self._line(f"http_error: status={status}")

    def start(self, text: str) -> int:
        try:
            status, payload = self._json_request("POST", "/v1/sessions")
        except ClientFailure as failure:
            self._line(f"outcome_unknown: session creation failed ({self._display(failure)})")
            return 1
        if status != 201 or not isinstance(payload, Mapping) or not isinstance(payload.get("session_id"), str):
            self._render_http_error(status, payload)
            return 1
        self._line(f"session: session_id={self._display(payload['session_id'])} context_generation={self._display(payload.get('context_generation'))}")
        return self.continue_turn(payload["session_id"], text)

    def continue_turn(self, session_id: str, text: str) -> int:
        return self._stream_turn(session_id, text)

    def cancel(self, request_id: str) -> int:
        try:
            status, payload = self._json_request("POST", f"/v1/requests/{self._segment(request_id)}/cancel")
        except ClientFailure as failure:
            self._line(f"cancellation_unknown: request_id={self._display(request_id)} detail={self._display(failure)}")
            return 1
        if status != 202 or not isinstance(payload, Mapping):
            self._render_http_error(status, payload)
            return 1
        fields = [f"request_id={self._display(payload.get('request_id', request_id))}", f"state={self._display(payload.get('state', 'unknown'))}"]
        if "outcome" in payload:
            fields.append(f"outcome={self._display(payload['outcome'])}")
        self._line("cancellation: " + " ".join(fields))
        return 0

    def status(self, request_id: str) -> int:
        return self._status(request_id)

    def _status(self, request_id: str) -> int:
        try:
            status, payload = self._json_request("GET", f"/v1/requests/{self._segment(request_id)}")
        except ClientFailure as failure:
            self._line(f"status_unavailable: request_id={self._display(request_id)} detail={self._display(failure)}")
            return 1
        if status != 200 or not isinstance(payload, Mapping):
            self._render_http_error(status, payload)
            return 1
        fields = [f"request_id={self._display(payload.get('request_id', request_id))}", f"state={self._display(payload.get('state', 'unknown'))}"]
        if "outcome" in payload:
            fields.append(f"outcome={self._display(payload['outcome'])}")
        self._line("status: " + " ".join(fields))
        return 0

    def _stream_turn(self, session_id: str, text: str) -> int:
        accepted_request_id: str | None = None
        terminal = False
        connection = self._connection()
        try:
            body = json.dumps({"input": text}, ensure_ascii=False, separators=(",", ":")).encode("utf-8")
            connection.request("POST", self._path(f"/v1/sessions/{self._segment(session_id)}/turns"), body=body, headers={"Accept": "text/event-stream", "Content-Type": "application/json", "Content-Length": str(len(body))})
            response = connection.getresponse()
            if response.status != 200:
                raw = response.read()
                try:
                    payload: object | None = json.loads(raw.decode("utf-8")) if raw else None
                except (UnicodeDecodeError, json.JSONDecodeError):
                    payload = None
                self._render_http_error(response.status, payload)
                return 1
            if not self._is_sse(response):
                response.read()
                self._line("protocol_error: expected text/event-stream response")
                return 1
            for event_name, payload in self._sse_events(response):
                event_type = payload.get("type", event_name)
                if event_type == "accepted" and isinstance(payload.get("request_id"), str):
                    if accepted_request_id is None:
                        accepted_request_id = payload["request_id"]
                        self._active_request_id = accepted_request_id
                if event_type == "terminal":
                    outcome = payload.get("outcome")
                    if accepted_request_id is not None and payload.get("request_id") == accepted_request_id and isinstance(outcome, str) and outcome in _OUTCOMES:
                        terminal = True
                        self._render_event("terminal", payload)
                    else:
                        self._line("terminal: malformed event")
                else:
                    self._render_event(str(event_type), payload)
        except (ClientFailure, OSError, ValueError) as failure:
            self._stderr.write(f"stream transport failure: {self._display(failure)}\n")
            self._stderr.flush()
        finally:
            connection.close()

        if terminal:
            return 0
        if accepted_request_id is None:
            self._line("outcome_unknown: stream ended before accepted; no replay was attempted")
            return 1
        self._line(f"stream_interrupted: request_id={self._display(accepted_request_id)}; passive status lookup follows")
        return self._status(accepted_request_id)

    def _sse_events(self, response: HTTPResponse):
        event_name = "message"
        data_lines: list[str] = []
        while True:
            try:
                raw = response.readline()
            except (OSError, ValueError) as failure:
                raise ClientFailure(str(failure)) from failure
            if raw == b"":
                if data_lines:
                    raise ClientFailure("stream ended in an incomplete event frame")
                return
            try:
                line = raw.decode("utf-8").rstrip("\r\n")
            except UnicodeDecodeError as failure:
                raise ClientFailure("stream contained invalid UTF-8") from failure
            if not line:
                if data_lines:
                    try:
                        payload = json.loads("\n".join(data_lines))
                    except json.JSONDecodeError as failure:
                        raise ClientFailure("stream contained invalid JSON") from failure
                    if not isinstance(payload, Mapping):
                        raise ClientFailure("stream event was not an object")
                    yield event_name, payload
                event_name = "message"
                data_lines = []
            elif line.startswith(":"):
                continue
            elif line.startswith("event:"):
                event_name = line[6:].lstrip()
            elif line.startswith("data:"):
                data_lines.append(line[5:].lstrip())

    def _render_event(self, event_type: str, payload: Mapping[str, Any]) -> None:
        if event_type == "accepted":
            self._line("accepted: " + " ".join(f"{name}={self._display(payload.get(name, 'unknown'))}" for name in ("session_id", "request_id", "trace_id", "context_generation")))
        elif event_type == "ack":
            self._line(f"acknowledgement: {self._display(payload.get('message', 'Work is continuing.'))}")
        elif event_type == "content_delta":
            content = payload.get("content")
            if isinstance(content, str):
                self._stdout.write(self._display(content))
                self._stdout.flush()
                self._content_open = True
            else:
                self._line("content_delta: malformed event")
        elif event_type == "proposal":
            proposal = payload.get("proposal")
            if isinstance(proposal, Mapping):
                self._line("proposal: UNTRUSTED DRY-RUN=" + self._display(proposal.get("dry_run", "unknown")) + " " + " ".join(f"{name}={self._display(proposal.get(name, 'unknown'))}" for name in ("proposal_id", "action", "target")))
            else:
                self._line("proposal: UNTRUSTED DRY-RUN malformed event")
        elif event_type == "error":
            error = payload.get("error")
            if isinstance(error, Mapping):
                fields = [f"{name}={self._display(error[name])}" for name in ("code", "category", "retryable", "action_outcome", "message") if name in error]
                self._line("error: " + " ".join(fields))
            else:
                self._line("error: malformed event")
        elif event_type == "terminal":
            self._line(f"terminal: outcome={self._display(payload['outcome'])}")
        else:
            self._line(f"event: type={self._display(event_type)}")


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Render Oriel's text API lifecycle over HTTP/SSE.")
    parser.add_argument("--endpoint", "--base-url", "--url", default="http://127.0.0.1:8080", help="Gateway HTTP base URL")
    subcommands = parser.add_subparsers(dest="command", required=True)
    start = subcommands.add_parser("start", help="Create a session and submit its first turn")
    start.add_argument("text")
    continuation = subcommands.add_parser("continue", help="Submit a turn to an existing session")
    continuation.add_argument("session_id")
    continuation.add_argument("text")
    cancel = subcommands.add_parser("cancel", help="Request cancellation for a live request")
    cancel.add_argument("request_id")
    status = subcommands.add_parser("status", help="Passively inspect request status")
    status.add_argument("request_id")
    return parser


def main(argv: list[str] | None = None, stdout: TextIO | None = None, stderr: TextIO | None = None) -> int:
    args = build_parser().parse_args(argv)
    output = stdout or sys.stdout
    errors = stderr or sys.stderr
    try:
        client = TextClient(args.endpoint, output, errors)
    except ValueError as failure:
        errors.write(f"invalid endpoint: {failure}\n")
        return 2
    try:
        if args.command == "start":
            return client.start(args.text)
        if args.command == "continue":
            return client.continue_turn(args.session_id, args.text)
        if args.command == "cancel":
            return client.cancel(args.request_id)
        return client.status(args.request_id)
    except KeyboardInterrupt:
        if client._active_request_id is None:
            output.write("\ninterrupted: outcome_unknown; do not replay the turn.\n")
        else:
            output.write(f"\ninterrupted: run status {client._display(client._active_request_id)} to recover the authoritative outcome.\n")
        output.flush()
        return 130


if __name__ == "__main__":
    raise SystemExit(main())
