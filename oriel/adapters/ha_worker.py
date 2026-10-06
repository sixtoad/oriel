"""Private, availability-only channel to Oriel's separately started HA worker."""
from __future__ import annotations

import json
import os
from pathlib import Path
import socket
import stat
import time
from typing import Any

from ..application.ports import HaWorkerAvailability


PROTOCOL_VERSION = "1"
MAX_FRAME_BYTES = 256
DEFAULT_TIMEOUT_SECONDS = 1.0


class HaWorkerChannelError(RuntimeError):
    """A worker-channel failure whose detail must not cross the adapter boundary."""


def availability_request() -> bytes:
    """Return the sole closed request supported by the initial worker protocol."""
    return _frame({"version": PROTOCOL_VERSION, "type": "availability"})


def ready_response() -> bytes:
    """Return the sole successful response supported by the initial protocol."""
    return _frame({"version": PROTOCOL_VERSION, "state": "ready"})


def unavailable_response() -> bytes:
    """Return the sole unavailable response supported by the initial protocol."""
    return _frame({"version": PROTOCOL_VERSION, "state": "unavailable"})


class UnixHaWorkerClient:
    """Reduces all local worker replies and failures to a bounded availability fact."""

    def __init__(self, channel: Path, timeout_seconds: float = DEFAULT_TIMEOUT_SECONDS) -> None:
        self._channel = channel
        self._timeout_seconds = timeout_seconds

    def availability(self) -> HaWorkerAvailability:
        try:
            if not _private_socket(self._channel):
                return HaWorkerAvailability("unavailable")
            with socket.socket(socket.AF_UNIX, socket.SOCK_STREAM) as connection:
                deadline = time.monotonic() + self._timeout_seconds
                connection.settimeout(_remaining(deadline))
                connection.connect(str(self._channel))
                connection.sendall(availability_request())
                return _parse_availability(_receive_frame(connection, deadline))
        except (HaWorkerChannelError, OSError, TimeoutError, ValueError):
            return HaWorkerAvailability("unavailable")


def receive_request(connection: socket.socket) -> None:
    """Accept only the one fixed availability request; never return caller material."""
    if _parse_frame(_receive_frame(connection)) != {"version": PROTOCOL_VERSION, "type": "availability"}:
        raise HaWorkerChannelError("invalid worker request")


def _parse_availability(frame: bytes) -> HaWorkerAvailability:
    value = _parse_frame(frame)
    if value == {"version": PROTOCOL_VERSION, "state": "ready"}:
        return HaWorkerAvailability("ready")
    if value == {"version": PROTOCOL_VERSION, "state": "unavailable"}:
        return HaWorkerAvailability("unavailable")
    raise HaWorkerChannelError("invalid worker response")


def require_private_channel_parent(channel: Path) -> None:
    """Reject a channel outside a non-writable local runtime directory."""
    try:
        parent = channel.parent.stat()
    except OSError:
        raise HaWorkerChannelError("invalid worker channel") from None
    if not channel.is_absolute() or not stat.S_ISDIR(parent.st_mode) or parent.st_mode & 0o022:
        raise HaWorkerChannelError("invalid worker channel")


def _private_socket(channel: Path) -> bool:
    try:
        require_private_channel_parent(channel)
        material = channel.lstat()
    except (HaWorkerChannelError, OSError):
        return False
    return stat.S_ISSOCK(material.st_mode) and material.st_uid == os.getuid() and not material.st_mode & 0o077


def _receive_frame(connection: socket.socket, deadline: float | None = None) -> bytes:
    material = bytearray()
    while len(material) <= MAX_FRAME_BYTES:
        if deadline is not None:
            connection.settimeout(_remaining(deadline))
        chunk = connection.recv(MAX_FRAME_BYTES + 1 - len(material))
        if not chunk:
            break
        material.extend(chunk)
        if material.endswith(b"\n"):
            if len(material) > MAX_FRAME_BYTES:
                raise HaWorkerChannelError("oversized worker frame")
            return bytes(material)
    raise HaWorkerChannelError("invalid worker frame")


def _remaining(deadline: float) -> float:
    remaining = deadline - time.monotonic()
    if remaining <= 0:
        raise TimeoutError("worker channel deadline elapsed")
    return remaining


def _parse_frame(frame: bytes) -> dict[str, str]:
    if not frame.endswith(b"\n") or len(frame) > MAX_FRAME_BYTES:
        raise HaWorkerChannelError("invalid worker frame")
    try:
        value = json.loads(frame[:-1].decode("utf-8"), object_pairs_hook=_unique_object)
    except (TypeError, ValueError, UnicodeDecodeError, json.JSONDecodeError, RecursionError):
        raise HaWorkerChannelError("invalid worker frame") from None
    if not isinstance(value, dict) or any(not isinstance(key, str) or not isinstance(item, str) for key, item in value.items()):
        raise HaWorkerChannelError("invalid worker frame")
    return value


def _unique_object(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
    result: dict[str, Any] = {}
    for key, value in pairs:
        if key in result:
            raise HaWorkerChannelError("duplicate worker field")
        result[key] = value
    return result


def _frame(value: dict[str, str]) -> bytes:
    return json.dumps(value, separators=(",", ":"), sort_keys=True).encode("utf-8") + b"\n"
