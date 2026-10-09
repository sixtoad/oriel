"""Separately started bounded worker that alone receives HA connection material."""
from __future__ import annotations

import argparse
import os
from pathlib import Path
import signal
import socket

from .ha_worker import DEFAULT_TIMEOUT_SECONDS, HaWorkerChannelError, _remaining, receive_worker_request, execution_response, ready_response, require_private_channel_parent, unavailable_response


from .ha_execution import HarmlessHaExecutor
from ..application.ports import ActionExecutionRequest
from ..domain.ha_manifest import CanonicalProposal, OPERATION_ID, TARGET_ALIAS, MANIFEST_REVISION


CONNECTION_INPUT_ENV = "ORIEL_HA_WORKER_CONNECTION_REF"


def _stop(_signum: int, _frame: object) -> None:
    raise SystemExit(0)


def serve_once(channel: Path, environ: dict[str, str] | None = None) -> None:
    """Serve one fixed local availability request without contacting a provider."""
    source = os.environ if environ is None else environ
    input_present = _valid_connection_input(source.get(CONNECTION_INPUT_ENV))
    _serve(channel, input_present, once=True)


def serve_forever(channel: Path, environ: dict[str, str] | None = None) -> None:
    """Serve fixed availability requests until the independently managed worker stops."""
    source = os.environ if environ is None else environ
    input_present = _valid_connection_input(source.get(CONNECTION_INPUT_ENV))
    _serve(channel, input_present, once=False)


def _serve(channel: Path, input_present: bool, once: bool) -> None:
    require_private_channel_parent(channel)
    created = False
    try:
        with socket.socket(socket.AF_UNIX, socket.SOCK_STREAM) as listener:
            _bind_private(listener, channel)
            created = True
            listener.listen(4)
            while True:
                with listener.accept()[0] as connection:
                    _respond(connection, input_present)
                if once:
                    return
    finally:
        if created:
            try:
                channel.unlink()
            except OSError:
                pass


def _bind_private(listener: socket.socket, channel: Path) -> None:
    """Create the socket atomically with owner-only permissions."""
    previous_umask = os.umask(0o177)
    try:
        listener.bind(str(channel))
    finally:
        os.umask(previous_umask)


def _respond(connection: socket.socket, input_present: bool, executor: HarmlessHaExecutor | None = None) -> None:
    executing = False
    try:
        connection.settimeout(DEFAULT_TIMEOUT_SECONDS)
        request = receive_worker_request(connection)
        if request["type"] == "availability":
            connection.sendall(ready_response() if input_present else unavailable_response())
        else:
            executing = True
            deadline = float(request["deadline"])
            proposal = CanonicalProposal(OPERATION_ID, TARGET_ALIAS, (("desired_state", request["desired_state"]),), MANIFEST_REVISION)
            result = (executor or HarmlessHaExecutor()).execute(ActionExecutionRequest(proposal, deadline))
            response = execution_response(result)
            connection.settimeout(_remaining(deadline))
            connection.sendall(response)
    except (HaWorkerChannelError, OSError, TimeoutError):
        if executing:
            return
        try:
            connection.sendall(unavailable_response())
        except OSError:
            pass


def _valid_connection_input(value: object) -> bool:
    return isinstance(value, str) and 0 < len(value) <= 256 and "\x00" not in value


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Run Oriel's private HA availability worker.")
    parser.add_argument("--channel", required=True, type=Path)
    args = parser.parse_args(argv)
    if not args.channel.is_absolute() or args.channel.exists():
        return 2
    previous = signal.signal(signal.SIGTERM, _stop)
    try:
        serve_forever(args.channel)
    except (HaWorkerChannelError, OSError):
        return 2
    finally:
        signal.signal(signal.SIGTERM, previous)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
