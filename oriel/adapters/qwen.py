"""OpenAI-compatible streaming adapter for an operator-selected local worker."""
from __future__ import annotations

from dataclasses import dataclass
import json
import os
from typing import Any, Callable, Iterable, Iterator, Mapping
from urllib.error import HTTPError, URLError
from urllib.request import HTTPRedirectHandler, Request, build_opener

from ..application.ports import ModelChunk, ModelInput, ModelOutcome, ModelProposal, ModelStreamItem
from ..domain.proposals import validate_proposal
from .configuration import OpenAICompatibleProfile


MODEL_DEADLINE_SECONDS = 30
MAX_PROVIDER_RECORD_BYTES = 8 * 1024
MAX_TOOL_ARGUMENT_BYTES = 8 * 1024
PROPOSAL_DIALECT = "oriel-proposal-v1"


class _NoRedirect(HTTPRedirectHandler):
    def redirect_request(self, request: Request, fp: Any, code: int, message: str, headers: Any, newurl: str) -> None:
        del request, fp, code, message, headers, newurl
        return None


def _open_pinned(request: Request, timeout: float) -> Any:
    return build_opener(_NoRedirect()).open(request, timeout=timeout)


class ProviderStreamFailure(RuntimeError):
    """A provider failure whose details must never cross the model port."""


class CredentialResolver:
    """Resolves an adapter-private credential reference only when one is selected."""

    def resolve(self, reference: str) -> str:  # pragma: no cover - protocol-shaped default
        raise NotImplementedError


class EnvironmentCredentialResolver(CredentialResolver):
    """Uses a profile-selected environment variable without exposing its value."""

    def __init__(self, environ: Mapping[str, str] | None = None) -> None:
        self._environ = os.environ if environ is None else environ

    def resolve(self, reference: str) -> str:
        value = self._environ.get(reference)
        if not isinstance(value, str) or not value:
            raise ProviderStreamFailure("credential unavailable")
        return value


@dataclass
class OpenAICompatibleStreamingModel:
    """Incrementally translates one OpenAI-compatible SSE response inward."""

    profile: OpenAICompatibleProfile
    credentials: CredentialResolver | None = None
    opener: Callable[..., Any] = _open_pinned

    def stream(self, input: ModelInput) -> Iterable[ModelStreamItem]:
        request = self._request(input)
        try:
            response = self.opener(request, timeout=MODEL_DEADLINE_SECONDS)
            with response:
                status = response.status if hasattr(response, "status") else response.getcode()
                content_type = response.headers.get_content_type() if hasattr(response.headers, "get_content_type") else response.headers.get("Content-Type", "").split(";", 1)[0]
                if type(status) is not int or not isinstance(content_type, str) or status < 200 or status >= 300 or content_type.lower() != "text/event-stream":
                    raise ProviderStreamFailure("worker response unavailable")
                yield from _decode_openai_sse(_iter_sse_records(response))
        except (HTTPError, URLError, OSError, UnicodeError, ValueError, ProviderStreamFailure):
            raise ProviderStreamFailure("worker stream unavailable") from None

    def _request(self, input: ModelInput) -> Request:
        body = {
            "model": self.profile.model_revision,
            "messages": [{"role": message.role, "content": message.content} for message in input.messages],
            "stream": True,
            "max_tokens": self.profile.max_tokens,
        }
        headers = {"Accept": "text/event-stream", "Content-Type": "application/json"}
        if self.profile.credential_ref is not None:
            if self.credentials is None:
                raise ProviderStreamFailure("credential unavailable")
            headers["Authorization"] = "Bearer " + self.credentials.resolve(self.profile.credential_ref)
        return Request(self.profile.request_url, data=json.dumps(body, ensure_ascii=False, separators=(",", ":")).encode("utf-8"), headers=headers, method="POST")


def _iter_sse_records(response: Any) -> Iterator[str]:
    """Decode one bounded UTF-8 SSE record at a time from arbitrary byte chunks."""
    data_lines: list[bytes] = []
    record_bytes = 0
    while raw_line := response.readline(MAX_PROVIDER_RECORD_BYTES + 1):
        if len(raw_line) > MAX_PROVIDER_RECORD_BYTES or not raw_line.endswith(b"\n"):
            raise ProviderStreamFailure("oversized worker record")
        line = raw_line[:-1].rstrip(b"\r")
        record_bytes += len(raw_line)
        if record_bytes > MAX_PROVIDER_RECORD_BYTES:
            raise ProviderStreamFailure("oversized worker record")
        if not line:
            if data_lines:
                try:
                    yield b"\n".join(data_lines).decode("utf-8", "strict")
                except UnicodeDecodeError:
                    raise ProviderStreamFailure("invalid worker encoding") from None
            data_lines = []
            record_bytes = 0
        elif line.startswith(b":"):
            continue
        elif line.startswith(b"data:"):
            data_lines.append(line[5:].lstrip(b" "))
        elif line.startswith((b"event:", b"id:", b"retry:")):
            continue
        else:
            raise ProviderStreamFailure("invalid worker SSE")
    if data_lines:
        raise ProviderStreamFailure("unterminated worker SSE")


def _decode_openai_sse(records: Iterable[str]) -> Iterator[ModelStreamItem]:
    decoder = _OpenAIStreamDecoder()
    for record in records:
        if record == "[DONE]":
            terminal = decoder.finish()
            yield from decoder.drain()
            yield terminal
            return
        decoder.accept(record)
        yield from decoder.drain()
    raise ProviderStreamFailure("worker stream ended early")


class _OpenAIStreamDecoder:
    def __init__(self) -> None:
        self._saw_content = False
        self._tool: _ToolCall | None = None
        self._pending: list[ModelStreamItem] = []
        self._finish_reason: str | None = None

    def accept(self, record: str) -> None:
        if self._finish_reason is not None:
            raise ProviderStreamFailure("worker sent data after completion")
        document = _strict_json(record)
        if not isinstance(document, dict) or not isinstance(document.get("choices"), list) or len(document["choices"]) != 1:
            raise ProviderStreamFailure("invalid worker frame")
        choice = document["choices"][0]
        if not isinstance(choice, dict) or not isinstance(choice.get("delta", {}), dict):
            raise ProviderStreamFailure("invalid worker frame")
        delta = choice.get("delta", {})
        content = delta.get("content")
        if content is not None:
            if not isinstance(content, str) or self._tool is not None:
                raise ProviderStreamFailure("mixed worker content")
            if content:
                self._saw_content = True
                self._pending.append(ModelChunk(content))
        if "tool_calls" in delta:
            if self._saw_content:
                raise ProviderStreamFailure("mixed worker content")
            self._accept_tool_calls(delta["tool_calls"])
        finish_reason = choice.get("finish_reason")
        if finish_reason is not None:
            if finish_reason not in {"stop", "tool_calls"}:
                raise ProviderStreamFailure("unknown worker completion")
            if finish_reason == "stop" and self._tool is not None:
                raise ProviderStreamFailure("invalid worker tool completion")
            if finish_reason == "tool_calls" and self._tool is None:
                raise ProviderStreamFailure("missing worker tool call")
            self._finish_reason = finish_reason

    def drain(self) -> Iterator[ModelStreamItem]:
        while self._pending:
            yield self._pending.pop(0)

    def finish(self) -> ModelOutcome:
        if self._finish_reason == "stop":
            return ModelOutcome("completed")
        if self._finish_reason == "tool_calls" and self._tool is not None:
            proposal = self._tool.proposal()
            self._pending.append(ModelProposal(proposal))
            return ModelOutcome("completed")
        raise ProviderStreamFailure("worker stream ended early")

    def _accept_tool_calls(self, value: object) -> None:
        if not isinstance(value, list) or len(value) != 1:
            raise ProviderStreamFailure("invalid worker tool call")
        call = value[0]
        if not isinstance(call, dict) or set(call) - {"index", "id", "type", "function"} or type(call.get("index")) is not int or call["index"] != 0:
            raise ProviderStreamFailure("invalid worker tool call")
        function = call.get("function")
        if not isinstance(function, dict) or set(function) - {"name", "arguments"}:
            raise ProviderStreamFailure("invalid worker tool call")
        if self._tool is None:
            if set(call) != {"index", "id", "type", "function"} or type(call["id"]) is not str or call["type"] != "function" or function.get("name") != PROPOSAL_DIALECT or not isinstance(function.get("arguments"), str):
                raise ProviderStreamFailure("invalid worker tool call")
            self._tool = _ToolCall(call["id"], function["arguments"])
            self._tool.check_size()
            return
        if set(call) != {"index", "function"} or set(function) != {"arguments"} or not isinstance(function["arguments"], str):
            raise ProviderStreamFailure("invalid worker tool call")
        self._tool.append(function["arguments"])


@dataclass
class _ToolCall:
    call_id: str
    arguments: str

    def append(self, fragment: str) -> None:
        self.arguments += fragment
        self.check_size()

    def check_size(self) -> None:
        if len(self.arguments.encode("utf-8")) > MAX_TOOL_ARGUMENT_BYTES:
            raise ProviderStreamFailure("oversized worker tool arguments")

    def proposal(self) -> Mapping[str, object]:
        value = _strict_json(self.arguments)
        if not validate_proposal(value):
            raise ProviderStreamFailure("invalid worker proposal")
        return value


def _strict_json(value: str) -> object:
    def unique_object(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
        result: dict[str, Any] = {}
        for key, item in pairs:
            if key in result:
                raise ProviderStreamFailure("duplicate worker JSON key")
            result[key] = item
        return result

    def reject_constant(_value: str) -> None:
        raise ProviderStreamFailure("invalid worker JSON")

    try:
        return json.loads(value, object_pairs_hook=unique_object, parse_constant=reject_constant)
    except (TypeError, ValueError, json.JSONDecodeError, RecursionError):
        raise ProviderStreamFailure("invalid worker JSON") from None
