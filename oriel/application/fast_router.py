"""Pure, bounded rules for responses that do not need model work."""
from __future__ import annotations

from dataclasses import dataclass
import re
import unicodedata
from typing import Iterable, Literal, Mapping

from ..domain.ha_manifest import canonical_ha_proposal

from .ports import ModelMessage


RULE_REVISION = "1"
CLARIFICATION_TEXT = "Please clarify your request."

_SPACE = re.compile(r"\s+")
_LIVE_TIME = re.compile(r"\b(time|date|day)\b.*\b(now|current|today)\b|\bwhat time is it\b|\b(?:current|today(?:'s)?|now)\s+(?:time|date|day)\b|\bwhat(?:'s| is)\s+(?:the\s+)?(?:current\s+|today(?:'s)?\s+)?(?:time|date|day)\b")
_LIVE_WEATHER = re.compile(r"\b(?:what(?:'s| is)|tell me|show me)\b.*\b(weather|forecast|temperature)\b|\b(weather|forecast|temperature)\b.*\b(now|today|current|outside|tomorrow)\b|\bweather forecast\b")
_LIVE_MUSIC = re.compile(r"^(?:please\s+)?(?:play|pause|stop)\b.*\b(music|song|audio|playlist)\b|\b(?:can|could|would)\s+you\s+(?:play|pause|stop)\b.*\b(music|song|audio|playlist)\b")
_LIVE_HOME_STATE = re.compile(r"^(?:is|are|what|which)\b.*\b(light|lights|door|doors|window|windows|thermostat|home|house|alarm|lock|locks)\b|\b(?:tell|show)\s+me\b.*\b(light|lights|door|doors|window|windows|thermostat|home|house|alarm|lock|locks)\b.*\b(is|are|on|off|open|closed|locked|unlocked|status|state)\b|\b(light|lights|door|doors|window|windows|thermostat|home|house|alarm|lock|locks)\b.*\b(is|are)\b.*\b(on|off|open|closed|locked|unlocked)\b")
_PROTECTED = re.compile(r"\b(delete|erase|unlock|disarm|bypass|protected action)\b")
_CONTROL = re.compile(r"\b(turn on|turn off|set|open|close|lock|unlock|arm|disarm|enable|disable)\b")
_MUSIC_CONTROL = re.compile(r"\b(play|pause|stop)\b")
_ACTION_REQUEST = re.compile(r"^(?:please\s+)?(?:turn on|turn off|set|open|close|lock|unlock|arm|disarm|enable|disable|play|pause|stop)\b|\b(?:can|could|would)\s+you\s+(?:turn on|turn off|set|open|close|lock|unlock|arm|disarm|enable|disable|play|pause|stop)\b")


@dataclass(frozen=True)
class NormalizedTurn:
    """The normalized, bounded material a rule is permitted to inspect."""

    input: str
    context: tuple[ModelMessage, ...]


@dataclass(frozen=True)
class FastRoute:
    """A typed decision that is independent of transport and providers."""

    route: Literal["content", "clarification", "limitation", "denial", "proposal", "qwen"]
    rule_revision: str = RULE_REVISION
    content: str | None = None
    error_code: str | None = None
    error_category: str | None = None
    error_message: str | None = None
    proposal: Mapping[str, object] | None = None


def normalize_turn(text: str, context: Iterable[ModelMessage]) -> NormalizedTurn:
    """Normalize already-bounded user text and prior context deterministically."""
    return NormalizedTurn(_normalize(text), tuple(ModelMessage(message.role, _normalize(message.content)) for message in context))


def route(text: str, context: Iterable[ModelMessage], proposal_deadline: str | None = None) -> FastRoute:
    """Select a deterministic response or explicitly defer to the model."""
    turn = normalize_turn(text, context)
    request = turn.input

    if _PROTECTED.search(request) or _has_multiple_actions(request):
        return FastRoute("denial", error_code="fast_route_denied", error_category="policy_denial", error_message="This request is not allowed.")
    if _LIVE_WEATHER.search(request):
        return FastRoute("limitation", content="Live weather lookup is unsupported.")
    if _LIVE_TIME.search(request):
        return FastRoute("limitation", content="Live time lookup is unsupported.")
    if _LIVE_MUSIC.search(request):
        return FastRoute("limitation", content="Music playback is unsupported.")
    if _LIVE_HOME_STATE.search(request):
        return FastRoute("limitation", content="Live home-state lookup is unsupported.")
    if request in {"create a reviewed harmless light proposal on", "create a reviewed harmless light proposal off"}:
        return FastRoute("proposal", proposal=canonical_ha_proposal(request.rsplit(" ", 1)[1]))
    if request == "create a synthetic proposal":
        if proposal_deadline is None:
            raise ValueError("synthetic proposals require an application deadline")
        return FastRoute("proposal", proposal=_generic_proposal(proposal_deadline))
    if request == "oriel help":
        return FastRoute("content", content="Oriel can provide limited deterministic responses.")
    if _is_action_request(request) or request in {"do that", "make it so", "change it"}:
        return FastRoute("clarification", content=CLARIFICATION_TEXT)
    return FastRoute("qwen")


def _normalize(value: str) -> str:
    return _SPACE.sub(" ", unicodedata.normalize("NFKC", value).casefold()).strip(" .!?")


def _has_multiple_actions(request: str) -> bool:
    if not _is_action_request(request):
        return False
    actions = _CONTROL.findall(request) + _MUSIC_CONTROL.findall(request)
    return len(actions) > 1 or bool(actions and re.search(r"\b(and|then)\b", request))


def _is_action_request(request: str) -> bool:
    return _ACTION_REQUEST.search(request) is not None


def _generic_proposal(deadline: str) -> dict[str, object]:
    """Return the one fixed generic proposal grammar allowed on the fast path."""
    return {
        "proposal_version": "1.0",
        "proposal_id": "fast-router-proposal-v1",
        "action": "generic_action",
        "target": "synthetic:fast-router",
        "arguments": {"values": []},
        "dry_run": True,
        "idempotency": "fast-router-v1",
        "deadline": deadline,
        "confirmation": {"required": True, "evidence": None},
    }
