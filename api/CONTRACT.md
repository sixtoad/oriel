# Oriel Text API contract 1.0

This directory freezes the version `1.0` text boundary before a gateway runtime exists. All JSON uses UTF-8 and `application/json`; an accepted turn uses `text/event-stream`. The OpenAPI index is [openapi.yaml](openapi.yaml), and every JSON schema declares Draft 2020-12.

## Paths and failures

The only paths are `POST /v1/sessions`, `POST /v1/sessions/{session_id}/turns`, `POST /v1/sessions/{session_id}/reset`, `DELETE /v1/sessions/{session_id}`, `POST /v1/requests/{request_id}/cancel`, and `GET /v1/requests/{request_id}`. IDs are opaque. A status lookup is passive: it never starts or replays work.

Before acceptance, invalid input returns 400, a missing or expired reference 404, a conflict 409, and overload 429. Each body is the safe `error` envelope: stable `code`, `category`, bounded `message`, `retryable`, available correlation IDs, and optional `action_outcome` only. Categories are `invalid_input`, `conflict_or_expired_reference`, `overload`, `policy_denial`, `dependency_unavailable`, `timeout`, `cancellation`, `uncertainty`, and `internal_failure`. The first three categories map directly to the pre-accept HTTP cases; policy, dependency, timeout, cancellation, uncertainty, and internal failures after acceptance are typed stream errors followed by exactly one terminal event. `GET /v1/requests/{request_id}` returns only the passive `in_progress`, `terminal`, or `unavailable` status shape; it cannot initiate or replay work.

## Turn stream lifecycle

`POST .../turns` is non-replayable. The first event is `accepted`. Each event has request, session, and trace IDs, a context generation, and a strictly increasing positive `seq`. Only `accepted`, `ack`, `content_delta`, `proposal`, `validation`, `action_state`, `error`, and `terminal` occur. `terminal` appears exactly once, last, with `completed`, `denied`, `failed`, `cancelled`, or `outcome_unknown`; no later event is valid.

`content_delta` always carries `content`; `error` always carries the safe error object; and `terminal` always carries `outcome`. `ack` is optional, nonterminal, at most once, before useful content, and has no outcome because it does not state approval or completion. Unknown event fields are permitted for stream evolution. Unknown fields in configuration, manifests, proposals, and action-result documents are rejected.

Cancellation returns a typed acknowledgement. `cancellation_requested` means a live request received the cancellation request and has no outcome; its eventual stream terminal is authoritative. `already_terminal` means a terminal outcome won the race and includes that outcome. Passive `in_progress` and `unavailable` status similarly have no outcome, while `terminal` has exactly one. No cancellation response replays content or starts work.

## Canonical JSON and limits

Canonical JSON is UTF-8 with lexicographically ordered object keys, no duplicate keys, and no non-finite number. Limits are 16 KiB input, 32 context messages and 64 KiB total context, 10 open sessions, 2 active turns, queue depth 8, a 30-second model deadline, 64 KiB accumulated streamed content, and 8 KiB serialized event data.

## Configuration and generic actions

Configuration is an immutable restart-applied document. The explicit `--config` path wins over `ORIEL_CONFIG_PATH`, which wins over the packaged default. There are no per-field environment overrides. It contains connection references only, never secret values. Invalid core configuration leaves the service unready; invalid optional skill configuration disables that skill.

The manifest, proposal, and result grammar is generic and versioned. Targets are synthetic, every manifest action is disabled by default, and proposals carry a deadline, dry-run flag, idempotency key, and confirmation evidence. These artifacts select no real operation, target, provider, endpoint, or credential.

## Evidence procedure

Run `python3 scripts/validate_api_contract.py api/examples` offline. Run `python3 scripts/sse_probe.py --self-test` to obtain local direct-delivery and disconnect evidence. A supported direct and Ingress placement must each run the probe and retain only sanitized timing/outcome evidence. Until an Ingress run is recorded, Ingress evidence is incomplete; the local self-test is not a gateway or Ingress result.
