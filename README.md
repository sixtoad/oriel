# oriel

A private, portable voice assistant gateway with policy-controlled tools.

See the [roadmap](docs/ROADMAP.md) for the staged migration from the existing
Home Assistant Office speaker to a portable, policy-controlled assistant.

The [synthetic evaluation corpus](evaluation/README.md) defines versioned,
offline expectations for supported, denied, and deferred scenarios. Validate
its definitions and run its tests with Python 3.12:

```sh
python3 scripts/validate_corpus.py evaluation/corpus.json
python3 -m unittest discover -s tests -v
```

The [offline measurement-result format](evaluation/RESULTS.md) separately
scores supplied, sanitized fixture events. Its committed demo is synthetic and
does not claim a baseline, Oriel capability, performance, or release result:

```sh
python3 scripts/score_results.py evaluation/fixtures/synthetic-results.json
```

The [frozen text API contract](api/CONTRACT.md) defines the Phase 1 public
boundary without selecting a gateway runtime. Validate its sanitized fixtures
offline, then run its disposable local direct SSE probe:

```sh
python3 scripts/validate_api_contract.py api/examples
python3 scripts/sse_probe.py --self-test
```

The probe is neither a gateway nor Ingress evidence. Run it through each
supported direct and Ingress placement separately; until sanitized Ingress
evidence is retained, that criterion remains incomplete.

The [harmless Home Assistant capability contract](docs/harmless-home-assistant-skill.md)
defines the one selected synthetic light capability. It remains disabled while
the restricted provider call path is unverified; the contract test is offline
and makes no Home Assistant call:

```sh
python3 -m unittest tests.test_harmless_home_assistant_skill -v
```

A [sanitized captured reference-text report](evaluation/captures/2026-09-17-reference-text/README.md)
replays a real existing local-Qwen backend measurement. It is deliberately
limited to the text backend and does not represent full Assist or voice evidence.

## Text-core bootstrap

The dependency-free bootstrap has health endpoints, volatile sessions, ordered
SSE turns, passive request status, and a deterministic fake model. From a clean
checkout with Python 3.12, run:

```sh
python3 scripts/demo_text_gateway.py --self-test
```

It prints stable JSON for `/live`, `/ready`, and an internal fake-model text
turn. To exercise the public stream locally, start `python3 -m oriel`, then in
another terminal use the dependency-free reference client:

```sh
python3 scripts/text_client.py start 'hello'
# Copy the displayed session_id to continue an existing session.
python3 scripts/text_client.py continue <session_id> 'another question'
# Copy the displayed request_id to request cancellation or inspect it passively.
python3 scripts/text_client.py cancel <request_id>
python3 scripts/text_client.py status <request_id>
```

Pass `--endpoint http://host:port` before the subcommand for another local
gateway address. `start` creates a session and submits its first turn;
`continue` submits a turn to the supplied session. The client prints
`accepted` identifiers immediately, labels the optional acknowledgement apart
from streamed answer text, and reports the final typed terminal outcome. A
proposal is always labelled `UNTRUSTED DRY-RUN`; it is a proposal, never a
completed action.

If a stream ends after `accepted` but before `terminal`, the client performs
one passive `status` lookup for that request and does not replay the turn. If
the stream ends before `accepted`, it reports `outcome_unknown` and does not
make a status request or resend the POST. HTTP errors, cancellation responses,
and status responses are rendered from their structured fields. For an easy
local disconnection/recovery exercise, stop the client after it prints its
`accepted` line and run `python3 scripts/text_client.py status <request_id>`;
the gateway treats the disconnected stream as cancellation, so its eventual
terminal status is authoritative.

Session context is volatile and isolated by its opaque session handle. A first
turn may supply `context`; later turns use the retained transcript only. Reset
it with `POST /v1/sessions/<session_id>/reset`, which returns the next context
generation, or remove it with `DELETE /v1/sessions/<session_id>`. The runtime
cleans up sessions after 30 minutes idle or 24 hours total lifetime.

The stream starts with `accepted`, emits ordered `content_delta` events, and
ends with one `terminal` event. Cancelling a live request returns
`cancellation_requested`; a repeated request is safe, and a completed race
returns `already_terminal` with its outcome. A disconnected stream is
cancelled. Status lookup is passive and is the reconnect path; it never
replays a turn. Cancellation asks a local upstream to stop cooperatively, but
the gateway still discards late upstream output if it cannot be interrupted
immediately. Session context remains volatile. The runtime stores only request
correlation metadata and terminal outcomes in a local owner-only SQLite ledger
for 24 hours; it never stores prompts, context, model output, credentials, or
action material. Its path defaults to `oriel-request-ledger.sqlite3` and can be
selected with `python3 -m oriel --ledger /path/to/ledger.sqlite3`. The bootstrap
uses the packaged non-secret fixture and makes no provider or Home Assistant call.

Before a model call, the gateway also applies deterministic fast rules. `oriel help`
returns a fixed local response. Ambiguous control language asks for clarification;
live weather, time, music, and home-state requests state that those capabilities are unsupported;
protected or multiple-action language is denied. `create a synthetic proposal` emits
one dry-run generic proposal and never dispatches a tool. All other text continues to
the configured model. These rules do not use the evaluation corpus or expose its
fixture identifiers or state through the API.

## Configuration activation

Configuration is selected as one whole document: `--config`, then
`ORIEL_CONFIG_PATH`, then the packaged default. It is strictly validated and
activated once at process start by revision. The provider `connection_ref` is
opaque; only the composition root resolves it to an adapter-private profile.
Changing that profile mapping takes effect after restart, never by rewiring a
running process. The available effective configuration view is sanitized to
the API version, active revision, provider readiness/profile label, and names
of disabled optional skills; it never contains a reference, endpoint,
credential, header, or model setting.

## Mutation testing

Mutation testing checks whether the focused gateway tests reject small changes
to the shipped `oriel/` package. It is a development-only check and does not
add a runtime dependency. It requires Python 3.12 or later, `uv`, and a
POSIX-capable environment (Linux or WSL). Set up the pinned tool version and
run the baseline with bounded parallelism:

```sh
uv sync --group dev
uv run --group dev mutmut run --max-children 4
```

Inspect the reproducible text result and browse individual mutations with:

```sh
uv run --group dev mutmut results
uv run --group dev mutmut browse
```

`mutmut` stores resumable results in the ignored `mutants/` directory. Remove
that directory before a fully fresh run; it automatically reruns when
`pyproject.toml` or `uv.lock` changes. The runner uses POSIX process forking;
use a Linux environment or WSL. Review survivors and timeouts from `results`
before treating a run as a baseline. Oriel has no mutation-score threshold or
CI gate until that review is complete.
