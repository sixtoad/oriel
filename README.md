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
another terminal create a session and submit a turn:

```sh
curl -s -X POST http://127.0.0.1:8080/v1/sessions
curl -N -X POST http://127.0.0.1:8080/v1/sessions/<session_id>/turns \
  -H 'content-type: application/json' --data '{"input":"hello"}'
curl -s http://127.0.0.1:8080/v1/requests/<request_id>
```

The stream starts with `accepted`, emits ordered `content_delta` events, and
ends with one `terminal` event. Status lookup is passive and never replays a
turn. The bootstrap uses the packaged non-secret fixture, makes no provider or
Home Assistant call, and stores nothing persistently.

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
