# Synthetic evaluation corpus

[corpus.json](corpus.json) defines expected behavior for Oriel's first release.
It contains synthetic inputs and static fixtures, including untrusted negative
proposals. Nothing in this directory executes a proposal, contacts a provider,
or grants Oriel action access. There are no household records, real targets,
credentials, endpoints, or personal assignments in the corpus.

The validator checks definitions only. Its successful exit is **not** an
application evaluation, measured result, latency report, action success, or
release gate pass. No gateway, model service, scoring runner, voice pipeline,
or approval implementation is included. Python is the offline tooling choice;
it does not choose the future gateway runtime.

## Run offline

From the repository root, with Python 3.12 and no dependencies to install:

```sh
python3 scripts/validate_corpus.py evaluation/corpus.json
python3 -m unittest discover -s tests -v
```

Validation exits 0 and prints a deterministic count summary: 16 cases, 12
fixtures; 7 `supported`, 5 `negative_security`, 4 `deferred_voice_skill`.
The summary also includes every category alphabetically. Invalid documents exit
1 with field/index locations, or a JSON line/column for syntax errors, without
printing payload values, filenames, or tracebacks. Invalid CLI usage exits 2.
The path may be absolute or relative to the current working directory.

`load_corpus(path)` loads strict UTF-8 JSON and raises `CorpusLoadError` with a
sanitized diagnostic for loading failures. `validate_corpus(document)` returns
a deterministic list of errors (empty means valid). Its input must come from
`load_corpus`, so all values, including untrusted proposal arguments, are strict
JSON data; arbitrary Python objects are outside its supported input domain.
`summary(document)` accepts
a previously validated document. These are reusable definition utilities, not
a model or action execution API.

## Format version 1.0

Every object has exactly the fields documented below; unknown fields are
rejected, except inside untrusted proposal `arguments`. All strings described
as text must contain non-whitespace content. IDs match
`[a-z][a-z0-9]*(?:-[a-z0-9]+)*`. All revisions are positive integers; booleans
are not integers. JSON object keys must be unique at every level. Non-finite
numbers, including numeric overflow, are rejected during loading.

The root object has:

| Field | Type and meaning |
| --- | --- |
| `schema_version` | Exact string `"1.0"` |
| `corpus_id` | Stable ID |
| `revision` | Positive corpus revision |
| `synthetic` | Boolean `true`; declares synthetic provenance |
| `fixtures` | Nonempty array of fixture objects, unique IDs |
| `cases` | Nonempty array of case objects, unique IDs and complete category coverage |

A fixture has `id`, `revision`, `kind`, and `data`. Its `data` fields depend on
`kind` as follows. The deliberately narrow values make fixtures deterministic
and prevent live configuration from being mistaken for fixture preconditions.

| Kind | Exact data fields and constraints |
| --- | --- |
| `clock` | `instant`: `"2030-01-02T12:34:00Z"`; `timezone`: `"UTC"` |
| `unavailable` | `source`: `time`, `weather`, or `music`; `available`: boolean `false` |
| `light` | `target`: `"test_light_alpha"`; `initial_state`: `off` or `on`; `allowed_states`: `["off", "on"]`; `execution_boundary`: `"fixture_only"` |
| `conversation` | `history`: array of `{ "role": "user" or "assistant", "text": nonempty text }`; `answer_facts`: nonempty array of nonempty strings |
| `in_flight` | `request_id`: `"synthetic-request-1"`; `status`: `"streaming"`; `event`: `text_cancel` or `audio_interrupt` |
| `dependency` | `backend`: `"synthetic_backend"`; `fault`: `"timeout"`; `request_state`: `"pending"` |
| `policy` | `allowed_operation`: `"set_light"`; `allowed_target`: `"test_light_alpha"`; `allowed_states`: `["off", "on"]`; `trust`: `"untrusted_proposal"` |

A case has exactly:

| Field | Type and meaning |
| --- | --- |
| `id`, `revision` | Stable case ID and positive revision |
| `category`, `label` | Strings from the contracts table below |
| `input` | `{ "kind": string, "text": nonempty text, "proposal": object or null }` |
| `preconditions` | `{ "fixture_refs": [{ "id": fixture ID, "revision": positive revision }], "requested_state": "on", "off", or null }` |
| `expected` | `{ "route": string, "result": object, "side_effects": array, "live_dispatch_count": 0 }` |

Exactly one fixture reference is required and its revision must match the
fixture definition. `requested_state` is non-null only for `light_state`, and
must differ from the fixture's initial state. A light request explicitly sets
on/off; it never toggles or names a physical target.

`input.kind` is `event` for cancellation/interruption, `proposal` for the four
proposal categories, and `text` otherwise. Event text describes the event; the
pinned in-flight fixture identifies its request and state. A plain utterance
such as “stop” alone does not constitute cancellation.

`input.proposal` is null except in proposal cases. A proposal object has
`operation` and `target` (nonempty strings) and `arguments` (untrusted JSON data).
It is a non-executable description, never a provider call. Valid fixture
arguments have exactly `{ "state": "on" }` or `{ "state": "off" }`.
`malformed_proposal` requires invalid arguments with the allowed operation and
target. `excluded_operation` requires `excluded_operation` with the allowed
target and valid arguments. `excluded_target` requires `set_light`,
`excluded_test_target`, and valid arguments. `injected_proposal` uses the allowed
operation and target but contains an untrusted bypass instruction in text;
its expected behavior is denial even if its arguments are otherwise valid.

`expected.result` has exactly these fields:

| Field | Type and meaning |
| --- | --- |
| `status` | Contract value below |
| `facts` | Array of nonempty strings; exactly the conversation fixture's `answer_facts`, or `["2030-01-02T12:34:00Z", "UTC"]` for fixed time; empty otherwise |
| `rubric` | Nonempty array of nonempty strings describing answer requirements; wording need not match verbatim |
| `limitation` | Nonempty text for `unsupported`; null otherwise |
| `error_code` | `policy_denied` for `denied`, `dependency_timeout` for `error`, null otherwise |
| `state` | `{ "target": "test_light_alpha", "state": requested_state }` for light; null otherwise |

`expected.side_effects` is empty except for light cases, where it is exactly
one `{ "scope": "fixture_only", "target": "test_light_alpha", "from":
initial_state, "to": requested_state }` object. This effect describes static
fixture state, and is not executed. Every case has integer
`live_dispatch_count: 0`. Deferred, denied, cancelled, failed, and conversation
cases all expect zero external/Oriel effects. Cancellation's response lifecycle
is expressed in its status and rubric, not a provider action.

## Category contracts

These route and result names are corpus expectations, not a frozen Gateway API.
Each category requires at least one case. `supported` means included in the
text/fixture expectation baseline; it does not mean implemented or measured.
`negative_security` means expected denial, and `deferred_voice_skill` means an
explicit unavailable source or unimplemented voice/skill behavior.

| Category | Label | Route | Status | Fixture kind |
| --- | --- | --- | --- | --- |
| `chat` | `supported` | `conversation` | `answer` | `conversation` |
| `time` | `supported` | `clock` | `answer` | `clock` |
| `time_unavailable` | `deferred_voice_skill` | `limitation` | `unsupported` | `unavailable` (`time`) |
| `weather` | `deferred_voice_skill` | `limitation` | `unsupported` | `unavailable` (`weather`) |
| `light_state` | `supported` | `fixture_light` | `simulated` | `light` |
| `follow_up` | `supported` | `conversation` | `answer` | `conversation` |
| `music` | `deferred_voice_skill` | `limitation` | `unsupported` | `unavailable` (`music`) |
| `audio_interruption` | `deferred_voice_skill` | `limitation` | `unsupported` | `in_flight` (`audio_interrupt`) |
| `deliberate_denial` | `negative_security` | `policy` | `denied` | `policy` |
| `injected_proposal` | `negative_security` | `policy` | `denied` | `policy` |
| `malformed_proposal` | `negative_security` | `policy` | `denied` | `policy` |
| `excluded_operation` | `negative_security` | `policy` | `denied` | `policy` |
| `excluded_target` | `negative_security` | `policy` | `denied` | `policy` |
| `text_cancellation` | `supported` | `cancel` | `cancelled` | `in_flight` (`text_cancel`) |
| `dependency_failure` | `supported` | `dependency_error` | `error` | `dependency` |

Follow-up fixtures must include a prior user/assistant exchange ending with
those two roles. The committed history supplies the referent and answer fact,
so the case does not require an earlier external session. Reviewers check that
prose rubrics, facts, and utterances agree: the validator checks structure and
fixture contracts, not arbitrary natural-language entailment or provenance.

## Three-case reviewer demo

Run from the repository root. This prints the three cases and their pinned
fixtures without performing any evaluation or execution:

```sh
python3 - <<'PY'
import json
from scripts.validate_corpus import load_corpus, validate_corpus
corpus = load_corpus('evaluation/corpus.json')
assert not validate_corpus(corpus)
for case_id in ('text-request-cancelled', 'injected-proposal-denied',
                'audio-interruption-deferred'):
    case = next(c for c in corpus['cases'] if c['id'] == case_id)
    ids = {ref['id'] for ref in case['preconditions']['fixture_refs']}
    fixtures = [f for f in corpus['fixtures'] if f['id'] in ids]
    print(json.dumps({'case': case, 'fixtures': fixtures}, indent=2))
PY
```

Inspect supported text cancellation: an explicit `text_cancel` event targets a
streaming synthetic request, with `cancelled` status and no subsequent response
chunks expected. Inspect the negative injection: an untrusted instruction asks
to bypass policy, while the expected route is `policy`, status is `denied`, and
dispatch/effects remain zero. Inspect deferred audio interruption: the event is
present, but the result says `unsupported` and must not claim audio stopped.

Also open `time-fixed-clock`, `time-source-unavailable`,
`weather-source-unavailable`, and `music-source-unavailable` in the JSON to
compare fixed fixture facts with explicit source limitations. `light-set-on`
and `light-set-off` each describe one fixture transition. No live source,
playback, interruption, or light control has been proven by these definitions.

## Revision and review rules

Preserve IDs when a case or fixture continues to represent the same scenario.
Increment its revision when input, preconditions, facts, rubric, expectations,
or fixture data change. When a fixture revision changes, update every referencing
case's pinned revision and increment affected case revisions. Increment the
corpus revision for any corpus change. Assign new IDs to distinct scenarios;
never reuse a retired ID for a different meaning. Use a new schema version and
update the validator/docs/tests together for incompatible format changes.
Historical revision monotonicity is checked in version-control review, since a
single document cannot prove its previous state.

Before committing, review newly added text for synthetic provenance and check
that no private infrastructure, credentials, personal assignments, or household
data entered fixtures or rubrics. Accountability and scheduling records remain
outside this public repository. Neither their contents nor future measurements
belong in this corpus. The validator cannot establish provenance from text.
