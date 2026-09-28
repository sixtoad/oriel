# Offline measurement results

`results-schema.json` describes version 1.0 measurement records. The normative
validator and scorer is `scripts/score_results.py`; it uses only Python's
standard library, reads supplied JSON files, and does not capture evidence,
contact providers, execute corpus proposals, or read a system clock.

The committed [synthetic fixture](fixtures/synthetic-results.json) is a format
demo, not a baseline or Oriel measurement. Its output must not be used as
capability, performance, release, or action evidence. Later separately captured
and sanitized records may declare `"provenance": "captured_sanitized"` and use
the same format; no payloads or transcripts belong in a record.

## Run the reproducible demo

From the repository root:

```sh
python3 scripts/score_results.py evaluation/fixtures/synthetic-results.json
python3 scripts/score_results.py evaluation/fixtures/synthetic-results.json > /tmp/synthetic-summary.json
cmp /tmp/synthetic-summary.json evaluation/fixtures/synthetic-summary.json
python3 -m unittest discover -s tests -v
```

The first command prints stable, versioned JSON. The committed
`synthetic-summary.json` is its exact expected output. The runner validates the
committed corpus before it resolves the corpus ID, revision, case IDs, or case
revisions. `--corpus PATH` selects another already-captured corpus definition,
but that definition must satisfy the corpus validator first.

## Record fields

A record requires its corpus ID/revision; component and model IDs/revisions;
generation strategy and output bound; named load condition; anonymized resource
profile; method; and provenance. Each sample pins a corpus case ID/revision,
declares `text` or `voice`, `cold` or `warm`, and has one terminal outcome:
`success`, `failure`, `deferred`, or `unsupported`.

Each sample also contains expected and observed correctness. They are either
both booleans, which makes that sample assessable, or both `null`, which requires
the `not_assessable` side-effect outcome. A side-effect outcome is separately
recorded as `none`, `expected`, `unexpected`, or `not_assessable`; it does not
execute or describe a live action.

Every sample supplies all eleven named stage entries: `wake`, `end_of_turn`,
`final_transcript`, `route`, `first_model_token`, `first_useful_content`,
`acknowledgement`, `first_audio`, `static_validation`, `action_completion`, and
`final_audio`. An observed entry has only `observed_monotonic_ms`, and its offset
is its local timestamp minus that sample's `anchor_monotonic_ms`. Each timestamp
must be at or after the anchor. Stage names do not impose an event order; for
example, acknowledgement and static validation may interleave with stream
stages.

An unmeasurable local span instead has exactly one `unavailable_reason`. A
cross-system or otherwise incomparable event has exactly one `uncertainty`.
Both are bounded (64-character) lowercase hyphenated identifiers, so a record
cannot retain arbitrary payload-like text in a summary. They remain missing in
the output; the scorer never treats either form as zero milliseconds. Summary
stage reports show eligible, observed, unavailable, and uncertain counts plus
separate `unavailable_reasons` and `uncertainty_reasons` maps.

## Scores and denominators

The output groups submitted samples by population and thermal condition. It
does not synthesize absent groups. Terminal outcome counts remain visible in each
group. Reliability uses every submitted sample except `unsupported`: its
numerator is terminal `success`, so failures and deferred samples remain in the
denominator. Correctness uses every explicitly assessable sample; a failed turn
therefore remains in the correctness denominator when it has a correctness
assessment. Unsupported cases are counted separately and have no reliability
denominator entry.

For each stage, percentiles use the deterministic nearest-rank rule over only
observed local offsets: sort the offsets, select one-based
`ceil(q * n)`, and report that value for p50 and p95. No observed offsets means
both percentiles are `null`.

Invalid input exits 1 with sanitized field locations or a sanitized JSON error.
Duplicate keys, unknown fields, wrong corpus references, mixed stage state
fields, non-finite/type-invalid timestamps, non-finite computed offsets,
timestamps before the anchor, arbitrary stage reasons, failed turns without a
false observed correctness assessment, and inconsistent correctness
assessability are rejected. Diagnostics do not echo the supplied file name,
payloads, or a traceback.
