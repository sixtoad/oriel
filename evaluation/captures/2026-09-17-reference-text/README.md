# Sanitized reference text capture — 2026-09-17

This is a captured, sanitized baseline for the existing local Qwen conversation
backend. It uses the `chat-greeting` synthetic corpus case, a direct
OpenAI-compatible text request, temperature zero, and a 1,024-token client cap.
It contains two successful warm text samples plus one structured unsupported
voice entry. `first_useful_content` is the time at which the non-streaming client
received the complete usable response; first-token latency is deliberately
unavailable. The direct backend probe has no observed Assist transcript or route
stage, so those spans are also unavailable rather than zero.

The checked reference environment reported a running Home Assistant core and a
reachable local Qwen text backend. The operator confirms that the existing Assist
pipeline is configured and in use. This capture does **not** invoke that pipeline:
it does not measure its wrapper, tool dispatch, STT, TTS, wake-word handling, or
end-to-end voice. The voice entry says `assist-entry-uninstrumented`, rather
than representing an attempted voice run. It made no tool call and no device or
Home Assistant state change. Those spans are represented as unavailable rather
than zero.

The transcript is synthetic and sanitized. It has no endpoint, credential,
household, infrastructure, or personal-assignment data. The raw transport
responses and any access material are intentionally not retained in this
repository.

Reproduce the report from the repository root:

```sh
python3 scripts/score_results.py evaluation/captures/2026-09-17-reference-text/results.json
```

This report is evidence for the preserved text backend only. It is not a G0 pass,
a full Assist-pipeline evaluation, a voice result, an Oriel measurement, or a
release claim. A later capture must use an explicitly instrumented Assist entry
point to fill the pipeline-level text and voice spans.
