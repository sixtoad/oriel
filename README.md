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

A [sanitized captured reference-text report](evaluation/captures/2026-09-17-reference-text/README.md)
replays a real existing local-Qwen backend measurement. It is deliberately
limited to the text backend and does not represent full Assist or voice evidence.
