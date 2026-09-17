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
