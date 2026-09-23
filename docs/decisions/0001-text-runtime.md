# D-1: CPython 3.12 standard-library text runtime

Oriel's first runnable text core uses CPython 3.12 and only the standard
library: `argparse`, `dataclasses`, `http.server`, `typing.Protocol`, and
`unittest`. This gives the bootstrap runtime zero third-party runtime
dependencies and keeps it compatible with the repository's Apache-2.0 license.

The core depends inward on a provider-neutral `ModelPort`, rather than on a
model SDK or service. Its clock, state, telemetry, and deny-by-default tool
ports follow the same pattern. This decision selects neither an audio runtime
nor a Home Assistant integration; those choices remain deferred.

Oriel supports CPython 3.12 patch releases. Update to the current supported
3.12 patch during normal maintenance, validate this dependency-free runtime
with its documented demo and test suite before adoption, and record any
compatibility exception as a new decision. A future minor-version upgrade is
an explicit decision rather than an implicit runtime drift.
