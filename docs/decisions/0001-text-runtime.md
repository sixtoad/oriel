# D-1: CPython 3.12 standard-library text runtime

Oriel's first runnable text core uses CPython 3.12 and only the standard
library: `argparse`, `dataclasses`, `http.server`, `typing.Protocol`, and
`unittest`. This gives the bootstrap runtime zero third-party runtime
dependencies and keeps it compatible with the repository's Apache-2.0 license.

The core depends inward on a provider-neutral `ModelPort`, rather than on a
model SDK or service. Its clock, state, telemetry, and deny-by-default tool
ports follow the same pattern. This decision selects neither an audio runtime
nor a Home Assistant integration; those choices remain deferred.

The bootstrap uses Onion/Clean architecture: domain values and application use
cases depend inward only, while filesystem configuration, HTTP, and fake
implementations are outer adapters. `oriel.__main__` is the composition root.

| Logical ring | Package | Allowed dependencies |
| --- | --- | --- |
| Domain | `oriel.domain` | Domain values and standard language facilities |
| Application | `oriel.application` | `oriel.domain` and application-owned ports/types |
| Adapters | `oriel.adapters` | Domain/application contracts plus infrastructure libraries |
| Composition root | `oriel.__main__` | All rings, solely to wire them together |

`tests/test_architecture_boundaries.py` enforces this direction, rejects
dynamic and infrastructure imports from inward rings, and keeps the HTTP
adapter free of gateway and tool-policy ownership.

Oriel supports CPython 3.12 patch releases. Update to the current supported
3.12 patch during normal maintenance, validate this dependency-free runtime
with its documented demo and test suite before adoption, and record any
compatibility exception as a new decision. A future minor-version upgrade is
an explicit decision rather than an implicit runtime drift.
