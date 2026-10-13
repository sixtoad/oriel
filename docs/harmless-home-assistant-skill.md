# Harmless Home Assistant capability contract

`harmless-home-assistant-skill.json` is the versioned, built-in documentation
contract for Oriel's one reviewed Home Assistant capability. It is not a
plugin, configuration input, adapter, or authorization mechanism.

Inspect it directly or run its offline check:

```sh
python3 -m unittest tests.test_harmless_home_assistant_skill -v
```

The contract selects one synthetic light alias and one explicit `on` or `off`
operation. Its argument object is closed. Reads are limited to `power_state`,
`observed_at`, and `freshness`. The disabled executor may report completion only
after a fresh observation made after dispatch has a matching power state; service
acceptance is not completion evidence. The initial deadline is five seconds.

Selection evidence is deliberately separate from action enablement. The
sanitized audit conclusion records that the reviewed bounded effect was judged
harmless, without publishing a real entity, connection, provider response, or
automation detail. The synthetic read fixtures include fresh, stale, and
unavailable evidence while keeping the selected read shape inspectable without
provider I/O.

The text gateway can render the reviewed synthetic fact only after closed
manifest validation. Its structured fact event contains only `power_state`,
`observed_at`, and `freshness`; fresh observations are named as fresh, stale
observations remain stale, and unavailable reads say so without inventing a
current state. This demo does not contact Home Assistant or expose an entity,
connection, credential, or provider response.

The read request has its own closed operation ID and accepts exactly the
operation, synthetic target, and approved field list recorded in the JSON
contract. It is not an action proposal and cannot carry desired state,
dispatch controls, or provider details.

Execution remains disabled. The negative permission fixture records that the
restricted existing call path is unverified, expects zero dispatch, and leaves
G2 incomplete. Selecting this contract grants no credential, authentication,
or authority; live activation must independently revalidate the required
permission and completion evidence before it can enable an action. Its
permission checklist requires an existing restricted connection, proof that the
effective path permits only this contract operation, an excluded-operation denial,
and a sanitized evidence record; an account label or broad privileged connection
cannot satisfy it. Idempotency belongs to the Oriel action ledger, is never
caller-selectable, returns the existing action status for a duplicate, and never
automatically redispatches an uncertain action.

Dry-run is an explicit simulation only. A successful preview requires a
reviewed enabled manifest injected by a focused test; the shipped manifest
stays disabled. It validates the canonical synthetic alias and closed `on` or
`off` argument independently, returns `simulated`, and records zero action I/O.

Brightness, toggle, scenes, scripts, arbitrary services or attributes,
templates, URLs, generic executor inputs, authentication, and privileged
actions are excluded from this contract.

## Disabled execution increment (Story 3.8)

The separate text commands `turn on the reviewed harmless light` and
`turn off the reviewed harmless light` select execution intent. Existing
`create a reviewed harmless light proposal on/off` commands remain previews.
Model proposals alone never select execution. The turn HTTP request is unchanged.

Execution requires the current enabled manifest plus immutable, revision-matched
prerequisite evidence for direct/indirect effect review, a restricted existing call
path, denial of an excluded operation, and a usable confirmation observation.
Operator flags and availability alone cannot supply this evidence. Shipping
composition supplies neither enablement nor prerequisite evidence, and the worker
has no live provider binding. Future live activation requires a reviewed,
worker-local fixed provider binding; real mapping and connection material must
never enter gateway requests or results. No live operation or credential change
was performed for this increment.

The worker's injected fixed-light provider accepts only explicit `on`/`off` and
an absolute monotonic deadline. The same budget (at most five seconds) covers
channel connection, request/reply I/O, the single dispatch, and observation.
Every provider I/O must respect that deadline. Channel timeout cannot authorize
another attempt. Providers are never given caller-selected URLs, services,
attributes, or entity mappings.

`confirmed` requires a fresh matching observation fetched after dispatch, even
when the light was already in the requested state. Acceptance alone, cached or
stale facts, mismatch, transport ambiguity, observation loss, and elapsed budgets
cannot produce success. Results expose only a closed status, reason, evidence
strength, and optional bounded power state and UTC observation time. Required
reservation/audit precede dispatch; result evidence is persisted atomically with
its terminal action state before publication. Existing ledgers migrate their
constraints transactionally without losing reservation or audit rows. Migration
failure blocks action readiness.

Cancellation and policy changes retain the commitment fence. Cancellation
suppresses generated content; durable confirmed evidence may accompany a cancelled
turn. Ambiguous evidence instead requires `outcome_unknown`. Recovery never
replays, and late confirmation cannot overwrite an unknown action. Result-write
failure degrades readiness and cannot emit confirmation.

Run the controlled-fixture demonstration and failure matrix with:

```sh
python3 -m unittest tests.test_ha_execution tests.test_ha_worker -v
```

These tests include matching observations, on/off intent, acceptance with lost
observation, stale/mismatched/cached evidence, deadline expiry, cancellation,
recovery, and old-ledger migration/reopen. Fixture evidence is explicitly scoped
`controlled_fixture` and cannot enable a worker configured for `live`.
**Live acceptance remains pending, and G2 remains incomplete.** Synthetic tests
do not establish real permission restrictions or a live observed state change.
