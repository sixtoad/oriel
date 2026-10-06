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
`observed_at`, and `freshness`. An eventual executor may report completion only
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
or authority; a later implementation must independently establish the required
permission and completion evidence before it can enable an action. Its
permission checklist requires an existing restricted connection, proof that the
effective path permits only this contract operation, an excluded-operation denial,
and a sanitized evidence record; an account label or broad privileged connection
cannot satisfy it. Idempotency belongs to the Oriel action ledger, is never
caller-selectable, returns the existing action status for a duplicate, and never
automatically redispatches an uncertain action.

Brightness, toggle, scenes, scripts, arbitrary services or attributes,
templates, URLs, generic executor inputs, authentication, and privileged
actions are excluded from this contract.
