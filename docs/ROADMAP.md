# Oriel roadmap

## Purpose

Oriel is a private, portable voice-assistant gateway. It owns conversation and
tool routing; Home Assistant is a deliberately constrained tool provider, not
Oriel's brain or identity authority. This roadmap preserves the Office-speaker
work already completed and turns it into independently deliverable milestones.

## Product invariants

1. **Local-first and portable.** One gateway image runs as a Home Assistant OS
   app, externally, or in a hybrid topology.
2. **Conversation is not execution.** Models propose typed actions; they never
   receive broad Home Assistant, Keycloak, or system credentials.
3. **Least privilege.** Tools are explicit and allow-listed. Oriel starts with
   no sensitive actions.
4. **No invented identity.** A voice is anonymous unless Oriel owns a
   cryptographically bound audio-to-action identity session.
5. **Responsive speech.** Turn detection, fast routing, acknowledgement,
   streamed output, and barge-in are product features.
6. **Keep HA useful.** The existing HA Assist pipeline remains a safe fallback
   while Oriel is introduced in parallel.

## Preserved Office baseline

The following is confirmed working today:

```text
FutureProofHomes Satellite1 (Office; ESPHome; Okay Nabu)
  -> Home Assistant Assist pipeline
  -> Wyoming STT on ai-gpu
  -> NPU Whisper (whisper-v3:turbo)
  -> Extended OpenAI Conversation
  -> resident Qwen 3.6 35B-A3B on ai-gpu
  -> Piper (en_US-lessac-medium)
  -> Satellite1
```

Physical wake, transcription, a safe tool call, and audible Piper response
were confirmed. The operating runbook remains in the homelab repository at
`docs/home-assistant-voice.md`.

### Constraints discovered

- HA Assist has no supported global hook that intercepts every action from an
  existing conversation agent.
- Standard Assist/Wyoming does not carry verified speaker identity from raw
  audio to the later conversation/tool request.
- HA entity permissions apply only when a call has a `Context.user_id`; a
  system/anonymous context is not a deny-all user.
- Automations and in-core callers are not delegated executions of the user who
  created them. Native HA permissions cannot express action-specific LoA.

Until Oriel owns an authenticated end-to-end voice session, the Office HA
pipeline is anonymous and must expose only explicitly safe capabilities.

## Target architecture

```text
speaker or app client
  -> Oriel Gateway
       -> turn detection / STT / session management
       -> fast deterministic router and acknowledgement
       -> Qwen conversation worker
       -> Tool Router
            -> safe Home Assistant adapter
            -> music, calendar, knowledge, future skills
            -> Oriel Guard (protected-action broker)
       -> streamed TTS

Oriel Guard
  -> Keycloak step-up
  -> future room authenticator (fingerprint/FIDO2)
  -> one-time action grant
  -> protected Home Assistant executor
```

Pipecat is the leading runtime candidate, but the choice is deferred until the
Satellite1 external-audio transport has been proven.

## Delivery roadmap

### Phase 0 — preserve and measure the baseline

**Outcome:** a repeatable compatibility target; Oriel has no HA action access.

- Version a test corpus: normal chat, time/weather, safe light control, music,
  a follow-up question, interruption, and a deliberately denied request.
- Capture versions/endpoints without committing credentials or device keys.
- Add spans for wake, end-of-turn, final transcript, selected route, first
  model token, first audio, authorization, action completion, and final audio.
- Record current latency and define target budgets.

**Gate:** corpus results and baseline latency are reproducible.

### Phase 1 — Oriel Core, text-only

**Outcome:** a portable conversation gateway independent of audio and HA.

- Define a stable API for request, streamed response, session ID, trace ID, and
  typed tool proposal.
- Reuse the existing OpenAI-compatible Qwen endpoint as the first backend.
- Add explicit conversation retention/redaction policy.
- Add a deterministic first responder that classifies obvious work and emits an
  acknowledgement when full reasoning exceeds the latency budget.
- Test routing, streaming, cancellation, and backend failure.

**Gate:** Oriel matches the agreed text baseline without possessing an HA token.

### Phase 2 — safe Home Assistant skill

**Outcome:** HA is Oriel's first skill, not a general service-call API.

- Implement named, typed, allow-listed Home Assistant tools.
- Start with read-only home context and one harmless Office action.
- Validate target/arguments, apply timeouts and idempotency rules, emit audit
  events, and support dry-run.
- Use a dedicated low-privilege HA identity wherever the path supports it; do
  not use an owner/admin token for normal tool execution.
- Prove that prompt injection, malformed arguments, and excluded actions fail.

**Gate:** safe corpus actions succeed; non-manifest actions cannot run.

### Phase 3 — parallel Office voice gateway

**Outcome:** replicate the Office experience without losing HA fallback.

- Verify Satellite1/ESPHome options for an external gateway; do not assume HA's
  native Assist transport can simply be repointed.
- Reuse Whisper and Piper; add wake/VAD input, streamed audio, interruption,
  and barge-in.
- Keep the HA Assist pipeline selectable as the fallback.
- Compare Oriel and HA using the Phase 0 corpus and telemetry.
- Acknowledgements are deterministic and never claim a request was completed or
  approved before it is true.

**Gate:** controlled Office trial meets the agreed latency/reliability targets
and rolls back to HA immediately.

### Phase 4 — portable packaging

**Outcome:** others can run Oriel without adopting this homelab.

- Publish a versioned OCI image, configuration reference, security model, and
  quick-start documentation.
- Add a least-privilege HAOS app wrapper with app-specific persistence and an
  Ingress administration UI.
- Add external deployment instructions/automation using the same image.
- Add a thin HA custom integration for health/configuration and optional speaker
  integration; it must not duplicate the gateway.
- Support `local`, `external`, and `hybrid` modes, plus upgrade and backup/
  restore guidance.

**Gate:** a new user deploys locally or externally, connects a model backend,
uses the safe HA skill, and upgrades without modifying HA internals.

### Phase 5 — identity foundations

**Outcome:** voice LoA1, explicitly not sensitive-action authority.

- Define an independent voice-verifier interface.
- Issue signed, short-lived LoA1 assertions containing subject, verifier,
  device, issue/expiry time, replay ID, confidence, and liveness outcome.
- Bind assertions to an immutable Oriel session beginning at audio capture;
  never infer identity from timing or a transcript.
- Add enrolment, revocation, consent, retention, spoof/replay testing, and
  false-accept/false-reject reporting.
- Map a verified speaker to a Keycloak subject only for personalisation and
  other low-risk policy.

**Gate:** Oriel reliably reports verified, unknown, or unavailable identity
without changing protected devices.

### Phase 6 — protected actions and Keycloak step-up

**Outcome:** sensitive actions are approved before they reach HA.

- Deploy Oriel Guard as a separate authorization service and pending-action
  store.
- Evaluate policy over subject, role, channel, room/device, canonical action,
  target, current LoA, and risk level.
- Create one-time, short-lived transactions bound to exact action and Keycloak
  subject.
- Start Keycloak LoA2/3 step-up; validate issuer, audience, expiry, subject,
  and `acr` before issuing an action grant.
- Use a protected HA executor that accepts a valid one-time grant only. Direct
  ordinary HA access remains denied at the tool/proxy boundary.
- Respond clearly with pending, approved, expired, denied, or failed; never
  silently retry a protected action.

**Gate:** one protected action completes once after step-up; replay, target
change, wrong user, expiry, and direct invocation all fail.

### Phase 7 — room hardware approval

**Outcome:** physical LoA2/3 approval, such as a future Office fingerprint
reader.

- Prefer FIDO2/WebAuthn hardware or a reader providing a cryptographically
  verifiable assertion; never export raw fingerprint templates.
- Add a Keycloak Authenticator SPI or broker-mediated back-channel that verifies
  reader identity and an action-bound challenge.
- Speak a clear room-specific approval request and reject ambiguous/stale
  approvals.
- Test reader loss, duplicate readers, wrong-person approval, spoofing, network
  loss, and recovery.

**Gate:** the reader approves only the exact pending transaction for the correct
person, with no biometric material stored by Oriel.

### Phase 8 — ecosystem and hardening

**Outcome:** more skills without weakening trust boundaries.

- Add optional Music Assistant, calendar, knowledge, and web skills through
  separate manifests and consent/configuration flows.
- Add browser/app clients with Keycloak identity and an explicit step-up UI.
- Add memory scopes, export/delete controls, audit views, and observability.
- Threat-model external tools, run prompt-injection/adversarial tests, and
  publish a security support policy.

## Assurance tiers

| Tier | Example | Authority |
| --- | --- | --- |
| Anonymous safe | Weather, timer, selected lights/music | Static allow-listed tool |
| Verified LoA1 | Low-risk personalisation | Bound voice assertion |
| LoA2 | Unlock, disarm, sensitive change | Keycloak step-up + action grant |
| LoA3 | High-impact irreversible action | Stronger flow + explicit confirmation |

Only the anonymous-safe tier belongs in v0.1.

## Decisions made

- **Name:** Oriel.
- **License/repository:** public `sixtoad/oriel`, Apache-2.0.
- **Deployment:** HAOS app and external deployment are both first-class.
- **Reuse:** existing local Qwen, Whisper, Piper, Music Assistant, HA, and
  Keycloak services remain usable backends.
- **HA role:** constrained tool provider and fallback voice system, not the
  general-agent runtime or speaker-identity authority.
- **Security:** the future broker is separate from HA Core and the model; no
  model gets broad operational credentials.

## Open decisions

1. Pipecat, LiveKit Agents, or a smaller dedicated runtime.
2. Satellite1's viable external-audio path and whether it needs a bridge.
3. Initial implementation language and public skill/plugin protocol.
4. State store, encryption/key management, and backup format.
5. HA authentication for the safe adapter.
6. Voice-verifier model, enrolment, liveness, and accuracy thresholds.
7. First protected action and the actual room-authenticator hardware.
8. Trademark, package-registry, namespace, and domain availability for Oriel.

## Explicit v0.1 non-goals

- Replacing Home Assistant.
- Unrestricted shell, browser, arbitrary HTTP, or administrative HA tools.
- Voice biometrics, Keycloak customization, fingerprint processing, or sensitive
  actions.
- A cloud-model requirement.
- Moving or deleting the working Office HA Assist pipeline.

## Key risks and controls

| Risk | Control |
| --- | --- |
| Regressing the speaker | Parallel rollout, corpus, telemetry, HA fallback |
| Hallucination/prompt injection | Typed allow-listed tools, independent authorization, dry-run |
| HA coupling | Stable APIs, thin integration, no Core patch in v0.1 |
| Spoof/replay | Session-bound signed assertions, liveness, expiry, replay IDs |
| Boundary collapse | Separate model, gateway, broker, and executor credentials |
| Slow interaction | Fast router, streamed TTS, interruption and latency budgets |
| Privacy loss | Local defaults, retention controls, no raw biometric storage |

## Immediate next work

1. Turn this roadmap into the Oriel product brief and PRD.
2. Create the Phase 0 corpus and latency-measurement format.
3. Implement the Phase 1 Gateway API skeleton.
4. Run an audio-runtime and Satellite1-transport spike before changing the
   selected Office pipeline.
