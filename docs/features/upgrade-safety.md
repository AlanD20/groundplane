# Safe Controller and Agent updates

## Purpose and scope

Update Groundplane through its normal Console, CLI and API without interrupting
application workloads or relying on a failed candidate to recover. Keep application
containers, data, routing and durable Tasks intact. The Console may briefly
reconnect, but it must recover the result of the same update operation.

This feature belongs to the [production initiative](../../CAPABILITY_MAP.md).
[ADR0074](../decisions/0074-native-controller-upgrade-recovery.md) defines native
Controller activation and recovery; [architecture.md](../architecture.md) defines
the shared process boundaries. Application Scripts do not perform self-update.

## Functional requirements

- Before activation, identify the current and candidate releases, check their
  compatibility, and validate private staging and recovery state.
- Pause new Agent assignments and drain admitted sends before checking for active
  work. Busy work returns `resource.in_use`; an update must not abort it.
- Wait for active work within the update's drain bound. Do not replay application
  Scripts or migrations as part of the update.
- If preparation fails or is cancelled before publication, release only that
  operation's pause. Keep removal/revocation restrictions and newer Agent
  generations intact. Reconnecting must not bypass a pause.
- A committed replacement, or one whose commit result is unknown, must not reopen
  the original Agent generation. Reconcile an unknown result before dispatch resumes.
- Retain the update's phase and result across Controller interruption. Restore
  unfinished Agent holds from the durable Task before opening its channel.
- Recover a compatible predecessor binary and Agent image after candidate startup
  or readiness failure. Recovery must work when the candidate cannot run.
- Report a failed update with recovered health as a failed update, not success.
  Reject incompatible persistent formats before replacing the running binary.

## Non-functional requirements

- Preserve secret isolation, prior runtime identities and durable recovery evidence.
- Preserve operation ownership during cancellation, reconnect, concurrent lifecycle
  changes and lost commit responses. Never automatically retry unknown application
  effects or weaken a permanent lifecycle restriction to release an update pause.
- Keep staging and recovery private. Use the immutable input, compatibility and
  timing rules in ADR0074; do not add arbitrary host execution or new update surfaces.
- Keep Console/CLI/API parity. Production, provider and other-host changes need
  explicit operational approval; a feature requirement is not that approval.

## Technical design

| Responsibility | Owner |
| --- | --- |
| Agent sessions and assignment admission | [agentchannel](../../internal/controller/agentchannel/) |
| Agent replacement and lifecycle reconciliation | [localagent](../../internal/controller/localagent/) |
| Native Controller update and recovery | [controllerupgrade](../../internal/controller/controllerupgrade/) |
| Host operations | [infra](../../internal/infra/) |
| Wiring these modules together | [app](../../internal/app/) |

An update pause belongs to one operation and is reversible only before replacement
publication. Permanent lifecycle restrictions are separate and remain in force.
This separation lets rejected preparation resume work without reopening removed
or replaced generations.

For an unknown publication result, an unchanged read cannot prove that the write
did not commit. A same-value, primary-revision compare-and-swap establishes the
ordering needed to resolve that uncertainty. Keep the hold until lifecycle
reconciliation resolves it; restart recovery uses the original durable Task.
ADR0074 specifies the native journal, predecessor guard and activation sequence.

## Acceptance

Use real session-registry and lifecycle behavior, with test doubles only at
storage and host boundaries. Adjacent tests must cover rejected preparation,
overlapping permanent restrictions, reconnect, cancellation, send-drain races,
unknown commit results and restart recovery. Retain the existing replacement,
token and generation checks. Run affected concurrency tests with the race detector.

The full operator journey must prove immutable staging, bounded drain,
pre-activation cancellation, candidate failure, predecessor recovery and preservation
of application workloads. Source inspection is not live qualification.
Use the [verification policy](../delivery.md#verification-ladder) for focused checks,
generation checks and final CI; all temporary state stays repository-local.

## Current status

Requirements were approved on 2026-09-10. Local implementation and bounded QA
are recorded in [Agent admission](../acceptance/2026-09-10-agent-update-admission.md),
[native coordinator](../acceptance/2026-09-10-native-controller-coordinator.md),
[update surfaces](../acceptance/2026-09-10-native-update-surfaces.md) and
[upgrade QA](../acceptance/2026-09-10-native-upgrade-qa.md).

Whole-build Tunnel continuity and remaining recovery qualification are unresolved.
Live mutation checks are paused for the [storage incident](../acceptance/2026-09-10-build-capacity-incident.md).
These records do not establish production readiness. Current operational limits
and unfinished qualification are in [head.md](../head.md) and [the task list](../../tasks/todo.md).
