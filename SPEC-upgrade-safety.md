# Spec: upgrade-safety

Status: approved outcome/scope, 2026-09-10. See `CAPABILITY_MAP.md`. Record new
Controller activation/surface choices are recorded in ADR0074 and mirrored in
the authoritative contracts. The first increment repairs existing Agent behavior.

## Objective and authority

Upgrade Controller/Agent through normal GP surfaces without interrupting
application workloads, losing Task authority or depending on a failed candidate
to recover. Preserve application containers, data and routing. The Console may
briefly reconnect and must recover the same durable operation result.

Authority: `docs/mvp.md`; ADR0010/0016 Agent lifecycle; `docs/architecture.md`
native executor/capability ownership; `docs/standards.md` typed errors, one
Runner, consumer-owned side-effect seams and bounded files.

## Reversible Agent update preparation

Acquire an operation-owned assignment pause and drain admitted sends before
checking the selected generation for active work. Busy work fails with existing
`resource.in_use` and is never aborted. Pre-publication errors/cancellation
release only that pause; irreversible fences and newer generations remain
intact. A committed or uncertain durable replacement does not authorize reopening
the original generation. Reconnect cannot bypass a pause. Destructive lifecycle
fences remain monotonic.

Resolve unknown replacement publication with a same-value primary-revision CAS
barrier. An unchanged read cannot prove non-commit. Retain unresolved holds for
automatic lifecycle reconciliation; restore restart holds from durable Task
identity before opening the Agent channel.

## Coordinated update acceptance

- Immutable current/candidate identities and compatibility are known before
  activation; staging/recovery state is private and validated.
- Bounded drain waits for active work without replaying Scripts/migrations.
- Pre-activation cancellation retains current runtime and resumes dispatch.
- Durable phase/result ownership survives Controller interruption.
- Native recovery restores compatible predecessor binary/image after failed
  startup/readiness without depending on the candidate being healthy.
- Successful rollback is a failed update with recovered health, not false update
  success. Incompatible persistent formats fail before swap.

## Structure, style and commands

Session admission: `internal/controller/agentchannel`; Agent sequencing:
`internal/controller/localagent`; infrastructure: `internal/infra`; composition
only: `internal/app`. Controller update gets its own capability module. Tests
live with their owner and carry `// Rationale:` comments. Follow existing style:

```go
return errs.New(errs.KindResourceInUse, "local Agent still has active Task assignments")
```

Use installed Go1.26.7 and repo-local temporary/cache paths:

```sh
mkdir -p .tmp/production-mvp-20260910/go-tmp .tmp/production-mvp-20260910/go-cache
export TMPDIR="$PWD/.tmp/production-mvp-20260910/go-tmp"
export GOTMPDIR="$TMPDIR"
export GOCACHE="$PWD/.tmp/production-mvp-20260910/go-cache"
export GOTOOLCHAIN=local
export PATH="/home/www/.local/share/mise/installs/go/1.26.7/bin:/home/www/.local/share/nvm/versions/node/v24.19.0/bin:$PATH"
go test -race -count=1 ./internal/controller/agentchannel ./internal/controller/localagent
go test -race -count=1 ./internal/app -run 'Test.*(Agent|Update|Upgrade)'
make generate
make ci
```

Use focused `-run` selections during red/green and full gates at qualification.

## Tests and boundaries

Use real Registry/lifecycle behavior, with doubles only at storage/host seams.
Cover rejected preparation, overlapping irreversible fences, reconnect,
cancellation, send-drain races and unknown commit responses under the race
detector. Retain existing replacement/token/generation proof.

Always preserve secret isolation, prior runtime identities, durable evidence and
Console/CLI/API parity. Ask before production/provider/other-host changes. Never
use host-executing application Scripts for self-update, auto-retry unknown
application effects or substitute source inspection for QA. Record unavailable
QA resources and continue independent implementation.
