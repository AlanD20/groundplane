# Archived: Floor MVP Volume removal integration plan

Historical planning snapshot, superseded by the completed floor handoff in
`docs/acceptance/2026-09-09-integrated-floor-qa.md`. The unchecked statements
below record the original plan, not current implementation status.

Status: owner approved; existing work is consolidated into verified commits.
This cutover is the next implementation task.

This is one remaining floor-MVP slice, not a redefinition of the overall goal.
Manual Script completion, remaining source-family guards, retained-runtime
Volume/Entry mutations, oversized Blueprint publication, live acceptance, and
delivery gates remain open as recorded in `docs/head.md`.

## Outcome and authority

Connect the accepted ADR0049 Volume removal operation to the real DELETE,
Controller/Agent channel, bounded filesystem helper, retry, and terminal
publication. ADR0051 remains the sole desired-state publisher; ADR0062 owns
Script source references. No compatibility path, second desired head, or
Controller-owned filesystem deletion is introduced.

Relevant contracts:

- `docs/decisions/0049-volume-identity-and-removal.md`
- `docs/decisions/0051-blueprint-staged-revision-publication.md`
- `docs/decisions/0062-prepared-script-source-reference-generations.md`
- `docs/architecture.md`, managed Volume and Script reference sections
- `docs/standards.md`, transaction, module, test, and file-size rules

## Current evidence

- `internal/volume/mutations.go` publishes removal through the ordinary desired
  revision method. The new local source-removal guard checks existing Script
  counts, but does not acquire a durable Volume retirement operation.
- `internal/controller/volume_plan.go` constructs a fixed removal plan. Its
  directory step begins with a nil cursor.
- `internal/infra/etcd/volumeremoval` implements runtime/checkpoint records and
  repository methods, but has no production callers. `Create` writes separately
  from desired publication; `FinalizeCheckpoint` advances a runtime checkpoint
  separately from Task/idempotency finalization. Neither is an atomic integration
  point as currently shaped.
- `proto/agent.proto` has an Environment directory terminal result but no Volume
  checkpoint exchange. Terminal reporting cannot replace durable per-call
  authorization, completion, and reconnect recovery.

## Required sequence

Impact and bounded evidence → one desired/operation/Task publication → assignment
and consumer detachment → durable pending path call → bounded helper call →
durable completion/cursor → repeat or atomic terminal finalization.

Timeout retains operation ownership and cursor. Retry creates the next Task
attempt without changing the operation, original response, desired revision,
accepted impact, or immutable Volume key.

## Implementation decisions and guardrails

1. The owning Volume capability controls retirement and cleanup sequencing.
   Script reference ownership stays in the existing source-reference module;
   Volume code only compares its count and membership fences.
2. Before changing publication, close a concrete typed preparation seam between
   the Volume runtime owner and the existing desired publisher. The current
   child package imports `etcd`, so the root publisher cannot import it back.
   Do not solve this with callbacks carrying arbitrary mutations, dynamic type
   recovery, duplicated runtime records, or a generic reference framework.
3. Publication atomically requires source absence and acquires the accepted
   retirement/removal ownership. Source acquisition atomically excludes that
   ownership until finalization. Checking once before dispatch is insufficient.
4. The Controller records each exact pending request before the Agent mutates
   the filesystem. A reconnect resends that request or consumes its committed
   completion; it does not reset or infer the cursor.
5. Preserve the accepted bounds: 128 filesystem mutations, 30 seconds per call,
   768 KiB response, six-hour attempt, and each ADR0049 transaction budget.
6. Remove the fixed nil-cursor removal path in the same cutover that connects
   the durable path. Do not retain two selectable implementations.

## Scope boundaries

- Work locally on main; no subagents, review loops, or new worktrees.
- No deployment until the complete operation journey and its failure recovery
  are verified and the remaining release blockers are addressed.
- The owner confirmed on September9 that Volume removal includes the required
  Backup-policy transform and its persistence/publication changes. Implement
  that dependency without another approval request. Unrelated Backup/restore
  work remains paused. Preserve historical Backup data and all required guards;
  do not omit the atomic transform or weaken its fences.
- Real-host verification remains limited to disposable QA `10.25.0.2`, after
  local proof and the existing deployment restrictions are satisfied.
- No direct cleanup of old rootless Script records and no source-count repair.

## Tasks and checkpoints

The ordered tasks and exact verification targets are in [todo.md](todo.md).
Each implementation increment includes a failing reproduction, its correction,
and a passing focused proof. New integration tests must use real publication,
assignment, and checkpoint methods, with doubles only at storage/host seams.

The first checkpoint closes publication and retirement. The second closes
bounded execution/reconnect. The final local checkpoint closes timeout, retry,
terminalization, and the complete operator journey. None substitutes for the
remaining floor-MVP or live acceptance requirements.

## Risks to resolve early

- Import direction between the existing publisher and runtime owner.
- New comparisons overflowing existing transaction budgets.
- Lost committed responses causing a duplicate destructive helper call.
- Generic Task completion releasing ownership before directory absence.
- Pending calls crossing retry/assignment ownership incorrectly.
- Desired-state changes invalidating a running removal or recreating its key.
- Source reservations arriving after the initial absence read.
- Backup policy integration requiring authority outside the current pause.

The owner approved this cutover after reviewing the plan. Backup/restore and
deployment remain paused. Start with task 1; no further plan approval is required.
