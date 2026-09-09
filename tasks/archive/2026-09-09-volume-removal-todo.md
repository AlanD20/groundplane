# Archived: Volume removal cutover tasks

Historical task snapshot, superseded by the completed floor handoff in
`docs/acceptance/2026-09-09-integrated-floor-qa.md`. Retained unchecked items
below are not the active task list.

Plan: [plan.md](plan.md). Status: owner approved; task 1 publication regression
reproduced locally. The owner confirmed that required Volume-removal Backup-policy
changes are authorized; proceed with atomic integration. The cutover is incomplete.

Use repo-local Go temporary/cache directories for every command. Tests run
with `-race -count=1`. File lists are task ownership bounds, not permission to
expand into unrelated code. Split any increment that needs more than five
handwritten files; generated outputs follow their source contract. The bounded
mechanical record extraction in ADR0071 migrates its existing consumers in one
commit to avoid temporary aliases or duplicate codecs; behavior increments
retain the five-file bound.

## 1. Reproduce the actual publication/cleanup gap

- [x] Add a real-publication journey that reserves a Script Volume source and
  exercises removal, rather than only the prepared condition fragment.
- [ ] Demonstrate that partial directory cleanup needs retained Controller
  request/cursor authority across reconnect.
- [ ] Record the exact failing boundary; preserve the existing positive controls.

Dependencies: none. Scope: small, two new Volume journey fixture/test files in
`internal/infra/etcd` using the actual Volume mutation and Controller plan APIs.
Verification: focused `TestVolumeRemovalProduction*` reproduction, then the
corresponding passing proof as each boundary below is connected.

Current uncommitted evidence: `TestVolumeRemovalProductionProtectsPreparedScriptSource`
passes through real creation, Task acknowledgement, source preparation, and
DELETE publication. `TestVolumeRemovalProductionPublishesDurableOperation` fails
after accepted DELETE with `state.conflict: Environment Volume removal runtime
is incomplete`. Both use real repositories over the existing MVCC fake.
The focused `go test -race -count=1 ./internal/infra/etcd -run
'^TestVolumeRemovalProduction'` selection is red, not a completed delivery gate.
Source inspection additionally shows a fixed nil-cursor removal plan and no
Volume checkpoint member in Agent `WorkerOutput`; executable partial-cleanup
and reconnect proof remains outstanding.

## 2. Close the typed publication seam

- [ ] Define the concrete preparation input/result and its single state owner
  without an import cycle or arbitrary transaction callback.
- [ ] Preserve the existing record authority and accepted atomic publication
  contract; record an ADR only if a new architectural choice is required.
- [ ] Add exact transaction-count/size and malformed-input proofs.

Dependencies: 1. Scope: medium, Volume preparation owner, its tests, the narrow
publisher input, and a task-scoped contract record.
Verification: focused preparation tests and compilation of `etcd/volumeremoval`
and the existing desired publisher.

## 3. Publish removal and retirement atomically

- [ ] Publish desired head, removal runtime/ownership, Task, and immutable root
  response together; failure exposes none of them.
- [ ] Compare Script count plus forward-membership absence and exclude new
  acquisition under the same retirement authority.
- [ ] Preserve equal DELETE replay and all accepted impact/ownership fences.

Dependencies: 2. Scope: medium, common desired publisher, Volume preparation,
source-acquisition owner, and focused transaction tests.
Verification: real-publication journey, both retirement/reservation orderings,
lost commit response, and exact publication budgets.

### Checkpoint: publication

- [ ] New and existing source-removal tests pass.
- [ ] No independently published removal runtime or duplicate desired authority.
- [ ] Backup transform dependencies are preserved or explicitly held for scope
  authorization, never silently removed.

## 4. Define the closed Volume checkpoint exchange

- [ ] Bind request and response to operation, current Task/assignment, ordinal,
  and exact request digest.
- [ ] Carry bounded pending request and completion evidence, not arbitrary paths.
- [ ] Regenerate protocol outputs; reject unspecified/invalid states.

Dependencies: 3. Scope: medium, `proto/agent.proto`, generated outputs, one
closed checkpoint parser/model, and focused validation tests.
Verification: generation diff and focused protocol validation tests.

## 5. Connect Controller authorization and recovery

- [ ] Authorize consumer detachment and each path call from durable runtime and
  current assignment ownership.
- [ ] Commit completion/cursor before authorizing the next request.
- [ ] Replay pending and committed calls exactly across Controller restart.

Dependencies: 4. Scope: medium, a Volume checkpoint owner, Agent-channel
dispatch, composition, and focused tests.
Verification: pending-call replay, lost completion response, stale assignment,
and source-reservation exclusion tests.

## 6. Connect bounded Agent execution

- [ ] Execute only Controller-authorized pending calls through the existing
  descriptor-relative helper.
- [ ] Preserve request identity and wait for durable completion acknowledgement
  before continuing; retain all accepted per-call bounds.
- [ ] Replace the old fixed nil-cursor removal procedure without a dual path.

Dependencies: 5. Scope: medium, Volume plan builder, Agent Volume runtime,
worker dispatch, and their focused tests.
Verification: multi-batch tree, reconnect before/after a helper response,
symlink/mount/ownership rejection, and unchanged unrelated data.

### Checkpoint: bounded execution

- [ ] A tree larger than one mutation batch completes through durable cursors.
- [ ] Restart never resets, skips, or renumbers an authorized pending call.
- [ ] No filesystem effect is authorized solely by Task presentation state.

## 7. Connect timeout and successor attempts

- [ ] Timeout terminalizes only the attempt, retaining removal ownership,
  pending call/cursor, and the pending root response.
- [ ] Retry creates the exact next attempt under current ownership while
  preserving the operation, origin Task, desired revision, and accepted impact.
- [ ] Pending-call recovery and stale assignment races fail closed.

Dependencies: 6. Scope: medium, Volume attempt repository, generic Task timeout
and retry composition, and focused tests.
Verification: timeout at each durable checkpoint, repeated retry, original
DELETE replay, and ownership races.

## 8. Finalize the complete operation atomically

- [ ] Require proved directory absence and no pending call; terminalize the
  Task/root marker and install retention while releasing runtime ownership.
- [ ] Do not republish desired state or prune Script references.
- [ ] Prove read-only replay and recovery of a lost final response.

Dependencies: 7. Scope: medium, Volume finalization, Task terminal integration,
idempotency retention integration, and focused tests.
Verification: exact final transaction budget, interruption/replay, and retained
root-response TTL.

## 9. Finish the operator journey and handoff

- [ ] Real mutation/publication/assignment/checkpoint/retry/finalization journey
  passes with renamed Volume labels and unchanged immutable keys.
- [ ] Update runtime list/detail projection and existing polling consumers only
  where needed to expose the accepted deleting/checkpoint contract.
- [ ] Record local versus live evidence and remaining floor-MVP blockers.

Dependencies: 8. Scope: medium increments, Volume reads/projection, existing
consumer contract tests, journey tests, and checkpoint documentation.
Verification: Volume package, affected Controller/Agent/persistence tests,
existing Script-source regressions, applicable generated/build checks. Do not
run paused full CI, Backup/restore gates, or deployment without the required
scope change.

### Final local checkpoint

- [ ] Every accepted removal checkpoint has executable evidence.
- [ ] No standalone runtime transition substitutes for an atomic publication.
- [ ] The old fixed removal route is gone; no compatibility switch remains.
- [ ] Outstanding Backup-dependent and live acceptance work is explicit.
- [ ] This slice is not reported as completion of the overall floor MVP.
