# Manual Script terminal-reference cleanup

Owner: Script execution / Task persistence.
Severity: operator lifecycle blocker.
MVP-required: yes, for exercised manual Script removal.

September9 disposition: the owner-approved, exact-target migration released
only the three proven-clean legacy body-reference pairs under CAS. Normal
Script removal then completed as `task_01M23D4XMY7BGB5XJ9JNHBEXFS`; the disposable
Script is absent and its original execution history is preserved.
See [migration evidence](../acceptance/hosting-floor.md).
The remaining acceptance is fresh live execution through the locally integrated
prepared-source runtime after deployment. The diagnosis below describes the
pre-migration baseline; no automatic legacy repair was added.

After the manual publication compare-key repair, QA Script
`scr_01M2104ECJZ050QH7RMDSCFSWG` completed one execution, failed another,
and aborted a third. All three Tasks are terminal, but normal Script removal
rejects `resource.in_use: active Script executions fence deletion`.
Exact Tasks and runtime evidence are in
[management checks](../acceptance/hosting-floor.md).

The disposable Script `qa-manual-cas-proof` remains in `qa-application` on
QA 10.25.0.2, generation 3, body `sleep 60`. No execution remains running in
the observed Task results. No direct record deletion or guard bypass was used.

Read-only inspection of the execution records confirms that all three are
`cleanup_proven` with `ActiveReference=true`. Each carries container, body, and
execution-directory absence proof. Their canonical ADR 0062 operation source
roots are absent. The inspection selected only execution identities, checkpoint,
outcome, cleanup flags, and root presence; it did not print snapshots or inputs.

The deployed baseline's missing integration is broader than a terminal counter decrement:

- `ScriptRepository.PublishExecutionWithTask` in `script_execution.go` still
  publishes the old body-only forward/reverse references and increments the
  Script primary directly. It does not prepare or activate ADR 0062's complete
  immutable source set.
- `prepareScriptTaskAcknowledgement` handles Script removal Tasks, not manual
  `TaskScript` execution. The legacy `finalizeReleaseHookExecutionBatch` handles
  release hooks only. Neither closes manual execution references.
- The prepared-source module and Blueprint terminal-release path already exist,
  but cannot be applied to these old terminal Tasks by inventing an active root
  or a missing closing report. ADR 0062 explicitly treats a missing operation
  root as corruption and forbids best-effort reference reconstruction.
- The Agent plan schema currently carries runner-source revisions but no
  source-set membership digest. Integration must also close the accepted
  publication/assignment/start binding, not merely add a terminal release call.

Implement complete fixed-revision source capture and prepared publication for
manual executions, then assignment/start and retry fences, bounded source
release, and atomic Task terminalization. Cover completed, failed, running-abort,
pending-abort, replay, and interrupted release. Preserve ordinary transaction
limits and the existing immutable-source deletion guards. Old QA terminal
records need an explicitly supported disposition; they must not be silently
adopted into the new protocol or repaired by clearing their aggregate count.

Acceptance: completed, failed, and aborted manual executions release only their
own references after the required cleanup proof, idempotently and under CAS;
uncertain or active executions still block deletion. Regression tests must cover
these terminal paths and replay. Remove the disposable QA Script through the
normal API after correcting its supported recovery path.

## Consolidated local integration

Committed work on main now prepares the manual immutable source set and
publishes its root atomically with the Task. Assignment and checkpoint fences
bind the execution's sealed membership count/digest to that root. Runner
Networks carry their actual owning Environment, including backing-owned
Networks. Pending Abort uses the prepared-source bounded release path.

The normal manual terminal path now requires exact assignment-owned
`cleanup_proven` evidence. It saves the original Agent report and closes the
execution with the root transition, drains bounded reference batches, and
removes the root/report with Task terminalization and retention. Controller
reconnect resumes the original report without redispatch. Conflicting reports
and new events cannot alter closing authority.

Before-start failure now terminalizes with an active `available` root and the
Task's exact retention deadline, without releasing references. Retry atomically
transfers the existing execution and root to the new Task without changing the
plan, snapshot, or counts. Claim installs a fresh assignment and returns the
root to `undecided`. Focused race tests pass for these transitions and reject
retry after start authorization or at the retention deadline. The complete
source-reference module race suite also passes.

Retention expiry now atomically records `expiry_before_start` absence proof and
the root's `retry_expiry` path, then drains bounded batches before admitting
journal pruning. The original terminal Task and retention index are compared,
not rewritten. Pruning compares root and reverse-membership absence. Discovery
skips attempts held by a live/newer retry without removing their retention
indexes, so unrelated expired Tasks can progress. Interrupted expiry preserves
the original deadline and terminal bytes; normal release cannot resume or
finalize an expiry root. The source-module race proof covers 35 memberships,
an unknown committed batch followed by restart, and the existing 16-member
read/96-operation physical bounds.

Manual publication now resolves an existing idempotency marker before source
preparation. Pending and terminal replay are read-only; a committed publication
with a lost response retains its active references and replays without writes.
Assigned-before-start Abort now commits Controller-owned absence evidence with
source closure under the exact execution/Task/assignment fences. A winning
start-authorization checkpoint prevents that transition and keeps the Task
nonterminal pending real cleanup. Interrupted assigned Abort resumes its
original report without redispatch. These paths pass focused race tests.

Manual plan and private-body admission now bind the execution to its matching
active source root at the execution's fixed read revision. The regression uses
`BuildManualScriptPlan`, `NewScriptExecutionRecord`, and normal prepared Task
publication: the generated plan and body are accepted before root mutation,
then rejected for a changed digest/count, releasing root, retry-available root,
or absent root. All five cases returned both artifacts before the fix. A Task
without its single step now rejects validation instead of indexing an empty
slice. The admission methods live together in `script_assignment_admission.go`,
shrinking the oversized execution-record module. The fixture seeds Release
history/source inputs; it does not prove serving-Release discovery or physical
Agent execution. The complete `ManualScript` test selection passes with
`-race -count=1` after this admission change (2.428s).

Before-start deadline collection now uses the durable Script execution to
classify absence of effects instead of the generic uncertain Compose result.
Both `ExpireTimedOutTasks` and `TimeoutAgentAssignments` previously returned
zero indefinitely for an overdue unstarted Script. They now terminalize at
the deadline with the same active retry root and retention deadline; replay
is read-only and ordinary Retry succeeds. A start checkpoint injected before
the terminal CAS prevents terminalization and preserves assignment/reference
authority, including during subsequent scans. Both collector paths pass the
focused timeout race selection (1.335s); no started execution is declared
clean merely because its deadline passed.

Startup now recovers unpublished preparations before HTTP handlers, Agent
dispatch, and schedulers start. It discovers bounded descriptor pages and
uses the existing CAS-fenced abandonment path with durable membership identity
and preparation/release cursors; the original in-memory source list is not
required. Partial preparation, sealed preparation, and interrupted abandonment
are covered, including unknown committed phase/batch/final-delete responses,
preservation of shared counts belonging to a published operation, and rejection
of invalid cursor/phase/digest or coexisting root authority. A manual publication
whose writes and deferred cleanup both fail is recovered through the actual
Script primary codec: its body count returns to zero without creating a Task.
The startup composition rejects unreadable recovery state rather than handing
authority to live handlers. This does not repair rootless historical QA records.

`TestManualScriptServingReleaseToTerminalSourceJourney` now publishes and
acknowledges a Controller-generated Release plan, creates a Script normally,
loads sources through `LoadExecutionSources`/`ResolveServing`, generates the
manual plan, reserves/publishes sources, claims and admits plan/body, times out
before start, retries with the same sealed plan, records cleanup, terminalizes,
and removes the Script through its normal removal Task. No source aggregate or
Release fence is hand-seeded for this journey. Agent/Docker acknowledgements
are supplied hermetic evidence, and this case has no Entry or Attach.

That journey exposed a real reservation failure when the Service has no
runtime sidecar. Desired Service existence does not require that optional
record. Manual Service membership now pins the execution's immutable snapshot,
with exact execution/Environment/Service/digest validation, while final
publication retains desired-source and serving-Release fences. Substituted
Service ids, owners, and snapshot digests reject without reserving counts.
Blueprint staged Service evidence remains unchanged; ADR 0062 records the
closed manual snapshot proof.
The `ManualScript|ScriptSourceReference` selection passes with
`-race -count=1` after this journey and correction (2.772s).

`TestManualScriptServingReleaseEntryJourney` extends the same journey with a
service-scoped plain ENV Entry, published desired metadata and an immutable
value generation. Actual `TaskMaterializationResolver` and
`ScriptArtifactService` build bindings and deliver the pinned value on both
the initial assignment and Retry. The generated/admitted plan contains no
Entry plaintext; the exact generation has one removal fence through Retry and
none after terminal cleanup. Corruption injected after real value resolution
rejects the assignment and clears both owned body and Entry buffers; subsequent
uncorrupted admission succeeds. Both serving-Release journeys pass with
`-race -count=1` (1.629s). This is not encrypted/file Entry, standalone live
Entry mutation, Attach, or physical Agent execution evidence.

`TestManualScriptServingReleaseSecretFileJourney` now covers an encrypted
literal file Entry using a fresh in-memory age identity, the real Protector,
immutable encrypted-generation repository, and materialization resolver. It
checks exact ciphertext storage, absence of a parallel plaintext-generation
record, redacted published metadata, and the exact file target, numeric owner,
and mode0600 in the binding. Assignment, corruption rejection/clearing, Retry,
reference release, and normal Script removal use the same journey. The fixture
now binds the derived Blueprint Entry lookup and checks metadata in the
published projection, matching the read contract; a standalone primary is not
required for a Blueprint Entry. `ManualScript|ScriptSourceReference` passes
with `-race -count=1` (3.228s). No installed key or host was accessed.

The reusable Secret-file variant now retains distinct ciphertext digests for
the Entry generation and its project-owned Secret. It verifies the Secret
membership's exact owner, revision, and ciphertext digest and rejects normal
Secret removal while prepared/assigned, while timed out with Retry available,
and after reassignment. After cleanup, ordinary Script and Secret removal Tasks
complete. This is a source-retention/assignment proof, not full Entry creation
or physical Agent acceptance.

That journey exposed missing Secret retirement fences. Before the correction,
normal deletion could publish after Script preparation, and Script preparation
could reserve ciphertext after deletion started. Both orderings have failing
regressions. Failed deletion Retry could also reacquire the Secret after a
new Script reservation; a separate regression failed before the Retry fix.
Preparation now compares the Secret deletion tombstone absent while acquiring
its count. Deletion dispatch and Retry require both count and forward-prefix
absence. Direct, Task, and parent-deletion Secret finalizers retain those exact
absence conditions in their destructive transaction. A count without a forward
member, a member without a count, or a corrupt zero count rejects as corruption.
No source records are synthesized or repaired by these paths.

The hierarchy fixture's basic store ignores prefix comparisons. These deletion
journeys therefore reuse the existing prefix-aware transaction fake, with its
fault injection disabled, to exercise actual prefix failure evidence. The
focused Secret/manual/source-reference race selection passes (3.653s), and the
complete source-reference module passes (1.175s). The subsequent distinct-
ciphertext reusable Secret journey passes separately with race detection
(1.494s). The Secret finalizer extraction shrinks its oversized parent module.

Entry source protection is now integrated locally. A reproduced removal bug
allowed deleting all generations while an older generation was reserved, even
though ordinary editing correctly retained it. All-generation count and
forward-prefix absence now protect direct removal, removal Task dispatch,
successful Task finalization, parent finalization, and the shared desired
publication path used by API Entry removal and Blueprint omission. Source
preparation also compares Entry retirement absence. A second reproduced bug
allowed failed-removal Retry to take authority back after a Script reservation;
Retry now reacquires the same absence proof and succeeds after normal source
release. No generation limit or second aggregate was introduced.

All four serving-Release manual journeys pass, including rejection of Entry
removal during active and retry-open retention and successful removal after
cleanup. The old-generation edit control, corrupt count/membership absence
proofs, retirement ordering, and dispatch/direct/parent final-CAS reservation
races pass. The latter inject real source preparation after the read proof,
using the prefix-aware transaction fake, and preserve the winning references
and every Entry generation. The combined Entry/Secret/manual/source-reference
selection passes with `-race -count=1` (3.987s); the complete source-reference
module passes separately (1.152s).

Service source protection is also integrated locally. The serving-Release
journey reproduced removal preflight accepting a referenced Service. Separate
proofs reproduced parent finalization accepting a privately prepared source,
and source preparation succeeding after retirement started. Preflight, removal
Task publication, successful completion, and parent finalization now retain
the exact Service count and forward-prefix absence conditions. Source-count
acquisition compares Service retirement absence. The existing prohibition on
Retry of a failed Service removal remains unchanged: a fresh DELETE is required.

All four serving-Release journeys now check Service preflight during active and
retry-open retention, then acceptance after cleanup. The parent finalizer test
also injects real preparation just before its final transaction, verifies that
the Service survives, then releases the source and completes normally. The
combined Service/Entry/Secret/manual/source-reference race selection passes
(3.739s). The complete source-reference module passes (1.153s), and existing
Service persistence/application regression selections pass (1.131s/1.222s).
These do not prove live Service removal or its full dispatched host procedure.

Volume desired-publication protection is also consolidated locally. A real
snapshot-backed source reservation reproduced acceptance of an explicit Volume
removal in the shared publication preflight. The desired-removal proof now
covers removed Entries and the one explicitly targeted removed Volume, retaining
the exact Volume count and forward-prefix absence in the final transaction.
Slug edits that retain the stable id remain allowed. Tests cover corruption,
preparation before removal, reservation injected after the read proof, conflict
classification, and successful absence comparison after normal abandonment.
The combined source-removal/manual regression selection passes with race
detection (4.089s), as does the complete Volume package (1.204s).

This does not complete Volume source protection. The current removal publisher
and plan path do not call the existing bounded Volume-removal runtime repository.
Retirement, durable cleanup checkpoints, and source exclusion through physical
destruction still need integration under ADR0049/0062. See
[Volume mutation blocker](volume-mixed-runtime-mutations.md). Network removal
also remains open: its Script membership is snapshot-backed and may belong to
another Environment. Audit the remaining source-family removal boundaries
before treating allocation of their counts as proof of deletion protection.

Focused race tests reproduce the original terminal leaks before the fix and
pass for completed, failed-after-start, and aborted executions, missing-cleanup
rejection, terminal replay, and interruption before release or finalization.
Publication, claim, Pending Abort, source capture, and Network-owner tests also
pass. These are local persistence/projection proofs, not live Agent acceptance.

The shared-path regression selection still fails
`TestBlueprintHookRecoveryTerminalReport` at native predecessor admission. The
same test fails identically on an unchanged-main archive; this failure predates
the local integration. The other selected closing/reconnect tests did not fail.
The broader Task regression selection also exposes
`TestTaskRepositoryTimesOutExactAgentGenerationAssignments` and
`TestTaskRepositoryTerminalReplayDoesNotPermitNewWrites`; both fail identically
on unchanged main. Generic journal fixtures now use a plain update Task rather
than a manual Script lacking required execution/source authority. Dedicated
Script tests retain their actual Script publication and checkpoint fixtures.
The expanded Blueprint regression selection also fails
`TestBlueprintPublisherTerminalPreservesExecutedArtifact/addressable` with a
corrupt Release record in the successor proof. The identical failure reproduces
on unchanged `403b0275d`; the baseline archive's tracked contents were verified
against HEAD. The portless control passes. This is not a green broader package.

Remaining before deployment: other source-family removal fences, Attach journeys, and live manual
consumer acceptance, and the explicitly supported disposition of the existing rootless QA
terminal records. Do not deploy this partial integration or remove old QA
references by hand. No full persistence-package, CI, or MVP acceptance is claimed.
