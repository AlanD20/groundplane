# Backup recovery and execution boundaries

- Decision status: Accepted; implementation and qualification remain partial
- Scope: Backup authority, artifact publication, restore safety, Agent recovery,
  and the managed PostgreSQL execution boundary

## Recovery is driven by a sealed snapshot

Backup schedules use one five-field UTC cron format across authored state and
human interfaces. [The schedule owner](../../internal/common/backupschedule)
parses fields and finds the latest missed occurrence directly, rather than
replaying every missed minute. Controller coordination still prevents overlap
and repeated dispatch. Replacing the old stored calendar grammar advances the
storage epoch: an old Controller cannot safely read or roll back new policies.
No dual parser or old-format alias is retained.

Each Environment has at most one Backup Policy. A run freezes the policy,
Connector, ordered source catalog, relevant desired revisions, prior Service
intent, encryption era, deadlines, object targets, and restore dependencies
before work is assigned. Retry resumes that snapshot; it does not rebuild a
plan from newer desired state or mutable labels. Environment coordination owns
the scheduling floor and serializes Backup work with release, storage,
configuration, Attach, and deletion mutations.

The Controller remains the authority for locks, checkpoint acceptance, point
commit, adoption, retention, and terminal delivery. The Agent may mutate only
from one authenticated assignment and its exact resume state. Every
irreversible boundary is write-before-mutation and Controller-acknowledged.
Lost acknowledgement is resolved by the same immutable identity; it is never a
reason to infer success or repeat a destructive action.

A Recovery Point becomes visible only after the object upload, immutable
object identity, size, hashes, required metadata, and source cleanup evidence
have committed. Retention deletes in stable order and records remote absence
before removing local ownership. Environment deletion may adopt retained
prune or remote-cleanup obligations, but never abandons an object merely
because the Environment primary is gone. Age-key rotation affects future
points; an old identity is request-scoped, never stored, and loss or ambiguity
requires another explicit operator request.

## Artifacts are validated before target mutation

Recovery Point grouping uses a capture-metadata companion published atomically
with each Point. It survives producing-Task retention and is removed with the
Point. Presentation metadata is not part of the immutable Restore/prune snapshot:
adding grouping must not change an existing assignment's authority or require
rewriting Point history. [The capture owner](../../internal/infra/etcd/backupruntime/recovery_point_capture.go)
and [publication](../../internal/infra/etcd/backup_recovery_point_commit.go) enforce
the same-revision binding. A missing companion means unrecorded provenance;
timestamps are never used as a substitute identity.

The accepted source formats are the canonical Environment Config archive, the
canonical managed-Volume archive, PostgreSQL 16 `pg_dump` custom format and
MySQL 8.4 logical SQL format.
Optional age encryption wraps the source stream without adding another
compression layer. Object metadata and Recovery Point evidence bind both the
plaintext and stored representations. The current format owners are linked
below; their encodings and validation rules are not copied here.

Config capture preserves the exact desired Entry descriptors and selected
immutable values from one linearizable snapshot. Restore fully downloads,
decrypts, hashes, and validates the archive before staging a new complete Entry
generation. Publication replaces the whole set. Once the first publication
mutation is acknowledged, recovery rolls forward from the durable generation
and never restarts the restore from scratch.

The Restore Task seals the Recovery Point's complete archive evidence and one
Controller-allocated Entry/render generation. Download cannot select a newer
object, and reconnect cannot invent another generation. The Task also pins the
predecessor desired revision and head revision: restored configuration must not
be rebuilt against later Environment edits. Selected values first enter immutable
Entry generations with native receipts; replay verifies the retained ciphertext,
not a freshly sealed replacement. The extra private
validation copy uses an unlinked file charged to the same bounded staging lease;
it must not bypass capacity admission through an ambient temporary directory.
Capture and Restore share one Config transfer owner. Restore grants credit at
completed Entries so authenticated readers can advance only after durable
acceptance, without waiting on a window that requires the next Entry to fill it.
Controller restore staging protects every received frame, including metadata
and value hashes, with its existing key. A batch and its native credit commit
together; an uncertain write cannot grant more input or select new ciphertext.
The final grant follows a durable complete-generation seal. Live publication
uses that sealed input, not an acknowledged prefix or current Secret values.

The sealed predecessor also identifies managed file destinations and surviving
Services. The archive selects the replacement content; the predecessor authorizes
removal of files no longer selected. Both sides use the same dotenv encoder and
file-plan owner. Before live publication, the Controller computes the expected
file proof from the staged immutable values. The Agent writes those files through
the ordinary descriptor-confined helper, then independently verifies every output
and removal before reporting the matching proof. This updates managed files, not
the environment already loaded by running processes.

File verification and source cleanup are separate acknowledgements. The Agent
closes its private validation spool and removes the exact source stage before
acknowledging cleanup; only then can it retire the transfer journal. Startup may
discard an interrupted download before archive validation, resume the exact full
representations after validation, or finish cleanup after acknowledged file
verification. None of these dispositions permits selecting a newer object or
reapplying files after cleanup. Admission and terminal recovery must be connected
before this path is operationally available.

Volume archives represent a descriptor-confined tree containing only
directories and regular files. Mount crossings, links, devices, sockets,
special files, unsafe paths, metadata outside the supported set, and tree
changes during capture fail closed. Restore first stops the Services whose
sealed prior intent requires it, constructs and validates a hidden sibling,
journals each construction/finalization mutation, exchanges it with the live
tree atomically, then removes the replaced tree. After exchange, recovery is
forward-only and Service recovery is resumed from its cursor.

PostgreSQL capture is one continuous `pg_dump --format=custom --compress=0`
stream. Restore validates a list pass before stopping consumers, terminating
connections to the selected database, and applying one clean,
single-transaction `pg_restore`. It does not capture a cluster, WAL, roles, or
ACLs. Once restore-apply start is acknowledged, the attempt is spent: exact
input, terminal, reap, Exec inspection, and post-restore proof are required;
missing or contradictory proof is recovery-required and never authorizes a
second apply.

MySQL uses the upstream container's fixed `mysqldump`/`mysql` protocol rather
than a GP database image or operator-supplied shell command. Its schema changes
are not one atomic transaction. Recovery must inspect the original Exec and
its retained terminal/input evidence; an unknown outcome never permits another
restore-apply. [The execution owner](../../internal/infra/docker/mysql84execution)
attests image, labels and the exact data Volume around each operation.
Post-restore verification uses `CHECK TABLE` through the upstream `mysql` client;
it does not depend on the separately packaged `mysqlcheck` executable.

Database archive evidence records actual server and backup-tool versions before
dump-start acknowledgement. Recovery uses those pinned versions, not fresh
probes that could relabel retained bytes. Restore preflight uses an authenticated
Agent read with the same runtime attestation. The review digest binds the Point,
Attach, consumers, exact runtime revisions, container and observed restore tools.
Version differences require explicit operator acknowledgement; execution rechecks
that target before effects. This separates operator-owned compatibility risk from
GP's non-negotiable identity, format and integrity checks.

Capture resume retains the original prepared archive alongside later upload
progress. Otherwise an acknowledged upload would erase the metadata needed to
authenticate the retained source. Unknown-size capture reserves storage as it
writes; a format maximum is not a prediction of the required disk space.

An interrupted PostgreSQL dump before preparation may resume only its original
retained bytes. The worker must match them to the original Docker Exec and
helper output evidence, then validate the archive. Incomplete bytes fail the
attempt rather than launching another dump. Before preparation, derived
ciphertext may be discarded and regenerated; after preparation it is fixed.

## Agent restart and delivery fail closed

Backup work uses closed versioned assignments with stable ids, revisions, authority
digests, assignment generations, absolute deadlines, contiguous checkpoint
sequences, and a preceding-checkpoint fence. Stable ids, not names, identify
resources. Capture and restore have a six-hour absolute budget; prune has a
thirty-minute budget. Reconnect does not extend either budget.

The single-host channel is bootstrapped over a root-only local Unix socket and
then uses the Agent token; adding TLS to that local bootstrap is not an MVP
security boundary. The authenticated process generation is distinct from the
durable Agent generation. Backup traffic uses bounded message sizes, bounded
per-assignment queues, and one fair channel writer so a Config or Volume stream
cannot starve readiness, checkpoints, abort, or terminal delivery.

S3 credentials, the current age identity, and an explicitly supplied old age
identity use separate task-scoped, chunked secret slots. Config values use the
typed Config transfer instead. Secret material is excluded from Tasks,
checkpoints, staging inventories, logs, results, and terminal receipts, and
owned buffers are cleared after use.

On startup the Agent advertises zero capacity and not-ready delivery state,
quiesces workers, inventories bounded staging state, and waits for the
Controller's exact disposition plan. It journals and applies that plan before
acknowledging it and becoming Ready. Silence is not discard authority. Any
stage that may contain an unclassified irreversible mutation blocks Ready
until it is resumed, safely retired, or declared recovery-required.

An unacknowledged plan may be superseded when native Task state changes, such
as a timeout during inspection. The replacement names the previous plan's digest
and covers the same inventory. The Agent journals the replacement; an old
acknowledgement cannot acknowledge the new decision.

A declared recovery-required disposition pins the failed Restore's immutable
native outcome and exact retained files. The Agent verifies and holds those files
with no growth, cleanup or worker authority. Unrelated work may become Ready;
the failed Environment's operation lock and recovery evidence remain protected.
Declaration is not successful recovery or permission to repeat the destructive step.

Upload intent is checkpointed before `Put`; completion is checkpointed before
the verifying `Head`; only the Controller publishes the Recovery Point.
Terminal reports remain in the Agent journal until the Controller acknowledges
their exact native attempt and result. Reconnect replays that report, not a new
execution. A disconnected stream, Task state or time passage is not delivery proof.

Session teardown quiesces Backup work; it is not an operator Abort. Redispatch
uses the retained checkpoint rather than publishing a false terminal result.
Prune credentials are resolved only for still-owned objects. An acknowledged
exact-object deletion retires that object's payload requirement, not the
remaining dispatch or its validation checks.

## Managed PostgreSQL uses a closed helper and private gate

Published releases carry an authenticated dual-platform scratch image containing
only PostgreSQL backup clients, their libraries, the helper and private gate.
The database image is unmodified upstream PostgreSQL 16 Alpine. Native `--ref`
builds publish only the host architecture's tooling; they do not fabricate an
unbuilt platform or depend on a previous published release.

The installer extracts tooling into a digest-owned directory, verifies its
inventory against the authenticated image and mounts it read-only. Clients use
their bundled loader and libraries, not those of the patched database image.
Backing creation retains the catalog. GP software updates cannot substitute new
tools for an existing Backing. Database patch Deploys preserve the same Volume
and tooling mount; verified provisioning and Deploy outcomes publish one current
PostgreSQL runtime authority consumed by Backup. See
[the record owner](../../internal/infra/etcd/backingpostgresruntime/record.go).
Container and mount attestation admits only that exact read-only tooling bind
alongside the data Volume and prevents any other workload mount
from shadowing the helper, gate, state directory, clients, socket, or runtime
identity. Container and host root are trusted boundary actors; database uid 70
is not trusted with supervisor state.

The shared protocol permits only the fixed probe, dump, restore-list,
connection-termination, restore-apply, verification, stop, execution-evidence,
recovery-inventory and evidence-retirement operations. It
has no shell, arbitrary executable or argument vector, arbitrary SQL,
password, TCP, filename, ambient environment, or diagnostic mode. A root
Docker Exec helper validates the request and launches the fixed client through
a private gate. Before client exec, the gate proves its parent and file
descriptors, then runs as uid/gid 70 with no supplementary groups, no
capabilities, `NoNewPrivs`, the selected seccomp policy, and bounded file
descriptors.

Helper state is root-only, descriptor-confined, hash-chained, fsynced, and
phase-monotonic. The original parent is the sole process allowed to reap its
child. Recovery authenticates exact process identity through pidfds and never
uses a name, negative pid, or non-parent wait as authority. Ambiguous identity,
release consumption, exec, stream, termination, or reap state retains the
record and blocks container replacement, upgrade, ordinary Backup work, and
positive Agent readiness.

Execution evidence is selected by the original nonce and canonical request
digest. It exposes only complete terminal, parent-reap and stream evidence.
Recovery also requires successful inspection of the original Docker Exec and
the selected container; reading helper state alone is not completion proof.
The resumed Restore then verifies the database and continues Service recovery,
without invoking restore-apply again.

Successful dump and restore evidence is retired after the Controller acknowledges
source cleanup. A durable retirement marker precedes deletion and survives an
interrupted cleanup. Fully reaped, stream-complete failed non-RestoreApply
operations can also be retired without being called successful. Failed RestoreApply
and ambiguous or unreaped attempts cannot be retired through that path. Absence
is not execution proof.

The Agent records a private execution marker before invoking a source's helper.
It survives source-file cleanup and successful individual sources until the
whole Task is classified. Startup inventories every helper record, including
probes and archive-list passes. A completed Task only retires its Agent marker:
its old database may already serve a newer operation. A failed Task's cleanup
must prove the retained helper namespace safe before releasing its backing lock.

Backup and Restore atomically lock the selected Backing Environments as well as
the consumer Environment. Lock acquisition advances each backing mutation epoch,
so competing publications and planners prepared before acquisition cannot cross
that boundary. Exact lock deletion releases it. Failed Tasks retain every started
PostgreSQL source's guard, including sources whose file cleanup already succeeded;
their staging acknowledgement releases those guards atomically after inspection.
This separates file cleanup from proof that no helper remains active.

The [helper runtime](../../internal/postgres16helper/main.go), private gate and
Agent execution path are implemented but unqualified. Restart reconciliation and
successful-evidence retirement are connected for retained running assignments.
Startup inventory, backing locks and execution-marker retirement are connected
in source; generated integration and runtime qualification remain outstanding.
Source implementation does not establish the security contract at runtime.
Terminal Restore uncertainty remains explicitly recovery-required,
with no automatic second attempt or new post-terminal assignment.

## Native protocol ownership

The [Task redesign](task-execution-redesign.md) owns execution identity,
assignment, terminal-report replay and the coordinated cutover. Backup uses
those native attempts and typed operation receipts; it has no separate generic
Task-result or terminal-retirement protocol. Message fields and transfer limits
belong only to [the protobuf](../../proto/agent.proto) and its compiled owners.

Staging recovery must cover each inventoried recovery key exactly once and bind
the retained file identities, sizes and hashes. A disposition may authorize no
growth, bounded growth or exclusive unknown-size growth; missing evidence grants
none of them. Config and Volume transfers retain their directional framing,
credit, committed cursors and content-chain identity across reconnect.

Before the durable artifact-prepared receipt, Config encryption may restart only
after the Controller verifies the complete retained source and completed transfer
against captured input. The persisted disposition retires only derived ciphertext,
never the source or transfer journal. Its replay accepts already-removed ciphertext,
not missing source or changed files. After preparation, reuse the authenticated
selected ciphertext; neither startup nor reconnect permits replacement bytes or
a second upload. These obligations remain required even where qualification is
incomplete; deleting an obsolete wire inventory does not qualify recovery.

## Consequences

- Backup correctness depends on immutable authority and durable progress, not
  on best-effort cleanup or replaying a command.
- Restores intentionally prefer a visible recovery-required state over a
  second destructive attempt whose first result is unknown.
- The native execution protocol requires one clean coordinated cutover; there
  is no negotiation or parallel legacy decoder.
- Accepted design is not implementation or QA evidence. Capability and
  qualification documents remain the authority for availability.

## Source navigation

Wire fields live in
[`proto/agent.proto`](../../proto/agent.proto). Backup plan construction and
Controller workflow live in
[`internal/controller/backup`](../../internal/controller/backup/run_plan_builder.go)
and [`internal/infra/etcd/backupplanning`](../../internal/infra/etcd/backupplanning/planner.go).
Durable run, checkpoint, retention, and point state live in
[`internal/infra/etcd/backupruntime`](../../internal/infra/etcd/backupruntime/types.go)
and [`internal/infra/etcd/backupretention`](../../internal/infra/etcd/backupretention/repository.go).

Config Restore file planning and its aggregate proof are owned by
[`backupconfigmaterialization`](../../internal/common/backupconfigmaterialization/plan.go).
Ordinary and restored environment-file content share
[`dotenvfile`](../../internal/common/dotenvfile/render.go); host writes use the
existing [`materialization runtime`](../../internal/agent/materialization/runtime.go).

Canonical format code is owned by
[`internal/common/backupconfig`](../../internal/common/backupconfig/config.go),
[`internal/common/backupvolume`](../../internal/common/backupvolume/model.go),
[`internal/common/backuppostgres`](../../internal/common/backuppostgres/postgres.go),
and [`internal/common/backupformat`](../../internal/common/backupformat/format.go).
Agent staging, native checkpoint delivery and retained reports are in
[`internal/infra/backupstage`](../../internal/infra/backupstage/backupstage_linux.go),
[`internal/agent/checkpointdelivery`](../../internal/agent/checkpointdelivery/backup.go)
and [`internal/infra/agentworkjournal`](../../internal/infra/agentworkjournal/backup.go).
Native operation settlement and retention are owned by
[`backupwork`](../../internal/infra/etcd/backupwork) and
[`the Controller completion owner`](../../internal/controller/backup/work_completion.go).
The PostgreSQL helper vocabulary is in
[`internal/common/postgres16protocol`](../../internal/common/postgres16protocol/protocol.go).
These links identify current owners; they do not prove the accepted protocol or
PostgreSQL confinement runtime is complete or qualified.
