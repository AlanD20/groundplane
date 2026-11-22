# Upgrade continuity and recovery

Status: proposed maintainer design and open qualification work. This does not
accept a product contract, authorize mutations, or claim snapshot restore exists.
The continuity requirement remains in [product scope](../mvp.md#supported-deployment-and-trust-boundary); ordinary update
behavior remains in [update safety](../features/upgrade-safety.md).

## Required outcome and current boundary

Controller and Agent coordinate work; they do not serve application traffic.
Stopping, replacing or rolling back either must not restart, delete, disconnect or
rewrite a healthy workload, ingress, DNS, backing service or persistent data.
Reconciliation proves managed identities before resuming, not blindly recreates.

The one-host design can preserve application traffic during a compatible update,
but cannot keep the control plane uninterrupted: there is one Controller listener
and one etcd member. Continuous control-plane availability needs a separate
redundancy and leadership design beyond the current MVP.

Current strengths:

- etcd deliberately outlives Controller shutdown, while Controller shutdown stops
  only its local Agent container, not serving containers
  ([Controller lifecycle](../../internal/app/controller_lifecycle.go),
  [etcd lifecycle](../../internal/infra/docker/etcdcontainer/etcdcontainer.go)).
- Application preservation already observes exact Compose project, Service,
  release, image, ownership labels, replica inventory and stable-proxy routing
  without running Compose or reloading traffic
  ([preservation observer](../../internal/infra/docker/composehelper/recovery_preserve.go),
  [serving inventory](../../internal/infra/docker/composehelper/serving_inventory.go)).
- Native updates retain immutable predecessor bytes and a Task-bound journal;
  the predecessor watchdog selects candidate or rollback independently of the
  candidate process
  ([release activation](../../internal/infra/controllerrelease/activation.go),
  [watchdog](../../internal/controller/controllerupgrade/watchdog.go)).
- Preflight closes Agent assignment admission and checks executable, manifest,
  storage, Agent identity and execution quiescence. Qualification checks the
  candidate process, listener, linearizable etcd health and authenticated Agent
  before accepting it
  ([preflight](../../internal/controller/controllerupgrade/coordinator_activation.go),
  [qualification](../../internal/controller/controllerupgrade/coordinator_qualification.go)).

These authority boundaries do not prove held requests, ingress/DNS, backing
access, volume data, candidate failure or rollback on the selected release.

## Open correctness and operability gaps

| Gap | Required behavior |
| --- | --- |
| Transient etcd unavailability | Pause publication and dispatch, expose degraded health and resolve ambiguous transaction outcomes. A timeout is not proof that a write failed. It must not cascade into serving-resource lifecycle actions. |
| Retained recovery ownership | Distinguish a closed attempt from physical quiescence. Only a named, supported operation may cross an update after a fresh authenticated process/host-effects proof under closed admission; uncertainty remains blocking. |
| Immutable evidence | Completion uses the exact attempt-bound receipt bytes and encoded field presence. A new reader must not re-encode defaults, consult mutable current state, rewrite history or accept an alternate hash. |
| Identity domains | Credential generation, executor generation and process nonce remain separate. A fixed-revision proof must bind the durable executor to the authenticated process used for quiescence. |
| Predecessor compatibility | Recovery runs predecessor code. Its journal must remain readable by that predecessor; new proof belongs in task-scoped compatible records retained through rollback. |
| Build contention | Source preparation shares host CPU and disk with etcd and applications. Preparation needs bounded resource admission and must stop or throttle before etcd latency threatens control-plane correctness. |
| Startup reconciliation | Missing, conflicting or unverifiable durable/runtime identity fails closed. Startup must not treat absence in etcd as authority to recreate or remove a surviving serving resource. |

etcd documents that unrelated disk activity can cause long fsync latency, request
timeouts and temporary leader loss ([disk tuning](https://etcd.io/docs/v3.6/tuning/#disk)).
GP already exposes host resource and bounded etcd maintenance status through Host,
and etcd exports WAL/backend latency, readiness and database metrics
([GP host projection](../../internal/controller/host/service.go),
[etcd monitoring](https://etcd.io/docs/v3.6/op-guide/monitoring/)). Monitoring
should scrape those sources at a conservative interval and keep time series out
of etcd. Control-plane metric labels need bounded categories rather than Task or
request IDs. Per-resource telemetry needs stable resource/container identities,
with cardinality and retention bounded by the monitored inventory. Shared
backing usage is observed once at the backing owner; consumer attachments may
show connection context, but do not receive an invented allocation of the
underlying usage.

## Proposed Recovery Authority seam

Add one deep Controller module, tentatively `internal/controller/upgraderecovery`,
which owns three conceptual operations: assess to an immutable assessment, export
to a verified receipt, and plan against the observed runtime.

Infrastructure adapters provide read-only etcd snapshot/status, native journal and
receipt reads, and Docker inventory. They do not decide whether a runtime can be
adopted. The assessment binds one fixed revision to storage epoch, Agent channel,
active native journal and Task, immutable receipts, assignment hold, qualified
executor/process, selected releases and exact serving inventory.

An etcd snapshot and Docker observations are not one atomic read. Record their
revisions, identities and observation times separately, detect changes, and
revalidate the plan under the appropriate writer/assignment hold before mutation.

The plan has four closed outcomes:

- **Preserve:** exact durable and runtime identity; no Docker lifecycle action.
- **Adopt:** only a resource owner with an accepted proof may attach an exact
  surviving runtime to current state through a new typed receipt. Adoption never
  alters earlier Task history. Until such a proof exists, this outcome is blocked.
- **Cleanup:** only an exact non-serving effect captured by its unfinished attempt;
  there is no global label prune.
- **Block:** missing, foreign, ambiguous or changed identity remains untouched for
  operator review.

An operator-facing assessment/export/reconciliation action must have Console, CLI
and API parity. A last-resort action while the Controller is unavailable is limited
to the native recovery guard/local bootstrap exception;
it consumes a verified plan and must not expose arbitrary etcd or Docker commands.

## Recovery flows

### Compatible normal update

Keep the current prepare, preflight, activation, qualification and predecessor
rollback path. Epoch and channel compatibility, immutable artifacts, closed Agent
admission, fixed execution identity and journal authority are checked before
cutover. Serving Docker resources remain outside the Controller/Agent lifecycle.
The candidate is accepted only after coordinator readiness and a read-only serving
continuity assessment. Failure restores the predecessor and reports the original
update failed; Retry is a new explicit operation after recovery.

### Reconciliation after coordinator interruption

Start with assignments and mutating schedulers closed. Read durable state at one
revision and observe the current Docker runtime. Preserve only exact identities.
An old desired record is not permission to replace a healthy newer runtime, and a
running container is not evidence that its Task succeeded. Resolve each unfinished
operation through its owning journal/receipt. Unproven resources remain fenced.
Safe scopes may resume only when global journal authority and their claims permit
it, and no later revision invalidated the assessment.

### Snapshot export and last-resort restore

The Store already has a live complete-etcd snapshot primitive but no restore
workflow ([snapshot adapter](../../internal/infra/etcd/store_snapshot.go)). Use a
live snapshot and record its independently checked hash, revision, key count and
size; copying `member/snap/db` can omit WAL data. Official commands and integrity
behavior are described by [etcd disaster recovery](https://etcd.io/docs/v3.6/op-guide/recovery/).

An etcd snapshot is not application data, Volume/database backup, Controller
encryption identity, configuration, managed image inventory or empty-host disaster
recovery. Restoring it must never replace encryption keys or imply those external
assets are recoverable.

Never restore a stale snapshot over a running cluster or while Controller, Agent,
installer, recovery guard or maintenance may write. A last-resort flow must:

1. fence all GP writers while leaving serving data-plane containers running;
2. export the current broken state before selecting an older snapshot;
3. restore to a new directory/logical cluster and verify integrity, epoch and
   every Task/journal/receipt join while isolated;
4. atomically select the validated directory, then start Controller in read-only
   recovery with Agent assignments still closed;
5. compare restored records with the live serving inventory and block every
   unproven adopt, cleanup or recreation; and
6. resume mutations only after the plan settles and a new post-recovery export is
   verified. Retain the prior directory and both exports until explicit retirement.

etcd recommends a revision bump and marking the bumped range compacted when stale
watch caches must be invalidated
([revision restore](https://etcd.io/docs/v3.6/op-guide/recovery/#restoring-with-revision-bump)).
That mechanism invalidates watches; it does **not** prove that GP's fixed-revision
captures remain readable or consistent. Before reopening mutation, GP must prove:

- every pinned preparation/input/receipt revision needed for recovery is present;
- every watch resubscribes from a valid post-restore view; and
- the native journal still joins the immutable Task, attempt and receipts.

If compaction or snapshot age removed required history, the owner-specific
operation remains unresolved and recovery fails closed. No revision bump may be
used to rewrite accepted receipts or manufacture their missing inputs.

## Incompatible storage epoch

An epoch change is a separate migration, not Both/All and not snapshot rollback.
It needs a pre-migration export, all-writer fence, explicit forward converter,
validation of every owner/Task/receipt/runtime reference, atomic cutover and a
post-migration export. The point of no return must be explicit: after writes resume
in the new epoch, restoring the old snapshot would discard accepted work. Rolling
compatibility requires a designed mixed-version reader/writer interval; equal
release labels do not establish it.

## Ordered delivery and proof

1. **Observe and qualify continuity.** Add bounded etcd/update metrics. Inject
   Controller stop, Agent stop, transient etcd timeout/unknown commit, candidate
   startup failure and predecessor rollback.
   Prove unchanged container/image/mount/proxy identities, held HTTP/WebSocket,
   ingress/DNS, backing access and data sentinels; no cleanup/recreate call occurs.
2. **Assess, export and reconcile.** Implement read-only fixed-revision assessment,
   verified export and deterministic plans. Race the assessment with a mutation;
   it must become stale. Test exact preserve plus changed image, mount, routing,
   receipt, foreign label and ambiguous ownership blockers. An interrupted plan
   resumes without duplicate effects or history edits.
3. **Offline restore, then migration.** In isolation, prove all-writer fencing,
   broken-state export, integrity failure, stale snapshot, revision
   invalidation, missing pinned input, watch resubscription and journal mismatch.
   Restore never mutates serving resources before operator-approved resolution.
   Separately prove converter interruption/resume, old-reader refusal after the
   point of no return and unchanged application data plane.

Until these slices pass, snapshots are export evidence, not an automatic recovery
mechanism, and continuity claims apply to the serving data plane rather than an
uninterrupted Console/API.
