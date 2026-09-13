# Remaining runtime qualification

- Owner: primary delivery owner for the affected feature
- Severity: high for missing data-safety or exercised destructive-path proof;
  medium for unavailable health; low for isolated presentation/fixture hygiene
- MVP-required: yes for the actual hosting and recovery paths exercised by Gate A/B

This file records unresolved proof, not a historical implementation journal.
The [task list](../../tasks/todo.md) owns scheduling; [head.md](../head.md) owns
operational authority. The owner authorized fresh QA with disposable data on
2026-09-12, without restoring incident-affected state. That state's integrity
remains unresolved; the fresh run does not qualify it.

## Storage and source retirement

The normalized desired revision is the sole Volume authority;
the remaining flat `VolumeRecord` is read projection, not a writable primary.

Remaining acceptance includes mounted-Volume consumer detachment, crash/Retry
recovery, real manual-Script source consumption and source-family retirement.
In particular, qualify prepared references through physical destruction and
cross-Environment Network ownership; allocating a source count alone is not
proof that every deletion path fences it. Keep unchanged workload bytes, native
Release ownership, proxy identity and persistent-resource authority. No coverage,
ownership or immutable-source guard may be relaxed to pass the journey.

Requirements are in [Volumes and Entries](../features/storage-and-entries.md),
[Script execution](../features/setup-scripts.md), and their routed technical
contracts. Local source-retention proof and retained wider failures are in
[Script evidence](../acceptance/script-execution.md#manual-immutable-source-integration).
Recheck code and relevant evidence before implementing an older suspected gap.

## Blueprint integration

The 692,408-byte historical failed publication and 412,657-byte local reproduction
were corrected without raising the 262,144-byte marker limit. The local running
marker measured 71,808 bytes in the local reproduction.
That closes the old blanket oversized-publication blocker, not every Blueprint
recovery case or the newer explicit-context first-Apply/reapply requirements.
Preserve exact witnesses and all record, assignment and transaction ceilings.

Acceptance is the actual current producer, source-retirement and recovery paths,
then the current full bundle through normal surfaces. Do not replay a stale
desired revision over a working Environment merely to repeat an old test.

The owner confirmed per-Service native recovery authority on 2026-09-12. Contracts,
claim validation, Agent admission, restoration and independent observation now use
the same captured native artifact. The applied Environment witness remains
independently fenced; it is not a recovery fallback. Local mixed Worker proof
covers serving restoration, first-candidate absence, immutable history and terminal
replay in `.tmp/native-recovery-dlIg4I6U/mixed.log`. This closes the earlier local
artifact-identity mismatch, not live recovery qualification. Owner: primary delivery
owner. Remaining acceptance is the real operator journey and full CI.

The broader local proof passes in `agent-final.log`, `authority-final.log`,
`executionplan-final.log` and `renderer.log` in the same directory, with race
checking and coverage. It includes native rollback predecessors, source-revision
conflicts, reconnect, terminal replay and malformed-authority rejection. Scoped
vet passes. The architecture gate reports the same 125 outstanding findings;
the three findings on edited mixed-fixture files concern imports already present
before this change. No baseline was raised.

## Same-name backing endpoints

Owner: Backing Service delivery owner. Live QA on 2026-09-13 exposed a DNS
collision when one consumer joined two distinct Valkey backing networks. Both
Attach URL facts used the bare hostname `valkey`. After Entry rebinding, the new
password was correctly present in the consumer and authenticated against the new
instance's address, but Docker DNS resolved that hostname to the old instance.
Removing the old Attach restored correct resolution and both native realtime
replicas passed actual authentication and subscription checks.

This blocks qualification of overlapping same-name backing connections, including
that migration window. It is not an authentication-mode failure. Evidence is
`cache-cutover-diagnostic.log` and `cutover-finish-after-dns.log` in
`.tmp/qa-recovery-repair-20260913-63vxXPTf/`. Fact construction currently uses the
Backing Service name in `internal/app/attach_mutations.go`.

Acceptance: give each backing endpoint a stable, unambiguous runtime identity
and use it consistently in HOST and URL facts. Attach one consumer to two
same-kind Backing Services with distinct credentials; prove each fact reaches
only its own instance before, during and after a credential/Entry cutover.
Keep isolation and authentication checks intact. Do not qualify this case by
hardcoding instance IPs, bypassing facts or weakening credentials.

## Service health and Route visibility

The old generic ingress hint was corrected locally using existing Route states.
It still needs deployment. Service observation and its Environment aggregate
are implemented locally but need live qualification; unavailable evidence must
not become Healthy. Desired intent
and successful old Tasks do not prove workload or public reachability.

Use [Services and Releases](../features/services-and-releases.md).
Acceptance is consistent Console/CLI/API
observation, correct unavailable/unhealthy semantics and the deployed Route
summary without claiming more than its evidence.

## Verification debt

The 2026-09-12 maintenance slice cleared five protobuf copy-lock findings and
seven Staticcheck findings across Agent, execution-plan, Agent-channel and etcd
packages. Plan validation now clones protobuf messages; runner snapshots remain
pointers. Unused assignments are removed, Zone conversions are explicit, and
late log-subscription cancellation uses the existing bounded cleanup context.
Scoped vet, Staticcheck, formatting and affected race tests passed. Evidence:
`.tmp/ci-cleanup-7arYNpkD/{vet-after,staticcheck-after,analyzer-focused,analyzer-etcd-focused,analyzer-zone-focused}.log`.

This is not a full-package or full-CI pass. The hook-recovery failure retained in
`analyzer-focused.log` is superseded by the native recovery proof above. The exact
affected terminal-publication, source-reference and Zone selections passed
separately. [Script acceptance](../acceptance/script-execution.md#explicit-context-qualification)
retains the broader qualification requirements.

The CoreDNS CLI fixture now exercises Component read, config read and full
Corefile replacement. The Blueprint fixture uses `environment blueprint apply`,
reads the current revision and asserts the exact `If-Match` fence on the PUT.
The complete CLI subtree passed race tests with coverage, and vet passed.
Evidence: `cli-subtree-corrected-loopback.log`, its matching coverage file and
`cli-vet.log` in the same evidence directory. The first local run was blocked
by sandbox socket permissions; the passing run used isolated loopback servers,
not a live Controller.

The subsequent minimum QA preparation corrected the 13 CLI ST1005 findings and
removed the two overwritten artifact assignments in
`internal/controller/component_action.go`. CLI/Controller Staticcheck and the
affected Component/CoreDNS race tests pass, with coverage, in
`.tmp/qa-fresh-20260912-NRo98B/{analyzer-scoped,component-race}.log`. This closes
those 15 analyzer findings, not full-CI or live qualification.

The integrated scan corrected a further 13 analyzer findings and a protobuf-copy
vet finding; the intentional nil-context negative test retains a single explained
SA1012 suppression. Full tagged Staticcheck and vet pass. The formatter gate now
excludes ignored caches, selects `gofmt` explicitly and preserves tool failures.
Its regression tests pass. The full tagged race run exposed four stale fixtures
and a terminal-event replay defect; focused corrections pass, including rejection
of changed replay identities and payloads. Exact evidence is in
`.tmp/qa-fresh-20260912-NRo98B/`.
The final full affected etcd, etcd/releasegroup and Controller Agent-channel
packages pass race tests with coverage in `persistence-full.log`. This closes
the retained package failures, not full CI or live workload qualification.

Privileged Backup staging qualification is unavailable locally because sudo needs
a password. The approval review rejected the proposed QA-host mount test; it was
not executed. Owner: QA delivery owner. Acceptance: obtain explicit authority and
pass the bounded mount test on an approved root-capable target. This remains
unqualified and does not expand initial hosting into Backup delivery.

The two Agent recreate-probe fixtures were repaired on 2026-09-12. They now use
sealed plans, explicit native predecessor references and validated assignment
authority. Historical labels remain unchanged; the blue-green transition uses
one replica on each side. Separate observations prove the historical blue slot
and the candidate/prior exact-count path, including unrelated collision filtering.
Four negative cases reject a missing seal, missing authority, changed witness
or unselected step before observation. Production validation is unchanged.

Evidence is in `.tmp/recreate-fixtures-Rpw7yBic/`: `before.log` reproduces both
missing-hash failures; `probes.log` and `recovery-evidence-final.log` pass with
race checking and coverage. Agent vet and Staticcheck pass (`vet-final.log`,
`staticcheck-final.log`). This unit fixture treats the Controller-owned authority
digest as opaque; it does not qualify Controller publication or live recovery.
The prerequisite-gate and hook-terminal fixtures now carry complete staged Release
authority and the executed resource set. Their three retained failure groups are
closed; the two mixed-recovery groups now have the native recovery proof above. Focused race tests
with coverage, including shared atomic-publication shapes, pass in
`.tmp/blueprint-recovery-ar6iu0wf/gate-terminal-final.log`. Scoped etcd vet and
Staticcheck pass in `etcd-vet-final.log` and `etcd-staticcheck-final.log` in that
directory. This fixture-only repair does not qualify full CI or live recovery.

Go 1.27 crashed the pinned analyzer in the recorded run; Go 1.26.7 ran it and
reported SA4006. That tool crash is not a source finding or a green result.
Exact current tasks and evidence are in [tasks/todo.md](../../tasks/todo.md).
Architecture-only cleanup remains separately [deferred](deferred-architecture-cleanup.md)
under its explicit authority, without waiving real safety, build or runtime defects.
