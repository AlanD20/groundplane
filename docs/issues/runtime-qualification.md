# Remaining qualification

This is a list of unresolved evidence and implementation limits, not a queue of
automatically authorized work. Recheck the candidate and current user scope before
acting. Completed fix narratives belong in Git history or the
[acceptance register](../acceptance.md#historical-evidence-register), not among open defects.

## Architecture baseline

The temporary 0.0.1 deferrals and older pattern, import and oversized-file
allowances have been retired. The [baseline](../../architecture-baseline.json)
freezes application wiring and caps direct root-etcd code at 75,000 lines.
[Delivery](../delivery.md#architecture-gate)
explains what a passing architecture gate establishes. Local checks do not replace
the live qualification below.

## Live logs and observations

The bounded live rerun [H82](../acceptance.md#bounded-logs-and-blue-green-failure-check)
passed Service and Environment tail/follow, continuing output, client cancellation,
reopen and a CLI tail sample. It closes the earlier 23-event burst failure for
that private synthetic path, not Console or public-ingress streaming.
Script output remains intentionally discarded, not a broken workload log source.
Also qualify serving health freshness, unavailable states and Route reachability
through the real Console/CLI/API; desired state is not live-health evidence.

## Hosting, configuration and recovery

Historical H35 and H46 close specific same-name backing overlap and failed-rollout
recovery sequences on their recorded candidates. They do not establish every
current Entry/Attach/Blueprint writer, source-retention path or recovery variant.

Closure requires the selected current operator sequence: Deploy, successful
Attach/Entry change, Detach, later Deploy and failed candidate. Prove actual
authenticated requests use the acknowledged pre-failure configuration, with exact
proxy/workload identity, unchanged unrelated data and no hidden desired rollback.
Include relevant interruption and replay variants. Qualify mounted-Volume removal
and Script-source retirement where those paths are in the deployment scope.

First Apply/reapply must retain unchanged Components, avoid duplicate hooks and
respect record/transaction bounds. A previous marker-size repair or constructor
check does not prove the whole operation. Do not replay stale desired input over
a working Environment simply to repeat historical evidence.

Blue-green conversion back to recreate and held WebSockets still need selected
live proof. [H81](../acceptance.md#blue-green-retention-and-both-slot-cleanup)
qualifies predecessor retention, inactive-slot replacement, a held HTTP stream
and both-slot Zone/Service cleanup, not those remaining variants.

[H83](../acceptance.md#complete-retained-recovery-and-installer-continuity)
closes H82's selected lifecycle and failed-start blockers and the subsequently
reproduced missing-retained-slot proof defect. Whole-set Stop/Start, failed
candidate restoration, explicit retained historical rollback and Destroy/data
preservation passed. This does not qualify arbitrary mixed-tag histories,
interrupted compensation, all lifecycle repetitions or concurrent removal.

## Installation, upgrades and reboot

The fresh installation run [H76](../acceptance.md#fresh-installation-dns-and-etcd-activation)
passed CoreDNS bootstrap and selected live etcd Save/Apply/recovery variants.
[H77](../acceptance.md#dns-lifecycle-repair-and-installer-update) repaired its
Deploy-accounting and removal-admission defects: normal installer update,
recreate/blue-green DNS stability and desired/last-applied DNS guards passed.
An approved Controller-only maintenance installation loaded the single-publication
fix. The original Zone Task settled as timed out; ordinary Retry completed without
changing its operation or plan. Zone removal then created an extra regular
workload, blocking Service removal. [H78](../acceptance.md#zone-runtime-repair-and-cleanup-blocker)
records the scoped network-only repair and passing full local CI. After approved
one-time orphan removal, normal Service removal preserved Volume data and
protected Volume removal completed.
Environment finalization rejected a retained empty Zone subnet registry as live
authority. [H79](../acceptance.md#environment-finalization-repair) repaired atomic
empty-registry retirement with non-empty reservation guards and passed full local
CI. [H80](../acceptance.md#installer-activation-and-selected-cleanup-qualification)
loaded that repair through approved maintenance, completing the original Task
without rewriting history, then passed normal public ref-installer activation.
Fresh serving-slot/proxy Zone removal, Service/data preservation, protected Volume
removal and Environment/Project/Tenant cleanup passed. H81 subsequently passed
normal Controller/Agent activation and both-slots-present cleanup.
The maintenance installation is not normal-upgrade qualification; H80's subsequent
normal update had no application workload and adds no traffic-continuity proof.
Concurrent DNS publication/removal remains unqualified. Do not bypass transaction,
projection, dependency or updater-idle protections to close these results.

H55 closed the missing-candidate error-classification defect; H65 passed a bounded
idle full-stack host return. Neither proves the current installer across every
supported OS/architecture or reboot with in-flight work.

Closure requires candidate-bound component update scopes, busy refusal, lost
responses, startup failure, original-Task recovery and successive upgrades with
real requests, held connections, jobs and data checks. Normal Controller restart
must leave etcd independent. Record unavoidable host-reboot downtime separately.
Selected production ingress, including Tunnel when used, needs its own continuity
evidence; private HTTP alone is not enough.

H83 adds normal public ref-installer Controller/Agent activation under private
5-request/second traffic, a held HTTP stream and background/data assertions.
Application container identities and independent etcd stayed unchanged. Busy
refusal, activation failure, interrupted updates and supported-platform coverage
remain separate requirements.

## Incomplete features

### Blueprint removal

The accepted Blueprint contract now removes omitted operator-managed resources,
except persistent Volumes, which require their protected Remove action. Entry
export and reconciliation include directly created Entries, and validation
reports omitted Entries as removals. One isolated live Entry omission removed
its public record while retaining unrelated Entry identities and a selected
synthetic secret value. A separate running-Service variant removed the managed
file without restarting the container; its absence reached the new process on
the next Deploy.

Failure, Abort, recovery and other exposure variants remain untested. Omitted
Scripts, Backup policy, native Compose configs and native Compose secrets now
have local reconciliation changes, but no live operator qualification. Service,
Zone, Route and Attach omissions still preserve or reject the missing resource;
they must not be reported as successful removal.

Closure requires completing those removal paths under their dependency and
recovery rules, then proving the selected current candidate changes real
Environment state without orphaning data or unrelated resources.

### Other limits

- Blueprint Validate shares the parser but does not run every Apply preparation
  check, including local image resolution and executable resource preparation.
  A successful preview can still be rejected by Apply. Native configs/secrets,
  external networks and Volume driver options also remain unavailable to current
  Apply despite parser support. See the [Blueprint reference](../blueprint.md#native-compose-with-groundplane-policy).
- Latest-wins Blueprint reconciliation has an accepted design but lacks complete
  publication, late planning, handoff and runtime integration. BP-13/14/15 cannot
  pass until that work is explicitly authorized and delivered.
- The accepted same-Apply Custom hook path is not implemented: a new owner
  still requires standalone Attach before a Blueprint can use its facts.
  Hook execution and Secret retention still need live qualification.
- [H84/H85](../acceptance.md#source-restore-retention-and-deletion-boundary) qualify
  selected Config, Volume and PostgreSQL Restore, key-era and retention variants.
  [H86](../acceptance.md#backup-owned-environment-deletion) proves Config/Volume
  point cleanup and parent deletion through ordinary Task Retry after the executor
  handoff repair. [H87](../acceptance.md#bounded-backup-fault-recovery) adds live
  daily catch-up/duplicate/overlap checks, upload/HEAD failures, corrupted
  Config/Volume download and selected Config, Volume, PostgreSQL and prune
  interruptions. Its Controller repair is not normal-upgrade proof, and the
  original pre-fix Config incident remains recovery-required. After the Attach
  cascade and destructive-order repairs, a fresh Config/Volume/PostgreSQL-owner
  Environment passed uncertain remote deletion, ordinary Retry and Controller
  interruption. Exact remote/local/runtime absence and credential revocation were
  independently checked; PostgreSQL data was retained under the administrator,
  and unrelated rows/Points were unchanged. Earlier invalid plans and failed
  observations remain evidence, not rewritten recovery claims. Custom/shared/
  granted Attach deletion and other archive/provider/checkpoint and arm64 variants remain
  unqualified.
  Valkey's safe source and restore format are undecided.
- A Project-owned trusted Runner completed real build/push/Fetch/Deploy, preserved
  its serving application on delivery failures, resumed after a same-boot listener
  stop and cleaned up its owned runtime ([H75](../acceptance.md#runner-delivery)).
  Full isolation, token-failure, quota/concurrency, reboot, registry-restart and
  arm64 variants remain unqualified; the selected journey does not close them.

See the owning [feature guides](../README.md#operators) and
[QA matrix](../qa-matrix.md) for required outcomes. Do not implement these gaps as
incidental documentation or QA cleanup.

## Historical storage incident

A September QA guest reported persistent-store write/fsync failures. Capacity
recovery did not prove that incident-affected data was intact. Later fresh QA did
not restore that state, so it cannot retroactively qualify it. This is a historical
data-integrity caveat, not evidence about today's host or an instruction to pause
all future QA. Any new fault run needs its own valid baseline and safety limits.
