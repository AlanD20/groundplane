# Remaining qualification

This is a list of unresolved evidence and implementation limits, not a queue of
automatically authorized work. Recheck the candidate and current user scope before
acting. Completed fix narratives belong in Git history or the
[acceptance register](../acceptance.md#historical-evidence-register), not among open defects.

## Architecture and tests

Production restructuring through `08ccb78b4` was explicitly implementation-only.
Tests, generated clients and gate/baseline metadata were not migrated or run.
Their compatibility with the new module boundaries is unverified. Source
organization alone cannot establish a passing build or architecture gate.

The historical 0.0.1 structural exceptions remain in the unchanged machine-readable
baseline/deferral files. Their old finding counts are not current measurements.
The owner later authorized production cleanup; the earlier prose saying cleanup
was forbidden is superseded. No exception permits ignoring a demonstrated safety,
security, compilation or runtime defect.

Closure: when authorized, reconcile the excluded test/generation/gate surfaces
with the actual production structure and complete the required delivery gate.
Do not add compatibility shims, relax bounds or manufacture green results.

## Proxy-probe cancellation

The 2026-09-20 release gate reproduced a failure in
`TestProxyProbeCancellationUnblocksAttachedRead`. Its fake attachment returns both
a connection and cancellation error; the adapter returns before arranging that
connection's close. This is a reproduced local sequence, not proof of the real
Docker client's error-response ownership contract. No closure is recorded here.

Closure: establish who owns an attachment returned with an error, correct the
responsible adapter or fixture and prove cancellation releases owned connections.
Do not remove the regression merely to pass CI. Reproduction details remain in
the pre-migration version of this file at `08ccb78b4`.

## Live logs and observations

The last recorded Environment follow stream closed after 23 events. The bounded
queue could cancel a normal tail burst; a local backpressure correction has
behavioral evidence, but the live rerun remains outstanding (LOG-02).

Closure: use an explicitly authorized running workload to prove Service and
Environment tail/follow, continued delivery, stop/reopen and cancellation cleanup.
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

## Installation, upgrades and reboot

H55 closed the missing-candidate error-classification defect; H65 passed a bounded
idle full-stack host return. Neither proves the current installer across every
supported OS/architecture or reboot with in-flight work.

Closure requires candidate-bound component update scopes, busy refusal, lost
responses, startup failure, original-Task recovery and successive upgrades with
real requests, held connections, jobs and data checks. Normal Controller restart
must leave etcd independent. Record unavoidable host-reboot downtime separately.
Selected production ingress, including Tunnel when used, needs its own continuity
evidence; private HTTP alone is not enough.

## Incomplete features

### Blueprint Entry omission

Source review during documentation migration found that
[Entry reconciliation](../../internal/controller/taskplanning/blueprint_entry_reconciliation.go)
selects omitted Blueprint-owned Entries for removal and can treat identity changes
as replacements. This conflicts with the accepted non-destructive omission rule.
No execution was run and no production fix was attempted. Operators must retain
existing Entry keys rather than rely on omission being harmless.

Closure requires an explicitly authorized behavior correction and its bounded
proof; documentation must not silently redefine removal as intended behavior.

### Other limits

- Latest-wins Blueprint reconciliation has an accepted design but lacks complete
  publication, late planning, handoff and runtime integration. BP-13/14/15 cannot
  pass until that work is explicitly authorized and delivered.
- New Custom hook owners require standalone Attach before their facts can be
  used in Blueprint-generated files. Single-apply fact production/consumption is
  deferred. Hook execution and Secret retention still need live qualification.
- Backup/Restore remains incomplete and deferred; Valkey's safe source and restore
  format are undecided. Metadata CRUD is not recovery evidence.
- Runner isolation, token handoff and host lifecycle remain incomplete against the
  accepted contract. Earlier local cleanup checks do not prove ready-state jobs.

See the owning [feature guides](../README.md#operators) and
[QA matrix](../qa-matrix.md) for required outcomes. Do not implement these gaps as
incidental documentation or QA cleanup.

## Historical storage incident

A September QA guest reported persistent-store write/fsync failures. Capacity
recovery did not prove that incident-affected data was intact. Later fresh QA did
not restore that state, so it cannot retroactively qualify it. This is a historical
data-integrity caveat, not evidence about today's host or an instruction to pause
all future QA. Any new fault run needs its own valid baseline and safety limits.
