# Remaining qualification

This is a list of unresolved evidence and implementation limits, not a queue of
automatically authorized work. Recheck the candidate and current user scope before
acting. Completed fix narratives belong in Git history or the
[acceptance register](../acceptance.md#historical-evidence-register), not among open defects.

## Architecture baseline

The temporary 0.0.1 deferrals have been retired. Older baseline allowances remain
explicit in [the baseline](../../architecture-baseline.json); they were not expanded
by the integration-test migration. [Delivery](../delivery.md#architecture-gate)
explains what a passing architecture gate establishes. Local checks do not replace
the live qualification below.

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

### Blueprint removal

The accepted Blueprint contract now removes omitted operator-managed resources,
except persistent Volumes, which require their protected Remove action. Entry
export and reconciliation include directly created Entries, and validation
reports omitted Entries as removals. One isolated live Entry omission removed
its public record while retaining unrelated Entry identities and a selected
synthetic secret value. No running Service or managed-file pickup was checked.
Other resource omission paths still preserve or reject the missing resource;
they must not be reported as successful removal.

Closure requires completing those removal paths under their dependency and
recovery rules, then proving the selected current candidate changes real
Environment state without orphaning data or unrelated resources.

### Blueprint validation disagreement

On the current QA candidate, Validate previewed creation of an implicit default
Zone without an authored subnet, while Apply rejected those same bytes because
each MVP Zone needs an explicit IPv4 subnet. Apply made no resource or Task
change. The test input was incomplete, but Validate must reject it before
showing an actionable preview. Running-Service Entry removal remains untested
until a valid isolated Service fixture is applied; see H71 in the
[qualification register](../acceptance.md#blueprint-and-entry-intent).

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
