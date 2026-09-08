# Manual Script terminal-reference cleanup

Owner: Script execution / Task persistence.
Severity: operator lifecycle blocker.
MVP-required: yes, for exercised manual Script removal.

After the manual publication compare-key repair, QA Script
`scr_01M2104ECJZ050QH7RMDSCFSWG` completed one execution, failed another,
and aborted a third. All three Tasks are terminal, but normal Script removal
rejects `resource.in_use: active Script executions fence deletion`.
Exact Tasks and runtime evidence are in
[management checks](../acceptance/2026-09-08-management-checks.md).

The disposable Script `qa-manual-cas-proof` remains in `qa-application` on
QA 10.25.0.2, generation 3, body `sleep 60`. No execution remains running in
the observed Task results. No direct record deletion or guard bypass was used.

Read-only inspection of the execution records confirms that all three are
`cleanup_proven` with `ActiveReference=true`. Each carries container, body, and
execution-directory absence proof. Their canonical ADR 0062 operation source
roots are absent. The inspection selected only execution identities, checkpoint,
outcome, cleanup flags, and root presence; it did not print snapshots or inputs.

The missing integration is broader than a terminal counter decrement:

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
