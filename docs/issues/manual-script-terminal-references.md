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

Diagnosis is incomplete. Publication increments `ActiveReferences`; deletion
checks it in `script_deletion.go`. Inspect manual terminalization and durable
cleanup checkpoints before changing reference-release authority. Do not infer
which of the three executions retains a reference from Task state alone.

Acceptance: completed, failed, and aborted manual executions release only their
own references after the required cleanup proof, idempotently and under CAS;
uncertain or active executions still block deletion. Regression tests must cover
these terminal paths and replay. Remove the disposable QA Script through the
normal API after correcting its supported recovery path.
