# Native release publication and startup composition — 2026-09-10

Scope: ADR0074 publication, selected-release continuity and recovery composition.
This is local proof, not a complete upgrade surface or QA activation.

- Staging's private canonical candidate pointer verifies the selected manifest
  and binary; absent, unsafe, symlinked and missing-release cases are covered.
- Healthy release selection is retained before a later journal replacement.
  An actual filesystem activation/qualification followed by failed second update
  preserves the first successful manifest. Active trials refuse new selection.
- Native publication uses the real Task and protected-idempotency repositories
  over a storage-boundary test double. Frozen predecessor/image/generation,
  active equal-key replay, changed-input mismatch and lost-commit-response replay
  pass. Host/process mismatch and unready Agent admission fail before writes.
- Both Agent actions select at request time, not Controller construction. Existing
  replay is resolved before live selection; YAML remains unchanged.
- Startup composes native and Agent recovery before listeners. An unfinished
  journal without its matching durable claim is rejected. Missing native
  bootstrap cannot route Controller recovery to the Agent executor.
- Generic native Task Retry is rejected before publication and by storage's
  retry clone owner. The storage regression initially accepted the clone, then
  passed after the guard. Its cohesive extraction shrinks oversized Task code;
  platform composition also shrinks the oversized app composition root.

Proof after pinned 120-column formatting, using Go1.26.7 and repo-local caches:

- Full `-race -count=1` suites: common/controllerupgrade,
  controller/controllerupgrade, controller/controllertask, infra/controllerrelease.
- Affected app and etcd `-race -count=1`: Agent enrollment/update/selection,
  Controller Task execution, Task retry and native retry refusal.
- Vet: controller/controllerupgrade, controller/controllertask, app,
  infra/controllerrelease. Etcd-wide pre-existing protobuf copy-lock findings
  remain in the qualification queue, not waived by this selection.

The filesystem suites use the real ownership namespace because the restricted
namespace remaps root uid. No trust check was relaxed. No remote state changed.
Host metadata, HTTP/CLI/Console action, guarded bootstrap/staging, generated
clients, real upgrade traffic proof and full CI remain to complete task2/6.
