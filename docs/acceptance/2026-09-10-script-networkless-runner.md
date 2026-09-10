# Networkless Script runner — 2026-09-10

Task4 local execution correction from main`ac1937643`. Explicit setup requires
zero networks. The existing Docker before-start helper sliced`networks[1:]`
unconditionally and panicked on that valid empty capture, before container start.
The helper now skips the primary only when present. No network is invented, and
existing primary/secondary connection behavior remains unchanged.

## Proof

Pinned Go1.26.7, `GOMAXPROCS=2`, `-p 2 -race -count=1`; all filesystem fixtures,
build overlays, caches and logs remain repo-local. No real Docker, QA or
production mutation was used.

- `script-explicit-network-red.log` reproduces the slice-bounds panic in the
  zero-network connector regression.
- `script-explicit-runner-red.log` reproduces that panic through actual
  `Runner.RunContainer`, using a read-only Go build overlay of the predecessor
  runner and an in-memory Docker client. The working checkout was not reverted.
- `script-explicit-runner-race.log` passes the full Docker Script-runner package,
  including actual create→start→output-drain→wait→captured-container/body cleanup
  with no network-connect call. The separate options test verifies pinned image,
  `0:0`, `/`, no ports/endpoints/restart/log storage and exact writable Volume
  grant with image copying disabled. Existing network and cleanup tests pass.
- `script-explicit-agent-barrier-baseline.log` passes the existing Blueprint
  setup/pre-hook failure, real Script cleanup-ack barrier and Docker Script
  checkpoint/failure tests. This is inherited-fixture baseline evidence, not yet
  the new explicit setup/consumer composition test.
- `script-explicit-runner-vet.log` passes the full Script-runner and Agent vet
  checks. Both touched Go files pass pinned120-column formatting; the existing
  oversized runner does not grow.

Logs and overlay inputs are in `.tmp/production-mvp-20260910/`. The correction
does not close task4: explicit initial-consumer ordering, failure/Abort/replay and
live qualification remain. Live storage/source integrity is still unqualified;
no live mutation has resumed. Other full-CI failures remain in the task6 queue.
