# Native update bootstrap and deployment client — local proof

Scope: ADR0074's first recovery-capable installation, immutable release staging
and the deployment client's normal Controller update Task. Work stays on main;
no production, ingress or host-network change is included.

## Delivered boundaries

- Source-derived Go build metadata pins the exact Controller bytes, display
  version, storage epoch and channel schema without executing candidate code.
  Deployment resolves the Agent registry digest before canonical manifest hashing.
- Descriptor-relative staging refuses symlinks, writable/untrusted ancestors,
  unsafe leaves, hardlinked inputs and changed published bytes. It holds the
  native journal lock while publishing immutable release files and selecting
  `candidate.json`; it never modifies activation or qualified-selection state.
- Bootstrap installs a predecessor-owned startup guard before service enable.
  Partial native installations cannot fall back to direct overwrite. Existing
  legacy installations require explicit maintenance bootstrap and a paged idle
  check. The old binary cannot fence new work: the operator must maintain that
  one-time maintenance boundary.
- Bootstrap rollback requires successful stop and removes only a newly created
  empty native layout. Recovery/release evidence prevents removal or restoration;
  failed stop preserves the installation and backups. The first candidate is
  selectable only after bootstrap rollback is disarmed.
- Normal guarded deployment exits before every legacy executable/config/unit
  overwrite and independent Agent update. `--stage-only` does not activate.
- The client fsyncs a release/key receipt before POST. Lost responses replay the
  same key; known Tasks are only reread, including after process restart. An
  unresolved receipt cannot be replaced with another release. A failed Task or
  bounded720-second client timeout never causes SSH rollback or Abort and retains
  the receipt/helper bundle for explicit resume.
- The former monolithic remote shell is split into cohesive first-bootstrap and
  Agent-enrollment modules; `scripts/deploy.py` shrinks from1531 to841lines.
  The new handwritten modules remain below600lines.

## Executed proof

- Failing-first tests for metadata, staging, partial bootstrap and protected
  client acceptance, followed by passing implementations.
- `make deployment-check`:38tests, including native branch, same-Task reconnect,
  rollback of an executing binary and unchanged installation after failed stop.
- Go1.26.7 `go test -race -count=1` in `internal/releasemeta` and
  `internal/common/controllerupgrade`; affected `TestControllerUpdate*` routes.
- Vet of both metadata packages; pinned golines and `git diff --check`.
- ExactNode24.19.0/npm11.17.0 release build, Console tests/build, Controller/CLI
  version injection and matching `bin/controller-release.json`.
- OpenAPI/client regeneration preserves the existing operator contract. The
  update route now uses the repository's approved schema-registration helper.

Evidence: `.tmp/production-mvp-20260910/native-deployment-tests.log`,
`native-bootstrap-build-retry.log`, `native-bootstrap-api.log` and
`native-bootstrap-architecture.log`. The first frozen npm install failed with
sandbox DNS `EAI_AGAIN`; the network-enabled pinned retry passed.

## Not yet proven

Disposable QA10.25.0.2 was reachable, Controller active and Python available;
no native root was installed at the read-only preflight. No new binary has yet
been deployed. Live bootstrap, A→B, bad candidate, busy drain, interruption and
HTTP/WebSocket continuity remain the next verification boundary.

The broad architecture gate fails on accumulated oversized/frozen-module drift,
stale baselines and cross-layer acceptance-test placement. Its exact output is
retained for task6; it is not a passing or unavailable-resource exception. Full
CI and production qualification remain required. This proof does not close the
whole upgrade-safety Task.
