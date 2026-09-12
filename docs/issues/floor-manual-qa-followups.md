# Manual-QA follow-ups — 2026-09-09

- Owner: Groundplane integration owner, prioritize with the owner's QA reports.
- Scope: visible limitations and non-runtime quality findings after the named
  floor mutation/Apply blockers were corrected. No full acceptance waiver.
- Severity: medium for missing live Service observation; low for the generic
  ingress hint and analyzer/fixture hygiene. None establishes a workload failure.
- MVP-required: yes before claiming complete operator health/quality acceptance;
  these remain explicitly disclosed in the current manual-QA handoff.

## Console health and ingress presentation

On the QA Environment, a fresh isolated browser shows `13 states unavailable`
and `observed unknown`, while host-level health, public HTTPS and actual Docker
health are good. `GET /services/{app-api-id}` returns runtime intent and Release
ledger metadata, not a live Service observation. All 54 inspected API requests
returned 200, and there were no error/warning console messages. This is not a
network failure and desired intent must not be presented as observed health.

The overview's public Route hint says `needs ingress component` unconditionally
in `console/src/features/environment/environment-page.tsx`; it does not prove
the configured Caddy/Tunnel path is absent. The public path passes live checks.

2026-09-12 update: the overview summary is corrected locally, with focused/full
Console tests, build and isolated browser proof. It uses existing Route states
and never claims public reachability; see
`../acceptance/2026-09-12-route-summary.md`. Live deployment and Service
observation remain pending. The Environment aggregate also needs to stop showing
Healthy when its Service observations are unavailable.

Acceptance: agree the bounded observation scope with the owner, expose the real
Controller observation consistently through Console/CLI/API, prove unavailable
versus unhealthy semantics, and make ingress presentation reflect its actual
provider state without deriving execution truth from declarations.

## Existing quality findings

The focused Agent Staticcheck run on installed Go1.26.7 with repo-local cache
reports SA4006 at `internal/agent/script_runtime.go:79`, an assignment overwritten
before use. Default Go1.27 panics in the pinned analyzer; do not use that panic
as a source finding. Focused vet, the changed convergence regressions and builds
pass. No Staticcheck-clean claim.

An overbroad `TestObserve` selector also matched
`TestObservedRecreateProbeAcceptsSealedBlueGreenPrior` and
`TestObservedRecreateProbeUsesEachSealedArtifact`, which reject fixture plans
without a 32-byte sealed hash. The focused correction selection passes; those
fixtures were not changed or bypassed. Acceptance: repair fixture authority at
its real seam, retain hash validation, and pass the exact tests and analyzer.

Architecture-only work remains separately deferred in
[the existing issue](deferred-architecture-cleanup.md). Full CI and full Gate A
certification are not claimed by this manual-QA handoff.
