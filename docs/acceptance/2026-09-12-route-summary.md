# Route summary — 2026-09-12

Task5's first local increment replaces the unconditional overview hint with
the existing API Route states. Empty collections say `no routes`; fully applied
collections say `provider applied`; otherwise the hint counts degraded, pending
and unserved Routes, including mixed public/internal exposure. It never claims
public reachability. No API, CLI, generated contract or runtime behavior changes.

The focused test failed before the summary existed and passes after integration.
All29Console test-file suites and the production build pass on pinned
Node24.19.0/npm11.17.0. Evidence is under`.tmp/production-mvp-20260910/`:
`route-summary-red.log`, `route-summary-green.log`,
`route-summary-console-tests.log` and `route-summary-console-build.log`.

An isolated Chrome session exercised the built Console with a read-only local
HTTP fixture, without any QA proxy or outbound requests. Mixed state text was
visible at320/768/1024/1440px, with Route card client/scroll widths equal at
134/350/162/266px. Screenshots at320and1440were inspected inline. Applied and
empty cases rendered the expected text after navigation; the error/warning
console was empty. The inspected21API requests were GETs returning200. The
task-owned tab and fixture server were closed; the blank browser tab was kept.

[The hosting feature](../features/kobwnewe-hosting.md) records the remaining observation/bundle work and
qualification boundaries. Actual Service observation is not implemented by
this change. Browser inspection also confirms the existing Environment header
can say Healthy while its Service state is unavailable; address that aggregate
with the observation projection. Live deployment remains paused behind storage
integrity, and required CI/recovery qualification is still outstanding.
