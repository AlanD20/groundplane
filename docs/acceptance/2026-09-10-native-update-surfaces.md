# Native Controller update surfaces — local proof

Date: 2026-09-10. Owner-approved production initiative, task2 operator increment.
This is not live upgrade, guarded installer, full CI or production qualification.
No QA/production runtime, ingress, firewall or application state was changed.

## Delivered boundary

- `controller.update`: Console review on `/platform/controller`,
  `groundplane controller update --release sha256:…`, and digest-only
  `POST /controller/update` → protected `202 {task_id}`.
- `GET /host` includes actual executing digest, guarded availability, nullable
  verified candidate and latest retained native Task. Queued and preparation
  failures remain visible without activation files. Invalid release metadata
  disables new updates without hiding independently valid Task history or
  falsifying Host health. CLI and generated clients preserve nulls and fields.
- The closed newest-native-Task index joins existing atomic Task publication,
  fixed-revision primary/index verification and retention deletion. It adds no
  separate summary write, Task filter, API endpoint or operator resource.
- Console retains release/key before publication and Task ID after acceptance
  in tab-scoped storage. Uncertain acceptance resolves the same request;
  read-only polling retains the same Task through disconnects and reloads.
  Terminal native Tasks never expose generic Retry. Existing Task inspection
  and pre-activation Abort remain the normal Task surfaces.
- Host/config fetching and native update state now belong to focused feature
  hooks composed by the Console store; the oversized store/types files shrink.

## Executed proof

Supported Go1.26.7 with repo-local temporary/cache paths:

- Focused race tests in `internal/controller`, `controller/controllerupgrade`,
  `internal/cli` and `internal/cli/apiclient`: endpoint/body/key validation,
  schema absence, publication/replay, Host and CLI/config projection pass.
- `TestLatestControllerUpdate*`, `TestNativeControllerUpdateHistory*` and
  `TestTaskPruning*` in `internal/infra/etcd` pass with race detection. New
  history includes pending Tasks, rejects orphan primaries and prunes atomically.
- `go vet` for Controller, native-update feature and both CLI packages passes.
- `make api` regenerates OpenAPI and both clients. Object nullability is an
  explicit union: a plain Go object pointer is not nullable in Huma's schema.
- Exact Node24.19.0/npm11.17.0 Console build and62tests pass. New behavioral
  tests cover lost acceptance/reload/same-key replay, durable Task retention,
  definitive rejection versus uncertainty and native Retry exclusion.

Two new regressions failed before correction: invalid release state hid valid
Task history; generic Task controls offered native Retry. Both now pass. The
CLI regression first failed for its missing command. Schema generation caught
the non-null object reference and now emits nullable client types.

## Browser evidence and limits

An isolated browser context loaded the actual built Console from a disposable
localhost fixture, with no proxy to QA/production. Initial and recovered page
loads had zero error/warning/issue messages; final11API reads returned200.
Injected lost acceptance and503outage produced only the expected transport
errors, visible in the UI; no uncaught application exception was observed.

The fixture ledger records two publication attempts with exactly the same
release and key, resolving to one Task. Reload before resolution retained the
request; subsequent503reads kept that Task ID and reconnect message. Recovery
rendered Failed with phase `recovered`, not Completed. Task inspection omitted
Retry; an unbootstrapped Host disabled Update while preserving healthy Host
facts and valid history. Escape restored focus to Update; Tab selected the
review confirmation and Enter submitted it. Headings are h1/h2/h3.

Viewport/document widths match at320/768/1024/1440px; screenshots were inspected
at320and1440. Local unthrottled trace: LCP507ms, CLS0.00. These are fixture/lab
observations, not remote network or production performance evidence. The browser
tool denied repository-path screenshot persistence; screenshots were inspected
inline. The task-owned tab was closed, preserving the owner's QA tab.

Ignored local evidence is in `.tmp/production-mvp-20260910/`: Console build/test
logs, API-generation log and `controller-ui-requests.json`. The disposable
fixture source is retained there, not shipped as product code.

Next: guarded bootstrap/staging, then live A→B, bad-candidate, active-work,
interruption and rollback qualification under application HTTP/WebSocket traffic.
