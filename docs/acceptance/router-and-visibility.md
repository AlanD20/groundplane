# Router and visibility evidence

This document consolidates the 2026-09-10 Caddy template/preflight/reload work
and the 2026-09-12 Route summary. Requirements live in
[Router templates](../features/router-template.md) and
[Kobwnewe hosting](../features/kobwnewe-hosting.md). Local, browser and QA
results are kept distinct; none is a production-readiness claim.

## Complete template and preview

ADR 0075 replaced the old insertion marker with one operator-authored Caddyfile.
The reserved namespace is `{gp.routes}` and
`{gp.route:HOST:PATH:FIELD}`, with `host`, `path` and `upstream` fields. Native
Caddy placeholders remain intact. Every Route must be covered by an explicit
upstream reference or the aggregate marker. Stable Service names and declared
ports remain upstream authority; no slot address, Route-binding resource or new
API was introduced.

Preview uses the Component's exact MVCC read revision across paged Services,
Zones and Routes, publishes nothing, and fails closed on inconsistent scope,
revision, cursor, ownership or activated-file state. The Console enforces a
32 KiB UTF-8/no-NUL file boundary and prevents stale upload completion from
overwriting a reopened draft. CLI replacement retains non-file Component fields.

Executed local evidence used Go 1.26.7 and Node 24.19.0/npm 11.17.0 with
repository-local state:

- registered Caddy, Component projection, etcd MVCC seam, app, CLI, API/core,
  staging, DNS projection and managed-config race selections passed;
- 201 paged Services, all three collections, inconsistent/failing reads and
  planner/file failures were covered;
- affected vet passed, generated protobuf/OpenAPI/clients were unchanged, and
  25 Console test-file suites plus production build passed; and
- architecture preflight remained red for accumulated size/frozen-module,
  baseline and cross-layer test-placement debt. The separate stale CoreDNS CLI
  fixture also remained open.

An isolated GET-only browser proxy to QA proved complete-file editing,
invalid-UTF-8 and 32,770-byte rejection, upload cancellation, keyboard/focus and
390/1440px layout without dispatching QA writes. Three expected 422 resource
errors were the only later console errors. The provider could not persist
screenshots; they were inspected inline. Evidence is under
`.tmp/production-mvp-20260910/`, including `caddy-template-red.log`.

## Guarded namespace transition

Source `27f194d12` upgraded QA to `0.0.0-qa.native20260910.8` through Task
`task_01M250FDHK70JJFC3Q9Z2J9T08`, created 06:35:56 UTC.

- Controller SHA-256:
  `fa6a03b96c402063471df579ae6a42449f5e9219187eab2c04f0a88f9438701a`.
- Agent registry digest:
  `008d39c1dc8475fcbc010de64483e82e5d8362bb0361555ae58485506e9a90e1`.

The uncached build/distribution/activation sample passed 3,000 held WebSocket
deliveries and 1,200 verified-TLS HTTP requests. Normal Configure Task
`task_01M250YYNPDB7TXZYHM3E9GCM0` completed 06:44:40 UTC and replaced only the
retired marker while preserving `auto_https off`, primary Zone, alias and IPv4.
Serving Caddyfile SHA-256 remained
`86af0b07deb16cc9b5bff2af79825665038692973c2c642692b15369aee373e8`.

Configure recreated Caddy and Tunnel because of new plan/generation labels, so
this was not interruption-free or file-only reload proof. The old Tunnel was
gone before log capture, so no clean Tunnel-log claim was made. Inspection then
found validation occurring after file write and Compose; invalid-file QA was
stopped before submission.

## Native preflight and rejected Configure

`TestMaterializationRejectsComponentFileWithoutPreflight` proved unvalidated
Caddyfile bytes could reach the writer. Materialization now validates the exact
sealed Component bytes before file write; missing, duplicate, mismatched,
changed, cancelled, close-failed or natively rejected input fails before write.
A terminating validator requires zero exit, complete output drain and cleanup.
An output-drain race failed before correction and passed afterward.

The pinned Caddy image was checked without serving mounts/network, as uid/gid
65534, with read-only root, private 16 MiB XDG tmpfs, 256 MiB memory, half a CPU,
64 PIDs, no retained logs and only `NET_BIND_SERVICE`. Capability-free execution
failed with `operation not permitted`; the retained single capability allowed
complete native policy and `tls internal` to exit 0, while an invalid global
directive exited 1. These were isolated image checks, not serving proof.

Focused Agent/materialization, validator, catalog/Caddy and transitive Route
prerequisite race tests and affected vet passed. Generated artifacts were
unchanged. Architecture remained red; evidence is under
`.tmp/production-mvp-20260910/` as `caddy-preflight-*`,
`caddy-qa-preflight-*` and `caddy-preflight-architecture.log`.

Guarded I from source `85a472476` completed Task
`task_01M252Z2VPKGZJ04QGFSFDM4W9` as
`0.0.0-qa.native20260910.9`:

- Controller SHA-256
  `44be776461eeb499d2dc9a9179f5aa5802b931b6540597b970bcd004ff1c7c53`;
- Agent digest
  `68adb0af63b85d04e16e9e932debc32b02390dceaf97a45399388dc4e2b91316`;
- qualified release
  `712f73626abdb60c67ee108a2fc03ec1278d83dd629accff24c35a7de35db2ea`.

Activation passed 3,000 held WebSocket deliveries. The longer whole-build HTTP
sample had two 502 failures before activation during another public Tunnel
timeout; this is retained as failed whole-path evidence.

Normal Configure Task `task_01M253SD6A7THATCAN842SR3KG` submitted an otherwise
valid template with one invalid native global directive. It failed at its first
step at 07:33:52 UTC; later steps did not execute. Caddy retained the same file
digest, container `2f69cbba8619` and Tunnel `b8827c3b7c94`; login and `/up`
returned 200 with TLS verification 0. The failed desired candidate remains and
must be repaired through normal Configure, never by patching files, containers
or durable records. Evidence includes `qa-native-update-i-deploy.log`,
`caddy-preflight-upgrade-first-failure.txt`, the continuity logs,
`caddy-invalid-native-configure.json` and
`caddy-invalid-native-retention.txt`.

## Retained runtime and file-only reload

File-only Configure previously recreated Caddy/Tunnel because volatile
plan/generation labels changed. It also used a single-file bind that pinned the
old inode. The correction retains Component ownership only when effective
Service, image, replica and resources match, excluding only those two labels.
The Agent permits a unique, correctly owned, pinned-image historical Component
generation and runs one non-forced no-dependencies reconciliation before
activation.

Caddy now mounts `components/caddy` read-only at `/etc/caddy`; old file binds,
writable/foreign parents and shadowing child mounts fail. Catalog identity is
v12. Initial mount migration recreates Caddy; later file-only changes are native
reloads. Operator-owned WebSocket policy, including `stream_close_delay`, is
unchanged.

Pure retention, producer/replay, route plan reconstruction, shared plan,
Blueprint release, Compose helper, registered Caddy/catalog and affected
Controller/etcd/app/Agent selections passed with race detection. Generated
artifacts were unchanged. Evidence is `caddy-reload-*` under
`.tmp/production-mvp-20260910/`. The architecture gate and shared-plan protobuf
copy-locks at `plan.go:322` and `service_lifecycle.go:27` remained red.

This correction has local proof but was not deployed before the storage
incident. Full-policy and file-only reload proof therefore remains open.

## Route summary

The 2026-09-12 local Console increment replaced an unconditional ingress hint
with existing API Route states: `no routes`, `provider applied`, or exact counts
of degraded, pending and unserved Routes. It includes mixed public/internal
exposure and never claims public reachability. API, CLI, generated contracts and
runtime behavior did not change.

The failing-first focused test passed after implementation. All 29 Console
test-file suites and production build passed. Evidence is
`route-summary-{red,green,console-tests,console-build}.log` under
`.tmp/production-mvp-20260910/`. An isolated no-outbound browser fixture checked
320/768/1024/1440px, applied/empty states, 21 successful GETs and an empty error
console. Screenshots were inspected inline. The change was committed but not
deployed; Service observation remained absent, so Environment health could still
appear while Service state was unavailable.

## Remaining qualification and authority

- First qualify the standalone storage incident; live mutations are paused.
- Deploy the retained-runtime/directory-mount correction.
- Repair the failed desired Caddyfile through normal Configure.
- Prove direct/public HTTP, WebSocket, internal-callback and deny policy, saved
  preview, primary Zone/IPAM and file-only reload without runtime replacement.
- Deploy Route summary and finish honest Service/Environment observation.
- Run full CI and production qualification.

Do not change host firewall, provider DNS/ingress, PKI authority, protocol or
production state under QA authority.
