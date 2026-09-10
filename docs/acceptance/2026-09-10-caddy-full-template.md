# Full Caddyfile templates — implementation checkpoint

Owner-approved production initiative, task3. Source and browser proof plus the
guarded QA upgrade and namespace transition. Full policy, bad-edit retention,
full CI and production acceptance are not yet qualified.

## Implemented contract

ADR0075 and `SPEC-router-template.md` replace the single insertion-marker model
with one complete file. The reserved namespace is `{gp.routes}` plus
`{gp.route:HOST:PATH:FIELD}`; fields are host, path and upstream. Native Caddy
placeholders survive. Every declared Route is covered by an upstream reference
or the aggregate marker. Stable Service names/declared ports remain upstream
authority; no slot addresses, Route binding resource or new API is introduced.

The generic Component config read now delegates the Component's MVCC read
revision to the Environment preview. Services, Zones and Routes use that exact
revision through every page. The compiled registration supplies the same plan
and activated SourcePath as mutation planning. Preview publishes nothing and
fails closed on inconsistent scope/revision, failed reads or missing/duplicate
activated files. Existing platform CoreDNS preview behavior is unchanged.

The focused Console editor imports/preserves complete UTF-8 files, validates
the32KiB byte limit and NUL exclusion, and cancels stale upload completion.
It displays the Controller's saved managed-file preview, with loading, error,
empty and disabled states. Preview is explicitly not unsaved-draft validation
or evidence of live application. Parent page shrinks; no new store field exists.
CLI file editing retains unedited fields from existing Component metadata,
without depending on a successful rendering of the file being replaced.

## Executed local proof

Go1.26.7, Node24.19.0/npm11.17.0, all managed temporary/cache paths repo-local:

- Registered Caddy pure race tests: complete native policy, reserved-reference
  resolution/errors, native placeholders, unchanged allocation/stable upstreams,
  default aggregation, input immutability and retired-marker rejection pass.
  Failing-first output is retained in `caddy-template-red.log`.
- Component read/projector race tests pass. Failing-first checks showed omitted
  Component read revision and absent projection. Tests now cover201Services
  over two pages, all three collections, empty views, inconsistent revisions,
  wrong Environment, read failure, cursor cycles and planner/file failures.
- Real etcd repository code over the MVCC memory-store seam retains the old
  Service/Zone/Route projection after a newer desired head is published. The
  Component read revision is deliberately newer than its modification revision;
  the test also proves these reads do not write. This is not a live-etcd test.
- Focused app registration, CLI full-file/Enable, Component API/core/staging,
  DNS projector and generic validation/managed-publication rollback race tests
  pass. Those generic managed-config tests cover the platform path, not the
  Environment's pre-Compose Caddyfile write; the live follow-up found that gap
  below. The CLI regression first
  failed because repair attempted the old saved preview GET.
- Affected Component/DNS/app/CLI vet passes. Protobuf, OpenAPI and both clients
  regenerate unchanged. Console25test-file suites and production build pass;
  the new file's three behavioral/contract tests also pass directly.
- Architecture preflight still fails on the accumulated size/frozen-module,
  baseline and cross-layer test-placement debt. It is not waived or a resource
  exception. The exact new preflight is retained for mandatory task6 corrections.
  The separate stale CoreDNS CLI fixture failure remains recorded in tasks/todo.

## Browser proof and limits

An isolated browser loaded the actual built Console on localhost through a
GET-only proxy to QA10.25.0.2. Non-GET requests were blocked by that server;
no QA mutation was dispatched. The operator's existing QA tab was preserved.

Complete native policy with no aggregate marker enables Save. Invalid UTF-8
upload and a32770-byte typed string block it with accessible errors. A rejected
file retains the draft; a valid complete upload preserves native placeholders.
A delayed upload disables editing/Save; closing and reopening before completion
prevents that old upload from replacing the reopened saved draft. Escape returns
focus to Configure. Document widths equal390and1440px viewports; the390px
dialog screenshot was inspected. No warning/error/issue console messages were
observed. Both task-owned local servers and the isolated tab were closed.

The subsequent GET-only browser check used upgraded QA. The retired marker
returned422, shown as an error with usable Refresh. A held read showed Loading
and disabled Refresh; closing the dialog aborted it, and its late completion
could not replace the reopened draft. After normal namespace replacement,
Refresh recovered without reopening the dialog and displayed the actual896-byte
read-only saved output while leaving its unsaved old draft unchanged. Three
expected422resource errors were the only browser console errors. The task-owned
tab/server were closed, preserving the owner's QA tab. The browser provider
could not save an evidence file into the local workspace; result snapshots are
retained in the session. Local evidence is under `.tmp/production-mvp-20260910/`.

## Guarded upgrade and namespace transition

Source27f194d12 upgraded QA to`0.0.0-qa.native20260910.8` through completed Task
`task_01M250FDHK70JJFC3Q9Z2J9T08`, created06:35:56UTC. Controller SHA256 is
`fa6a03b96c402063471df579ae6a42449f5e9219187eab2c04f0a88f9438701a`;
Agent registry digest is
`008d39c1dc8475fcbc010de64483e82e5d8362bb0361555ae58485506e9a90e1`.
The full uncached build/distribution/activation sample passed3000held WebSocket
deliveries and1200verified-TLS HTTP requests with zero failures. Application and
ingress container identities were unchanged by that upgrade.

Normal CLI Configure replaced only the retired marker, preserving `auto_https
off`, primary Zone, alias and IPv4. Task`task_01M250YYNPDB7TXZYHM3E9GCM0`
completed06:44:40UTC. Serving Caddyfile SHA256 remained exactly
`86af0b07deb16cc9b5bff2af79825665038692973c2c642692b15369aee373e8`.
The Configure path recreated Caddy and Tunnel containers because it reconciles
new Component plan/generation labels; application containers were retained.
This is not interruption-free Configure proof. The old Tunnel container was
already removed before its bounded log capture, so no clean-Tunnel-log claim
is made for H. The HTTP/WebSocket sample itself had already completed.

Inspection then found native validation after the Environment Compose step,
not before the serving-file write. Bad-file QA was paused before submitting an
invalid candidate. The failing-first Agent regression proved unvalidated bytes
could reach the writer. The correction and its proof are in
`2026-09-10-caddy-native-preflight.md`; deployment and end-to-end rejection proof
follow that correction, not the earlier generic platform tests.

## Next authorized QA checks

The captured current template is `{ auto_https off }` followed by the retired
aggregate marker. Preserve that native policy during the explicit namespace
transition; clearing to default would alter automatic-HTTPS behavior. The new
CLI can replace it even while saved preview rejects its retired marker. Existing
serving bytes are not automatically rewritten during a Controller upgrade.

Use the guarded native deployment, then normal Component Configure to adopt
the equivalent new marker and the full policy. Prove actual saved preview,
HTTP/replicated WebSocket/deny behavior and retained serving configuration after
a bad native Caddy edit. Preserve primary Zone/IPAM, router alias, Tunnel settings
and application identities. Do not change host firewall, provider DNS or PKI
authority. Full initiative qualification and production cutover remain separate.
