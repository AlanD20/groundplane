# Integrated disposable floor QA

## Manual-QA handoff — 20:09 UTC

All named runtime blockers in this integration are corrected and deployed.
Normal Agent update `task_01M23W0K7H1V666JJZFFYQVNPV` completed at
19:58:42.265871801 UTC. Agent container `c89f8ffb6666` runs the exact image and
binary recorded below. Controller remains `.7`; the normal config API selected
only the new immutable Agent image, with private before/request/after receipts.
The first update request immediately after Controller restart returned 500
without publishing a Task; after verified startup health, the normal retry
completed. No in-flight operation was replaced.

Exact unchanged complete-bundle reapply
`task_01M23W2CKV8RNQPTRGGYEJ84WN` completed at19:59:54.27041242 UTC; all22steps
completed. Native app workloads, serving green, retained blue, stable proxies
and the two Reverb replicas were unchanged. Caddy/Tunnel reconciled and became
healthy. There are84global Tasks, all terminal, with no next page. Controller,
Agent and etcd report healthy.

Public `https://koapi.aland20.com/up` and `https://ko.aland20.com/login` return200
with certificate verification0. Both checked public internal paths
`/internal/v1/health/ready` and `/internal/v1/identity-verification/status-events`
return404 with verified TLS. Rediscovered replica addresses are10.40.20.6/.7.
The existing ephemeral-channel probe passed both direct replicas, stable
app-reverb, Caddy alias and public TLS Tunnel; no application record was created.

Console delivery checks use exactNode24.19.0/npm11.17.0: scripts-disabled clean
install,23/23tests, production build, fingerprinted assets and embedded-asset
release smoke pass. The smoke temporarily removed generated `console/dist` and
the verified output was restored. Tagged production Controller builds locally;
no unnecessary Controller binary deployment followed the dev-tooling update.
Protobuf, OpenAPI and both clients regenerated with zero diff.

Only compatible transitive dependencies changed: Redocly1.34.19->1.34.20,
js-yaml4.3.1->4.3.2 and Hono4.13.3->4.13.7. No manifest range or forced override
changed. The YAML fix addresses the upstream
[merge-work limit advisory](https://github.com/nodeca/js-yaml/security/advisories/GHSA-2883-xcg3-v3hh);
the Hono patch includes the upstream
[boundary-string escaping fix](https://github.com/honojs/hono/releases/tag/v4.13.7).
The final native audit reports zero vulnerabilities;382registry signatures and
93provenance attestations verify. The stale Caddy optional-alias assertion now
matches ADR0069; no UI behavior was changed for that test.

The isolated real browser loads the deployed Environment, with54successful API
reads and no console errors/warnings. Reload still shows unavailable per-Service
observations; the Service API lacks live observation fields. The generic ingress
hint also remains. These are disclosed in
[manual-QA follow-ups](../issues/floor-manual-qa-followups.md), not hidden behind
Docker health. The task-owned browser tab was closed; the owner's tab remains.
Full CI, full Gate A certification, Gate B, all-MVP acceptance and production
readiness are not claimed. The owner-requested next step is manual QA of the
deployed floor, with subsequent concrete adjustments.

## Exact-reapply correction, local proof

Read-only diagnostic maps the failed step to Caddy health with
`blueprint_compose_procedure=none`. The task and current sealed artifact ids
match. Caddy and Tunnel have exact expected ownership labels. The retained
app-api blue slot is outside the artifact; the stable proxy retains its older
plan/generation labels. Those native containers are not selected by Caddy's
health check. The existing health scoping was enabled by candidate Release
presence, so a no-candidate reapply incorrectly used whole-project collision
rejection.

The Agent now selects the existing lifecycle convergence rule for Blueprint
health selections consisting entirely of Component-owned, unspecified-role
Services, even without a native candidate. No observer, helper, label,
mutation-selection or machine-plan validation is weakened. Original collision
evidence remains intact. The new regression fails with the exact live error
before correction and passes afterward, including health warm-up and rejection
of selected/renamed/unnamed containers, selected networks, Volumes, native
selection and non-Blueprint operations.

Focused race proof: Agent1.422s/5.6%, observer1.060s/53.7%, Controller managed
startup1.404s/1.8%. The initial `TestObserve` selector also matched two unrelated
`TestObservedRecreateProbe*` fixtures that reject missing sealed plan hashes;
no full Agent-suite claim. Focused vet passes. Default Go1.27 causes the pinned
Staticcheck to panic. Supported Go1.26.7 with repo-local XDG cache runs and
reports only pre-existing `internal/agent/script_runtime.go:79` unused body
assignment (SA4006), outside this correction. No analyzer-clean claim.
Static Agent SHA256 is
`71a5e605f0dbeb6bd172a71376a9cf34fb7e10ea077b2fccfc17d27c90d4c104`.
The QA-only package reuses its exact existing runtime layers and has registry
digest `a94a116e8e6514c89cc23eac56dcebebfc1479ba8d63ce018dc997d8a8417189`.
Entrypoint, empty command, user0:0, Docker29.1.3 and Compose2.40.3 pass.
Deployment and live reapply are not yet claimed.

## Current result: Entry cleanup and full running Apply pass; reapply blocked

Signed `5f2a6f47f` is deployed as `0.0.0-qa.floor20260909.7`, hash
`c057385379c28817635e8c12a3a29eeccb70cb4358833edeac0b11f6382a2d99`.
The exact `.6` binary remains `controller-before-entry-removal`; no Agent
update occurred. Normal env-probe removals completed as
`task_01M23TFX7DT142RNBWJ6B9CQNG` (19:32:07.178634049 UTC) and
`task_01M23TGN8HVJVMVF0T9HCKY78K` (19:32:31.905307933 UTC).
The original 14 Entry ids remain, with no probe ids or keys. The exact host
probe file is absent and generated app-api env content has no probe keys.
Green `e78d6ce64014`, blue `3471b7c4138c`, and proxy `4741053372db` were
unchanged by cleanup. Already loaded process variables persist until a later
deploy/reconcile, as the removal contract specifies.

A fresh canonical export plus the three unchanged non-secret companions is
in `.tmp/qa-floor-20260909/blueprint-current/`. It preserves all current Entry
bindings and image references; the host's app `dev` and `qa-floor-20260908`
tags both resolve to `023d784f3f53...`. Only the four previously prepared
Identity support URLs changed to `https://ko.aland20.com`. Validation passed.
The complete running Apply `task_01M23TR871DAW07J056E0KE3MP` completed
19:37:10.430782224 UTC. All four Identity workloads are healthy; the application
group and native stable proxies remain running. This closes the live oversized
publication attempt, but not exact reapply or full floor acceptance.

Exact reapply `task_01M23TVF1BW4YP95A7PA4T1E0N` failed at
19:38:32.416859061 UTC. Nineteen operations completed; step
`step_01M23TVF1BY692VHT9M8ZZHJ4S` failed. The bounded Agent log gives
`state.conflict: compose project ownership collision prevents convergence`.
The Task is terminal; do not bypass that ownership guard or delete containers
out of band. Diagnose the selected operation and retained ownership next.

Final Console checks used exact Node24.19.0/npm11.17.0. `npm ci` succeeded;
the test run stopped on the existing `platform-component-live-state-contract`
assertion that omits the now-supported optional Caddy `alias`. Build did not
run. Audit reports three transitive findings: js-yaml high (fixed in4.3.2),
its @redocly/openapi-core parent, and Hono moderate (fixed in4.13.5).
No forced remediation or lockfile edit occurred; the lockfile-only dry-run
reported no changes. These delivery checks remain open.
An isolated real browser loads the deployed Console and QA Environment with
no error/warning console messages. This read-only observation does not prove
all Console mutation parity. Public HTTPS/fanout checks after Apply remain.

## Live serving Entry checks and cleanup follow-up

Signed `fd2d41468` is deployed as `0.0.0-qa.floor20260909.6`, SHA-256
`968e27b4f52d1f8c2b6f1529e7b1ea5cefc8a33577870f221c70033829fe8f1f`.
Only the Controller restarted; its exact predecessor is retained as
`/usr/local/libexec/groundplane/controller-before-entry-proxy`. Before restart,
all 74 globally listed Tasks were terminal with no next page.

- Existing probe edit `task_01M23SBTVK7NQZKD3A0M623772` completed at
  19:12:29.876945381 UTC. Serving green contained `GROUNDPLANE_FLOOR_PROBE=two`.
- Bulk upsert `task_01M23SE9DTZXHM8H5Y8TPYWFGS` completed at
  19:13:49.623766947 UTC. The original Entry id survived; green contained
  `GROUNDPLANE_FLOOR_PROBE=three` and `GROUNDPLANE_FLOOR_BULK=bulk=value`.
- File create `task_01M23SG3RW7TJ97RGV8C5HD068` completed at
  19:14:49.746623455 UTC. Exact harmless contents and `33:33`, mode `0444`
  were verified inside green and at the authorized host path.
- Normal file removal `task_01M23SJ003KA7BE4EHE15VSDPS` completed at
  19:15:46.882734412 UTC. Normal environment-variable cleanup then rejected
  before publishing a Task; the two disposable env Entries remain for cleanup.

The stable proxy remains container `4741053372db` throughout; no manual proxy
restart or out-of-band workload/config edit was used. Host-health API/CLI
verification passed in
`.tmp/qa-floor-20260909/evidence/host-health-20260909T191210Z.JNwi1t`.
These are live Entry update/file results, not completed floor acceptance.

The bounded read-only cleanup diagnostic identifies an incorrect requirement
that Component-generated Services appear in authored `DesiredServices`.
Both Caddy and Tunnel are absent there but present in their sealed runtime
artifact, as the canonical projection validator allows. A real cleanup-plan
regression reproduces the error. The correction resolves generated names from
that artifact and verifies each exact Component owner. No resource/record limit
is enlarged and no generated authoring declaration is invented. The diagnostic
uses synthetic output bytes and refuses store writes; it is not a published
operation or decrypted-value proof. Normal cleanup follows the corrected build.

## Latest Entry serving-runtime attempt

Signed `014ccaa2d` is deployed as `0.0.0-qa.floor20260909.5`, Controller SHA-256
`1f19750ba5ac40b736343aaf6f8098d9e4e899e65c64a6c2552caae1c3002413`.
Only the Controller was restarted. The exact prior `.4` executable is retained
as `/usr/local/libexec/groundplane/controller-before-entry-serving`.
Host API reports the expected version and healthy Controller/Agent.

Editing the existing harmless probe to `two` rejected before publication with
`state.conflict: retained Blueprint resource configuration changed`. All 65
Tasks remain terminal, with no next page or new Task. The probe still requires
normal cleanup after serving-runtime verification.

The bounded read-only diagnostic compares resource names and changed paths,
never values. Evidence `.tmp/qa-floor-20260909/entry-runtime-resource-diff.jsonl`
identifies only `/content` changes in the Controller-generated proxy configs
for `app-api` and `app-reverb`; network and Volume definitions match. The
desired artifact still contains earlier proxy content. The test sequence
native Apply -> retained Blueprint -> modeled Deploy/Rollback reproduces the
same guard rejection. It must be corrected by replacing only the selected
proxies' owned config definitions, not by loosening persistent-resource guards.
The original local proof and live failure remain separate evidence.

- Target: owner-authorized disposable QA `10.25.0.2`, no other host
- Scope: retained Volume/Entry mutation, prepared manual Script lifecycle,
  and full Blueprint update; not Backup, Gate B, or full MVP acceptance
- Source: signed `63c57c378`, following Volume/Entry/manual integrations

## Runtime deployment

The tagged Controller and static Agent were built with version
`0.0.0-qa.floor20260909`. Their SHA-256 values are respectively
`b848898e74a2c4f850413dba4a72bb8591f304ffb4af76414123b0b761495005` and
`1dd16ed37096f17b69298e68aa2b0dc1b2f374adf2f631c1786351d84e2a1eaf`.
The CLI hash is `c9dae5cd83bed5e8d275b2894ce0f758a77f6dcef875c5d6f43397d3a0061c40`.

The Agent was packaged on its verified existing immutable QA runtime layers,
not rebuilt through the complete production Dockerfile. The QA-local registry
reports `localhost:5000/groundplane-agent@sha256:9098cb81d0ffcc331816118d60c79e33b5cc6cd4b9c94095f9f5aa6507ee0378`.
Entrypoint, empty command, `0:0`, Docker 29.1.3 and Compose 2.40.3 pass.
No workload image was rebuilt or pulled by this packaging step.

Before restart, all 61 listed Tasks were terminal, no next page was present,
and the existing Agent was healthy with zero in-flight Tasks. The exact prior
Controller remains at `/usr/local/libexec/groundplane/controller-before-floor-20260909`.
Private config before/request/after receipts are in the host's
`/var/lib/groundplane/.tmp/qa-floor-20260909/` directory. The normal config API
changed only the immutable Agent image under its revision fence. Listeners,
networks, provider configuration and hosted resources were unchanged.

The new Controller started healthy. Normal `agent update --all` completed as
`task_01M23FC6TYJQN9R1G4HEQRRBB3` at 16:17:51.212206393 UTC, and the new Agent
reported healthy/idle. The previous Agent digest remains available. Rollback
requires explicit attention to any new durable operations; preserving a binary
does not promise old code can execute newly published work.

## First Volume attempt and reproduced correction

The repository verifier failed before Volume creation with HTTP 422:
`Blueprint retained runtime source authority is absent or unexpected`.
Evidence is retained in
`.tmp/qa-floor-20260909/evidence/volume-lifecycle-20260909T161836Z.jGKDmk/`.
No test Volume or removal Task was created; tunnel/runtime cleanup passed.
This run used the script's default system temporary path, which it removed;
subsequent invocations must set `TMPDIR` to repo-local ignored state.

The regression now starts from a genuinely published and acknowledged native
Release, retains it through a subsequent managed-only Blueprint, then calls the
real Volume mutation service and desired publisher. It reproduces the exact
422 rejection. Initial fixture setup failures (an authored generated Component
Service and an invalid no-action Blueprint) were not this production defect.

The shared publisher incorrectly applied Blueprint retained-source validation
to direct desired mutations. Only Apply assembles independent Release sources;
direct mutations preserve one typed, sealed desired baseline, compare its head,
and are forbidden to carry Blueprint domain/Release fragments. The check is
now applied only to the verified Apply source kind. Source/seal/head, ownership,
plan, deletion, and final transaction bounds are unchanged.

The corrected actual Volume create/edit/DELETE publication and reconstructed
DELETE plan pass. Existing Apply authority tamper/source-CAS rejection, retained
publisher, executed-artifact, resource-only Agent, and Entry terminal checks
pass with race detection: 3.505 seconds, selection-local 12.1% etcd coverage in
`.tmp/qa-floor-20260909/retained-direct-publication.cover`. The tagged corrected
Controller builds as `0.0.0-qa.floor20260909.2`; no Agent delta is needed.
Live reruns remain required. No floor acceptance is claimed from healthy
runtime or isolated local tests.

## Corrected publication deployed; dispatch mapping correction

Controller `0.0.0-qa.floor20260909.2` (SHA-256
`283128f9443c5ab20e0c397f9ccd7e5a4792b323ad13ae39eaee9393d13e98a5`)
was installed with the previous floor executable retained as
`controller-before-retained-direct`. Agent and workload state were unchanged.
A verifier retry failed before API calls because its repo-local SSH socket path
was too long; evidence `volume-lifecycle-20260909T162913Z.kOY5dq` is retained.
Use `TMPDIR=$PWD/.tmp`, not a deeper runtime directory, for this verifier.

The next run (`volume-lifecycle-20260909T163013Z.Xu6FGr`) completed real Volume
creation as `task_01M23G2WJP1SB14S0P2GAS84Z9` at 16:30:15.173521163 UTC.
The Volume is `vol_01M23G2WJP1SB14S0P2GAS84Z9`, immutable key
`qa-floor-volume-20260909`, current slug `qa-floor-volume-renamed-20260909`.
The generated Docker Volume and managed directory exist. Do not remove them
out of band: normal Retry and impact-confirmed DELETE are the next steps.

Rename Task `task_01M23G2Y5YEYRBJ1PZVZCZJVCM` was quarantined at dispatch:
plan id/hash/generation/target/steps matched, but its operation did not. It timed
out at 16:32:20.429774118 UTC without dispatch. The later normal Abort request
correctly returned `task.not_abortable` because timeout had already won.

The channel operation matcher omitted direct Volume and Entry `TaskUpdate`
reconcile plans. Both focused cases fail before correction. The matcher now
admits those exact resource/target kinds as reconcile and rejects Blueprint
Apply and wrong target kinds. Full plan validation remains mandatory. The
existing matcher moved into one bounded file so the oversized server shrank
by 52 lines; no allowance changed. Operation cross-pairs, Environment procedure,
actual stream wake dispatch and durable assignment epoch checks pass with race
detection: 1.214 seconds, selection-local 19.1% coverage in
`.tmp/qa-floor-20260909/resource-operation.cover`. Tagged Controller
`0.0.0-qa.floor20260909.3` builds; no new Agent is required. Live retry/cleanup
remain unproven until the normal operations below are completed.

## Volume lifecycle passes on the integrated runtime

Controller `7774ff184` is deployed as `0.0.0-qa.floor20260909.3`, SHA-256
`3b503a1445309bcaba8058366fa520f7a8ac2d64fa2e22b7db66d9e68853c2e8`.
The exact prior executable is retained as `controller-before-resource-operation`.
The timed-out rename's normal Retry completed as
`task_01M23GEEEJZJA7H2H74G1BAQZM` at 16:36:33.159961289 UTC. Normal
impact-confirmed DELETE completed as `task_01M23GFEWGA8XFF48EP0CG6HRY`
at 16:37:07.114185236 UTC. Read-only checks confirm the old Volume is absent
from the API, Docker, and its exact managed path. No application data was
removed; this disposable Volume had no seeded files.

The complete clean verifier rerun passed:
`.tmp/qa-floor-20260909/evidence/volume-lifecycle-20260909T164052Z.if5gUc/`.
Its Volume was `vol_01M23GPD8CAJVTN4SRBYWRFKVM`; the completed create,
rename, and removal Tasks are respectively:

- `task_01M23GPD8CAJVTN4SRBYWRFKVM`
- `task_01M23GPEV4JGGQX7RYZRSB3YFM`
- `task_01M23GPHFA609YG190B23VMSW8`

API/CLI parity, stable id/key/path, in-progress and terminal replay, bounded
impact confirmation, terminal Tasks, and exact Docker/directory cleanup passed.
Tunnel and repo-local runtime cleanup passed; manual cleanup is not required.
The earlier failed runs remain failed evidence, not retroactively green.
This proves an unconsumed disposable Volume on a retained deployment, not
mounted-consumer detachment, crash injection, or full floor acceptance.

## Live Entry and fresh Script blockers

The baseline contains 14 application Entries and three application Scripts.
One non-secret API-scoped probe Entry, `ev_01M23GRBEHF1TK3A0N36Z1DEC8`
(`GROUNDPLANE_FLOOR_PROBE=one`), was accepted. Its Task
`task_01M23GRBEHF1TK3A0N36Z1DEC8` completed at 16:42:01.842480125 UTC.
The generated service env file contains the probe, and the blue API slot was
recreated with it. However, the stable proxy still serves `app-api--green:8080`,
whose existing container lacks the probe. The serving Release is the green
rollback `dep_01M20YD5V7MC42EB1349DAZHY6`. Entry acceptance is therefore **not
passed**: plan completion did not update the serving workload. The disposable
Entry is retained for diagnosis and eventual normal removal. No proxy was
restarted or manually rerouted. Evidence is in the `entry-*.json` files beneath
`.tmp/qa-floor-20260909/` and the exact serving-container probe assertion.

A fresh API-created manual Script, `scr_01M23H18KYS4RGSRQN2QY10XG4`
(`qa-floor-manual-20260909`, target `app-api`, body `exit 0`), was accepted.
Normal Run rejected before execution with
`state.conflict: Script execution source is missing at its fixed revision`.
The disposable Script remains for diagnosis and normal cleanup. This is a
fresh-run failure, not the already completed legacy-reference migration.
At read revision 3139, read-only checks find the new Script metadata/body,
active Script set, hierarchy records, desired head, and desired root present.
The ordinary source loader and every selected Entry-generation read also pass
through a bounded read-only diagnostic. Reference preparation instead treats
valid authored Secret keys (`QA_APP_KEY`, for example) as stable Secret ids and
looks under nonexistent primary/value keys. The key form is explicitly valid
in `docs/blueprint.md`; a correction must resolve it at the pinned revision,
retaining stable source identities and the existing deletion guards. No source
is being invented or repaired out of band. Fresh success/failure/Abort and
normal cleanup remain unproven. Full-bundle Apply and required floor checks
also remain outstanding.

The real serving-Release/Entry/manual-publication journey now has a named-key
variant. It reproduced the exact live rejection before correction. Manual
reference preparation now calls the existing Secret resolution policy at the
execution's fixed read revision and records/deduplicates the resolved stable
Secret id. The same read observes metadata and retirement together; no source
record, public operation, protocol, or transaction ceiling changes. Existing
project override/platform fallback behavior remains, and a captured pre-removal
revision resolves its original Secret through either key or id instead of
substituting the later fallback.

The named-key and stable-id manual journeys, Secret repository checks, and
Secret deletion/source-exclusion checks pass with race detection after final
formatting: 2.529 seconds, selection-local 14.3% etcd coverage in
`.tmp/qa-floor-20260909/manual-secret-key-final.cover`. This includes prepared
reference retention through Retry, terminal draining and normal removal.
The tagged Controller builds as `0.0.0-qa.floor20260909.4`, SHA-256
`d667a8397a4f11350fb30bb2b60cde6ab5d65b4c26c33b88a8cc5c2a911d9fd0`.
No Agent change is required. Live deployment and Script rerun are next; the
Entry serving-slot defect remains open.

## Fresh manual lifecycle passes on QA

Signed `dcffe93f0` is now deployed as Controller `floor20260909.4` with the
verified hash above. The exact preceding executable remains at
`controller-before-manual-secret-key`. No Agent, workload, provider, or
listener configuration changed during this deployment.

The existing disposable Script then completed the normal live lifecycle:

| Case | Task | Terminal result (UTC) |
| --- | --- | --- |
| `exit 0` | `task_01M23J0DMFYMHZ9VNX064ZWGS4` | completed, 17:03:51.341595397 |
| `exit 7` | `task_01M23J1RQ547W1K3040GB74433` | failed as intended, 17:04:35.327271527 |
| `sleep 45`, then normal Abort after observing running | `task_01M23J2CD0XG99HC1AP3BERS5T` | aborted, 17:05:27.323336515 |
| Normal Script removal | `task_01M23J490SZZG48CAJMA97Z1J5` | completed, 17:05:56.778817398 |

The Script is now absent by stable-id read and list. The list contains exactly
the same three application Script ids as before the probe. Normal removal
passed the active-reference guard without migration, force deletion, or source
repair. Evidence is in the `script-*.json` and `scripts-{before,after}.json`
files under `.tmp/qa-floor-20260909/`. The deleted test bodies were only the
three inert probe commands above; Task history and local evidence remain.
This closes the fresh success/failure/Abort/cleanup journey, not live Retry,
crash injection, or independent filesystem inspection. Entry serving-slot
selection, full-bundle Apply and required floor checks are still outstanding.
