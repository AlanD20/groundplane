# Disposable hosting-floor evidence

This document consolidates the 2026-09-08 and 2026-09-09 deployment-management,
Blueprint, Volume, Entry and manual-Script evidence on disposable QA
`10.25.0.2`. It preserves intermediate failures because later corrections do
not make those runs pass. The final handoff is bounded evidence, not current
host health, Gate A, Gate B, full MVP acceptance or production readiness.

## Authority and baseline

The named scope was tenant `entensy`, project `kobwnewe`, Environment
`qa-application`. Controller access remained private. No production, provider,
firewall, backup/restore or other-host mutation was authorized.

The 2026-09-08 deployment-management run started from the existing application
and produced these results:

| Check | Recorded result | Identity or limit |
| --- | --- | --- |
| Fresh group deployment | Passed | `task_01M20WZAF3SKAWTT51P2TME0EN`, completed 16:18:32 UTC |
| Distinguishable image deployment | Passed | `task_01M20Y7MSW373TMNHV2CMNR1H8`, completed 16:40:29 UTC |
| Rollback preview and rollback | Passed | `task_01M20YD5V7MC42EB132PP4GS3Z`, completed 16:43:28 UTC |
| HTTPS and replicated WebSocket delivery | Passed after deployment and rollback | Both replicas, stable Reverb target, private router and public TLS Tunnel |
| Volume create | Blocked | Initial 500 corrected; rerun rejected retained-runtime coverage with 422 |
| Project Secret masked lifecycle | Passed | `sec_01M20YA9F0B5JZQ4S8A5RR64XW`; show returned no value |
| Service Secret Entry | Blocked before publication | Retained-runtime coverage rejection; no Entry created |
| Script create/edit | Passed | Stable id; generation 1 to 2 |
| Manual Script execution | Repaired and exercised | Success, intended failure and Abort below |
| Application logs | Passed | Bounded `service logs app-api --tail 5` |

The QA-only image `groundplane-rpi-app:qa-management-20260908` added only
`/opt/groundplane-qa-revision` and retained `www-data`. Four application group
members ran image
`sha256:a3e6d82cbf40575b930bee7ce25782d081a0614b683e52dd4a9a67f3eeb208e8`.
Rollback selected the earlier `qa-floor-20260908` Releases and returned API,
Reverb, queue and scheduler to image prefix `023d784f3f53`. Both WebSocket
replicas stayed healthy with zero restarts. Rollback to the current tag was
correctly rejected; the older `dev` tag was not used because it predated public
hostname configuration.

Raw evidence is under `.tmp/qa-management-20260908/`; the public-hostname fanout
probe is `.tmp/qa-fresh-20260908/reverb-public-hostname-fanout.php`.

Only disposable resources were removed: Script
`scr_01M20YG8KFCW9G0ZKWEE0GZ46A` by Task
`task_01M20YHEPD62KG0PGRSQ2VW8T2`, Script
`scr_01M20YCKRBT5217FXRQ6ZWG4RY` by Task
`task_01M20YHESGTTAM6E1K6J5DEHJ0`, and Secret
`sec_01M20YA9F0B5JZQ4S8A5RR64XW` by Task
`task_01M20YHETKJRQZ35FCDDE5QH90`. No test Volume or Entry was published.
The diagnostic predecessors were retained as
`controller-before-volume-diagnostic` and
`controller-before-manual-script-fix`; no diagnostic executable remained in
the source or active Controller path.

## Blueprint publication bounds

[Blueprint predecessor contract](../features/blueprints.md#bounded-native-predecessor-references)'s running-Service correction was tested through two real preparations
and publications for two Services, each with eight 3,000-character non-secret
environment values. The initial Apply installed actual serving Release
projections. A replicas-one-to-two update had previously produced a 412,657-byte
aggregate marker, exceeding the 262,144-byte record ceiling. Candidate render
inputs now remain in independent digest-bound records rather than being copied
into the marker.

| Shape | Initial Apply | Running update |
| --- | ---: | ---: |
| Aggregate marker bytes | 71,034 | 71,808 |
| Largest candidate render record bytes | 135,113 | 170,794 |
| Assignment native artifact bytes | 0 | 53,064 |
| Assignment durable record bytes | 1,208 | 141,511 |
| Publication comparisons / mutations | 26 / 19 | 37 / 19 |
| Publication protobuf transaction bytes | 82,352 | 84,892 |
| Claim comparisons / mutations | 13 / 7 | 17 / 7 |
| Claim protobuf transaction bytes | 8,577 | 430,154 |
| Plan protobuf bytes | 54,171 | 107,395 |
| Assignment wire protobuf bytes | 400 | 105,511 |

The actual Store request includes the `/groundplane` prefix and failure reads.
Every record remains at most 262,144 bytes; Blueprint publication keeps the
256-operations-per-arm and 1 MiB request limits, while ordinary transactions
retain the 96-operation bound. Timestamp encoding can change observed byte
counts slightly, so the measurements are not universal equality constants.

Real publication, reconstruction, claim, terminal acknowledgement, executed
artifact promotion, replay and tamper/race rejection passed. WorkerPool
admission used the real persisted plan but did not perform gRPC or Docker
effects. The combined race selection ran 61.758s with 14.1% etcd selection
coverage in `.tmp/blueprint-bounded-integration.cover`; final formatted checks
ran 11.772s with 11.7% in `.tmp/blueprint-reference-final.cover`. Tagged
Controller and Agent builds passed; schemas were unchanged. Live deployment and
full-bundle Apply were still required at that checkpoint, and the architecture
gate remained red in `.tmp/blueprint-reference-architecture.txt`.

## Volume publication and removal

The removal selector binds baseline and candidate ownership, runtime metadata,
the removed Volume id/key and exact cleanup targets. It selects only workload
containers whose baseline mounts the removed key, uses
`up --detach --no-deps <slot>`, and treats empty selection as a no-op. It does
not select every proxy or slot sharing a logical Service id. Retained candidate
mounts and any stopped container with a named-volume or overlapping bind fail
closed; no evidence authorizes deleting such containers out of band.

Focused local checks used `-race -count=1` and repository-local caches. Evidence
is retained as `.tmp/volume-selection-{red,green,native,affected,blueprint-boundary}.cover`.
These tests used a FakeRunner and did not themselves run Docker or the public
DELETE path.

The first live verifier failed before creation with 422
`Blueprint retained runtime source authority is absent or unexpected`; evidence
is `.tmp/qa-floor-20260909/evidence/volume-lifecycle-20260909T161836Z.jGKDmk/`.
The correction restricted retained-Release source validation to Blueprint Apply;
direct mutations preserve one sealed desired baseline. The local race selection
passed in 3.505s with 12.1% etcd coverage at
`.tmp/qa-floor-20260909/retained-direct-publication.cover`.

Controller `.2`, SHA-256
`283128f9443c5ab20e0c397f9ccd7e5a4792b323ad13ae39eaee9393d13e98a5`,
was deployed with predecessor `controller-before-retained-direct`. One verifier
then failed before API calls because its repository-local SSH socket path was too
long (`volume-lifecycle-20260909T162913Z.kOY5dq`); the required invocation uses
`TMPDIR=$PWD/.tmp` rather than a deeper path.

The next attempt created Task `task_01M23G2WJP1SB14S0P2GAS84Z9` and Volume
`vol_01M23G2WJP1SB14S0P2GAS84Z9`, immutable key
`qa-floor-volume-20260909`, renamed slug
`qa-floor-volume-renamed-20260909`. Rename Task
`task_01M23G2Y5YEYRBJ1PZVZCZJVCM` was quarantined because the channel operation
matcher omitted direct Volume/Entry `TaskUpdate`; it timed out before dispatch,
and Abort correctly returned `task.not_abortable`. The corrected matcher passed
the 1.214s/19.1% selection in
`.tmp/qa-floor-20260909/resource-operation.cover`.

Controller `7774ff184`, version `.3`, SHA-256
`3b503a1445309bcaba8058366fa520f7a8ac2d64fa2e22b7db66d9e68853c2e8`,
then completed Retry `task_01M23GEEEJZJA7H2H74G1BAQZM` and removal
`task_01M23GFEWGA8XFF48EP0CG6HRY`. A clean verifier at
`.tmp/qa-floor-20260909/evidence/volume-lifecycle-20260909T164052Z.if5gUc/`
used Volume `vol_01M23GPD8CAJVTN4SRBYWRFKVM` and Tasks:

- create `task_01M23GPD8CAJVTN4SRBYWRFKVM`;
- rename `task_01M23GPEV4JGGQX7RYZRSB3YFM`; and
- removal `task_01M23GPHFA609YG190B23VMSW8`.

API/CLI parity, identity/key/path stability, replay, impact confirmation and
Docker/directory absence passed. The proof covered an unconsumed disposable
Volume, not mounted-consumer detachment, crash injection, application data or
full-floor acceptance.

## Entry retained-runtime lifecycle

Entry create, edit and bulk upsert share one reconstructible procedure. Only
exposed workload instances are selected with `--no-deps`; stable proxies are
not Entry consumers. Delete keeps the Entry visible until a 120-second Agent
cleanup or 30-second never-applied Controller finalizer succeeds. Failed cleanup
retains desired/applied state and a pinned Retry. Candidate retention after root
response expiry and exclusion of a queued materialization writer were both
added after integration regressions.

Local evidence is in:

- `.tmp/entry-removal-agent.cover`;
- `.tmp/entry-removal-retention.cover`;
- `.tmp/entry-removal-controller-writer.cover`; and
- `.tmp/entry-retained-integration.cover` for the displayed Entry/desired
  mutation test selection.

`make proto` reproduced generated Agent files, tagged Controller/Agent builds
passed, and the architecture gate remained red. Initial evidence used fake
filesystem effects and did not qualify public HTTP/CLI or Docker.

Live QA then exposed that ordinary Rollback served green while the desired
baseline still selected blue. [Entry serving-runtime capture](../features/storage-and-entries.md#entry-changes-capture-serving-runtime) captures current immutable runtime and
replaces only Entry decorations. A second failure showed retained stable proxy
config colliding with the current captured config; only the selected proxy's
verified exclusively owned old config is replaced. Evidence is
`.tmp/qa-floor-20260909/entry-runtime-final.cover` and
`entry-proxy-config-final.cover`.

The earlier failed serving probe was Entry `ev_01M23GRBEHF1TK3A0N36Z1DEC8`,
with Task `task_01M23GRBEHF1TK3A0N36Z1DEC8`. Its generated blue slot contained
`GROUNDPLANE_FLOOR_PROBE=one`, but proxy `4741053372db` still served green
Release `dep_01M20YD5V7MC42EB1349DAZHY6`, so Task completion was explicitly not
Entry acceptance. Evidence remains in `.tmp/qa-floor-20260909/entry-*.json`.
Fresh Script `scr_01M23H18KYS4RGSRQN2QY10XG4` then produced the fixed-revision
source failure described below; it was not conflated with the legacy migration.

Controller `.6`, signed `fd2d41468`, SHA-256
`968e27b4f52d1f8c2b6f1529e7b1ea5cefc8a33577870f221c70033829fe8f1f`,
produced these live results while proxy `4741053372db` stayed unchanged:

| Action | Task | Recorded result |
| --- | --- | --- |
| Edit probe to `two` | `task_01M23SBTVK7NQZKD3A0M623772` | Completed 19:12:29.876945381 UTC; serving green contained the value |
| Bulk upsert | `task_01M23SE9DTZXHM8H5Y8TPYWFGS` | Completed 19:13:49.623766947 UTC; original id retained and both values served |
| File create | `task_01M23SG3RW7TJ97RGV8C5HD068` | Completed 19:14:49.746623455 UTC; content, uid/gid `33:33` and mode `0444` verified |
| File remove | `task_01M23SJ003KA7BE4EHE15VSDPS` | Completed 19:15:46.882734412 UTC |

Environment-variable cleanup initially rejected because generated Caddy/Tunnel
Services are not authored `DesiredServices`. The correction accepts their exact
Component runtime ownership without inventing authoring declarations. Evidence
is `.tmp/qa-floor-20260909/entry-removal-identity-fixture-final.cover`.
Controller `.7`, SHA-256
`c057385379c28817635e8c12a3a29eeccb70cb4358833edeac0b11f6382a2d99`,
completed cleanup Tasks `task_01M23TFX7DT142RNBWJ6B9CQNG` and
`task_01M23TGN8HVJVMVF0T9HCKY78K`; the 14 original Entries remained, the
probe file was absent and application container identities stayed unchanged.
Already loaded process environment remains until later deploy/reconcile by
contract.

## Manual Script lifecycle and one-time migration

The first manual publication repair removed a duplicate Blueprint-head compare.
Disposable Script `scr_01M2104ECJZ050QH7RMDSCFSWG`, slug
`qa-manual-cas-proof`, kept its id through generations 1–3:

| Body/action | Task | Result |
| --- | --- | --- |
| `test -r /var/www/html/artisan` | `task_01M2104XFFZEZH8EEQ6BEAQYY6` | completed 17:13:15 UTC |
| `exit 7` | `task_01M2106S9M61XWETZ4SRS8NPXP` | failed 17:14:16 UTC |
| `sleep 60`, observe running, Abort | `task_01M210864YX5YA5GJ254TNVT4X` | aborted 17:15:26 UTC |

Deletion then failed with `resource.in_use: active Script executions fence
deletion`. With explicit owner approval, a one-time repository-local tool
migrated only those three terminal records at fixed read revision 3010. Each
transaction compared 12 authorities and made four mutations; the shared count
went 3 to 2 to 1 to 0 at revisions 3011, 3012 and 3013. Replay made zero
mutations. Tool SHA-256 was
`d67f95196ce85e8b981b90bfa19540f69d779940c700dff8fea706cc73089b9c`.
Private before-images, receipts and revision-fenced inverses are under
`/var/lib/groundplane/.tmp/qa-manual-migration-20260909` with parent 0700 and
files 0600. They must not be published, and the inverse must not be replayed
after normal deletion changed its authority. Removal Task
`task_01M23D4XMY7BGB5XJ9JNHBEXFS` completed
2026-09-09T15:38:55.019775476Z. No automatic compatibility path was added.

A later fresh Script initially failed with
`state.conflict: Script execution source is missing at its fixed revision`
because valid authored Secret keys such as `QA_APP_KEY` were treated as stable
ids. The correction resolved keys at the fixed revision. Controller `.4`, signed
`dcffe93f0`, then proved:

| Case | Task | Terminal result |
| --- | --- | --- |
| `exit 0` | `task_01M23J0DMFYMHZ9VNX064ZWGS4` | completed 17:03:51.341595397 UTC |
| `exit 7` | `task_01M23J1RQ547W1K3040GB74433` | failed as intended 17:04:35.327271527 UTC |
| `sleep 45`, then Abort | `task_01M23J2CD0XG99HC1AP3BERS5T` | aborted 17:05:27.323336515 UTC |
| normal removal | `task_01M23J490SZZG48CAJMA97Z1J5` | completed 17:05:56.778817398 UTC |

Evidence is under `.tmp/qa-floor-20260909/` in `script-*.json` and
`scripts-{before,after}.json`. This does not prove Retry, crash injection or
independent filesystem behavior.

## Integrated floor handoff

The floor runtime began from signed source `63c57c378`. Initial Controller,
Agent and CLI SHA-256 values were respectively
`b848898e74a2c4f850413dba4a72bb8591f304ffb4af76414123b0b761495005`,
`1dd16ed37096f17b69298e68aa2b0dc1b2f374adf2f631c1786351d84e2a1eaf`
and `c9dae5cd83bed5e8d275b2894ce0f758a77f6dcef875c5d6f43397d3a0061c40`.
The QA Agent registry digest was
`localhost:5000/groundplane-agent@sha256:9098cb81d0ffcc331816118d60c79e33b5cc6cd4b9c94095f9f5aa6507ee0378`;
normal update Task `task_01M23FC6TYJQN9R1G4HEQRRBB3` completed.

The final canonical bundle is retained at
`.tmp/qa-floor-20260909/blueprint-current/`. It is installation-bound, not a
portable production template. Complete Apply Task
`task_01M23TR871DAW07J056E0KE3MP` completed 19:37:10.430782224 UTC. An exact
reapply then failed with ownership collision at step
`step_01M23TVF1BY692VHT9M8ZZHJ4S` in Task
`task_01M23TVF1BW4YP95A7PA4T1E0N`; this failure was preserved and not bypassed.

After the scoped convergence correction, static Agent SHA-256
`71a5e605f0dbeb6bd172a71376a9cf34fb7e10ea077b2fccfc17d27c90d4c104`
and registry digest
`a94a116e8e6514c89cc23eac56dcebebfc1479ba8d63ce018dc997d8a8417189`
were selected through normal Agent update Task
`task_01M23W0K7H1V666JJZFFYQVNPV`, completed 19:58:42.265871801 UTC.
Exact reapply Task `task_01M23W2CKV8RNQPTRGGYEJ84WN` then completed all 22
steps at 19:59:54.27041242 UTC without changing native workloads, serving green,
retained blue, proxies or the two Reverb replicas.

At that dated handoff, 84 global Tasks were terminal; Controller, Agent and etcd
reported healthy; public `/up` and login returned 200 with TLS verification 0;
two internal paths returned 404; and the five-path ephemeral probe passed both
replicas, stable Reverb, private Caddy and the public Tunnel without creating an
application record. Node 24.19.0/npm 11.17.0 clean install, 23 Console tests,
production build, embedded release smoke and zero-diff protobuf/OpenAPI/client
generation passed. The dependency audit recorded zero vulnerabilities, 382
registry signatures and 93 provenance attestations.

The compatible dependency changes were Redocly 1.34.19 to 1.34.20, js-yaml
4.3.1 to 4.3.2 and Hono 4.13.3 to 4.13.7; no manifest range or forced override
changed. The js-yaml update addressed GHSA-2883-xcg3-v3hh, and the Hono update
included its boundary-string escaping fix.

The browser made 54 successful reads with no console warnings/errors. It still
showed unavailable per-Service observations and a generic ingress hint. Go 1.27
crashed pinned Staticcheck; Go 1.26.7 ran it and retained
`internal/agent/script_runtime.go:79` SA4006. No analyzer-clean or full-CI claim
was made.

This handoff closes the bounded Volume, Entry, manual-Script and exact-reapply
journeys above. It does not prove mounted Volume detachment, Script Retry/crash,
live Service observation, full public policy, Gate A, Gate B, backup/restore,
all-MVP acceptance or production readiness. Verify current host state before
using these dated identities.
