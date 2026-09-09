# Integrated disposable floor QA

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
