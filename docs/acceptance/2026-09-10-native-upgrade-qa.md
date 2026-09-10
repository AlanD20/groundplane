# Native Controller upgrade QA

Target: disposable QA10.25.0.2 only. Production, provider ingress and host
firewall remain unchanged. This report separates the explicit legacy bootstrap
from subsequent normal native update qualification.

## Guarded baseline A — 03:51 UTC

Source7022a2c6e was deployed with `scripts/deploy.py --bootstrap`, version
`0.0.0-qa.native20260910.1`. This was the explicit one-time legacy maintenance
transition, not a native update Task. The packaged Agent image was built from
the checked-in Dockerfile, not the earlier QA runtime-layer shortcut.

- Controller and recovery executable SHA256:
  `4d6fed201291a03fe9b3524378d01bcec2de36f3e6a1df65ae8a10389bbf0d86`.
- Staged release:
  `sha256:59be873f8c10fb4429d911a3e799a8cf8df9131cf6c6581e189ad1c67ef34882`.
- Agent digest:
  `localhost:5000/groundplane-agent@sha256:7268003b038e7bf1fd45f303dafdbe551287c4a3c98b01b150e6a5483e3d4142`.
- Agent container `f227c3e21119`; normal Agent update
  `task_01M24Q1W3MRAQAN9DE2A2MPBW7` completed03:51:15.804UTC.
- Native root and releases directory are root-owned0700; recovery executable
  is root-owned0755. Effective `ExecStartPre` contains the fixed predecessor
  guard and completed successfully before Controller startup.
- Host reports Controller, Agent and etcd healthy and native update available.
  Candidate metadata matches the installed Controller; native history is null.
- Application container identities, serving/retained slots, proxies, Reverb
  replicas, Caddy and Tunnel are unchanged. Bootstrap replaced only Agent and
  Controller-owned bootstrap state. No application deployment was requested.

The updated project Host verifier passed API/CLI canonical-shape and stable-value
parity through its owned SSH tunnel, with service mutation `none` and retained
cleanup receipts. Its failing-first jq contract tests now enforce native update
metadata and null absence; the skill validator and shell syntax check pass.
Evidence: `.tmp/production-mvp-20260910/evidence/host-health-20260910T035725Z.YSd9M7`.

### Bootstrap traffic limitations

The first five-path WebSocket probe received30markers before public Tunnel
connection loss. Cloudflared logged QUIC connection failures03:51:01UTC and
reconnected; Controller stop began03:51:10UTC. This is a failed public continuity
sample preceding binary replacement, not proof of uninterrupted bootstrap or
proof that Controller restart caused it. No Tunnel container restart occurred.

The first local Python HTTP monitor received403responses while equivalent curl
requests returned200 with TLS verification0. It was stopped and preserved as an
invalid health instrument. A fresh curl-based HTTP monitor and five-path held
WebSocket probe were started after bootstrap; their native-update results belong
below, not to this earlier failed sample. No application records are created by
the ephemeral-channel probes, and no credentials are printed.

Raw evidence under `.tmp/production-mvp-20260910/`: `qa-native-preflight.txt`,
`qa-native-bootstrap-deploy.log`, `qa-native-bootstrap-proof.txt`,
`bootstrap-startup-journal.txt`, `bootstrap-public-failure-diagnostic.txt`,
`bootstrap-websocket-continuity.log`, `bootstrap-http-continuity.jsonl`.

## Bootstrap permission correction and A→B

The first normal deployment staged B but returned422before creating a Task.
Its immutable-file check exposed a bootstrap defect: installed Controller/guard
were0755rather than0500. A failing shell-install regression reproduced the exact
mode mismatch. Bootstrap now installs0500; its guard check rejects writable
executables, and Host availability also checks the installed predecessor identity.
Focused deployment and snapshot race tests pass. QA repair changed only those
two hash-verified file modes, without replacing bytes or restarting Controller.

The first distribution attempt also reproduced the public-path loss while A
remained running: the native request was rejected before activation, and
Cloudflared reported QUIC timeouts04:02:22UTC. HTTP samples included502s. This
remains a distribution qualification failure, separate from native activation.
Raw evidence: `qa-native-update-b-deploy.log`, `qa-native-422-paths.txt`,
`qa-image-transfer-network-events.txt` and `native-http-curl-continuity.jsonl`.

The already staged B was then accepted with the same protected key, without
retransferring images. Native Task `task_01M24R3HJHA1NAFVQXEPNDH9TG`, created
04:09:38.640UTC, completed with Host phase `healthy`. Controller now runs
`0.0.0-qa.native20260910.2`, SHA256
`6106db1660ba3c8a57ab850541850dc931aa97c720be34638753a54343b8552e`.
Its qualified release is
`sha256:aab1bc55201f56bd8d61394f561be16c4699f401dce3bc4555d72d278ff668c3`,
Agent image
`localhost:5000/groundplane-agent@sha256:22d02c3cd0129c02269e1119d916ec23fb3a8b0007c1a2ba4d34fac1e9b9f251`.
No transient recovery unit remained after completion. Application container
identities remained unchanged. Five held WebSocket paths and verified-TLS HTTP
sampling remained successful across this isolated native activation window;
the full600-iteration run completed with3000WebSocket deliveries and1200HTTP
requests, with zero failures.
Evidence: `native-activation-b-retry.log`, `native-b-completed-proof.txt`,
`qa-native-b-current.txt`, `activation-websocket-continuity.log` and
`activation-http-continuity.jsonl`, under the same evidence parent.

## Busy drain, cancellation and predecessor recovery

With B installed, native Task `task_01M24RE5RYG03GM1AD3GQSMJPV` exhausted
its120-second preparation drain and failed without aborting the active240-second
Script. A follow-up Script completed, proving dispatch resumed. Native Task
`task_01M24RMM78T3KG1CVS5H14RBA0` was then aborted through the CLI during
preparation; `task_01M24RMMCTXEF6FCG2WVFC6WQW` completed afterward. All three
disposable probe Scripts were removed through normal operations after their
Tasks settled. Controller B, Agent B and application identities were unchanged.

A privately staged non-executable candidate failed automatically, without SSH
restoration. Original Task `task_01M24RSDMV50HHENNR555A1G1W` remained failed
and Host phase became `recovered`, with Controller B and its Agent healthy.
An unready candidate was then interrupted only after binding a pidfd to the
exact journal-selected trial process and verifying its executable and argv.
Task `task_01M24RYAZFZ1GSKNTWHB3Z86A4` likewise retained its identity and
failed with automatic predecessor recovery. No manual binary or journal rollback
was performed. The valid B candidate was restaged afterward; private immutable
failed releases remain available as evidence.

Raw evidence: `native-busy-qa.log`, `native-abort-qa.log`, `bad-exec-update.log`,
`bad-exec-recovery-observation.txt`, `interrupted-native-qa.log`,
`qa-good-candidate-restaged.txt`, and the separate `recovery-*-continuity` logs.
Recovery probes completed600iterations:3000held WebSocket deliveries and1200
verified-TLS HTTP requests, with zero failures.

## Distribution correction under qualification

Two full unpaced OCI archive transfers correlated with public Tunnel timeouts
before any native activation. Internal paths and application container identities
remained stable. Shared-path congestion/resource burst is a hypothesis, not a
proven root cause. Distribution now sends64KiB chunks at a fixed maximum4MiB/s,
without catch-up bursts, and reaps only its owned save/load processes on failure.
Focused failing-first tests prove byte identity, pacing and child cleanup; all44
deployment tests pass. The
normal full-distribution journey must still qualify this correction under fresh
held public WebSocket and verified-TLS HTTP probes.

## Release C and qualification isolation

Source `c2eea5b2d` built version `0.0.0-qa.native20260910.3`. The paced archive
transferred1,294,483,968bytes. Native Task `task_01M24SR4NW9SYE0ZP84G4SAKJG`,
created04:38:22.140UTC, completed normally. Controller SHA256 is
`53248ff3f5390e119b9625190cfee3385370917e889d83fc81fe5be29773f6ea`; release is
`sha256:e35bb89e867fe96a7066d46013fcb27e07e955a8cc7fe3b6e6fc8fe2f1ef42db`.
Agent image is
`localhost:5000/groundplane-agent@sha256:233aedb40be555ef25bdc49ef274c745cc1ce86ded60169558a2647328c03ca2`.

This whole-build sample still failed before activation. All four Tunnel QUIC
connections timed out04:33:32–33UTC; the held public WebSocket dropped and one
HTTP request returned502. Pacing did not eliminate the failure. The captured QA
host sample showed no OOM/kernel event or sustained CPU/I/O pressure; HTTP
recovered while transfer continued. Local build-network disconnect04:32:20UTC
preceded the failure, but a separate no-op container did not reproduce it.
An isolated uncached build also passed its300-iteration held probe, with1500
deliveries. Neither build-only nor cached transfer reproduced the composed
failure. No host/provider network configuration was changed.

A fresh late probe passed all1500deliveries through five paths over300iterations,
including C activation. Repeating complete stage-only distribution with cached
Agent/Runner images then completed under600probe iterations:3000held WebSocket
deliveries and1200verified-TLS HTTP requests, with zero failures and no Tunnel
log events. This proves that named cached-distribution window, not whole-build
continuity or a root cause for the failed samples.

The transport now verifies local/target Docker content ids and reuses matching
images. Missing
images retain paced transport; runtime identity still comes from the registry.
Five failing-first reuse tests and all49deployment tests passed. The next trial
showed the target no longer retained that newer Runner image, so reuse alone did
not omit it. Native preparation now omits Runner build/transfer entirely, matching
its unchanged-runtime contract. Bootstrap still includes Runner and the remote
complete-layout preflight remains authoritative. New failing-first regressions
prove artifact selection and bounded CLI error handling; all53deployment tests
pass. Fresh uncached-build qualification of this narrower path remains pending.

Evidence: `qa-native-update-c-deploy.log`, `native-c-current.txt`,
`paced-websocket-continuity.log`, `paced-http-continuity.jsonl`,
`paced-late-websocket-continuity.log`, `paced-transfer-host-diagnostic.txt`,
`paced-transfer-tunnel-diagnostic.txt`, `qa-cached-stage-c.log`,
`cached-transfer-tunnel-log.txt`, `cached-*-continuity` logs, `qa-build-only.log`,
`build-only-websocket-continuity.log` and `build-only-vmstat.txt`.

## Coordinated Agent rollback and compatibility refusal

A valid B Controller paired with a deliberately unready digest-pinned QA Agent
exercised both recovery halves. Task `task_01M24SXMMDX2TBFJYVHTP2AH57`, created
04:41:22.317UTC, failed04:43:29.433UTC after the120-second authenticated-Ready
deadline. Groundplane automatically restored Controller C and Agent C, retained
the same failed Task and reported `recovered`. Stable Agent id and enrollment
time were preserved; replacement container `46dd5b2ba3de` runs the exact C digest.
No SSH rollback, application redeploy or Script replay was used. Held five-path
WebSockets and verified-TLS HTTP remained successful throughout recovery.

Storage-epoch and channel-schema mismatch fixtures each returned422
`validation.failed` before Task publication, with unchanged Controller digest
and native Task history. The exact good C candidate was restaged afterward.
The first HTTP instrument used an incorrect unversioned API path and returned400;
that attempt is retained separately and is not compatibility evidence.

The standalone Agent busy-path attempt returned `state.conflict: Agent already
runs the selected image` before update publication. It did not exercise busy
drain. A pre-native image-mismatch fixture remains required for that exact live
path; focused race tests and native busy/Abort proof remain valid. The disposable
Script completed and its normal removal Task completed.

Evidence: `bad-agent-build.log`, `bad-agent-publish.log`, `bad-agent-release.txt`,
`agent-rollback-native-qa.log`, `agent-after-rollback.txt`,
`incompatible-native-qa-02.log`, `agent-busy-rejection.txt`,
`agent-busy-probe-removed.txt`. Private failed releases remain retained as QA
evidence, not selected production inputs.

## Release D — cached Docker build

Normal C→D Task `task_01M24V9SG35EDX2S4BQJKNQRV8`, created05:05:29.091UTC,
completed with healthy Controller/Agent and unchanged application identities.
Controller version is `0.0.0-qa.native20260910.4`, SHA256
`455592f11c09634b07bce2430f911ea7d610984c5a44966c489b6fae4e8d38bc`, release
`sha256:f34915444677db2d85046b86f6725ce7fa7d04f2242682db33c6eab004bf34fb`.
Agent `2f6af43b5a25` runs
`localhost:5000/groundplane-agent@sha256:1f9242006e8920260d3bfd0b2962c2b6fd3f83fa38b0d5da3461f8e60d64b0f9`.
Held traffic remains successful; the full probe continues. Docker reused the
isolated build's compile layer, so this is not an uncached-build proof. Exact
image inspection found no reusable target Runner content; this trial still sent
the full archive. The new bootstrap-only Runner correction follows this trial.
Evidence: `qa-native-update-d-deploy.log`, `native-d-current.txt`,
`reuse-websocket-continuity.log` and `reuse-http-continuity.jsonl`.

## Remaining proof

Whole-build public traffic continuity remains under diagnosis. A→B→C, cached
distribution, native busy preparation, Abort, failed/interrupted Controller
recovery, coordinated Agent rollback and compatibility refusal pass in their
named live windows. Watchdog/changed-boot recovery and standalone Agent busy
drain retain local automated proof only.
No whole upgrade-safety, full-CI or production-readiness claim is made.
