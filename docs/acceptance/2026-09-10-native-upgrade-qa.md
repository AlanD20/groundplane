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
Recovery traffic probes are still running; no full-run claim yet.

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

## Remaining proof

Safe image distribution and coordinated Agent-failure rollback remain unproven
live. A→B, busy preparation, Abort and failed/interrupted Controller recovery now
pass; that does not erase the earlier failed distribution samples. Compatibility
refusal and watchdog/changed-boot recovery retain local automated proof only.
No whole upgrade-safety, full-CI or production-readiness claim is made.
