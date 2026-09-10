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

## Remaining proof

Normal A→B, failed candidate recovery, busy drain/pre-activation cancellation,
interruption, same-Task resume and HTTP/WebSocket continuity remain unproven.
No whole upgrade-safety, full-CI or production-readiness claim is made.
