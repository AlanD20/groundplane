---
name: verify-groundplane
description: Verify an implemented Groundplane capability through a requested operator journey or its named local acceptance gate, preserving bounded evidence and pre-existing state.
---

# Verify Groundplane

Use this project-local skill after a capability is implemented and operator-level
verification is requested. Start with the smallest journey crossing Controller API
and CLI behavior. Feature-specific instructions live under `features/`.

Remote journeys use an existing deployment; named hermetic or real-etcd gates
use their own test prerequisites. This skill does not bootstrap machines,
rewrite systemd configuration or expose public listeners. A mutation journey
may create or remove only its explicitly authorized test resources. Repository
tests and `make ci` remain separate, scope-dependent gates in
[delivery.md](../../../docs/delivery.md#verification-ladder).

## Workflow

1. Read [the feature map](features/README.md) and select the narrowest relevant journey.
2. Check [head.md](../../../docs/head.md) for the current target, authority and
   pauses. A recorded command or mutation flag is not user permission.
3. Confirm the selected guide's prerequisites, resource ownership and required
   environment variables. Validate ignored repository-local runtime/evidence
   paths; the [known tooling gap](../../../docs/issues/documentation-tooling.md)
   still requires explicit paths or an executable fix before affected commands.
4. Run only the selected script/action. Stop on missing authority, a failed
   preflight or an assertion failure; retain evidence instead of broadening repair.
5. Preserve its evidence directory on success or execution failure. A preflight
   failure before evidence initialization must be reported directly.
6. Report the target, assertions, evidence path, cleanup, and anything not proven.
   Never describe an unexecuted journey as passing.

## Safety

- Never hardcode a host, SSH identity, API token, or project-specific name.
- Remote journeys require a trusted host-key file and strict host-key verification.
  The foundation `trust` action is explicit enrollment, not permission to replace
  an existing trust file or treat matching network scans as independent trust.
- Tunnel to the Controller's remote loopback listener; do not change its bind
  address for verification.
- Remote observation and Volume journeys require the Controller service to stay
  active. Foundation lifecycle probes are the only named service-mutation path;
  use one only when that action and target are authorized and no pause applies.
- The dedicated Python tunnel supervisor owns the SSH child; shell cleanup may
  only write its unique stop file and boundedly poll its supervisor job,
  waiting only after readable procfs proves the job exited or is a zombie.
- The supervisor writes the ready receipt only after the live child passes a
  bounded, config-free `ssh -S <socket> -O check -- <target>` control-master
  proof; startup exit, timeout, or proof failure is recorded as failure.
- Use an isolated `known_hosts` file inside the evidence directory.
- Create a unique evidence directory; never overwrite a previous proof run.
- Retain response headers, API JSON, CLI JSON, and service evidence.
- Fail closed when prerequisites or assertions are missing.
- Do not delete failed-run evidence.

## Current journeys

For C01-lite Host health, read `features/host-health.md` and run:

```sh
.agents/skills/verify-groundplane/scripts/host-health-ssh.sh
```
For hermetic C11 Attach L2 acceptance, read `features/c11-attach-l2.md` and run:

```sh
.agents/skills/verify-groundplane/scripts/c11-attach-l2.sh
```

For the managed Volume vertical, read `features/volume-lifecycle.md` and run
the named script only after a test Environment is already provisioned. This
journey mutates only the explicitly supplied Volume slugs and removes the
verified Volume through its normal fixed-revision confirmation flow; it never
creates or deletes Tenant, Project, or Environment state.
