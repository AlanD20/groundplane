---
name: verify-groundplane
description: Prove a Groundplane operator journey against an already provisioned remote host while preserving bounded evidence and pre-existing service state.
---

# Verify Groundplane

Use this project-local skill after a capability is implemented and operator-level
verification is requested. Start with the smallest journey crossing Controller API
and CLI behavior. Feature-specific instructions live under `features/`.

This skill verifies an existing deployment. It does not bootstrap a machine,
rewrite systemd configuration, expose a public listener, or provision product
state. Repository tests and `make ci` remain separate required gates.

## Workflow

1. Read `features/README.md` and select the narrowest relevant journey.
2. Confirm its prerequisites and required environment variables.
3. Run only the script named by the feature document.
4. Preserve its evidence directory on success or execution failure. A preflight
   failure before evidence initialization must be reported directly.
5. Report the target, assertions, evidence path, cleanup, and anything not proven.
   Never describe an unexecuted journey as passing.

## Safety

- Never hardcode a host, SSH identity, API token, or project-specific name.
- Require a pre-provisioned host-key file and use strict host-key verification.
- Tunnel to the Controller's remote loopback listener; do not change its bind
  address for verification.
- Require the Controller service to be active; never start, stop, or restart it.
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

## Current journey

The first path is C01-lite Host health. Read `features/host-health.md` and run:

```sh
.agents/skills/verify-groundplane/scripts/host-health-ssh.sh
```
