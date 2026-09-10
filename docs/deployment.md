# Controller installation and native updates

This runbook implements ADR0074's machine-bootstrap exception. Normal native
activation remains the same Console/CLI/API Controller update Task. It does not
authorize a production target or changes to ingress, host firewall or other hosts.

## Prerequisites

Use the repository-pinned Go, Node and npm toolchains, local Docker and SSH,
an existing supported Ubuntu24.04 host of the same architecture, a private root
SSH identity and an independently verified, pre-populated known-hosts file.
The target needs Docker/Compose, the loopback registry, curl, Python3, flock and
systemd. Optional `--setup` provisions the documented host prerequisites; use
it only with explicit host provisioning authority.

All examples use operator-supplied paths and a target address. The Controller
remains private; `--ip` configures its LAN listener only at initial bootstrap.
The native update route does not change listeners or host networking.

## Initial installation or one-time legacy transition

```sh
python3 scripts/deploy.py --key /path/to/key --known-hosts /path/to/known_hosts \
  --ip HOST_IP --version RELEASE_VERSION
```

For an existing legacy Controller, add `--bootstrap` during a maintenance window.
Stop new operator/scheduled work and wait for existing Tasks to finish; the
installer checks all retained Task pages and refuses nonterminal work. An old
binary cannot provide an admission fence retroactively. No active Task is aborted
by this check. A host already using native recovery refuses `--bootstrap`.

The first installation backs up existing files, creates the private release root,
installs the Controller and predecessor recovery executable, then installs the
unit's `ExecStartPre` guard before enabling the service. Controller/recovery
executables are root-owned0500, matching native immutable input validation.
It installs the CLI,
bootstrap Agent/Runner image configuration and age identity only on this path.
Agent enrollment/update must settle before the first candidate is made selectable.
On failure, restoration requires a successful Controller stop. Unresolved Agent
work, native evidence or failed restoration retains the private recovery files;
follow the reported state before another deployment. Never remove recovery data
to bypass a failed preflight.

## Normal release deployment

Run the same command without `--bootstrap`. On a guarded host it:

1. Builds release assets and source-derived Controller compatibility metadata.
2. Publishes/resolves the immutable Agent OCI digest and verifies the transferred
   Controller bytes against the build descriptor.
3. Publishes an immutable manifest/binary under
   `/var/lib/groundplane/controller-updates/releases/<sha256>` and atomically
   selects it in `candidate.json`.
4. Persists a protected request receipt, submits `POST /controller/update` and
   follows that exact Task through the brief Controller disconnect.

Only a `completed` Task is success. A recovered failed update remains failed.
The Controller owns drain, activation, Agent replacement and recovery. Deployment
does not overwrite executables, YAML, keys or systemd units, abort work, or
redeploy application Services. CLI and Runner distribution/configuration are not
part of the native Controller/Agent release; they remain unchanged on this path.

To stage for later operator review, add `--stage-only`. It requires an already
guarded host and never submits an update. Review the candidate in the Console's
Controller page, or activate the printed digest explicitly:

```sh
bin/groundplane --host PRIVATE_CONTROLLER_URL controller update --release sha256:DIGEST
bin/groundplane --host PRIVATE_CONTROLLER_URL task show TASK_ID
```

Compatibility is enforced again by the running Controller before activation.
An incompatible or modified candidate cannot be activated. Staging never executes
candidate code and never updates the qualified Agent selection.

## Interrupted deployment client

The private `deployment.json` receipt retains one release, idempotency key and
accepted Task id. A lost POST response is resolved with the same key/body. Once
the Task id is known, polling never submits another update. The client waits up
to720seconds; unresolved outcome is exit2, failed/rejected update is exit1.

On failure, deployment prints the exact retained helper path. Resume that helper
on the target with `python3 PRINTED_HELPER_PATH --resume`; it uses fixed loopback
HTTP and the existing receipt, not a new key. Repeating deployment for the same
unresolved release also reuses its receipt. A different release is refused until
the earlier request settles. After terminal failure and completed native recovery,
a fresh explicit deployment is a new update attempt.

Do not manually replace the Controller, remove `journal.json`, or delete the
retained bundle to make an uncertain operation look complete. Host and Task
surfaces remain the operator evidence. The receipt contains no credentials.

## Local verification

`make deployment-check` exercises staging safety, partial bootstrap rejection,
empty-only rollback, idle preflight, durable acceptance replay, same-Task resume
and the installer native/legacy branch boundary. `make controller VERSION=...`
builds embedded Console assets, Controller and its matching build descriptor.
These checks do not substitute for live A→B, bad-candidate, drain, interruption
and workload-traffic qualification or the required complete `make ci` gate.
