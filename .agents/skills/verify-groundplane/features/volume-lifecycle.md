# C08 Managed Volume lifecycle over SSH

This journey verifies one controller-managed Docker Volume on an already
installed Controller/Agent/Docker host. It uses the remote Controller's
loopback listener through the same dedicated SSH tunnel supervisor as the
host-health journey. The API creates the Volume, the CLI lists, shows, edits,
and removes it, and the API deletion-impact operation supplies the fixed
revision and confirmation token used by the CLI removal.

## Prerequisites

- The `groundplane-controller.service` systemd unit is already active.
- The Controller's Agent and Docker runtime are already installed and ready.
- The supplied Environment is already provisioned and in `ready` state.
- The supplied Tenant, Project, and Environment are stable IDs belonging to
  the same hierarchy. The verifier never creates or deletes those resources.
- The Linux runtime permits the existing supervisor's parent-death protection
  and readable procfs ownership polling.
- Local commands: `ssh`, `curl`, `jq`, `cmp`, `go`, `mktemp`, `python3`,
  `sha256sum`, `timeout`, `date`, `mkdir`, `chmod`, `head`, `dirname`, `rm`,
  `sleep`, `cp`, `cat`, `tr`, and `wc`.
- `GROUNDPLANE_SSH_TARGET`: SSH destination such as `root@example.test`.
- `GROUNDPLANE_SSH_KEY`: path to the SSH private key.
- `GROUNDPLANE_SSH_KNOWN_HOSTS`: readable regular non-symlink file containing
  the target's trusted host key.
- `GROUNDPLANE_TENANT_ID`: pre-provisioned Tenant ID.
- `GROUNDPLANE_PROJECT_ID`: pre-provisioned tenant-owned Project ID.
- `GROUNDPLANE_ENVIRONMENT_ID`: pre-provisioned Environment ID in that Project.
- `GROUNDPLANE_VOLUME_SLUG`: unused, generic test slug for the created Volume.
- `GROUNDPLANE_VOLUME_RENAMED_SLUG`: unused replacement slug, different from
  `GROUNDPLANE_VOLUME_SLUG`.

The verifier initializes the [repository environment](../../../../docs/agents.md#supported-tooling-invocation).
Optional runtime and evidence overrides must use validated ignored repo-local paths.

Optional variables:

| Variable | Default |
| --- | --- |
| `GROUNDPLANE_VOLUME_COMPOSE_KEY` | `GROUNDPLANE_VOLUME_SLUG` |
| `GROUNDPLANE_SSH_PORT` | `22` |
| `GROUNDPLANE_REMOTE_HTTP_ADDR` | `127.0.0.1:8080` |
| `GROUNDPLANE_LOCAL_HTTP_PORT` | dynamically allocated loopback port |
| `GROUNDPLANE_EVIDENCE_DIR` | `.tmp/verify-groundplane/` |
| `GROUNDPLANE_CLI_PATH` | empty; build the CLI from the current workspace |

The two supplied slugs and the Compose key must not already exist in the
Environment. The script fails closed rather than claiming ownership of an
existing resource. It may leave the newly created Volume in place if a later
assertion fails; `result.txt` records its ID so an operator can inspect and
clean that resource through the normal API flow without losing evidence.
When `GROUNDPLANE_CLI_PATH` is set, it must name a non-symlink executable
regular file. The verifier copies it into its private runtime directory and
records its checksum instead of compiling another copy.

## Journey

```sh
.agents/skills/verify-groundplane/scripts/volume-lifecycle-ssh.sh
```

The script performs these bounded checks:

1. It validates the Tenant/Project/Environment hierarchy and confirms the
   requested slugs/key are unused.
2. It calls `POST /volumes` with a stable idempotency key, proves that an exact
   duplicate returns `idempotency.in_progress` while the create Task is active,
   then repeats the request after completion and compares canonical JSON
   responses to prove terminal safe replay.
3. It runs `volume list` and `volume show` through the CLI, then edits the
   slug through `volume edit`. API and CLI reads must show the same stable ID,
   immutable Compose key, and derived path before and after the rename.
4. It follows every `GET /volumes/{id}/deletion-impact` page at limit 40,
   verifies the fixed identity/revision/digest join and final token, then
   invokes CLI `volume remove --impact-token ... --confirm-key ...`.
5. It polls each returned Task through `GET /tasks/{id}` and also proves the
   completed projection with `task show`. After removal, the Volume, Docker
   `gp_vol_<lowercase-volume-id>`, and managed directory must be absent.
6. It records the Controller service status, tunnel receipts, API/CLI JSON,
   remote Docker/stat evidence, and bounded request/response evidence.

All API and CLI operations use the same tunnel URL and no human-API auth
token, as required by the MVP. The verifier never starts, stops, restarts, or
reconfigures a service and never reaches a public or Cloudflare address.

## Assertions and limits

The Volume response must contain the supplied Environment ID, the expected
slug/key, a stable `vol_` ID, and a validated absolute path ending in exactly
`/<key>` with no traversal. The rename must change only the slug. The managed
path must be a `0:0:700:directory` result from remote `stat`; the Docker observer
must return exactly `gp_vol_<lowercase-id>`. The completed Task must be
terminal `completed`, target the Volume, and be observable through both API
and CLI.

Each response body is capped at 1 MiB and each evidence output at 2 MiB. The
impact page must be at most 768 KiB, use no more than 40 items per request,
and terminate with a non-empty 64-character lowercase hexadecimal token.
The impact sequence is bounded to 1024 pages. Task polling is bounded to 120
one-second iterations. Tunnel ownership and shutdown use the existing
supervisor/procfs safety contract.

## Evidence, cleanup, and unproven behavior

Evidence is written below a unique run directory and is retained on success
or failure. The disposable runtime directory contains only the temporary CLI
build, Go caches, SSH control socket, and stop file. If a mutation assertion
fails after creation, the Volume is intentionally
not guessed-at or force-deleted; `result.txt` records the created ID and
`manual_cleanup=required`.

The duplicate idempotency request is the only restart-safe replay observable
in this journey. It does not restart the Controller, Agent, Docker daemon, or
any workload. Crash injection, process interruption between Volume helper
checkpoints, Controller restart recovery, Agent reconnect recovery, complete
normalized-projection overflow rejection (as distinct from the raw 2 MiB
transport rejection), Console rendering, public Cloudflare exposure, and full
repository CI remain unproven.
