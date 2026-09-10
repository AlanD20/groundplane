# C01-lite Host health over SSH

This journey reaches a Controller already installed as a systemd service on a
remote Ubuntu host. It forwards the remote loopback HTTP listener through SSH,
then compares the REST API and CLI projections.

## Prerequisites

- The Controller systemd unit is installed and already active on the target.
- The target's runtime prerequisites are already configured.
- Linux procfs is mounted and readable for supervisor ownership polling.
- The Linux runtime permits `prctl(PR_SET_PDEATHSIG, SIGTERM)` for the
  supervisor-owned SSH child.
- Local commands: `ssh`, `curl`, `jq`, `cmp`, `go`, `mktemp`, `python3`,
  `sha256sum`, `timeout`, `date`, `mkdir`, `chmod`, `head`, `dirname`, `rm`,
  and `sleep`.
- `GROUNDPLANE_SSH_TARGET`: SSH destination such as `root@example.test`.
- `GROUNDPLANE_SSH_KEY`: path to the SSH private key.
- `GROUNDPLANE_SSH_KNOWN_HOSTS`: readable regular non-symlink file containing
  the target's trusted host key.

Optional variables:

| Variable | Default |
| --- | --- |
| `GROUNDPLANE_SSH_PORT` | `22` |
| `GROUNDPLANE_REMOTE_HTTP_ADDR` | `127.0.0.1:8080` |
| `GROUNDPLANE_LOCAL_HTTP_PORT` | dynamically allocated loopback port |
| `GROUNDPLANE_EVIDENCE_DIR` | parent for a unique run directory; defaults to `.tmp/verify-groundplane/` |

## Journey

```sh
.agents/skills/verify-groundplane/scripts/host-health-ssh.sh
```

The script requires the remote service to remain active, builds a temporary
`groundplane` CLI with an isolated Go cache, starts a dedicated Python
supervisor that owns a loopback-only SSH tunnel, and calls:

```text
GET /api/v1/host
groundplane --host <tunnel-url> --output json host show
```

## Assertions

Each API and CLI response must independently match the canonical Host shape:
the exact top-level and nested required keys, their JSON types, bounded integer
percentages and CPU load, the accepted etcd and Agent status vocabularies,
`etcd.node == "single-node"`, and the healthy native Controller identity. Agent
labels must be a sorted array of strings, including an empty array when no labels
are configured.
Controller update metadata follows `docs/api-cli.md`: actual binary digest,
availability/error and explicitly nullable candidate/history. Validate a present
candidate's pinned identities and compatibility integers, and retained history's
Task identity/status. Missing fields and empty synthetic objects fail the check.
This validates observation, not activation or recovery success.

The API may include a string-valued top-level `$schema` hint; the CLI must omit
it. After removing exactly that API field, the responses must have the same
recursive object path/type map. Cross-request value equality is limited to the
stable Host identity (`hostname`, `arch`, and `os`), CPU identity (`model` and
`cores`), and Controller `status`, `service`, and `version`. Live uptime, CPU
load, resources, Docker version, etcd health/size, and Agent health are validated
in each response but may legitimately change between the two requests.

## Evidence and cleanup

The evidence directory retains the exact API HTTP status and response headers,
API JSON, CLI JSON, schema-normalized JSON, recursive path/type maps,
stable-value projections, remote service status, supervisor ready/stopped/
failure receipts, supervisor wait status, and a result summary. Receipts and
all other evidence remain in the evidence directory; only the control socket,
stop file, and temporary CLI build live in the disposable runtime directory.
Each evidence file is bounded to 2 MiB, and the API body has a 1 MiB bound. The
shell writes only the unique supervisor stop file and polls its background job
for at most 630 seconds, calling `wait` only after readable procfs proves the
job is absent or a zombie. Existing but unreadable or ambiguous proc entries
fail closed without calling `wait`.
The supervisor owns the SSH `Popen`, enforces a 600-second absolute deadline,
uses Linux parent-death protection, and polls child liveness around a bounded,
config-free `ssh -S <socket> -O check -- <target>` control-master proof. It
writes `tunnel.ready` only after that proof succeeds, then terminates and waits
or kills that exact child. Startup exit, proof timeout, or proof failure writes
a failure receipt and tears down the child. If supervisor startup or shutdown
is ambiguous, cleanup fails closed and retains the runtime directory and
control socket as ownership evidence. The configured known-host file is opened
once, with `O_NOFOLLOW | O_NONBLOCK`, checked with `fstat`, copied atomically to
the evidence directory with a 2 MiB cap plus one byte, and only that copied
file is used for host-key verification. Evidence is retained on success and
execution failure.

This journey does not prove machine bootstrap, public Cloudflare exposure,
mutation capabilities, Console behavior, or full repository CI.
