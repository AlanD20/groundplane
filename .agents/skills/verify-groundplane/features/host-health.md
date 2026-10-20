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

Require healthy Controller, etcd and Agent observations, a working Docker
version, bounded CPU/resource percentages and the expected native Controller
service. Validate pinned update identities and native Task status evidence when
candidate/history observations are present; absence remains explicitly nullable.
A failed health assertion fails the journey, even when API and CLI agree.

Compare stable Host identity, CPU identity and Controller service/version between
API and CLI. Resource use and update observations can change between requests;
retain both responses instead of requiring identical live state or a frozen JSON
field inventory. The API's optional string-valued `$schema` hint is omitted by CLI.

This checks current observation and reachability, not software activation,
recovery, application traffic or Task execution effects.

## Evidence and cleanup

The evidence directory retains the exact API HTTP status and response headers,
API JSON, CLI JSON, normalized JSON, stable identity projections, remote service
status, supervisor ready/stopped/
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
