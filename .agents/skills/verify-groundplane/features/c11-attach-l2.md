# C11 per-service Attach L2 acceptance

This hermetic journey proves the implemented C11 Attach contract without
requiring an already provisioned PostgreSQL or Valkey host. It runs the
production durable repositories, Controller plan and HTTP handlers, CLI
command mapping, adapter compilers, and the Agent's PostgreSQL adapter runtime.
The external-service boundary is the Agent runner used by its focused test; it
returns bounded Docker and adapter results without contacting or mutating a
host.

## Prerequisites

- Go at the version declared by `go.mod`.
- Local commands: `go`, `cmp`, `date`, `mkdir`, and `mktemp`.
- A writable repository-local `.tmp/verify-groundplane` directory.

Optional variables:

| Variable | Default |
| --- | --- |
| `GROUNDPLANE_EVIDENCE_DIR` | `.tmp/verify-groundplane/` |
| `GROUNDPLANE_VERIFY_RUNTIME_DIR` | initialized `$TMPDIR` (repo-local `.tmp/tmp` by default) |

The [shared path rules](../../../../docs/agents.md#supported-tooling-invocation)
validate defaults and overrides before the verifier creates evidence or runtime state.

## Journey

```sh
.agents/skills/verify-groundplane/scripts/c11-attach-l2.sh
```

The runner first proves that every package-specific selection contains at
least one test. It then executes those exact C11-focused tests under the race
detector and regenerates OpenAPI to an evidence file before comparing it
byte-for-byte with the committed contract. It preserves all logs and the
generated contract in one unique evidence directory on success or failure.

## Assertions

- concurrent protected retries have one winner and one replay outcome;
- one Attach owns exactly one consumer Service, backing Project, backing
  Environment, backing Service, and durable backing Network;
- attach retry preserves identity, encrypted facts, and grant edges across
  repository reconstruction after an injected terminal failure;
- list responses expose fact keys and secret classification, never fact values;
- explicit fact reveal requires ready state and exact grant membership;
- PostgreSQL compiles its provision, grant, revoke, and detach procedure with
  stable database context, and the Agent runtime keeps its secret out of argv;
- Valkey provisioning keeps its ACL secret out of argv and places it on stdin;
- provision/grant precede targeted network reconciliation, while detach orders
  revoke, network removal, then deprovision;
- failed detach retains the Attach and exact Task plan, and successful retry
  atomically removes the Attach, facts, memberships, and outgoing grant edges;
- network-union construction mutates only the intended consumer membership;
- REST operations, CLI slug/default and `--id` paths, and the regenerated
  OpenAPI document retain the exercised Attach surface.

## Evidence and limits

The evidence directory retains each package's test enumeration, the exact
selection counts, race-test log, regenerated OpenAPI, byte-parity result, and
final summary. The verifier owns a unique non-symlink runtime directory for Go
caches and removes it on exit. This journey does not claim a live external
database, Docker Engine, browser or Console session, or remote Controller.
