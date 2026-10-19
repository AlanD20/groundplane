# C11 Attach local acceptance

This hermetic gate exercises current Attach publication, encrypted facts, captured
adapter procedures, Custom-hook cancellation and metadata-only Detach through the
native Task owners. It does not execute a complete Agent-backed Attach or Detach.

## Prerequisites

Use the Go version declared by the repository and the standard repository
environment. Required shell tools are checked by the script. No installed
Controller, database, Docker Engine or remote host is needed.

Optional evidence/runtime roots follow the
[shared path rules](../../../../docs/agents.md#supported-tooling-invocation).
The script preserves a unique evidence directory and removes only its own
temporary directory. It reuses the validated repository Go cache.

## Run

```sh
.agents/skills/verify-groundplane/scripts/c11-attach-l2.sh
```

The script rejects empty test selections, runs the selected current packages
under the race detector, and regenerates OpenAPI into evidence for byte parity.
The script owns exact package and test selections; do not copy those lists here.

## What it checks

- Native Custom and no-auth Valkey Attach publication, protected replay, captured
  resource identity and Controller settlement before first Deploy.
- Queued Custom Detach expiry, same-work Retry, shared Retry error propagation,
  atomic binding/index removal and rejection of a concurrent deployment.
- Captured password/no-auth identity, encrypted fact grants, credential procedure
  ordering and targeted PostgreSQL database context.
- Custom-hook cancellation joins execution before cleanup and keeps untrusted
  runner errors out of public diagnostics.
- Durable metadata ownership, scoped fact reveal, HTTP request behavior and CLI
  command mapping.

## Evidence and limits

Retained evidence includes test enumeration, race output, generated OpenAPI,
parity and a final receipt. These are separate focused checks, not one live
end-to-end workflow. Resource storage is MVCC memory and external effects use
captured procedures or hermetic runners.

Real database/credential effects, complete Agent-backed Attach/Detach, interrupted
command recovery, public Retry/Abort, remote Controller and rendered Console
qualification remain separate requirements in the [QA matrix](../../../../docs/qa-matrix.md).
