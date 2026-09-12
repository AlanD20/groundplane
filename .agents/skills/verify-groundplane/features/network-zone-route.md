# C07 Network Zone and Route real-etcd gate

This journey exercises C07's durable concurrency and restart boundary against a
caller-provided real etcd instance. It uses a unique, caller-owned key prefix and
does not require or mutate a deployed Controller service.

## Prerequisites

- `GROUNDPLANE_ETCD_ENDPOINT`: a reachable dedicated test etcd endpoint.
- `GROUNDPLANE_C07_ETCD_PREFIX`: a unique prefix matching
  `/groundplane-c07-acceptance/run-*/`.
- Local commands: `go`, `mktemp`, `date`, `mkdir`, `head`, and `rm`.

The verifier initializes the [repository environment](../../../../docs/agents.md#supported-tooling-invocation).
Optional runtime and evidence overrides must use validated ignored repo-local paths.

## Journey

```sh
.agents/skills/verify-groundplane/scripts/network-zone-route-etcd.sh
```

The script uses unique Go cache and temporary directories, runs the build-tagged
real-etcd acceptance test, and retains bounded output and a result receipt.

## Assertions

- Two concurrent exact Zone creates with one idempotency tuple converge on one
  durable Zone without returning a conflict.
- Two concurrent overlapping subnet reservations produce exactly one winner
  and one `state.conflict` result.
- Two concurrent exact Route creates converge on one durable Route.
- Zone and Route lists contain no duplicate replay records.
- Closing and reopening the store preserves the exact Zone and Route records.
- Cleanup deletes only `/v1/` beneath the unique configured test prefix.

## Scope

This gate proves real-etcd transaction, idempotency, subnet isolation, and
restart durability. API/CLI/Console parity remains covered by generated-contract,
controller, CLI, and Console tests. It does not prove Agent host effects or live
Caddy/Tunnel control; those require the deployed Controller/Agent journey and
the accepted C12/C14 Component APIs.
