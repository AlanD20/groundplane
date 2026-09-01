# ADR 0008: Use the official etcd v3 Go client

- Status: Accepted
- Date: 2026-08-20

## Context

Groundplane stores Controller state in etcd and exposes that storage through
the narrow `internal/infra/etcd.Store` interface. The implementation needs a
maintained v3 client without allowing etcd-specific request or response types
to leak into the domain, Controller, CLI, or public API packages.

The etcd project identifies `go.etcd.io/etcd/client/v3` as its official Go
client. Its documented lifecycle is explicit: construct a client with
`clientv3.New`, apply request deadlines through `context.Context`, and close
the client to release its connection goroutines.

Sources:

- <https://pkg.go.dev/go.etcd.io/etcd/client/v3>
- <https://github.com/etcd-io/etcd/blob/main/client/v3/README.md>

## Decision

Use `go.etcd.io/etcd/client/v3` at version `v3.6.13` behind
`internal/infra/etcd.Store`.

Only `internal/infra/etcd` may import the client module. The package owns
client construction, endpoint configuration, request deadlines, watch
translation, snapshot streaming, error translation, and shutdown. Callers
continue to depend only on the repository's `Store` interface and standard
library types.

Groundplane will not wrap the legacy `go.etcd.io/etcd/clientv3` import path,
shell out to `etcdctl` for ordinary persistence, or add a compatibility
adapter for another key-value store.

## Consequences

- The project uses the upstream-supported v3 API and its native gRPC
  transport.
- etcd upgrades remain isolated to one infrastructure package.
- Callers can use deterministic in-memory Store implementations in unit tests
  without importing etcd or gRPC types.
- The official client adds gRPC and protobuf transitive dependencies to the
  Controller build.
- Snapshot restore remains an explicit operational procedure; it is not
  silently performed by the runtime Store.
