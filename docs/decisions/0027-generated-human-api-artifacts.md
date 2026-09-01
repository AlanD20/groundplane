# ADR 0027: Generate the human API artifacts from Huma

- Status: Accepted
- Date: 2026-08-22

## Context

ADR 0002 chooses Huma v2, oapi-codegen v2, and openapi-typescript, but does not
fix their versions, generated paths, execution order, or the boundary between
generated operation code and Groundplane's transport policy. Encoding those
choices only in scripts would make the human API contract accidental.

The current migration is incomplete: `host.show` is Huma-typed while the
remaining routes still use the handwritten `net/http` table. A generator that
describes those handwritten routes separately would create a second contract;
a generated document containing only the typed operations is temporarily
partial and cannot be mistaken for S01 acceptance.

## Decision

### One document from one live API object

The canonical committed document is `/openapi.json` at the repository root.
It is OpenAPI 3.1 JSON serialized deterministically from the same
`huma.API.OpenAPI()` object used by the running Controller. `GET /openapi.json`
serializes that object directly; it never reads the committed file.

Huma's transport prefix remains `/api/v1`, represented by the document's one
relative Server URL. Operation paths therefore remain resource paths such as
`/host`, not duplicated `/api/v1/host` paths.

### Exact generators and outputs

The generation contract pins:

- `github.com/oapi-codegen/oapi-codegen/v2/cmd/oapi-codegen` at `v2.8.0` as a
  Go tool dependency;
- `github.com/oapi-codegen/runtime` at `v1.2.0` for generated Go client support;
  and
- `openapi-typescript` at `7.13.0` as an exact Console development dependency.

The Go client is committed at
`internal/cli/apiclient/generated/client.gen.go`. Its subpackage owns generated
models and operation methods. The parent `internal/cli/apiclient` package keeps
Groundplane's idempotency, timeout, streaming, and RFC 7807 policy and will
delegate to generated operations as each CLI vertical migrates.

The runtime-free TypeScript contract is committed at
`console/src/lib/api.generated.ts`. Console code imports types from this file;
runtime request policy remains in the Console store.

`make api` emits the document, then the Go client, then the TypeScript contract.
`make generate` emits protobuf and human-API artifacts. CI regenerates before
compilation and fails on any committed-artifact drift.

### Migration boundary

Only Huma-registered typed operations enter the document. There is no shadow
OpenAPI registry for handwritten handlers. Each later vertical migration must
register the typed Huma operation, remove its superseded `net/http` route, move
the CLI call to the generated operation, and type the corresponding Console
store call in the same change.

Until every operator-facing route is migrated and the independent operation
manifest matches the document, the generated document is explicitly partial,
S01 is not Accepted, and C22 remains incomplete even though its generated
TypeScript contract exists.

## Alternatives considered

### Describe handwritten routes in a parallel OpenAPI table

Rejected because paths, payloads, errors, and success statuses could drift
from the serving handlers without a compiler failure.

### Generate directly from the running HTTP endpoint

Rejected because generation would require a configured Controller, etcd,
Docker access, privileged runtime paths, and a free listener. Constructing the
Controller server object is deterministic and has no runtime dependencies.

### Generate the Go client into the handwritten transport package

Rejected because generated `Client` and constructor names collide with the
policy-bearing transport. A generated subpackage preserves one clear ownership
boundary and can replace handwritten per-operation path construction cleanly.

## Consequences

- generated artifacts are reviewable and reproducible from a clean checkout;
- serving and generation cannot disagree about registered typed operations;
- the partial migration is visible rather than hidden by duplicated metadata;
- adding a typed operation changes both generated clients in the same gate; and
- S01 completion still requires migrating every remaining human API operation
  and proving the full 1:1 manifest.
