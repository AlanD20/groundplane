# ADR 0071: One Volume-removal record contract for atomic publication

- Status: Accepted
- Date: 2026-09-09
- Capabilities: Managed Volume removal
- Refines: ADR 0056 persistence-format placement for the ADR 0049/0051 seam

## Context

ADR 0051's existing desired publisher must publish the initial Volume-removal
records in its own transaction. The Volume runtime repository already imports
the etcd primitives package containing that publisher. Importing the runtime
repository back into its parent would create a cycle and violate ADR 0056.
Duplicating runtime records or accepting arbitrary transaction callbacks would
create a second schema owner or bypass the closed capability boundary.

## Decision

`internal/infra/volumeremovalrecord` is the single concrete persistence-format
leaf for Volume-removal records. It owns their validated values, canonical
binary encoding, digests, record keys, and accepted size limits. It imports no
etcd client, repository, Task model, Agent protocol, or process capability.
This narrowly named format package is not a generic shared-record framework.

The desired publisher and `internal/infra/etcd/volumeremoval` may both consume
that format. The runtime repository still owns checkpoint transitions, storage
reads, assignment validation, and retry behavior. The desired publisher still
owns the one desired/operation/Task publication. The format leaf cannot perform
either operation and does not supply arbitrary store mutations or callbacks.

The replay locator is a plain value in the format contract. The runtime adapter
converts it explicitly to the existing idempotency API. Marker lookup, replay,
and retention authority remain in their existing owners. No Task or repository
aggregate crosses into the format leaf.

All record consumers move with the schema. No aliases, deprecated wrappers,
alternate decoder, or compatibility branch remain. The existing binary format
and physical keys are unchanged. This is a mechanical ownership move, not
publication wiring or permission to treat standalone runtime writes as atomic
desired publication.

## Verification obligations

- Pin pre-move runtime, attempt, and progress encodings and preserve their bytes.
- Preserve the existing runtime key and replay-locator admission boundaries.
- Keep runtime restart, checkpoint, attempt-chain, and root-replay tests green.
- Prove the later combined publisher consumes this same record contract and
  stays within ADR 0051's complete transaction budget; the schema move alone
  does not satisfy that obligation.
