# ADR 0021: Persist mutation idempotency as atomic protected evidence

- Status: Accepted
- Date: 2026-08-20

## Context

The human API requires one `Idempotency-Key` on every `POST`, `PUT`,
`PATCH`, and `DELETE`. Accepted ADR 0019 defines the canonical version-1
intent digest and requires its version and 32 digest bytes to be protected by
the Controller root age key. Accepted ADR 0013 fixes the observable replay,
mismatch, in-progress, retention, and unknown-outcome behavior, but deliberately
leaves the durable marker key, value, transition, and transaction-composition
contracts unresolved.

Those missing choices block every real mutation. Accepting a header without
atomically honoring it would falsely tell Console and CLI callers that a
transport retry is safe. A separate check-then-claim repository would also
permit two concurrent requests to apply the same mutation.

This ADR defines the complete durable contract accepted by the owner.

## Existing constraints

The decision must preserve these accepted rules:

- the key is 16 through 128 ASCII characters matching
  `[A-Za-z0-9._:-]+`;
- synchronous transport, schema, reference, and domain validation completes
  before a marker is claimed, and those preclaim failures are not replayed;
- a marker claim and its desired mutation or Task creation are one etcd
  transaction;
- an identical pending Task returns `idempotency.in_progress` with HTTP 409;
- an identical terminal marker replays the exact original public status and
  body, including the original Task id;
- the same lookup key with a different valid canonical intent returns
  `idempotency.mismatch` with HTTP 400;
- terminal markers are retained for 90 days, pending markers are never pruned,
  and pruning uses transactions within ADR 0013's 96-operation ceiling;
- an unknown transaction outcome may be declared committed only when a
  linearizable marker read proves the same protected intent;
- neither plaintext canonical intent bytes nor their digest may enter etcd, a
  key, a log, an error, a metric, a trace, or a Task event; and
- raw etcd keys and revisions never cross the infrastructure boundary or enter
  a public problem.

## Recommended decision

### 1. Use the marker primary as the unique lookup key

Logical keys continue to sit below the configured Controller prefix. A marker
has no public resource id. Its primary is:

```text
/v1/runtime/idempotency/<scope-kind>/<scope-id>/<encoded-method>/<encoded-route>/<encoded-key>
```

`scope-kind` is one of `platform`, `tenant`, `project`, or `environment`.
Platform uses the literal `-` scope id; every other scope id is a validated
stable owner id. Method, registered route template, and Idempotency-Key are
dynamic segments encoded exactly once as `~` plus unpadded base64url of their
UTF-8 bytes, following ADR 0013. The method is the validated uppercase method.
The route is the registered template below `/api/v1`, not a request URL. The
logical marker key must be at most 2 KiB.

This lookup scope means the same caller key may be used independently for two
different owners, methods, or registered routes. Path bindings and request
body remain in the protected canonical intent, so two resource ids on one
route still mismatch rather than sharing a marker.

The raw Idempotency-Key is never hashed into a key segment and is never used as
an etcd prefix. Its encoded segment is reversible because it is an identifier,
not authentication material. Keys and logical marker paths are nevertheless
excluded from logs and public errors.

For a destructive child-resource route whose owner cannot be recovered after
success, the same transaction also creates one replay locator:

```text
/v1/indexes/idempotency/by-replay-target/<target-kind>/<target-id>/<encoded-method>/<encoded-route>/<encoded-key>
```

Its strict schema-1 value contains the complete marker key. The MVP initially
admits only `target-kind=attach`. The target locator does not change marker
owner scope or canonical intent; it only recovers the accepted owner-scoped
locator before the deleted target is read.

### 2. Store one strict schema-2 marker union

Marker JSON rejects duplicate and unknown members and is encoded canonically.
All timestamps are UTC RFC 3339 with nanoseconds and a `time.UTC` location.
The common fields are:

```json
{
  "schema": 2,
  "kind": "direct",
  "state": "completed",
  "method": "PATCH",
  "route": "/projects/{id}",
  "scope_kind": "tenant",
  "scope_id": "tnt_...",
  "idempotency_key": "01ARZ3NDEKTSV4RRFFQ69G5FAV",
  "replay_target": {"kind":"attach","id":"att_..."},
  "intent": {
    "envelope_version": 1,
    "cipher": "age-x25519",
    "digest_algorithm": "sha256",
    "ciphertext_digest": "<64 lowercase hex characters>",
    "ciphertext": "<unpadded base64url>"
  },
  "response": {
    "status": 200,
    "content_kind": "application/json",
    "body": "<unpadded base64url of exact response bytes>"
  },
  "created_at": "2026-08-20T12:00:00Z",
  "updated_at": "2026-08-20T12:00:00Z",
  "terminal_at": "2026-08-20T12:00:00Z",
  "retain_until": "2026-11-18T12:00:00Z"
}
```

The `intent` object is the durable form of `secretvalue.Envelope`: envelope
version, cipher suite, ciphertext-only digest algorithm and value, and owned
ciphertext. It contains no plaintext canonicalization version or intent
digest. Restore and validation occur before decryption.

`response.status` must equal the success status registered for the operation:
direct `POST` is 201 JSON, direct `PUT` and `PATCH` are 200 JSON, and a direct
`DELETE` is 204 with `content_kind: none` and an empty body.
`content_kind` is `application/json` or `none`. `none` requires status 204 and
an empty body. JSON stores the exact response bytes once, as unpadded base64url,
so replay does not re-run a serializer or reread mutable state. Arbitrary
response headers are not persisted; the registered operation and content kind
reconstruct the public Content-Type. Exact replay bodies are limited to 128
KiB before base64url encoding. Protected intent ciphertext is limited to 4 KiB,
and the complete marker remains below the accepted 256 KiB record ceiling.

The closed union is:

| `kind` | Allowed `state` | Required additional fields |
| --- | --- | --- |
| `direct` | `completed` | terminal timestamps and response; no Task id |
| `task` | `pending` | `task_id` and response; terminal timestamps absent |
| `task` | `completed` | `task_id`, response, and terminal timestamps |
| `task` | `failed` | `task_id`, response, and terminal timestamps |

For a Task marker, `failed` covers Task `failed`, `aborted`, and `timed_out`.
The replay response remains the original accepted response, normally HTTP 202
with that Task id; marker state does not rewrite it into a later Task result.
Task persistence carries an internal-only locator object containing
`scope_kind`, `scope_id`, `method`, `route`, and `key` so its terminal
transition can update the exact marker. That object is stored only inside the
versioned persistence envelope and never enters the public Task schema.

### 3. Compare protected evidence without retaining a plaintext candidate

The current one-use `idempotentintent.Digest` is consumed when it is protected.
It therefore cannot also compare a conflicting durable marker. The clean API
extension is a protected-envelope comparison:

```text
CompareProtected(existingEnvelope, candidateEnvelope) -> equal | mismatch
```

The implementation opens the candidate envelope and, within that scoped
plaintext callback, opens the existing envelope and performs one constant-time
comparison over their exact 33 plaintext bytes: canonicalization version 1 and
32 digest bytes. Both Protector-owned buffers are cleared immediately after
their nested callbacks. No digest handle, plaintext copy, hexadecimal form, or
unprotected comparison value survives the call.

Malformed envelope metadata, ciphertext integrity failure, decryption failure,
wrong plaintext length, or unsupported canonicalization version is durable
corruption and returns the generic internal error. Only two valid protected
values that compare unequal produce `idempotency.mismatch`.

### 4. Compose the marker inside the typed mutation transaction

There is no separately callable `Claim` followed by a resource mutation. Each
Controller-facing typed repository mutation accepts one validated idempotency
claim and delegates to one shared etcd mutation coordinator.

Inside `internal/infra/etcd`, the typed repository builds an opaque, single-use
mutation plan containing its primary/index compares and success operations.
The plan exposes neither raw keys nor Store operations outside that package.
The coordinator validates the plan and marker, adds the marker-absent compare
and marker writes, enforces the 96-operation and 1 MiB transaction ceilings,
and calls `Store.Transact` exactly once. A consumed plan cannot be submitted
again.

The Controller-facing result is a closed union:

- `applied`, carrying the typed mutation result;
- `replay`, carrying the stored exact response; or
- a closed error: in-progress, mismatch, domain conflict, storage unavailable,
  or internal corruption.

If the transaction compare fails, its failure branch reads the marker and every
plan compare key at the same transaction revision. A present marker is restored
and compared with the candidate. A matching pending marker returns in-progress;
a matching terminal marker returns replay; a valid mismatch returns mismatch.
If the marker was absent at that revision, the typed repository classifies its
own failed primary/index compare from those reads. The coordinator never issues
a later read for a known compare failure and never guesses a domain conflict
from marker state.

### 5. Define direct and Task transitions exactly

A synchronous desired mutation has its final registered response encoded
before storage. Its one transaction writes the desired mutation, a terminal
`direct/completed` marker, and the retention index. A validation or domain
failure discovered before the transaction writes no marker. Only a committed
direct mutation or Task-backed operation has replay evidence. A storage failure
without durable evidence is not recorded as a replayable public failure.

A Task mutation writes the Task, operation history index, active-operation
index, FIFO queue membership, and a `task/pending` marker with the original
202 response in one transaction. Pending has no retention index. Claiming the
oldest Task CASes it from `pending` to `running`, removes queue membership, and
binds the Agent id, daemon generation, claimed pending Task revision, and
assignment time in one assignment record. The Task's terminal transition
updates the Task summary, deletes its assignment and active-operation index,
changes the marker to `completed` or `failed`, and creates its retention index
in the same transaction. Aborting before assignment performs the equivalent
terminal transaction while deleting queue membership. A pending marker without
its linked Task is corruption, not an invitation to rerun the operation.

Task garbage collection cannot delete a Task while a retained marker refers to
it. Marker pruning occurs first; Task pruning may remove the Task only in a
later transaction after no marker reference exists.

### 6. Resolve unknown transaction outcomes only from the marker

After a retryable or deadline Store result whose commit status is unknown, the
coordinator linearly reads the marker key. Missing evidence returns the original
retryable storage outcome. It never inspects a later resource value to infer
success.

When a marker exists, its key tuple and stored tuple must agree, its envelope
must restore, and its protected intent must compare with the candidate. A
matching marker proves that the atomic mutation or Task creation committed and
returns its current in-progress or replay outcome. A valid different intent is
mismatch. Corrupt evidence is internal. If the evidence reread itself is
unavailable, the outcome remains retryable and unknown.

### 7. Retain terminal evidence through a CAS-safe time index

Terminal transitions create:

```text
/v1/indexes/idempotency/by-retain-until/<20-digit-unix-nanoseconds>/<encoded-marker-key>
```

The final segment is one encoding of the complete logical marker key. Its
canonical JSON value is:

```json
{"schema":1,"marker_key":"/v1/runtime/idempotency/..."}
```

The zero-padded timestamp is the full Unix nanosecond value of terminal time
plus exactly 90 days. Codec validation reconstructs and compares the complete
timestamp, not only its seconds. Pending markers have no retention index and
are never pruned.

The daily collector reads at most 16 expired index entries and their markers at
one fixed revision. For a marker with a replay target it also reads and
validates that counterpart at the same revision. One worst-case pruning
transaction performs three compares and three deletes per marker: 16 markers,
48 keys, and 96 aggregate operations.
Blind deletion is forbidden. Without both compares, a new claim could recreate
an expired lookup key between the collector's read and delete, and the collector
could delete the new pending marker.

A failed compare restarts that batch from a fresh linearizable read. A missing
or malformed counterpart is internal corruption and aborts the batch before a
write. No etcd lease owns marker lifetime.

### 8. Preserve cancellation, error, and secrecy boundaries

Only an actually canceled caller context propagates cancellation. Etcd
Unavailable or an independent backend deadline is `storage.unavailable` with
HTTP 503. Marker races use the accepted in-progress, mismatch, and domain
conflict kinds. Codec, envelope, tuple, Task-link, or retention-index corruption
is the opaque internal error.

The idempotency key, logical marker key, ciphertext, ciphertext digest, exact
response body, and protected comparison bytes are excluded from logs, public
errors, metrics, traces, and Task events. Ciphertext and response bytes are
cleared from temporary buffers after canonical record encoding. Protected
plaintext and candidate comparison buffers are cleared on every success,
failure, and cancellation path.

## Consequences

- One unique etcd key is the serialization authority for every human mutation.
- Retrying an identical request cannot duplicate a desired mutation or Task.
- Marker comparison does not introduce a plaintext digest guessing oracle.
- Resource repositories must prepare opaque transaction plans rather than
  submitting their own transaction before or after an idempotency claim.
- Task terminal transitions and garbage collection gain an internal marker
  reference and ordering obligation.
- Exact replay consumes bounded additional etcd space for 90 days.
- The registered synchronous response status must be fixed by the operation
  manifest before a route can use this repository.

## Alternatives considered

### Store a plaintext request hash

Rejected because secret-bearing requests would give an etcd snapshot holder an
offline guessing oracle. ADR 0019 requires root-age-protected evidence.

### Hash the Idempotency-Key into one flat marker key

Rejected because it hides the required owner/method/route scope, complicates
operator-safe corruption diagnosis, and adds a collision assumption where
exact segment encoding already fits comfortably.

### Use a marker repository before calling the resource repository

Rejected because claim-then-mutate is not atomic. A crash leaves a marker that
does not prove the desired mutation or Task exists.

### Store only a resource or Task id and regenerate replay

Rejected because mutable state and serializers can change during the 90-day
window. The contract requires the original public status and body.

### Use an etcd lease or delete expired markers without compares

Rejected because leases delete independently of Task retention and blind
deletion races reuse of the same key after expiry.

### Retain a second plaintext digest for conflict comparison

Rejected because it extends sensitive plaintext lifetime unnecessarily. Nested
Protector callbacks compare the two protected values without returning bytes.

## Verification contract

Implementation requires focused and race-enabled tests proving:

1. exact segment encoding, scope separation, and a 2 KiB key ceiling;
2. strict canonical marker and retention-index codecs with no compatibility
   readers;
3. one winner under concurrent identical and mismatched claims;
4. claim plus direct mutation or Task creation in one Store transaction;
5. pending in-progress, terminal exact replay, and valid mismatch outcomes;
6. protected-envelope comparison compatibility, constant-time comparison, and
   complete plaintext clearing;
7. corruption, wrong version, malformed ciphertext, and tuple disagreement
   returning only internal;
8. unknown outcomes accepted only from matching linearizable marker evidence;
9. Task terminal transition and marker retention-index creation in one
   transaction;
10. direct response bytes at the 128 KiB boundary and marker records below
    256 KiB;
11. 90-day retention, pending preservation, fixed-revision scans, the
    expiry/reclaim race, and 24-marker CAS pruning batches;
12. caller cancellation versus backend deadline/Unavailable classification;
13. no key, digest, ciphertext, response, or plaintext leakage through errors,
    logs, formatting, JSON diagnostics, metrics, or traces; and
14. transaction preflight rejecting a plan over 96 operations or 1 MiB before
    any write.

The repository-wide integration test must crash or cancel at every boundary
between validation, protection, transaction submission, unknown-outcome read,
Task terminal transition, and pruning, then prove that no mutation is applied
twice and no committed mutation loses replay evidence.

## Integration boundary

This acceptance does not invent a Task transaction composition API. Task
creation and terminal integration may be wired only when the Task repository
can contribute its record and index mutations through the same opaque plan.
Until then, strict marker and index codecs and the atomic plan seam may exist,
but no handler may claim durable Task idempotency. This ADR does not approve
unresolved resource schemas or operation-manifest success statuses.
