# ADR 0045: Connector contract and credentials

## Status

Accepted for the MVP.

## Context

Groundplane needs an Environment-owned S3-compatible connector before backup
policies can be reproducible. S3-compatible endpoints do not expose a portable
way to derive virtual-hosted versus path-style addressing. Omitting that choice
from desired state would make behavior depend on an SDK default. Connector
credentials may be supplied directly or by reusable Secret reference, but a
read response must never return direct plaintext.

## Decision

The MVP supports exactly one Connector kind: `s3-compatible`. A Connector is
owned by exactly one Environment. Connector lookup never falls back across
Environments, Projects, or the platform scope. Names are unique within the
owning Environment and are immutable because the MVP has no Connector update
or rename operation.

The desired-state and API shape contains these decisions:

- `endpoint`: an absolute `http` or `https` URL without user information,
  query, or fragment. Its path is empty or `/` and storage normalizes it by
  removing the trailing slash.
- `bucket`: a required lowercase S3-compatible bucket name of 3 through 63
  characters.
- `prefix`: an optional relative key prefix. It has no leading slash or dot
  segment and storage normalizes a non-empty prefix to end in `/`.
- `region`: a required value of exactly 1 through 64 bytes, every byte in
  ASCII range `0x21..0x7e`. `auto` is a valid explicit value.
- `path_style`: a required Boolean decision. `true` selects path-style
  addressing and `false` selects virtual-hosted-style addressing.
- `credentials`: exactly the keys `access_key` and `secret_key`. Each value
  selects exactly one of `secret_ref` or direct `value`.

A `secret_ref` names an existing reusable Secret key. The Controller resolves
it through the owning Environment's Project, first Project scope and then
platform fallback, and requires an `env_var` Secret. Resolution is performed
when execution needs the credential, so Secret rotation does not require a
Connector rewrite. A direct `value` is a non-empty, write-only creation input.
The Controller encrypts it with the existing age secret-value protector before
committing the Connector. List and detail responses expose only credential
source kind and `secret_ref`; direct values are represented as `direct` and are
never returned.

`secret_ref` is deliberately late-bound and creates no Secret reverse
reference. Deleting a Secret is not blocked by Connector records; a later
resolution selects the remaining Project/platform fallback or fails closed if
no valid `env_var` value remains.

Connector creation and reads are synchronous. Creation uses protected
idempotency. Deletion is a Controller finalizer Task with a 30-second timeout;
the Connector remains visible but mutation-fenced until finalization. A failed,
aborted, or timed-out deletion clears the tombstone and retains the Connector.
Deletion is rejected while an enabled backup policy references the Connector.
ADR 0024 additionally rejects deletion while a Recovery Point or orphan reverse
reference exists. Those independent references remain until verified remote
absence and atomic authority removal.

CRUD performs no endpoint or credential network probe. A source adapter owns
only its typed capture/restore procedure and deterministic format. The
Agent-side `s3-compatible` Connector adapter owns AWS SDK Put, Get, Head, and
Delete calls. The Controller owns policy, scheduling, Recovery Points, and
retention, and resolves the Connector credential sources into transient Agent
slots without handling artifact bytes. ADR 0005 fixes the SDK versions, direct
configuration, addressing, checksum, object-size, multipart, reconciliation,
permission, and provider-conformance contract. Connector creation does not
claim that an unproved endpoint satisfies that contract.

Connector credential slots use the same bounded delivery fence as every other
task secret: `(task_id, assignment_id, step_id, purpose)`. The Agent accepts a
slot only for that exact active assignment and purpose and clears it when
consumed and again on completion, failure, cancellation, or reconnect. Durable
direct or Secret-backed credentials are re-resolved and redelivered on a valid
redispatch. This differs from an operator-supplied old backup identity, which
is request-only, never durable, and makes that restore non-retryable.

The private Backup slot protocol closes `purpose` to exactly `s3_access_key`,
`s3_secret_key`, `current_age_identity`, and
`operator_old_age_identity`; config Entry content is not a slot purpose. Every
header, chunk, and end record repeats the exact
`(task_id, assignment_id, step_id, purpose)` fence. Headers declare non-zero
total bytes and exact chunk count; chunks are one-based and contiguous with a
32 KiB payload ceiling; end repeats the chunk count. S3 credential slots have
a 256 KiB total ceiling, derived from the 255 KiB reusable Secret input ceiling
and the existing 256 KiB encrypted Connector credential-envelope ceiling. Age
identity slots use the existing 4 KiB identity ceiling. Non-final chunks are
exactly 32 KiB, so one header admits only one canonical frame sequence. The
Agent retains no completed slot and clears transient bytes after consumption
and again on every terminal, cancellation, stream-loss, and reconnect path.

The Agent constructs one static credential provider for the exact active
assignment only, immediately before constructing its S3 client, then clears its
owned mutable input buffers. The AWS SDK and HTTP signing boundary requires Go
strings and may copy them into SDK, HTTP, or TLS state. The assignment-scoped
provider and client necessarily retain their credential strings until the S3
work reaches a terminal path; terminal cleanup then drops every retained
provider and client reference. Those immutable strings and internal copies
cannot be reliably zeroized. The implementation must state that boundary
truthfully and must not claim complete in-process zeroization.

Credential delivery retains the accepted token-authenticated Controller-Agent
channel. This decision neither adds nor requires mutual TLS. The S3 adapter
does not consult ambient credential, proxy, redirect, logging, or custom-CA
configuration.

An endpoint/provider is conformant only when its acceptance gate proves the
conditional PutObject, CompleteMultipartUpload, and DeleteObject behavior,
exact immutable-metadata round trip, and version-identifier semantics required
by ADR 0005. A present VersionId, including literal `null`, and an absent
VersionId with its verified ETag are distinct immutable discriminators. A
metadata, VersionId, or ETag mismatch always fails closed and never authorizes
overwrite or deletion.

Wire tests and MinIO validate the provider-neutral adapter contract. R2 remains
the explicit accepted MVP target, and a separate live R2 conformance gate is
mandatory before C16 can be declared complete or release-accepted.

Accepted ADRs 0047 and 0048 preserve this Connector CRUD, credential, SDK, and
live-R2 contract and bind its canonical endpoint/region bounds and transient
credential slots into the sole Backup schema-1 authority. Decision acceptance
does not satisfy the required runtime or live-provider evidence.

## Persistence

The durable records and indexes are:

```text
/v1/records/connectors/<connector-id>
/v1/indexes/connectors/by-environment/<environment-id>/<connector-id>
/v1/indexes/connectors/by-name/environment/<environment-id>/<encoded-name>
/v1/secret-values/connectors/<connector-id>
```

The secret-value record is an encrypted credential bundle subordinate to the
Connector record. Connector deletion removes the primary record, both indexes,
the encrypted bundle, tombstone, and cleanup intent atomically after the
finalizer Task succeeds. The stable Connector replay-target index is not cleanup
state: it survives successful deletion with the terminal idempotency marker and
is removed only when that marker's accepted 90-day retention window is pruned.
This preserves exact replay of a protected DELETE after the primary record is
gone.
Secret-backed mode has no Secret reverse-reference record; its authored key is
resolved dynamically at use time.

## Consequences

Connector behavior is reproducible across SDK versions and endpoint vendors.
Direct credentials cannot be retrieved through the API. Secret rotation stays
independent of Connector identity. CRUD does not prove endpoint reachability;
execution surfaces authentication or connectivity errors through its Task.
