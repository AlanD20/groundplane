# Reusable Secrets and Connectors

## Purpose and scope

Reusable Secrets store operator-supplied values once at Project or Platform
scope. Consumers resolve a Project value before the Platform fallback. An
S3-compatible Connector is instead owned by one Environment and records where
Backup artifacts go, how the SDK addresses that endpoint, and how its access
and secret keys are obtained.

Reusable Secrets are distinct from secret Environment Entries and from Attach
facts. An Environment secret literal is subordinate encrypted Entry data. An
Attach fact is subordinate encrypted credential-owner data. Neither becomes a
reusable Secret resource merely because its value is confidential.

This document owns the feature behavior. Exact REST and CLI shapes remain in
[the API and CLI contract](../api-cli.md), Connector desired-state syntax in
[the Blueprint contract](../blueprint.md), and Backup use and qualification in
[Backups](backups.md).

## Functional requirements

### Reusable Secrets

A reusable Secret has exactly one owner: a Project or the Platform. Its
metadata contains stable id, scope, optional Project id, key, `env_var` or
`file` kind, canonical materialization reference, and update time. The value is
valid UTF-8 of at most 255 KiB before encryption and is never included in list,
detail, create, or delete responses.

Keys are unique within one scope. A key reference resolves the selected
Project index first and the Platform index second at one fixed revision. A
stable `sec_...` id is valid only when it names that Project's record or a
Platform record. There is no Environment Secret scope and no stop-propagation
marker.

An env-var reference is Controller-derived from stable ownership:
`secrets/.env.<project-id>` or `secrets/.env.edge`. A file Secret has an
operator-authored volume-relative path. It cannot be absolute, traverse with
`..`, contain backslashes, or use the materializer's reserved temporary
prefix.

Create accepts a write-only value. The CLI reads it from `--value-file PATH|-`;
plaintext is not accepted as an argv value. Value reveal is an explicit
Console/API operation and intentionally has no CLI counterpart.

Secret deletion is an asynchronous Controller finalizer. Publication
atomically tombstones the Secret and returns a Task, hiding it from reads and
resolution while retaining metadata, indexes, and ciphertext. Success removes
those records atomically with terminal Task state. Failure, timeout, or abort
removes only the tombstone and restores visibility; retry reacquires the fence.
Desired-state references do not block deletion. Later resolution uses the
remaining normal fallback or fails when no value remains.

Exact execution pins are different from desired references. Existing Script
source guards remain in force. Under [ADR 0079](../decisions/0079-pinned-task-configuration-recovery.md),
a recoverable Task also blocks deletion of its exact Secret value with
`resource.in_use` until recovery/retry authority releases it. Reservation,
deletion admission, Retry and finalization must fence the same membership.
A recovery pin identifies the operation, Secret metadata revision and ciphertext
digest; it contains no value bytes and creates no artificial Script reference.
Successful deletion must leave no retained value copy.

### S3-compatible Connectors

A Connector belongs to exactly one Environment. Names are unique within that
Environment and immutable. Connector lookup and Backup Policy selection never
fall back to another Environment, Project, or the Platform.

The MVP supports only `s3-compatible`. Its operator decisions are:

- an absolute HTTP or HTTPS endpoint without user information, query, or
  fragment and with an empty or root path;
- a lowercase 3..63-character bucket;
- an optional normalized relative prefix without a leading slash or dot
  segment;
- an explicit 1..64-byte printable-ASCII region, including `auto` when the
  operator selects it; and
- required `path_style`, because SDK defaults are not reproducible product
  intent.

Credentials contain exactly `access_key` and `secret_key`. Each independently
chooses exactly one source:

- `secret_ref` names a reusable `env_var` Secret key and resolves it at use
  time through the owning Environment's Project then Platform fallback; or
- direct `value` is a non-empty write-only creation input encrypted as
  Connector-subordinate state.

A direct Connector credential is not returned and does not become a reusable
Secret. A Secret-backed credential stores its authored key, not a reverse
reference or copied value, so rotation needs no Connector rewrite. Secret
deletion is not blocked by a Connector; later use resolves the remaining
fallback or fails closed.

Connector CRUD performs no endpoint or credential probe. Create and reads are
synchronous. Delete uses a Controller finalizer and is blocked by an enabled
Backup Policy, any Recovery Point, or orphan authority that still retains the
Connector. Failed or uncertain remote cleanup must retain the credential and
reference authority needed for retry and diagnosis.

## Non-functional requirements

- Reusable Secret and direct Connector ciphertext use the Controller's age
  protector. Metadata, indexes, ownership, uniqueness, ciphertext, tombstones,
  and protected replay commit under the relevant fixed-revision fences.
- Protected idempotency evidence for a reusable Secret stores only the value's
  SHA-256 inside encrypted intent. No public plaintext digest is exposed.
- Connector responses expose credential source metadata only. Direct values,
  resolved Secret values, provider diagnostics, object locators, and immutable
  object discriminators remain private.
- Credential delivery is fenced to the exact active Task assignment, step, and
  purpose. Header, chunk, and end records repeat that tuple. S3 credential
  slots are bounded to 256 KiB and individual chunks to 32 KiB. Transient bytes
  are cleared after consumption and on every terminal, cancellation,
  disconnect, or reconnect path.
- The Agent constructs the S3 client's static credential provider only for the
  active assignment. Go strings and SDK, HTTP, or TLS internals may copy and
  retain credentials until the operation terminates; the implementation must
  drop those references at terminal cleanup and must not claim complete
  in-process zeroization.
- The S3 adapter must not consult ambient credentials, proxy, redirect,
  logging, or custom-CA configuration. A credential or endpoint failure is an
  execution failure, not a reason to mutate Connector intent.
- A provider is conformant only after it proves the conditional object create,
  multipart completion, delete, immutable metadata, and VersionId/ETag rules
  required by the Backup contract. CRUD success is not provider qualification.

## Technical design

[ADR 0030](../decisions/0030-reusable-secret-record-and-fallback.md) owns the
reusable Secret record, fixed-revision Project/Platform fallback, value limits,
and delete finalizer. [ADR 0045](../decisions/0045-connector-contract-and-credentials.md)
owns Connector validation, encrypted direct credentials, late-bound Secret
references, S3 adapter responsibility, transient credential delivery, and
persistence.

Reusable Secret metadata and ciphertext are separate records. A Connector has
one Environment primary, owner and name indexes, and an optional encrypted
credential bundle. Its finalizer removes those records after reference and
remote-safety checks, while protected replay identity survives for its defined
retention window.

The Controller owns policy, scheduling, credential resolution, and transient
slot authorization but never handles artifact bytes. The Agent's typed
S3-compatible adapter owns SDK Put, Get, Head, and Delete. Backup source
adapters own capture and restore formats, not object-store transport.

## Acceptance

Reusable Secret proof covers Project and Platform ownership, scoped uniqueness,
fixed-revision fallback, stable-id scope checks, env-var and file validation,
the 255 KiB boundary, value masking and explicit reveal, stdin create, protected
replay, finalizer success, and failure/timeout/abort visibility restoration.

Connector proof covers endpoint, bucket, prefix, region, and path-style
validation; mixed direct and Secret-backed credentials; rotation through
late-bound fallback; redacted reads; protected replay; reference-blocked and
successful deletion; bounded transient slot cleanup; and negative tests for
ambient SDK configuration. Provider qualification separately proves the exact
immutable-object and conditional-operation contract against the selected live
target.

## Current status

The [capability index](../capabilities.md) records qualification for reusable
Secret lifecycle and Environment S3-compatible Connector lifecycle. That does
not prove credential use in an actual Backup, Restore, or production recovery.
Those execution and provider gates remain part of the Backup recovery gap.

The deletion-side recovery-pin guard has local regression coverage for initial
admission, direct and hierarchy finalization, Retry and malformed membership.
The `infra/tasksecretpins` module stages exact memberships with source/deletion
fences, supplies a constant-size atomic activation fragment, and releases members
in bounded batches only after an explicit release transition. Interrupted
preparation can be abandoned from its durable descriptor, with Task and active
operation absence checked on every mutation. Blueprint/Entry desired publication
and ordinary Release publication now activate the selected exact pins atomically
with their Task. Failed attempts retain them; Retry transfers the active attempt
without copying Secret values. Successful completion authorizes bounded cleanup.
Expiry checks the newest attempt's retention and recovery state, with a root
revision fence against a retry that starts and finishes during the check.
Startup abandons unpublished preparation and resumes authorized release; the
scheduler drains releases before daily history pruning. H18 records actual local
publication, acknowledgement, Retry, expiry and deletion checks. Remaining file
writers and live recovery still require qualification; these tests do not qualify
production hosting.
