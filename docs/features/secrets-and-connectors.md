# Reusable Secrets and Connectors

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

## Configuration and behavior

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
source guards remain in force. Under [Service release and recovery](../decisions/service-release-and-recovery.md#recover-the-pinned-configuration-not-current-configuration),
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

## Security and availability

Values are encrypted with the Controller's age protector. Credential delivery is
limited to the exact active operation. Ordinary responses and diagnostics must
not disclose values; transient references are released after use. GP cannot
promise complete in-process zeroization of copies held by SDK or TLS internals.

The S3 adapter uses the declared endpoint and credentials, not ambient host
credentials or proxy settings. Connector CRUD does not qualify a provider or
prove that Backup/Restore works. Those workflows remain incomplete and deferred.

[Storage and idempotency decisions](../decisions/storage-and-idempotency.md)
explain Secret ownership and resolution. [Backup recovery](../decisions/backup-recovery.md)
records provider requirements and credential retention during remote cleanup.
