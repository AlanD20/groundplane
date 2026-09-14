# ADR 0030: Persist reusable Secrets by project or platform scope

- Status: Accepted
- Date: 2026-08-22
- Accepted: 2026-08-22

## Context

Reusable Secrets are distinct from environment Entries. The MVP permits them
at project and platform scope, resolves project values before platform
fallbacks, and stores every value under the Controller age key. Existing core
and public structs could not represent platform scope or the Console's Secret
key, and the CLI create shape omitted the value entirely.

Environment-scoped values are not Secret resources. They are Entry resources
with subordinate encrypted generations. The Console's remaining
environment-Secret fixtures and stop-propagation copy are superseded and must
be removed when the live Secret/Entry stores replace the fixture aggregate.

## Decision

One Secret metadata primary stores stable id, `project | platform` scope,
optional Project id, key, `env_var | file` kind, canonical materialization ref,
and update time. Its Controller-key envelope is stored separately at
`/v1/secret-values/secrets/<secret-id>`. Metadata, owner membership, scoped-key
uniqueness, and ciphertext are created and deleted in one transaction under
Project/Tenant revision and deletion fences.

An env-var Secret key is a valid environment-variable identifier. Its ref is
Controller-derived: `secrets/.env.<project-id>` for a Project or
`secrets/.env.edge` for the platform. A file Secret has an operator name and a
canonical volume-relative path. Paths cannot be absolute, escape with `..`,
use backslashes, or use the materializer's reserved temporary prefix.

Secret keys are unique within one scope. A key reference resolves the Project
index first and then the platform index at one fixed etcd revision. A stable
`sec_...` reference resolves exactly that record but is accepted only when it
belongs to the selected Project or to the platform. There is no
stop-propagation marker or environment Secret scope.

The public create request carries `key`, `kind`, write-only `value`, and
exactly one of `project_id` or `platform: true`; file kind also carries `path`.
Responses never include the value. `GET /secrets/{id}/value` is the explicit
Console/API-without-CLI reveal endpoint.

The CLI will read create content from `--value-file PATH|-`; `-` means stdin.
It will not accept plaintext as an argv value and will not add a reveal command.

Decoded create values are valid UTF-8 and at most 255 KiB. The durable
ciphertext remains capped at 256 KiB, leaving one KiB for the locked
single-recipient age envelope. Protected idempotency evidence stores only the
value's SHA-256 inside the encrypted intent, not the value or a public
plaintext digest, so the existing 4 KiB intent bound still holds.

Deletion is a strict asynchronous finalizer. `DELETE /secrets/{id}` atomically
publishes a protected Controller `remove` Task, its replay-target index, and a
Secret deletion tombstone, then returns `202 {task_id}`. The tombstone makes
the Secret absent from detail, reveal, owner lists, and key resolution while
metadata, indexes, and ciphertext remain durable. A fenced Project override is
skipped by normal project-before-platform fallback.

The Controller Task has a 30-second claim deadline and no host effects. Its
successful acknowledgement atomically deletes metadata, both indexes,
ciphertext, and the tombstone with the terminal Task state. Failure, timeout,
or abort removes only the tombstone and restores visibility. A Task retry
atomically reacquires the tombstone for the new attempt. Desired-state
references do not block deletion; subsequent validation resolves the remaining
normal fallback or fails when no Secret remains. A synchronous delete hidden
behind a `202` response is forbidden.

Exact execution pins are distinct from ordinary desired references. ADR 0062
protects Script source generations; the owner-approved ADR 0079 extension also
rejects deletion while a recoverable Task holds the exact Secret value. Return
`resource.in_use` until recovery/retry authority releases it. Deletion and pin
reservation share a concurrency fence, including delete Retry. No hidden value
copy survives successful deletion.

## Consequences

List and detail paths cannot expose ciphertext. Fallback cannot observe a
Project index at one revision and platform metadata at another. Cross-Project
stable ids behave as not found. Deletion cannot leave ciphertext or an index
behind after removing metadata, and an interrupted finalizer cannot strand a
hidden Secret.
