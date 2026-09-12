# ADR 0029: Bind each environment Entry to an immutable value generation

- Status: Accepted
- Date: 2026-08-22
- Accepted: 2026-08-22

## Context

ADR 0020 accepted immutable plain and encrypted Entry value generations and a
closed Task reference, but deliberately left atomic Entry mutation integration
unfinished. The remaining contract must preserve four product rules at once:

- listable desired metadata never contains secret plaintext;
- every create or edit selects exact bytes that survive retry and restart;
- fact and reusable-secret sources remain live references in desired state; and
- deleting an Entry atomically removes all ciphertext owned by that Entry.

The existing human API example and its Go/CLI stand-ins also disagreed about
whether a fact was flat or nested, while ADR 0020's accepted explicit `uid` and
`gid` requirement was absent from those stand-ins.

## Decision

The primary `/v1/records/entries/<entry-id>` stores `environment_id`, the typed
Entry desired metadata, and `current_value_generation_id`. Every create and
edit transaction writes that primary and exactly one new immutable `cfg_...`
generation together. The generation binds the same Environment and Entry ids.

Plain literal bytes remain visible in desired metadata and must exactly equal
the selected plain generation. A secret literal is represented by source kind
`literal` with no literal bytes in the primary; its selected generation is
encrypted. Fact and reusable `secret_ref` sources remain typed live references;
the selected generation contains the resolved bytes used by the current
materialization, while a later render may resolve a new generation.

Entry type, destination, secret storage class, and file ownership are immutable
under edit. Source and exposure may change. A file Entry always carries both
explicit numeric `uid` and `gid`, including when either value is zero. An env
Entry carries neither.

`PATCH /entries/{id}` carries exactly `{source, exposure}` and returns the
updated Entry synchronously. Its protected marker remains Environment-scoped,
but the same transaction also publishes a replay-target index keyed by the
stable Entry id, method, route, and idempotency key. Replay resolves that index
before reading the Entry primary, so the exact response remains recoverable
after a later delete. Canonical replay evidence represents every literal,
plain or secret, only as its SHA-256 digest; durable value storage still follows
the Entry's immutable plain or encrypted storage class.

Historical generations remain immutable while Tasks may reference them.
Successful terminal deletion finalization removes the Entry primary,
Environment owner index, and both plain and secret generation prefixes in one
compare-and-mutate transaction.
The etcd Store therefore supports an explicit prefix-delete mutation; prefix
semantics are invalid for puts.

Protected deletion publishes a visibility-preserving tombstone, immutable
removal intent, and Task atomically. Applied Entries use an Agent-owned
120-second materialization-removal Task; never-applied Entries use a
Controller-owned 30-second no-op finalizer. Failure, timeout, and abort retain
the primary and generations. Success removes them in the same transaction that
terminalizes the Task.

The human JSON API uses the flat discriminated fact source documented in
`api-cli.md`: `{kind:"fact", attach_id, fact}`. Blueprint YAML retains its
authored nested shape `source.fact.{attach,key}`. The boundary adapter converts
between the two; they are not wire aliases.

Membership-index values are exact stable-id bytes. This corrects ADR 0013's
unused JSON-wrapper description to match every repository's fixed-revision
index verifier; there is no compatibility decoder or dual representation.

## Consequences

Accepted [Backup artifact contract](../features/backups/artifacts.md) preserves this canonical Entry primary and immutable value-
generation model for Config restore. Restore upserts present artifact Entry ids
and deletes omitted Entries through read-hidden bounded primary/index
roll-forward; it does not add an Environment-wide active-generation read model
or resolve captured desired sources to different current bytes.

An Entry primary can never point at an absent current generation after a
successful transaction. A stale mutation loses its revision compare without
publishing either metadata or bytes. Secret list/detail reads remain
metadata-only, while reveal and materialization resolve the selected encrypted
generation through the Controller Protector.

Prefix deletion removes an unbounded number of historical generations as one
etcd transaction operation, so retention does not weaken atomic deletion or
consume the 96-operation transaction budget.
