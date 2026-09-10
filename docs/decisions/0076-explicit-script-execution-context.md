# ADR 0076: Explicit Script execution context and hook order

- Status: Accepted within the owner's approved setup-Scripts initiative
- Date: 2026-09-10
- Capability: C09 Scripts
- Amends: ADR0040 execution projection/image source and intra-Service hook order;
  ADR0062 reference preparation and all cleanup/retry guarantees remain

## Context

A real consumer may run unprivileged with read-only storage while its one-time
preparation needs different tools and ownership permissions. Inheriting that
consumer's image/user/mount policy made a permanent sleeping initializer Service
necessary. Script slugs also served as an implicit order, coupling rename to
migration sequencing.

## Decision

Keep the required immutable real Service association and the existing one-off
runner. Add an optional complete execution context and independent numeric
`order`. Omitted context is inherited; explicit context names a digest-pinned
image, numeric uid/gid and exact managed Volume/Entry grants. There is no ambient
Service environment or network in explicit mode. Migrations needing normal
Service networking can use inherited mode. This deliberately avoids adding a
general-purpose network-permission or job orchestration surface for local setup.

Volume grants are same-Environment stable identities with canonical container
targets and an explicit read-only flag; a setup writer may be granted read-write
access while real consumers mount that Volume read-only. Entry selection remains
bounded by the associated Service's exposure. It cannot grant another Service's
credentials. No grant is inferred from an omitted list. Grant references resolve
from the same fixed desired projection as their materialization/source evidence;
Blueprint names are immutable Volume/Entry authored keys, API references are ids.
Explicit managed mounts set Docker's`volume-nocopy`: creating the runner must
not populate an empty Volume from image contents before the Script begins.

Before publication, the authenticated Agent's existing bounded read-only image
lookup resolves an explicit repository digest to its local Docker config id.
The immutable runner snapshot records the explicit source and the real Release's
image separately. The former selects the one-off image; the latter still binds
the consumer candidate/serving Release. Neither can impersonate the other.
Inherited snapshots continue selecting only the applicable Release image.
No runtime mutable-tag lookup, image pull or synthetic initializer Release exists.

The complete explicit context is captured by the immutable runner snapshot and
covered by its digest and plan hash. Publication compares the selected Script
primary and exact source revisions, while prepared memberships fence the body,
snapshot and declared resources. Editing order/context does not allocate a new
body generation; it changes future captures only. Existing operations never
reconstruct their runner from the edited Script or live Docker/container state.
Missing/foreign resources or missing images reject execution before publication.

`order` is0through65535, default0. Each Service's selected hooks in one phase
sort by order then current ASCII slug. Service dependency topology and explicit
Release Group order still precede that per-Service ordering. It does not select
additional hooks, make manual Scripts automatic, or introduce cross-Task edges.
The existing global pre-hook cleanup barrier still precedes every Blueprint
candidate consumer start; exact reapply still executes no hooks.

Manual runs retain the serving-Release precondition. First-start setup uses an
ordinary pre-deploy hook associated with the real candidate consumer. Authors
provide tools and own atomic, idempotent output; GP does not roll back arbitrary
data writes. Timeout, output disposal, durable outcome, exact Abort/cleanup,
unknown-state fencing and refusal to retry after start authorization do not change.

Create/edit/read expose both fields through the existing Console/CLI/API actions.
Execution replacement is whole-value, including an explicit inherited reset;
grant lists are not partially merged. Unknown fields, mixed modes, malformed
numeric users, mutable image references and unsafe mounts fail validation.

## Alternatives and consequences

A permanent initializer misrepresents application state and duplicates lifecycle.
Docker exec lacks the existing runner's ownership and cleanup proof. A new Jobs
engine or dependency DAG is unnecessary for these setup and migration needs.
Explicit network grants are not added: inherited migrations already have the
real Service's networking, while storage/TLS setup is local.

The implementation must prove separate image authority through frozen candidate
replay, not merely accept a different image string. Source removal, unknown
publication and retry tests remain mandatory. The extension cannot be declared
delivered from grammar/UI proof alone; see`SPEC-setup-scripts.md`and task4.
