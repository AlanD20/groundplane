# Networks, Connectors, and shared access

Status: Accepted MVP decisions. The live R2 gate and complete Valkey delivery
proof remain required; decision acceptance is not runtime qualification.

This document records the host network-allocation boundary and the shared
service credentials that cross from Controller-owned desired state into
bounded Agent execution.

Product authority: [Backing services and Attaches](../features/backing-services.md),
[Secrets and Connectors](../features/secrets-and-connectors.md),
[Backups](../features/backups.md), and
[Components and routing](../features/components.md).

## Environment network allocation

Machine bootstrap supplies one `environment_pool` that is disjoint from the
system and Runner pools. It is allocation authority, not a Docker network.
Every Environment, including a backing Project's `main` Environment, reserves
one globally unique child pool. Every Zone reserves one child subnet and is the
only layer materialized as a Docker bridge.

Replacing an Environment pool is valid only when it still contains every
existing Zone and overlaps no reservation. Zone ownership, name, subnet, and
internal flag are immutable; topology changes add, move, or remove Zones.
Reservations remain fenced until physical cleanup succeeds. Platform Component
networks are generated artifacts rather than Zone CRUD resources.

This hierarchy exists because Compose projects share one Docker daemon and do
not provide separate IPAM spaces. Tenant-wide ranges would require predicting
future capacity too early, while unscoped bridge allocation would permit
collisions.

Shared backing bridges may permit peer reachability. The MVP does not claim
tenant network isolation; strict peer firewalling is post-MVP.

Current source: [network reservation planner](../../internal/infra/etcd/networkreservations)
and [network persistence](../../internal/infra/etcd/network).

## Service DNS targets

DNS points to a reserved Service proxy address, not a workload replica. Replicas
can change during recreate or blue-green deployment without changing the name's
target. Proxy and Component reservations share the Zone allocator to prevent
collisions. The reservation commits with the sealed Release Task and is released
only after proven Service removal; failed Deploy or runtime destruction retains it.

Record admission requires an acknowledged proxy artifact carrying that address.
Desired and last-applied DNS references fence resource deletion atomically, so a
pending or failed resolver update cannot silently free its target. Explicit-IP
records deliberately have no resource ownership.

Current source: [proxy reservations](../../internal/infra/etcd/networkreservations/service_proxy_addresses.go),
[DNS reference authority](../../internal/infra/etcd/dnsrecords), and
[proxy rendering](../../internal/controller/composerender/service_proxy.go).

## Runner registry resolution

The accepted [Runner delivery design](runner-isolation.md#one-private-registry-and-existing-dns)
reuses CoreDNS for the same-host registry. Keep one managed name/address mapping
and make it reachable from the host Docker daemon and the Runner's rootless
daemon. A container-only DNS setting cannot govern a daemon's image push/pull.
This requires private DNS reachability, not a public listener or a second resolver.
The registry is shared infrastructure, not an Environment Zone or a public Route.
The listener and daemon wiring are private host infrastructure; existing Route
DNS behavior is unchanged.

## Connector and credentials

The MVP Connector kind is `s3-compatible`. A Connector belongs to exactly one
Environment and never falls back across Environments, Projects, or platform.
Its name is a unique, editable label; retained operations reference its stable id.
Edits atomically replace metadata, credential ciphertext and name lookup with
revision and Environment-operation fences. Destination changes additionally
require empty Recovery Point and orphan memberships, preventing a new destination
from breaking old Restore or cleanup authority. Idle credential rotation uses
the same destination; historical records are not rewritten and old values are
not retained as hidden recovery copies.

Desired state makes endpoint, bucket, key prefix, region, and path-style versus
virtual-host addressing explicit. These values are validated and normalized
before persistence so SDK defaults or deployment environment cannot change
behavior. Connector creation is synchronous and performs no network probe;
reachability and provider behavior are execution evidence.

Each access-key and secret-key value is either a direct write-only value or a
late-bound reusable Secret reference. A reference resolves in the owning
Environment's Project with platform fallback and must yield an environment-
variable Secret. Direct values are encrypted before commit. List and detail
show only source kind and reference; no plaintext is returned. Secret rotation
does not rewrite the Connector, and deleting a referenced Secret exposes normal
fallback or a later closed resolution failure rather than mutating the
Connector.

Deletion is a Controller finalizer Task. It remains mutation-fenced and visible
until finalization, and it is blocked by active Backup Policy, Recovery Point,
or orphan authority. Success removes record, indexes, encrypted bundle, and
cleanup state atomically while preserving retained idempotency replay evidence.

Credential delivery is transient and fenced to the exact Task, assignment,
step, and purpose. Frames are bounded and canonical; the Agent clears owned
mutable buffers after consumption and on every terminal or reconnect path.
The S3 provider and HTTP signer necessarily retain immutable Go string copies
for the active request lifetime; implementation must not claim complete
in-process zeroization. Durable values are re-resolved for valid redispatch;
operator-supplied old recovery identity remains request-only and non-retryable.

Current source: [Connector use cases](../../internal/controller/connectors),
[Connector persistence](../../internal/infra/etcd/connectors), and
[credential transfer](../../internal/agent/backupsecrettransfer).

## S3-compatible backup access

The S3 adapter uses AWS SDK for Go v2 behind Groundplane's Connector interface.
It constructs configuration directly from typed execution input. It does not
consult ambient AWS files or environment, enable proxy discovery or redirects,
log SDK requests, accept custom CAs, or expose SDK types to backup core.

Uploads are immutable. Small objects use one conditional put; larger objects
use sequential multipart sizing that stays within the S3 part limit through
the accepted maximum object size. Bodies carry explicit MD5 and required SDK
checksum behavior. Completion and deletion are conditional on verified object
evidence, so an existing or changed object is never overwritten or deleted.

Mutation retries are reconciliation, not blind repetition. Ambiguous multipart
creation first enumerates and cleans only the exact key below Groundplane's
exclusive prefix. Ambiguous completion reads the final object and accepts only
exact size, metadata, and immutable discriminator; explicit conflicts start a
fresh upload only after authoritative absence. Ambiguous deletion rereads the
exact key and version before deciding whether success or another conditional
attempt is safe.

New uploads select a valid returned ETag for conditional access. A returned
VersionId alone does not prove a provider supports version-targeted operations;
R2 returns one while rejecting version-selected HEAD. If an ETag is absent,
the returned VersionId remains a literal identity: absent, present-empty and
present-literal-`null` are distinct states. Once an identity is sealed, reads,
deletion and reconciliation must retain its exact kind and value. Invalid or
mismatched selected evidence fails closed; a failed request never switches to
the other discriminator. Unsupported provider operations are rejection, not
temporary unavailability.

A backup becomes durable only after upload and exact `HeadObject`
verification. Connector credentials need the narrow object-prefix and
multipart permissions plus authoritative listing needed to prove absence;
versioned providers also require version-targeted read and delete permissions.

Provider-neutral wire and MinIO evidence exercise the adapter, but Cloudflare
R2 is the explicit MVP target. A separate live R2 gate must prove conditional
put, multipart completion, delete, metadata round trips, version-present and
version-absent behavior, and conflict reconciliation before the Backup
capability is release-accepted.

Current source: [S3 adapter](../../internal/infra/s3compatible) and
[shared validation](../../internal/common/s3connector).

## Valkey authentication

Valkey creation requires the operator to choose `username_password`,
`password`, or `none`; there is no default and no inference from missing
credential bytes. The choice is immutable instance policy, is exposed equally
by Console, CLI, API, and Blueprint-derived attachments, and every Attach
inherits it.

Named mode creates one ACL identity and password per credential owner.
Password-only mode gives each owner an independent password on the shared
default user; detach removes only that password and cannot selectively close
already-authenticated default-user connections. No-auth mode explicitly
enables credential-free access and returns no role or password fact. The UI
must explain that any reachable client can access it.

All modes share the same keyspace and Pub/Sub channels; ACL identity is access
control, not data isolation. Consumer ACLs exclude administrative commands.
Groundplane uses a separate named bootstrap administrator that is never
exposed through Attach facts. ACL state is persisted to the backing volume,
initialized only when absent, and saved successfully before ready facts are
published.

Management uses discrete fixed `valkey-cli` command mode, with only the final
secret passed through stdin. Interactive stdin mode is not execution evidence
because it can exit successfully after a server error. Typed procedure and
encrypted identity state carry the explicit mode across execution; no runtime
configuration inspection invents it.

Current source: [Valkey adapter](../../internal/adapters/valkey9) and
[Agent backing authentication](../../internal/agent/backingadapter/authentication.go).

Valkey implementation is authorized, but all three modes still require
lifecycle, fact, restart, and cross-surface proof before delivery is claimed.
