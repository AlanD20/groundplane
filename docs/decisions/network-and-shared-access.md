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

## Environment DNS ownership

Status: accepted design; authoring, stable record identities, shared rendering,
atomic claims, resolver-backed settlement and update scheduling are locally implemented.
Scoped direct edits, activation reads and Console/CLI/API actions are locally
connected, as are resolver-backed direct-removal receipts and Blueprint admission.
These paths are not behavior-qualified. Container DNS opt-in is separate,
unfinished work.

An Environment owns a record regardless of its chosen hostname namespace.
Namespace labels are captured decisions, not a live projection of renamable
owners. This prevents a rename from silently changing published names or a
shorter Tenant namespace from widening Service lookup.

Record intention lives in the same immutable Blueprint desired input as other
Environment decisions, not a second editable DNS store. Canonical export retains
namespace labels; selected-Service Apply compares DNS as shared state. Resolver
selection reads that head and its Zone identities at one revision, including
unrendered heads. Missing effective runtime is fenced absence, not permission to
select a preceding Blueprint's runtime.

A direct DNS edit uses the existing metadata-revision publisher, not full Apply.
It changes one stable record, preserving unrelated intentions, value generations
and rendered runtime bytes. Its head comparison, hierarchy fence, hostname claim
and replay response commit together. Namespace edits capture current owner labels;
ordinary target edits retain the existing namespace through hierarchy renames.
Only the edited name needs a new claim, keeping single-record transactions bounded.
The shared resolver loop retires obsolete claims after matching successful input
and observation, including edits with no Blueprint parent. Waiting records still
retain their names. Storage publication does not itself mean DNS activation.

Metadata-only removal has no Blueprint parent. After successful resolver
acknowledgement and hostname retirement, reconciliation records absence in the
existing unit ledger using that real resolver Task and configuration proof.
It neither invents an Agent assignment nor rewrites immutable ownership. The
receipt lets revision retention release the omitted source; outstanding unit
claims prevent competing settlement. One receipt per pass bounds mutation size.
Operator reads use one storage revision and report saved/applied/failed states
separately, linking the actual resolver Task rather than returning a synthetic
Task from synchronous edits.

Record intention, hostname ownership and acknowledged answers are separate.
A missing Service retains its name reservation without an answer. Rebinding uses
the replacement's stable id and acknowledged proxy; desired or candidate runtime
is not serving evidence. The last-applied resolver retains its exact answer set
so removing desired metadata alone cannot prove withdrawal.

The platform renderer and configuration preview use one combined selection of
platform records and Environment records. Sealed resolver inputs retain each
binding's authored revision; successful observations retain those captures and
the exact rendered answer set. A waiting record cannot be supplied an answer
by an unrelated Route or platform record.

Hostname claims commit with native desired-state, parent Task and idempotency
publication. Superseded claims stay reserved until exact withdrawal is proved;
retirement joins the current head and resolver observation in bounded transactions.
DNS units wait on the existing shared resolver Task, rather than dispatching no-op
children. Successful settlement retains its exact acknowledged binding; failed
automatic requests remain available for explicit Retry rather than looping.
Platform records and Routes compare the same claims. Route-name changes also
rewrite one shared generation in their publication transaction: this closes the
race with a claim scanning a newly created Environment. Namespace changes retain
the old claim until its answer has been withdrawn.
Shared candidate validation also rejects a DNS/Route hostname overlap inside one
Blueprint; checking only previously stored claims cannot detect two new competing
declarations. The registry name is reserved at name validation, not only when a
Service becomes resolvable.

Direct Service removal uses its existing durable removal intent to select a
waiting record. It waits outside the Agent queue's runnable selection, so the
resolver Task can withdraw the answer before workload cleanup starts. Address
release checks the acknowledged resolver again. Failure or deadline expiry before
dispatch terminalizes removal without deleting the Service or freeing its address;
it does not fabricate an Agent assignment or restoration result. Platform DNS
still requires an explicit record edit before accepting Service removal.

Resolver terminal reconciliation compares Environment bindings as well as Route
projection changes. Automatic attempts are recorded by the native Task publisher,
including terminal successors, rather than only by the periodic scheduler.

Runtime intent is not activation proof. Lifecycle admission retains its Task id
in the Service's operational state until that exact Task succeeds or a subsequent
Deploy is acknowledged. Failed, aborted and pending lifecycle work cannot make
an older proxy record eligible merely by requesting running intent. DNS selection
compares this operational state alongside the immutable runtime. Stop/Destroy
withdraw Environment answers while retaining their names and address reservations;
successful Start can restore the answer. An ineligible same-named declaration
waits rather than blocking the resolver for unrelated Environments.

Owning sources: [DNS intention](../../internal/common/dnsrecords/environment.go),
[direct desired edits](../../internal/infra/etcd/desiredauthoring/dns.go),
[target selection and claims](../../internal/infra/etcd/dnsrecords), and
[acknowledged resolver answers](../../internal/infra/etcd/platformcomponents/observation.go).

## Internal-Zone resolver access

The selected topology uses each opted-in Zone's existing bridge gateway, not an
additional network. Docker keeps its embedded Service/alias resolution and sends
unanswered queries from the container namespace to that on-network upstream.
[Moby's resolver](https://github.com/moby/moby/blob/v27.0.3/libnetwork/resolver.go)
permits this on internal networks while refusing host-namespace forwarding that
would bypass their isolation. The execution boundary requires Docker 27 or newer
and verifies the bridge's reserved gateway.

CoreDNS must bind only the captured, materialized private gateways alongside its
existing private listeners. A wildcard public listener, an outbound bridge or an
independent resolver would change the access boundary and is not this design.
The [listener source](../../internal/controller/dnsresolver/zone_listeners.go)
combines current authored heads with exact owned bridge inspection. Publication
compares those heads and the shared DNS generation; the immutable render input
includes the selected addresses. Failed requests stay suppressed for unchanged
inputs, and a failure from an older DNS generation cannot fail a newer Apply.

The Agent checks local answers over UDP and TCP on every captured private bind
before acknowledging the rendered configuration. Full Blueprint Service-child
publication captures that acknowledgement; waiting stays in the Controller so a
single-worker Agent can still execute the resolver Task. Existing containers
change DNS only through their configured deployment strategy.

An admitted native Zone-removal fence withdraws its listener even while direct
removal retains the old desired head. The queue skips that removal until CoreDNS
acknowledges withdrawal, leaving a single-worker Agent free for the resolver Task.
Successful finalization compares the acknowledgement again before releasing the
subnet. A later unrelated resolver Task is permitted only when its captured
forward input excludes the gateway and its recovery predecessor matches that
same acknowledgement. Empty runtime metadata alone cannot prove resolver absence;
the native bootstrap marker or a completed shutdown supplies that evidence.
Failure retains the resource and its reservation. Failed requests remain suppressed
for unchanged removal fences; a new admitted fence cannot reuse an older failure
as its own outcome. [Withdrawal coordination](../../internal/infra/etcd/zone_removal_dns.go)
owns these comparisons. This path is local and not behavior-qualified.

Direct releases retain their native operation instead of invoking Blueprint Apply.
Their atomic publication includes a hidden network-only Task derived from the
already captured Compose artifact. The workload waits in the queue until that
Task completes and CoreDNS acknowledges the selected gateways. Claim publication
compares both facts; later desired edits cannot change the captured network input.
Native release Retry remains recovery-only and does not repeat this forward
prerequisite. Known no-effect failures use the native release finalizer, and a
bounded durable scan resumes cleanup after a Controller interruption. Task
retention keeps the prerequisite available until its parent retires.
[Prerequisite coordination](../../internal/infra/etcd/release_network_claim.go)
owns the queue evidence. Interrupted preparation uses the existing managed-network
recovery machinery with an explicit read-only preparation variant, separate from
network removal. Its captured plan pairs each forward Ensure with an observation
step. Recovery accepts absence or exact ownership and configuration; it does not
create, remove or disconnect a network. Durable completion of every observation
is required before the original failure settles. Conflicting or unreadable state
keeps ownership restricted. Task presentation reads these prerequisite facts at
the same revision as the parent rather than inventing workload progress.
This path is local and not behavior-qualified.

The [Zone resolver decision](../../internal/common/zonedns/configuration.go)
and [typed CoreDNS input](../../component-sdk/dnsresolver/types.go) define the
selected topology; compilation is not connectivity qualification.

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

Current source: [Valkey adapter](../../internal/adapters/valkey) and
[Agent backing authentication](../../internal/agent/backingadapter/authentication.go).

Valkey implementation is authorized, but all three modes still require
lifecycle, fact, restart, and cross-surface proof before delivery is claimed.
