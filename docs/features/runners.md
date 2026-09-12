# Runners

## Purpose and scope

Groundplane manages persistent GitHub Actions self-hosted Runners on the MVP
host. A Runner is a Tenant- or Project-owned control-plane resource, not a
Project and not a general Agent workload. It gives workflows Docker-compatible
jobs without granting access to the host Docker daemon, another Runner, an
Environment, or reusable Groundplane credentials.

The MVP manages the local Runner runtime and its GitHub registration. It does
not generate GitHub tokens, deregister the Runner in GitHub, autoscale,
provision ephemeral pools, place Runners across hosts, support GitHub
Enterprise, or update a Runner in place. Removal is local-only; the operator
removes the separate GitHub registration manually.

This is the feature entrypoint. The exact implementation contracts are routed
to:

- [desired identity, ownership, and runtime epochs](runners/runtime.md#desired-and-lifecycle-identity);
- [write-before-mutation journal, rollback, and recovery](runners/runtime.md#write-before-mutation-ownership-journal), including [unknown results and bounded dispatch](runners/runtime.md#unknown-results-dispatch-bounds-seal-and-abort-receipt) and [restart/reboot transfer](runners/runtime.md#restart-reboot-and-bounded-storage);
- [canonical GitHub locators](runners/runtime.md#canonical-github-locator), [custom labels](runners/runtime.md#custom-labels), and [immutable dual-platform images](runners/runtime.md#immutable-runner-image-and-disabled-updates);
- [stable topology and identities](runners/runtime.md#per-runner-topology-and-stable-identities), [bounded host identity slots](runners/runtime.md#bounded-host-identity-slots), [required machine inputs](runners/runtime.md#required-supplied-inputs), and [DNS materialization](runners/runtime.md#dns-materialization-contract);
- [machine bootstrap](runners/runtime.md#machine-bootstrap-slot-roots-and-release-authority), [host control](runners/runtime.md#sealed-host-control-helper), [lifecycle order](runners/runtime.md#canonical-lifecycle-order), [network/nft isolation](runners/runtime.md#rootful-network-and-nft-policy), [systemd runtime](runners/runtime.md#exact-systemd-user-runtime), [Docker policy proxy](runners/runtime.md#rootless-docker-policy-proxy), [rootful Runner creation](runners/runtime.md#registration-document-and-rootful-runner), and [transient-token failure and ownership proof](runners/runtime.md#transient-token-failure-and-ownership-proof); and
- [detailed runtime acceptance evidence](runners/runtime.md#acceptance-evidence).

## Functional requirements

### Ownership, identity, and quota

- A Runner has stable `run_` id, mutable Tenant-unique `slug`, immutable
  canonical `github_url`, canonical custom labels, and a derived immutable
  GitHub-visible name. The image is release-selected, digest-pinned, and not a
  public input.
- A direct Runner has exactly one Tenant owner index. A Project Runner has
  exactly one Project owner index and inherits the Project's Tenant. No Runner
  has both indexes.
- A Tenant may have at most five Runner records across its direct and Project
  scopes. Provisioning, failed creation, pending removal, and failed removal
  retain their quota claim. The combined quota is not a second owner index.
- A slug edit changes only the mutable label. It does not re-register, change
  the GitHub URL/name/labels/image/owner, advance the runtime epoch, or create a
  Task.

### Local isolation and allocation

- Each Runner owns one deterministic host-identity slot: a unique unprivileged
  host UID/GID and non-overlapping 65,536-id subordinate UID and GID blocks.
  Allocation takes the lowest free slot and is persisted before host mutation.
- Each Runner owns one `/29` from `runner.network_pool`. That pool is a
  canonical IPv4 `/24`, `/25`, or `/26` and is pairwise disjoint from
  `environment_pool` and `system_pool`. No Runner subnet is allocated from
  `system_pool`.
- Each Runner has one dedicated rootless Docker daemon and socket. Workflow
  containers and service containers are children of that daemon and user
  namespace.
- The Runner never receives the host Docker socket, host networking,
  privileged mode, `CAP_NET_ADMIN`, an Agent Task channel, or a reusable
  Groundplane credential. It joins no Environment, backing, platform, or other
  Runner network and publishes no host port.
- UID/cgroup-scoped egress policy blocks all Groundplane private pools except
  the authenticated Controller endpoint while retaining DNS and ordinary
  outbound internet. Another Runner remains unreachable even when it has the
  same Tenant owner.
- Exhaustion of the Tenant quota, host slots, subordinate ranges, or Runner
  subnet inventory is `resource.in_use`. Groundplane never expands a range,
  guesses an allocation, or exposes a public identity-pool management API.

### Create and retry

- Create accepts exactly one of `tenant_id` and `project_id`, an owner-shaped
  canonical GitHub URL, slug, optional canonical labels, and a fresh GitHub
  registration token.
- Before any host effect, one Controller transaction publishes the
  `provisioning` Runner, sole owner index, combined quota claim, host identity,
  subordinate ranges, `/29`, immutable replay evidence, active operation, and
  native Controller Task.
- The API stages the raw token only in a bounded, zeroing, in-process broker.
  The native Controller executor consumes it once. The persistent Agent never
  fetches, receives, persists, recovers, or relays it, and the Runner has no
  token-pull endpoint.
- The token, its digest, and reversible derivatives never enter durable state,
  Task input, idempotency hashes, events, logs, argv, files, helper input, or an
  OCI environment. The pinned configure child may receive it only as the one
  token entry in its explicitly constructed process environment.
- The sole stdin exception is a one-use bootstrap message from the Controller
  to the pinned Runner entrypoint over Docker attach. The entrypoint passes the
  token to configure through that environment entry, not through child stdin,
  and closes the bootstrap input before Listener or job admission.
- Publication failure drops the staged token. A Controller exit before
  consumption loses it. If registration cannot be proved, the Task fails
  `registration_token_required`; recovery never guesses or reuses secret
  material.
- Failed creation retains the Runner and every allocation. Runner Retry takes
  a fresh token and the same Runner id, quota claim, slot, UID/subordinate
  ranges, and `/29`; generic Task Retry rejects Runner creation Tasks.
- Success atomically binds the exact runtime ownership evidence, changes the
  lifecycle to `ready`, and completes the Task. A transport replay returns the
  original Task and never feeds a replacement token into that attempt.

### Observation and removal

- `online` is Controller-observed local runtime state. It is never desired or
  operator writable. Missing, stale, mismatched, or drifted evidence projects
  offline.
- Remove first publishes a typed deletion tombstone, immutable cleanup intent,
  active operation, replay evidence, and native Controller Task while leaving
  the Runner and all quota/allocation fences visible.
- Removal Params retain exactly `runner_tenant_id`, `runner_owner_kind`,
  `runner_owner_id`, `runner_host_slot`, and `runner_network_cidr` so terminal
  replay can finish after the Runner record is deleted. They contain no host
  UID, subordinate range, credential, token, token digest, or derivative.
- Cleanup stops and removes only ownership-proved local Runner state. Success
  atomically deletes the Runner and sole owner index, releases quota, slot,
  subordinate identities and `/29`, clears intent/tombstone, and completes the
  Task. Failure clears the active removal fence but retains the Runner,
  ownership evidence, and every allocation for protected retry.
- Groundplane never calls GitHub to deregister on remove.

### Operator surfaces

The operator capability has 1:1 Console, CLI, and REST coverage. The exact
wire and command grammar remains in `docs/api-cli.md`.

- List supports Tenant combined-quota scope or exact Project scope; detail
  exposes public identity, owner, lifecycle, Task links, observed online state,
  and absolute observation time, but no host allocation, image, path,
  credential, or raw inspection details.
- Add/create, Runner-specific retry, slug edit, and local remove are the only
  mutations. There is no public image, placement, scale, deregister, online,
  allocation, owner, GitHub URL, GitHub name, label-edit, or runtime-update
  mutation.
- The Console shows `N / 5`, keeps provisioning/failed/deleting records in the
  count, polls authoritative Tasks, requires a fresh masked token for create
  and retry, and warns that removal is local-only.

## Non-functional requirements

- One executor owns the lifecycle: the native Controller Task executor. A
  short-lived release-pinned privileged helper is allowed only as a closed,
  credential-free implementation detail. The persistent Agent has no Runner
  create/retry/remove/inspect authority.
- Allocation, intent, Task publication, terminal state, and release are
  transactional and replay-safe. Unknown external results are observed by
  exact immutable identity within fixed dispatch and time budgets; names alone
  never authorize adoption or deletion.
- Runtime ownership is canonical, size/cardinality bounded, epoch-scoped, and
  sealed before readiness. Cleanup may touch only the sealed identities and
  deletes ownership only after journaled absence and a durable receipt.
- Same-boot restart may reuse a runtime only when all sealed ownership bytes
  match. Host reboot never reuses stale process, namespace, socket, network,
  container, or token identities; it uses the bounded epoch-transfer and
  cleanup protocol.
- Corrupt, forked, oversized, ambiguous, substituted, or mismatched ownership
  evidence fails closed without external mutation. Authentication to the
  current Controller authority is required before recovery performs effects.
- Runner images are immutable, self-update is disabled, and each release
  publishes equivalent `linux/amd64` and `linux/arm64` variants under one
  multi-platform index.

## Technical design

### Resource and lifecycle state

Desired identity, lifecycle, allocation, runtime observation, deletion intent,
RuntimeOwnership, and Task records are separate durable concerns. A
`runtime_epoch` is allocated exactly once before each attempt that may
materialize or re-attest a runtime; it never wraps or reuses an abandoned
value. The same epoch identifies every unit, process, network, nftables chain,
socket, container, path, journal, and proof for that materialization.

The native Controller executor applies create, retry, observation, reboot
reconciliation, and remove through closed repository, host-control, rootful
Docker, systemd, nftables, and rootless-runtime seams. Workload and platform
data-plane effects remain Agent-owned; Runner authority does not generalize.

### Attempt-token handoff

The API validates a fresh token, stages it in the ephemeral broker, then
atomically publishes the attempt. The executor detaches the buffer with a
one-consume operation for configure. Broker state is not recovery state:
if registration cannot be proved after process loss, the attempt fails and
requires a fresh-token Runner Retry.

This replaces the superseded Agent relay and entrypoint token-pull designs.
There is no `/internal/runner-registration/*` or
`/internal/runner-bootstrap/*` token/config protocol in the current design.

The owner-approved transport uses the Controller's existing rootful Docker
connection to send one bounded token message to the pinned entrypoint's stdin.
The exact [bootstrap transport](runners/runtime.md#one-use-bootstrap-token-transport)
binds delivery to the attempt and full container identity, closes input, and
allows the token only in the configure child's explicit environment. Owned
buffers are cleared after use; uncertain delivery is never resent.

This narrow exception avoids a separate secret-distribution service. It does
not permit a pull endpoint, Agent relay, Task/helper payload, Docker `exec`,
registration socket, token file/argv, fifth bind, stored OCI environment, or
token-bearing response. Configure and Listener stdin never carry the token.

### Allocation and cleanup

Machine bootstrap validates the three pairwise-disjoint network pools and the
finite identity ranges before mutations are served. Slot `i` deterministically
selects host UID/GID and one subordinate block from each configured range; its
allocation record stores the Runner id. The Runner subnet registry is rooted
only in `runner.network_pool`.

Every host mutation is predeclared in the ownership journal. Recovery adopts
only an exact, uniquely discovered identity; zero candidates may retry within
budget, and multiple or mismatched candidates conflict. Cleanup proceeds in
the fixed order that keeps nftables isolation installed until all Runner UID
processes and network endpoints are gone, then removes owned paths and claims.

## Acceptance

The feature is accepted only when focused automated and Ubuntu 24.04 host
evidence proves all of the following on `linux/amd64` and `linux/arm64`:

1. Console, CLI, and API have matching list, show, add/create, slug-edit,
   Runner-retry, and local-remove behavior, including exact replay and public
   error semantics.
2. Create/retry publishes the native Controller Task and every allocation
   before host mutation. Five-Runner quota and sole owner-index invariants hold
   under concurrency, failure, retry, and deletion.
3. Raw registration-token bytes and derivatives are absent from every durable
   store and log. Publication failure and restart-before-consumption erase the
   broker; the latter terminates `registration_token_required`, and retry uses
   a fresh token with identical allocations. Bootstrap input is bounded, tied
   to the exact attempt/container, consumed once and closed before jobs. Only
   configure receives the token environment entry; Listener does not. Partial
   or uncertain delivery is never resent, and recovery requires exact
   registration proof or a fresh-token Retry.
4. Each Runner receives a unique slot, UID/subordinate blocks, `/29`, rootless
   daemon/socket, rootful Runner container, network/nft policy, and immutable
   dual-platform image. Host socket, cross-Runner/private-pool reach,
   privileged mode, host networking, forbidden capabilities, and reusable
   Groundplane credentials are impossible.
5. Journal crash points, issued-unknown discovery, dispatch/deadline bounds,
   seal/receipt transactions, same-boot restart, host reboot epoch transfer,
   corrupted evidence, substitution, and complete cleanup all fail closed and
   retain ownership/allocation fences until absence is proved.
6. Removal is local-only, never calls GitHub, and releases quota, slot,
   subordinate identities, and `/29` only after complete ownership-proved
   cleanup. Failed removal remains retryable without widening its frozen
   cleanup identity.
7. The detailed evidence matrix in
   [the runtime contract](runners/runtime.md#acceptance-evidence) passes for the
   exact released image, helper/proxy/runtime artifacts, kernel, systemd,
   Docker API, and both supported host architectures.

## Current status

The product contract, Controller-owned lifecycle and one-use bootstrap transport
are accepted. Controller persistence and part of the native lifecycle exist,
but the isolated host runtime, exact release/runtime artifacts, approved token
transport implementation, full operator parity and acceptance evidence remain
incomplete. The existing stdin scaffold does not prove the bounded transport
or runtime isolation contract; missing implementation does not narrow it.

Under the current working assumption that an external builder supplies the
hosting images, Runner completion is not a Gate A or Gate B prerequisite.
Owner confirmation of that assumption remains pending in
[delivery.md](../delivery.md#delivery-state). The broader Runner capability
remains required and unfinished.
