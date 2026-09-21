# Runner isolation and recovery

- Decision status: Accepted; current implementation and qualification are
  incomplete
- Scope: Local GitHub Runner identity, host isolation, secret handoff,
  ownership, cleanup, and reboot recovery

## The Controller owns one closed local runtime

The native Controller Task executor is the only Runner executor. It owns
planning, the host slot, journal, Docker calls, helper invocations, inspection,
recovery, and cleanup. The persistent Agent has no Runner Task payload,
credential, helper, journal, inspection, or host-mutation role. Untrusted
workflow code therefore never receives the Agent channel or another
Groundplane control-plane credential.

The Runner id is the stable identity. Its mutable slug is only the Groundplane
operator label; the GitHub-visible name derives from the id and is not another
input or index. One immutable canonical GitHub organization or repository URL
defines scope. Changing that scope means replacing the Runner rather than
renaming remote identity. Groundplane does not use the GitHub API as runtime
status authority and does not deregister the remote Runner.

Each host materialization receives one never-reused positive runtime epoch
before mutation. The same epoch binds the host slot, account, subordinate-id
ranges, paths, units, network, firewall policy, sockets, containers, processes,
registration evidence, and durable ownership. Desired-state changes do not
silently advance it. The release selects one immutable digest-pinned Runner
image with equivalent `linux/amd64` and `linux/arm64` children; there is no
operator image, mutable tag, ambient download, or self-update path.

## Every effect is journaled before mutation

One hash-chained, fsynced journal records the complete immutable plan, exact
resource requests, rollback requests, authenticated Controller authority,
deadlines, dispatch budgets, effect identities, observations, seal, cleanup
receipt, and terminal state. A helper or external mutator is called only after
its issued state and bounded time reservation are durable. Controller loss
removes mutation authority; it does not start autonomous cleanup.

Unknown create results use the full expected ownership tuple and bounded
zero/one/many discovery. Zero may permit a byte-identical retry, one may be
adopted only after exact inspection, and multiple or mismatched candidates are
an ownership conflict. Names alone never authorize adoption, removal, or
repair. Lost start, stop, remove, slot, helper, and host mutations are resolved
against their already-bound identities. Abort rolls back only effects that the
journal proves were issued; it never invents a new forward action.

Ready ownership is sealed only after every helper is absent and every durable
resource identity is recorded. Cleanup freezes those exact bytes, removes in
reverse dependency order, proves absence, commits the Controller receipt, and
only then deletes local ownership and garbage-collects the journal. A partial
cleanup retains the ownership and claims. This deliberately prefers an
offline/conflicted Runner to mutation of an unrelated same-name resource.

## Reboot creates a new epoch, not a replay

A same-boot restart resumes the unique journal chain and observes issued work
before retry. A pre-seal boot change enters bounded abort. A sealed boot change
never restarts an old process, namespace, socket, container, token, or epoch;
the Controller reconciles the old ownership before materializing the next
epoch.

The sole preservation exception is a Controller-authorized `replace_runtime`
transfer for exactly these seven retained runtime/registration paths: the slot root, its control
directory, the Runner home, `registration-v1.json`, `.runner`, `.credentials`,
and `.credentials_rsaparams`. This does not retain the request-only registration
token. Old ownership plus the sealed transfer remains
authoritative while old claims are removed and exact new-epoch claims are
installed. Accepting the new ownership, consuming the transfer, and retiring
the old ownership is one transaction. Crash states therefore have an old
owner, old owner plus transfer, or new owner; there is no unowned interval.
Missing registration-invalidation proof or any path/claim mismatch keeps the
old ownership and requires an explicit token-bearing recovery plan.

## Machine bootstrap is separate release authority

Authenticated one-time machine bootstrap, not the Agent, installs the offline
package closure, launchers, Docker policy proxy, security profiles, slot mount,
and fixed slot anchors. A release-bound receipt and current-boot receipt must
match before Runner work is admitted. Machine artifacts survive individual
Runner cleanup. Changing the slot pool or release contract requires zero live
ownership, claims, and journals; the MVP has no in-place migration.

The machine supplies finite host-uid, subuid, subgid, and Runner-network
ranges. One slot deterministically binds a uid/gid, complete subordinate-id
blocks, and one `/29`; allocation takes the lowest free slot and is atomic with
the network reservation. Exhaustion fails rather than expanding, probing, or
guessing. Bootstrap rejects collisions with existing accounts, subordinate
assignments, protected routes, Groundplane pools, and Docker networks.

Release-owned resolver inputs replace ambient host resolver data. The rootless
daemon gets a validated private resolver view and the rootful Runner receives
the release-owned embedded resolver as a read-only bind. Resolver sources,
mount identities, namespace identities, and bytes are part of ownership and
recurring attestation. Drift takes the Runner offline and requires epoch
reconciliation; live rewriting is not a repair path.

## Workflows get a rootless Docker capability, not host Docker

Every Runner has a dedicated host uid/gid, subordinate ranges, home, data root,
private rootless Docker daemon, rootful bridge Network, and fail-closed nftables
policy. Firewall isolation is installed before any Runner-uid process starts.
Cleanup removes the rootful Runner first, then daemon and proxy state, the
Network and user runtime, proves no process with the Runner uid while the
firewall remains installed, and removes the firewall last.

The GitHub Runner container itself is rootful only in Docker topology; its
process runs as the allocated uid/gid with a read-only root filesystem, all
capabilities dropped, no-new-privileges, the release seccomp policy, and no
host Docker socket, bus, Controller, Agent, or credential mount. Its only
runtime binds are the policy-proxy socket, Runner home, embedded resolver, and
inert registration document. Privileged Docker-in-Docker and a raw host socket
are rejected because either would let workflow code control Groundplane
resources.

The dedicated rootless daemon is exposed to the Runner only through the
release-pinned Unix-socket policy proxy. The proxy authenticates each peer by
pidfd, uid/gid, process start, cgroup, namespaces, ownership labels, uid/gid
maps, and no-new-privileges. It matches requests against a closed,
release-bound route manifest, injects the required security options and
ownership labels, and rejects privileged settings, Groundplane-reserved
labels, host networking, port publication, plugins, Swarm, BuildKit sessions,
secret/SSH mounts, and unsupported APIs.

Build contexts are fully spooled and validated before forwarding: bounded tar
or single-member gzip only, safe normalized paths, bounded expansion and entry
count, no special files, sparse entries, escaping links, cycles, or duplicate
targets. Build and object mutations are serialized, pre/post inventories bind
the authoritative final image id, and cleanup removes only proven new
unreferenced residue. Proxy-created volumes are deterministic, labeled,
journaled, and removed only after their exact parent is absent. Host binds are
restricted to owned Runner paths and held by descriptor identity across
container restarts.

## Privileged host control is closed and disposable

Host account, filesystem, systemd, nftables, process, socket, and cleanup
operations use a release-pinned, credential-free helper with an
operation-specific request, profile, mounts, capabilities, AppArmor/seccomp
policy, and typed proof. There is no shell, generic command, caller-selected
path, runtime-selected profile, or general privileged service. At most one
helper exists per Runner, and it is retired and proved absent before another
is created or ownership is sealed.

The helper request and response are deterministic, self-hashed, bounded
records. Invalid framing or schema produces no response and no mutation. Once
a request may have reached the target, the executor never reattaches or
resends it; it removes the helper, observes the exact target independently, and
continues only from a durable proof. Security profiles limit mistakes and
confused-deputy access but do not claim containment from compromised trusted
host-control or proxy code.

Exactly two disabled, `Restart=no` systemd user units run the policy proxy and
rootless Docker. Their fragments, launchers, environments, cgroups, limits,
process identities, sockets, and rootless namespace evidence are release-bound
and owned. No third socket unit, automatic restart, ambient manager
environment, or alternate launcher is accepted.

## Registration tokens are transient and one-use

The API places a fresh GitHub registration token in a bounded, zeroing,
in-process Controller broker keyed to one Task attempt. The token is absent
from desired state, Tasks, journals, ownership, events, logs, argv, files, OCI
configuration, helper input, Agent traffic, and durable container environment.

After the exact container and registration document are verified, the
Controller attaches stdin before the already-journaled start, writes one
length-prefixed token, closes input, and consumes the broker value. The pinned
entrypoint gives the token only to the direct configure child's explicitly
constructed environment, clears owned buffers afterward, disconnects stdin,
and admits the listener only after non-secret registration proof and nonce
invalidation. This is a narrow bootstrap exception, not a general secret-input
channel.

If delivery may have started, the token is never resent or recovered after a
Controller restart. Exact registration proof may allow recovery; otherwise the
attempt ends `registration_token_required`, retains its ownership for safe
cleanup, and only an explicit Runner retry with a fresh token may proceed.

## Current code does not satisfy the full accepted contract

The current implementation contains the Controller Task path, planning,
allocation, a basic rootless runtime, a revision journal, durable ownership,
and the one-use token broker. Those pieces are not evidence that the complete
release bootstrap, closed host-control protocol, proxy route policy, build
confinement, exact ownership/transfer journal, DNS contract, reboot recovery,
or dual-architecture qualification above exists.

Until those missing boundaries are implemented and accepted, code must fail
closed rather than weaken them with a host-socket fallback, durable token,
Agent relay, mutable image, privileged container, name-based cleanup, or
in-place epoch reuse. Capability status and qualification evidence, not this
accepted decision, determine what operators can rely on.

## Consequences

- Isolation costs one dedicated account, subordinate-id blocks, rootless
  daemon, proxy, Network, and firewall policy per Runner.
- A Controller restart may intentionally lose an unused registration token;
  fresh explicit retry is the safe recovery contract.
- Host-root compromise remains outside the threat model. Workflow uid code and
  nested containers remain untrusted.
- Drift and ambiguous recovery make the Runner unavailable until exact
  reconciliation; availability never overrides ownership proof.
- Rootless port publishing, Runner self-update, operator-selected names or
  images, GitHub API status, and automatic GitHub deregistration remain outside
  the MVP.

## Source navigation

Current Runner publication and lifecycle orchestration live in
[`internal/controller/runner`](../../internal/controller/runner/executor.go),
with native execution in
[`internal/controller/controllertask`](../../internal/controller/controllertask/runner.go).
Plan and allocation ownership live in
[`internal/core/runner`](../../internal/core/runner/plan.go) and
[`internal/common/runnerallocation`](../../internal/common/runnerallocation/runtime.go).

The current host runtime is in
[`internal/infra/docker/runner`](../../internal/infra/docker/runner/runtime.go),
the revision journal is in
[`internal/infra/runnerjournal`](../../internal/infra/runnerjournal/journal.go),
and durable Runner ownership is in
[`internal/infra/etcd/runner_runtime_ownership.go`](../../internal/infra/etcd/runner_runtime_ownership.go).
The one-use token owner is
[`internal/controller/runner/token_broker.go`](../../internal/controller/runner/token_broker.go).
These are navigation pointers to partial current code, not claims that the
accepted isolation and recovery design is implemented or qualified.
