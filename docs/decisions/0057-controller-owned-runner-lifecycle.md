# ADR 0057: Controller-owned Runner lifecycle for the MVP

Status: Accepted for the MVP

## Context

The accepted modular-monolith architecture assigns isolated Runner runtime
lifecycle to the native Controller Task executor. ADR 0050 predates that
closure and assigns parts of the same lifecycle to the persistent Agent,
including an Agent execution payload and token-delivery path. Those clauses
conflict with the authoritative MVP contract and would add a second owner for
Runner isolation before the MVP is complete.

Runner registration tokens are transient secrets. A create or retry decision
must be durable before host mutation starts, but the raw token must never be
written to etcd, Task input, events, logs, or another durable store.

The MVP must run on both supported Linux host architectures. A Runner release
that is restricted to ARM64 would prevent deployment on x86_64 hosts.

## Decision

For the MVP, the native Controller Task executor exclusively owns Runner
create, retry, and remove host effects. This is the closed Runner exception in
ADR 0056; it is not general authority for workload or platform data-plane
operations.

The lifecycle is:

1. The human API validates the Runner request and a fresh GitHub registration
   token.
2. An in-process, zeroing token broker stages the raw token for the prospective
   Task attempt.
3. The Controller atomically reserves quota, host identity, rootless UID/GID
   ranges, and one dedicated `/29`, persists Runner intent, and publishes the
   native Controller Task. Publication failure immediately drops the staged
   token. The broker is ephemeral and is not a recovery mechanism.
4. The native Controller executor consumes the token once and applies the
   closed Runner lifecycle through the existing Runner lifecycle and runtime
   seams.
5. The Task terminal transaction records the durable outcome and releases or
   finalizes allocation state according to the accepted Runner transition.

If the Controller exits before consuming the token, the token is lost by
design. Recovery fails closed with `registration_token_required`; the operator
uses the existing retry action with a fresh token. The Controller never asks
the Agent to recover, fetch, or persist the token.

Each Runner keeps its existing isolation contract:

- a dedicated rootless Docker daemon and socket;
- one dedicated `/29` allocated from `runner.network_pool`, which is reserved
  from `system_pool` at bootstrap;
- a unique host identity and non-overlapping subordinate UID/GID ranges;
- no host Docker socket; and
- no network reachability to another Runner, including a Runner owned by a
  different tenant.

The persistent Agent does not create, retry, remove, or inspect isolated
Runner runtimes. It continues to own workload and platform data-plane host
effects. A short-lived privileged or root helper is permitted only as a
closed implementation detail of the native Runner runtime procedure; it does
not become a new executor or product service.

The release-owned Runner image is immutable and digest-pinned at execution.
The release must publish equivalent `linux/amd64` and `linux/arm64` variants.
Architecture selection is a release/runtime concern and does not change the
Runner API, Blueprint, allocation model, or Console behavior.

## Supersession

This decision supersedes ADR 0050 only where ADR 0050:

- assigns Runner host effects to the persistent Agent;
- requires an Agent Runner execution payload or Agent token-pull path;
- makes the Agent the recovery owner for a lost registration token; or
- restricts the Runner release to `linux/arm64`.

ADR 0050 remains authoritative for Runner product behavior, allocation,
isolation, security boundaries, lifecycle ordering, and terminal cleanup where
it does not conflict with this decision, `docs/mvp.md`, or ADR 0056.

## Consequences

- Runner create, retry, and remove remain durable Controller Tasks with one
  executor and one terminal protocol.
- Registration-token loss after a Controller restart is explicit and
  operator-recoverable rather than hidden by durable secret storage.
- No Runner lifecycle RPC is added to the Agent protocol for the MVP.
- Existing Controller Runner lifecycle code is completed rather than replaced
  with an additional brokered Agent subsystem.
- Workload and platform mutations remain Agent-owned; this decision cannot be
  used to move them into the Controller executor.

## MVP acceptance

The Runner vertical is complete when focused proof demonstrates:

1. API, CLI, and Console parity for create, retry, edit, and remove.
2. A create or retry publishes a native Controller Task before host mutation.
3. The raw GitHub registration token is absent from durable state and logs.
4. Restart before token consumption terminates with
   `registration_token_required`, and retry accepts a fresh token.
5. The five-Runner tenant quota and all allocation claims remain atomic.
6. Each Runner receives a unique `/29`, host identity, rootless daemon, and
   non-overlapping subordinate ID ranges.
7. Runners cannot reach each other and never receive the host Docker socket.
8. The same release path works on `linux/amd64` and `linux/arm64` hosts.
