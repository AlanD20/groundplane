# ADR 0055: Revision-bound Network observation and backing Blueprint scope

- Status: Deferred post-MVP
- Date: 2026-08-25

## Current boundary

The MVP does not select an existing Zone when it creates a Backing Service.
[ADR 0037](0037-backing-service-network-selection.md) owns the current atomic
create contract: each new Backing Service creates and owns one new dedicated
Zone inside its explicit Environment pool. There is no uploaded backing
Blueprint or authored external-network alternative in that operation.

This document preserves constraints for a possible post-MVP existing-Zone
design. It does not authorize implementation, reserve wire fields, fix storage
schemas, change Blueprint grammar, or claim acceptance.

## Why the design is deferred

An existing Docker or Moby Network cannot by itself authorize reuse. The
runtime can report Engine facts, but it does not know the current etcd Zone
revision, Blueprint head, Controller projection, mutation fences, or deletion
state. A Task result or a last-known-good snapshot can also become stale after
the physical network changes.

The former proposal tried to close observation wire messages, persistent
snapshots, a 512-Zone limit, parser scope, facade fields, and Backing Service
admission in one implementation specification. Those choices were not
accepted, were not implemented or qualified by the proposal, and are too broad
to retain as historical execution instructions.

## Requirements for a future decision

Any future existing-Zone feature must make one accepted product decision that
closes all of these constraints together:

1. The Controller remains the sole admission authority. It issues an
   authenticated, fixed-revision request that identifies the exact durable
   Zone, owner, projection, render generation, mutation epoch, and relevant
   operation and deletion fences.
2. The Agent returns actual stable Moby list-and-inspect facts. It must not
   copy expected labels into an observation or claim that Docker can attest an
   etcd revision.
3. Only a complete, fresh observation may authorize selection. Missing,
   collided, invalid, drifted, expired, wrong-owner, or wrong-revision evidence
   fails closed. New bad evidence must not fall back to an older good result.
4. Final Backing Service publication uses one fixed-revision compare-and-swap
   over the selected durable records, observation authority, and all required
   absent fences. A failed comparison starts a new attempt; it does not swap in
   newer evidence under an existing protected intent.
5. The new backing Environment still receives an explicit globally
   non-overlapping network pool. The Controller must not derive or omit that
   pool from an externally owned Zone.
6. Physical external membership has one authority. It is generated from the
   accepted Backing Service/Attach decision, not accepted through a competing
   authored external-Network grammar or a compatibility alias.
7. Runtime observation remains private authenticated Agent evidence, not a
   public refresh capability. Public API, CLI, and Console surfaces expose only
   stable product identities and failure categories, never Docker ids, private
   labels, observation keys, digests, revisions, leases, or topology evidence.
8. API, CLI, Console, Blueprint parsing, rendering, Controller persistence,
   Agent wire behavior, retries, and host proof must change in one coherent
   vertical. A new field or parser branch alone cannot make selection safe.

## Decisions still required

Before this can leave Deferred status, an accepted design must choose and bound:

- which existing Zone owners may be selected and the dependency/deletion rules
  created by sharing them;
- the authenticated observation request and response shape, freshness window,
  count and size limits, scheduling, and reconnect behavior;
- immutable snapshot publication, expiry, corruption handling, and the exact
  admission compare set;
- the canonical public network identity and whether a new facade field is
  required;
- any backing Blueprint scope or parser changes, including how they avoid a
  second body or external-network grammar; and
- focused protocol, persistence, race, parser, public-parity, and real-host
  acceptance evidence.

The earlier candidate name `backing_network_id`, candidate schema-1 field
numbers, record paths, transaction arithmetic, 512-Zone ceiling, and proposed
`kind: backing` parser details are not accepted requirements. They may inform a
new review, but they must be re-evaluated against the then-current wire,
Blueprint, facade, and host contracts.

## Consequences

Current Backing Service creation remains narrow and deterministic: one new
backing-owned Zone, no existing-Zone selection, and no observation dependency.
A later design cannot use stale runtime evidence or silently widen the current
create request. Deferral leaves no claimed implementation or acceptance proof.
