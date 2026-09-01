# ADR 0034: Task-owned Component candidates

- Status: Accepted
- Date: 2026-08-22

## Context

Environment Components are public desired-and-runtime records, but applying a
new Caddy or Cloudflare configuration has an external host effect. Publishing
new Component state before the Agent succeeds would make the Controller claim
that an unproven runtime is active. Caddy address allocation also has opposite
timing requirements: an enable or Zone move needs its candidate address before
dispatch, while a disable must retain its active address until removal succeeds.

## Decision

One Agent Task may own one private immutable Component candidate set for an
Environment. The active Component records remain unchanged while that Task is
pending or running. An Environment-scoped active-intent index rejects a second
candidate based on the same active state.

Candidate publication records the exact current Component record and revision,
the exact replacement, and every address transition. A newly required Caddy
address is reserved in the same transaction that publishes the candidate and
Task. Existing addresses remain reserved during execution.

The terminal Task transaction owns the outcome:

- `completed` promotes every candidate and releases only superseded addresses;
- `failed`, `aborted`, or `timed_out` retain every active Component and release
  only newly reserved candidate addresses;
- terminal replay validates the retained terminal intent and never repeats a
  Component or address mutation.

The intent is internal Task state, not a second public Component model. Its
candidate payload remains immutable when terminal status and time are added.

## Consequences

Component state cannot get ahead of Agent reality. Enabling, disabling, and
moving Caddy across Zones are restart-safe and deterministic. Environment
Component reconciliations serialize for the MVP instead of attempting to merge
concurrent candidate graphs.
