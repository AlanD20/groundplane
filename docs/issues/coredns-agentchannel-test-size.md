# CoreDNS Agent-channel test size

- Status: deferred until successful Kobwnewe hosting and fresh user authorization;
  scheduling superseded by [Deferred architecture cleanup](deferred-architecture-cleanup.md)
- Owner: future architecture-cleanup owner
- Severity: low
- MVP-required: no under the current deployment-first authority

## Evidence

The original replacement CoreDNS candidate left
`internal/controller/agentchannel/server_test.go` at 1,008 lines. The
architecture gate limits test files to 1,000 lines unless they are represented
by the finite oversized-file baseline. At that time `main` did not report this
path and the candidate did. This is test organization only and does not affect a
runtime, persistence, security, or operator contract.

That wording is retained as historical evidence. The later frozen
`787ab7a95` snapshot recorded the path at 1,581 lines and is reproduced in the
superseding issue. The gate has not been rerun on current `main`; no current
line-count finding or green result is claimed.

## Acceptance criteria

After successful hosting and fresh user authorization, move coherent
Agent-channel test groups into focused sibling test files so
`server_test.go` is at or below 1,000 lines. Do not widen the oversized-file
baseline. The focused Agent-channel race suite and `make architecture-check`
must retain no candidate-owned finding for this path.
