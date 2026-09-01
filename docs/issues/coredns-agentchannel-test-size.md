# CoreDNS Agent-channel test size

Owner: integration owner
Severity: low
MVP-required: yes

## Evidence

The replacement CoreDNS candidate leaves
`internal/controller/agentchannel/server_test.go` at 1,008 lines. The
architecture gate limits test files to 1,000 lines unless they are represented
by the finite oversized-file baseline. Current `main` does not report this
path; the candidate does. This is test organization only and does not affect a
runtime, persistence, security, or operator contract.

## Acceptance criteria

Move one coherent Agent-channel test group into a focused sibling test file so
`server_test.go` is at or below 1,000 lines. Do not widen the oversized-file
baseline. The focused Agent-channel race suite and `make architecture-check`
must retain no candidate-owned finding for this path.
