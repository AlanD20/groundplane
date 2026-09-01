# ADR 0007: One-step Agent pairing

Status: superseded by ADR 0011 and ADR 0016

ADR 0011 replaces public one-time pairing with a Controller-provisioned
runtime token over the local Unix socket. ADR 0016 makes the native Controller
the sole owner of the local Agent container and its complete lifecycle. The
decision below is retained only as historical context and is not an MVP
compatibility contract.

## Context

The initial scaffold exposed both `agent join` and `agent approve <token>`,
while REST and the gRPC `Connect` message both appeared to consume the same
one-time token. That created an undefined two-consumer lifecycle and a pending
approval state that the MVP does not need.

## Decision

`groundplane agent join` calls bootstrap endpoint
`POST /api/v1/agent-join-tokens`. The Controller generates a cryptographically
random token, stores only its hash in etcd, and returns the plaintext exactly
once. The operator places that token in `/etc/groundplane/agent.token`.

The Agent initiates the gRPC stream and includes the token in `Connect`. The
Controller atomically validates and consumes the stored hash, creates the
Agent record, and returns Controller-owned runtime config over the stream.
Reusing the token fails. There is no pending Agent, REST pair endpoint, or
approval command.

Agent enrollment and the token-mint endpoint are machine bootstrap surfaces,
so ADR 0006 explicitly excludes them from Console/CLI/API operator parity.

## Consequences

- One token has one consumer and one durable lifecycle.
- Plaintext tokens are never stored and are returned only by the mint call.
- Pairing completion is authoritative on the Agent's gRPC connection, not an
  earlier REST request.
- Removing `agent approve` and `/agents/pair` is a clean contract replacement;
  neither receives an alias or compatibility handler.
