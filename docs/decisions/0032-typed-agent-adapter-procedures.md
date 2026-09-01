# ADR 0032: Typed Agent adapter procedures

## Status

Accepted for the MVP.

## Context

An Attach provisions a generated identity in a Backing Service and may grant
that identity access to databases owned by other Attaches. The Controller owns
durable desired state and task publication, while the Agent owns host-local
execution. Existing adapters compile their behavior into trusted SQL or exec
steps, but the version-one Agent execution protocol had no Attach operation.

Sending adapter-produced SQL, shell commands, executable names, arguments, or
templates over the control channel would turn the Controller and its stored
state into an arbitrary command transport. It would also duplicate the adapter
implementation across the Controller/Agent boundary.

## Decision

`ExecutionPlan` gains `ATTACH` and `DETACH` operations and a closed
`AdapterProcedure` step. The step contains only:

- the registered adapter key and lifecycle phase;
- stable Attach and backing Service ids;
- bounded adapter identity values: role, password, database, and grant target.

It contains no SQL, command, executable, argument vector, environment map, or
template field. The Controller resolves names, passwords, grant targets, and
phase from durable state. The Agent resolves `adapter_key` from the same
compiled registry and renders those already-resolved typed values through
`ProvisionSteps`, `GrantSteps`, `RevokeSteps`, or `DetachSteps`. An unknown
adapter key is a permanent validation failure.

The Controller may transmit a generated password only in a sealed, transient
Attach plan. Durable Task params and materializations remain empty. The Agent
must clear password bytes and adapter step secrets after execution. Logs and
task results never include identity values or rendered adapter steps.

Plan validation binds every adapter procedure to the plan's Attach target,
requires a stable backing Service id, constrains adapter identity syntax, and
allows phases only under the matching Attach or Detach operation. The existing
deterministic plan hash authenticates the complete typed procedure.

The Controller preserves the exact `<service-name>_<random-tail-6>` identity.
The 63-byte adapter bound means credential-backed Attaches reject Service names
over 56 bytes rather than accepting PostgreSQL's silent identifier truncation.
PostgreSQL's compiled adapter delimits every identity as a quoted SQL identifier
and escapes literal bytes locally; identity values never alter the fixed SQL
statement structure. A password is exactly 32 random bytes encoded as unpadded
base64url before encryption and plan sealing.

The Controller process remains the `systemd`-managed daemon. It creates,
starts, observes, replaces, and removes the Agent OCI container and its token;
the Agent does not have an independent host service lifecycle. Token
authentication is the MVP boundary. Mutual TLS remains post-MVP.

## Consequences

- Controller compromise cannot inject arbitrary executable text through the
  Agent protocol; it can select only behavior compiled into the Agent image.
- Adding an adapter does not require a protobuf command language, but both
  Controller and Agent binaries must ship the same registered adapter key.
- Adapter identity fields are intentionally narrower than general user input.
  They are Controller-generated implementation values, not public API fields.
- Attach plan resolution and Agent execution can now be implemented against a
  stable boundary without persisting plaintext identity secrets in a Task.
