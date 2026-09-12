# ADR 0009: Replace generic execution parameters with typed payloads

- Status: Proposed
- Date: 2026-08-20

The ADR remains Proposed for the general execution catalog. Accepted [Backup contract](../features/backups.md)
accepts only its closed Agent protobuf payload requirement for backup and
restore; it does not accept unrelated process, SQL, file, Compose, or reload
payload replacements.

## Context

Groundplane's execution boundary permits only the fixed `StepOp` catalog. The
current scaffold weakens that boundary by representing every payload as
`map[string]string`. Adapter implementations consequently contain unresolved
`<role>`, `<db>`, and `<generated>` placeholders and shell command strings.
The Agent cannot validate these structures exhaustively or execute them
without interpreting arbitrary text.

Two current examples are unsafe or incorrect:

- PostgreSQL identifiers and password literals cannot be inserted into SQL by
  string replacement. `psql` provides distinct quoted interpolation forms for
  SQL identifiers (`:"name"`) and literals (`:'name'`).
- `valkey-cli --pipe` imports commands encoded in the Valkey protocol; it does
  not restore an RDB snapshot. Valkey documents RDB as a server persistence
  file loaded during startup, while `valkey-cli --rdb` is the supported remote
  snapshot export.

Sources:

- <https://www.postgresql.org/docs/16/app-psql.html#APP-PSQL-INTERPOLATION>
- <https://valkey.io/topics/cli/#remote-backups-of-rdb-files>
- <https://valkey.io/topics/persistence/>

## Proposed decision

Replace `Step.Params map[string]string` with a closed, discriminated payload
model. Each `StepOp` accepts exactly one corresponding payload type, and plan
validation rejects a missing, extra, or mismatched payload before dispatch.

The model must enforce these rules:

- Process execution carries an allowlisted executable identifier and an argv
  slice. It never carries a shell command, redirection, pipeline, or generic
  executable path.
- PostgreSQL operations carry a fixed statement template plus separately
  typed identifier and literal variables. The Agent invokes `psql` directly
  with argv and uses its quoted interpolation forms; it never concatenates SQL
  values.
- File writes carry a root-relative destination, mode, content digest, and
  bundle reference. Secret bytes are not embedded in task events or logs.
- Compose and reload operations identify only generated project/component
  artifacts from the execution bundle.
- Backup and restore use the closed protobuf payloads accepted by [Backup contract](../features/backups.md).
  Those payloads carry only stable ids, captured revisions, closed enums,
  bounded control metadata, immutable object evidence, and task-scoped secret
  slots. Artifact and secret bytes never enter generic parameters, and the
  Controller never performs S3 artifact transfer. [Backup contract](../features/backups.md) separately permits
  bounded typed validated config Entry content from Controller to Agent for
  backup and from Agent to Controller for restore; neither direction is durable
  Task/event/log data. The remaining
  operation payloads in this list stay proposed.
- The Agent switch remains exhaustive over the fixed operation catalog. No
  fallback executes unknown input.

Valkey backup and restore are not part of the accepted MVP runtime subset.
[Backup contract](../features/backups.md) requires `strategy.not_implemented` before Task creation. A later
Valkey contract cannot be inferred from this proposed ADR.

## Consequences

- Generated plans are statically inspectable and can be validated before they
  reach the host.
- Adapter code cannot introduce an arbitrary shell escape through a parameter
  map.
- The protobuf execution bundle must use matching typed payload messages.
- Existing scaffold payloads are replaced directly; no legacy decoder or
  compatibility translation is retained.
