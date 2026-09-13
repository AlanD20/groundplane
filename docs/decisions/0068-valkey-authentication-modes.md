# ADR 0068: Valkey backing authentication modes

Status: Accepted for implementation; delivery proof pending

## Context

Password-only Redis clients cannot authenticate with a named Valkey ACL user.
Changing the application client is not a substitute for Groundplane supporting
the three authentication modes explicitly requested by the owner.

## Decision

Valkey backing creation requires an explicit `authentication`: `username_password`,
`password`, or `none`. No mode is a default. Missing, empty, null and unknown
values fail validation before any resource or Task publication. The Console
starts with no selection and cannot submit until the operator chooses a mode.
This is immutable instance policy, persisted on its
adapter Service and exposed by backing reads. Other adapters reject this input.
Every Attach inherits the instance mode; no Attach may weaken instance policy.
Console creation, CLI `--authentication`, and the existing create API expose
the same choice. Blueprint attachments inherit the selected backing policy.

Named mode creates one named ACL identity/password per credential owner.
Password-only mode adds one independent generated password per credential owner
to the shared `default` user. Detach removes only that owner's password, never
the default user or another password. Existing-credential dependencies retain
their current direct-owner and detach protections. Removing a password prevents
new authentication; it cannot identify or selectively disconnect connections
already authenticated as the shared default user.

No-auth mode explicitly enables the instance's default user without AUTH.
Its attachments provide HOST, PORT and credential-free URL facts, not ROLE or
PASSWORD facts, and generate no consumer credential. A self-owned binding still
anchors fact reuse. Attach/detach only manage membership; they never change the
instance's authentication policy. Any reachable client can access this instance;
the Console must explain this before creation.

All modes share the instance's keyspace and Pub/Sub channels; named identities
are not data isolation. Consumer ACLs permit data operations but not `@admin`
operations. A separate named Groundplane bootstrap administrator is never
exposed through Attach facts. Compiled adapter management uses that identity.
ACL changes must be saved to the backing data volume and survive restart;
bootstrap initializes the ACL file only when absent. Existing files are retained.
Management commands run in discrete `valkey-cli -e` command mode, with `-x`
transporting only the final secret token through stdin. The CLI stdin REPL
returns success even for server errors and is not valid execution evidence.
An unsuccessful `ACL SAVE` fails the operation; it must not publish ready facts.

Facts use `redis://user:password@host:port`, `redis://:password@host:port`, or
`redis://host:port` respectively. Named mode includes ROLE; password-only does
not. Credential-bearing URLs/passwords retain encrypted/reveal-only treatment.
No-auth URLs are not secret. Immutable typed procedure and encrypted identity
state carry the selected mode, validated before Agent effects. No mode is
inferred from missing password bytes or current mutable runtime configuration.

## Consequences

Changing a mode requires a new backing instance, not an implicit downgrade.
Existing explicit policies are retained; omission cannot select or migrate one.
This replaces the old requirepass-only bootstrap, not a compatibility layer or
repair of existing frozen QA Tasks. All three modes need lifecycle, fact,
restart and cross-surface proof before the source is deployed.

## References

- https://valkey.io/commands/auth/
- https://valkey.io/commands/acl-setuser/
- https://valkey.io/commands/acl-save/
- https://valkey.io/topics/cli/
