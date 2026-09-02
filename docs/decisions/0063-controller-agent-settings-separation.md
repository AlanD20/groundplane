# ADR 0063: Separate Controller and Agent settings from Host health

- Status: Accepted
- Date: 2026-09-01

## Context

The Platform Host page mixed a live host-health read model with Controller
startup guidance, Agent lifecycle actions, and a second copy of the Agent
configuration editor. This obscured resource ownership and made Controller
configuration read-only even though it is an operator-managed MVP input.

Controller and Agent configuration have different application semantics. The
Controller is a native systemd process whose configuration is read before
bootstrap dependencies exist. The Controller-owned Agent is a container whose
durable runtime configuration is reconciled without restarting running Tasks.

## Decision

Platform Host is health-only. It may link to Controller and Agent resources but
does not mutate or duplicate their settings.

The Console has dedicated `/platform/controller` and
`/platform/agents/{id}` pages. The existing Agent config singleton remains the
only Agent settings authority and keeps immediate drain-and-reconfigure
semantics.

The Controller page edits the exact native startup YAML through
`GET /controller/config` and `PUT /controller/config`. CLI mirrors these as
`controller config show` and `controller config set --file PATH`. A response
contains the absolute path, exact content, `sha256:` revision, and
`restart_required`.

Replacement accepts exact YAML plus the revision last read and a required
idempotency key. It validates with the same defaults, strict known-field
decoder, single-document rule, and semantic validator used at startup. The key
is durably bound to that exact expected revision and YAML intent before file
publication. Exact retries replay the stored response; same-key,
different-intent requests fail. Publication uses an atomic same-directory
temporary-file rename, file mode `0600`, and directory sync. Stale
replacement fails with a state conflict.

Saving changes the file immediately but does not mutate the running process.
`restart_required` is true whenever current bytes differ from the process
startup snapshot and becomes false after restart or an exact revert.

## Consequences

- Controller and Agent settings have one Console placement each.
- Host remains the bounded non-persistent read model accepted by ADR 0044.
- Operator-authored comments and formatting survive Controller config edits.
- The API does not claim unsafe partial hot reload of startup-only settings.
- The Controller bootstrap document remains outside etcd and in recovery
  exports; making it editable does not turn it into desired state.
