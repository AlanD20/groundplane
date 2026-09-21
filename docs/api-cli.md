# CLI and API usage

The Console and CLI call the same Controller API. Use
`groundplane --help` and `groundplane <resource> <action> --help` for the
installed command and flag inventory. [OpenAPI](../openapi.json), also served at
`/openapi.json`, owns exact HTTP paths, fields and responses.
Do not maintain a second handwritten endpoint catalogue.

## Select a scope

Resources are top-level nouns: `service`, `zone`, `entry`, `script`,
`backup`, `component`, and so on. Select their parents with
`--tenant`, `--project` and `--env`. Targets normally use scoped slugs or names;
`--id` selects stable ids for automation. Renaming invalidates the old label,
not the id.

```sh
groundplane host show
groundplane service list --tenant example --project app --env production
groundplane service show api --tenant example --project app --env production
```

Global `--host` selects the Controller, not an application hostname.
Route creation uses `--hostname`. `--config` selects CLI configuration;
`--output json` or `--output yaml` is useful for automation. Scope flags override
configuration, which overrides positional scope. Exact parsing and supported
aliases belong to [the CLI](../internal/cli).

Connect only to loopback or an explicitly configured trusted private interface.
The MVP human API has no authentication. Exposing this API through an application
Tunnel is forbidden.

## Perform an operation

1. Read the resource and its current state.
2. Choose the explicit action in the feature guide. Saving desired configuration,
   deploying it, stopping runtime and removing a resource have different effects.
3. Send one protected request. Preserve its idempotency key if acceptance is
   uncertain; do not manufacture a new request to resolve a lost response.
4. For an asynchronous response, follow the returned Task until its terminal
   outcome. HTTP 202 means accepted, not completed.
5. Check the operator-visible result. A successful Task alone does not prove
   application health, routing or data recovery.

Mutations use `Idempotency-Key` where required by their schema. Equal retries
return the original result; changed input under the same key conflicts.
Read/download exceptions follow the schema, not a blanket mutation wrapper.
Blueprint Validate and Apply also require the exact last-read quoted `If-Match`
revision. A stale revision requires rereading and reconciling the edit.

API errors use `application/problem+json` with stable dot-namespaced codes.
Branch on those codes rather than private diagnostic text. A conflict may require
a fresh read or an explicit operator decision, not an automatic retry loop.

## Common workflows

| Intent | Guide |
| --- | --- |
| Inspect, validate or apply a Blueprint | [Blueprint authoring](features/blueprints.md) |
| Deploy a tag, preview rollback or run a Release Group | [Services and Releases](features/services-and-releases.md) |
| Follow progress, Abort or Retry | [Tasks and logs](features/tasks-and-logs.md) |
| Read current Service/Environment output | [Transient logs](features/tasks-and-logs.md#logs) |
| Create shared infrastructure and connect a consumer | [Backing services](features/backing-services.md) |
| Configure Caddy or CoreDNS | [Components](features/components.md) and [templates](features/router-template.md) |
| Configure or update the host processes | [Platform](features/platform.md) and [updates](features/upgrade-safety.md) |

Lists use cursor pagination. Keep the original scope and filters while following
a cursor; changed queries or expired snapshots require a new first page.
Task event streams support sequence-based resume. Workload logs are transient
and do not support durable replay.

Read responses mask secret values. Supply secret creation input through the
documented file/stdin controls rather than argv. Explicit Secret/Attach fact reveal
uses the Console and resource-specific API; there is no CLI reveal command.
Do not treat a masked value as a replacement credential.

## Surface parity boundary

Every operator-facing Controller capability has one Console action, one CLI
command and one API endpoint, except the explicit sensitive-value reveal policy
above. A generated client is not proof that the Console action is connected.

The following non-product operations are outside that parity set:

- Local process commands: `controller serve` and `agent-run run`.
- Same-host diagnostics: `controller key show` and `controller etcd show`.
- Local CLI tooling: `version` and `completion`.
- Machine bootstrap: immutable release staging and the fixed private
  Controller startup/recovery modes. These are not binary-upload APIs.

Agent enrollment and normal Controller/Agent updates are operator capabilities,
not bootstrap exemptions. Internal maintenance Tasks are visible in Activity but
do not imply a public mutation endpoint. Adding an exception needs a product
decision.

Components use the public typed SDK, not private HTTP routes or the CLI.
Ordinary collections exclude Component-managed resources; the Component owns
their management. A known-id read does not grant a direct mutation right.

## For maintainers

Editable HTTP models and handlers live in [public API types](../pkg/api) and
[Controller modules](../internal/controller). Regenerate OpenAPI and both clients
after an approved wire change. Feature guides own the operator consequences;
[architecture](architecture.md#api-contracts-locked) owns generation boundaries.
