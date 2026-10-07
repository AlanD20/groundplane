# CLI and API usage

The CLI and Console use the same Controller API. This guide explains connection,
resource selection, requests and outcomes. Each [feature guide](README.md#features)
explains its actions' consequences; [capability status](capabilities.md) identifies
unfinished or unqualified behavior.

- [Connect and configure the CLI](#connect-and-configure-the-cli)
- [Select a scope](#select-a-scope)
- [Everyday CLI workflows](#everyday-cli-workflows)
- [Call the HTTP API](#call-the-http-api)
- [Protected requests and uncertain outcomes](#protected-requests-and-uncertain-outcomes)
- [Blueprint requests](#blueprint-requests)
- [Pagination and streams](#pagination-and-streams)
- [Errors](#errors)

Use `groundplane --help` and `groundplane <resource> <action> --help` for the
installed command and flag inventory. [OpenAPI](../openapi.json), also served at
`/openapi.json` on the Controller, owns exact HTTP paths, fields and responses.
Examples here illustrate workflows rather than maintaining another catalogue.
For an installed release, use that Controller's OpenAPI and the installed CLI's
help: the repository reference can describe changes newer than your installation.

## Connect and configure the CLI

The default Controller address is `http://127.0.0.1:8080`:

```sh
groundplane host show
groundplane --host http://127.0.0.1:8080 host show
groundplane service deploy --help
```

Connect only to loopback or an explicitly configured trusted private interface.
The MVP human API has no authentication. Exposing it through an application
Tunnel is forbidden; see [product scope](mvp.md).

The CLI reads `~/.config/groundplane/config.yaml` by default. Select another file
with `--config PATH` or `-c PATH`. A missing file uses built-in defaults; unknown
keys or invalid values fail. A useful configuration is:

```yaml
host: http://127.0.0.1:8080
tenant: acme
project: storefront
env: production
output: TABLE
no_color: false
```

`--host` overrides `GROUNDPLANE_HOST`, which overrides the configured `host`.
Scope and output flags override their configuration values. There are no
corresponding Tenant, Project or Environment environment-variable overrides.
The Controller URL contains only scheme and authority, with an optional trailing
slash; do not include `/api/v1`, credentials or query parameters.

| Global option | Purpose |
| --- | --- |
| `--tenant`, `-t` | Tenant scope |
| `--project`, `-p` | Project scope |
| `--env`, `-e` | Environment scope |
| `--id` | Interpret resource targets and parent scope as stable ids |
| `--output`, `-o` | `TABLE` (default), `JSON` or `YAML`; case-insensitive |
| `--no-color` | Request plain output |

## Select a scope

Commands use top-level resource nouns followed by actions:

```text
groundplane [global flags] <resource> <action> [target...] [action flags]
```

A target is usually a scoped slug or name. Select its parents with flags or
configuration; do not put a hierarchy before the resource noun.

```sh
groundplane project list --tenant acme
groundplane environment show production --tenant acme --project storefront
groundplane service list --tenant acme --project storefront --env production
groundplane service show api --tenant acme --project storefront --env production
```

An Environment action such as `show production` or `blueprint apply production`
takes the Environment name as its positional target. Commands operating inside
the Environment, such as `service list` or `environment logs`, use `--env`.
The `environment` noun also accepts the alias `env`.

Slugs and names may be renamed. Stable ids survive a rename; the old label
stops resolving. Use `--id` for automation, replacing every supplied parent
scope with its id too. A direct Service read can bypass parent lookup:

```sh
groundplane service show svc_01J00000000000000000000000 --id --output json
groundplane service list --env env_01J00000000000000000000000 --id --output json
```

Some commands already take ids, including Task actions, Route detail actions
and Component detail actions. Backup Restore takes a stable source id (`spt_…`),
not an Attach name or Volume slug; Backup Point removal takes a stable Point id
(`rp_…`). Follow each command's help rather than assuming every positional
argument is a slug. Secret commands use Project scope, or explicit `--platform`;
there is no Environment Secret scope.

Global `--host` selects the Controller. Route creation uses `--hostname` for
the application's hostname. Exact parsing and aliases belong to [the CLI](../internal/cli).

## Everyday CLI workflows

These examples use the configuration above and existing resources. Replace
names, image references and illustrative ids with your own.

### Inspect and deploy

```sh
groundplane service list
groundplane service show api
groundplane service edit api --image registry.example/storefront-api:2026-09-29
groundplane service deploy api
```

Edit saves desired configuration; Deploy applies a Release. Deploy does not
pull or build an image: it must already exist in the host Docker daemon. To
fetch explicitly, run `groundplane image fetch <reference>` and wait for its
Task to complete before deploying. `groundplane image ls` shows local images
and removal protection. See [Services and Releases](features/services-and-releases.md)
and [host images](features/platform.md#images).

Start selects running; Stop selects stopped; Destroy removes runtime while
retaining desired configuration and data. Remove deletes the Service definition
after checked cleanup. Consult the feature guide before destructive actions.

### Follow a Task

An asynchronous command returns a `task_id` without waiting for completion:

```sh
groundplane task show task_01J00000000000000000000000 --output json
groundplane task events task_01J00000000000000000000000
groundplane task list --limit 20 --output json
```

Task actions always take a Task id, without requiring `--id`. `pending` and
`running` mean work is in progress; `completed`, `failed`, `aborted` and
`timed_out` are terminal outcomes. Check the application, Route or data after
completion. A completed Task alone does not prove application health.

`task retry <id>` creates a new eligible attempt using captured inputs.
`task abort <id>` addresses the existing Task. Closing a stream or pressing
Ctrl-C stops the client request, not the Task. Restrictions and recovery limits
are in [Tasks and logs](features/tasks-and-logs.md).

### Read workload output

```sh
groundplane service logs api --tail 100 --follow
groundplane environment logs --tail 100 --follow
```

`--tail` is 0 through 1000 lines per container and defaults to 200.
`--follow` (or `-f`) streams new lines. Logs are transient, select containers
at stream setup and cannot resume durable history after reconnect.

### Supply a Secret

```sh
groundplane secret add DATABASE_PASSWORD --value-file ./database-password.txt
groundplane secret add DATABASE_PASSWORD --value-file - < ./database-password.txt
groundplane secret show DATABASE_PASSWORD
```

Secret creation reads a file or stdin; plaintext is not accepted as an argv
value. Ordinary Secret and Attach fact reads omit or mask confidential values.
Reusable Secret reveal uses the Console or Secret API and has no CLI command.
Attach facts also have an explicit CLI read, `groundplane attach fact <attach> <key>`;
it writes the selected value, so treat its output as confidential. A masked value
is not a replacement credential. See
[Secrets and Connectors](features/secrets-and-connectors.md).

### Automate CLI output

Use `--output json` or `--output yaml` to retain structured responses, including
`items` and `next_cursor` on paginated lists. Table output is for interactive
reading. Blueprint Show writes YAML directly and rejects an explicit `--output`;
Task Events writes one JSON event per line; workload logs write log lines.
Backup key export writes the private identity directly; use its `--file` option
instead of global `--output`. See [backup key handling](features/backups.md#encryption-keys).

Exit 0 means the CLI request succeeded, including acceptance of asynchronous
work. A downstream pipe closing early also exits 0; it does not prove complete
output was consumed. Exit 1 indicates an error; Ctrl-C uses exit 130. Automation
must inspect the Task's eventual status separately.

## Call the HTTP API

The base path is `/api/v1`. OpenAPI paths are relative to that base: combine a
path such as `/services/{id}` with it exactly once. Entity path parameters and
hierarchy filters use stable ids, not CLI slugs. Obtain ids from list/create
responses. HTTP scope does not inherit CLI configuration.

Read Services in one Environment:

```sh
curl -sS --get http://127.0.0.1:8080/api/v1/services \
  --data-urlencode 'environment=env_01J00000000000000000000000' \
  --data-urlencode 'limit=20'
```

Submit a Deploy for an existing Service whose configured image is already local:

```sh
curl -sS -X POST \
  http://127.0.0.1:8080/api/v1/services/svc_01J00000000000000000000000/deploy \
  -H 'Content-Type: application/json' \
  -H 'Idempotency-Key: storefront-deploy-20260929-001' \
  --data '{}'
```

HTTP 202 returns acceptance with a `task_id`; read
`GET /api/v1/tasks/{task_id}` for progress and outcome. Some action responses
contain additional fields; keep the full response when needed.

Delete one verified Recovery Point without a request body:

```sh
curl -sS -X DELETE \
  http://127.0.0.1:8080/api/v1/environments/env_01J00000000000000000000000/recovery-points/rp_01J00000000000000000000000 \
  -H 'Idempotency-Key: storefront-point-delete-20261006-001'
```

The HTTP 202 response identifies the deletion Task. The
[Backups guide](features/backups.md#recovery-points-and-retention) owns the
operation fences, exact-object absence proof and remote-failure behavior.

| Response | Meaning |
| --- | --- |
| `200` | Read or synchronous update succeeded |
| `201` | Resource created synchronously |
| `202` | Work accepted; follow its Task |
| `204` | Synchronous success with no response body |

The operation's OpenAPI response is authoritative. Do not assume every create
is synchronous or every DELETE has completed when its response arrives.
Use `application/json` for ordinary bodies; Blueprint upload uses multipart.
Ordinary JSON request bodies are bounded to 1 MiB, with smaller field-specific
limits where declared.

## Protected requests and uncertain outcomes

1. Read the resource and its current state.
2. Choose the explicit action in the feature guide. Saving desired configuration,
   deploying it, stopping runtime and removing a resource have different effects.
3. Send one protected request. Preserve its idempotency key if acceptance is
   uncertain; do not manufacture a new request to resolve a lost response.
4. For an asynchronous response, follow the returned Task until its terminal
   outcome. HTTP 202 means accepted, not completed.
5. Check the operator-visible result. A successful Task alone does not prove
   application health, routing or data recovery.

Mutating API requests require exactly one `Idempotency-Key`: 16 through 128
characters from letters, digits, `.`, `_`, `:` and `-` (ASCII only). Generate
a unique key for each new intent and retain it with the request. Blueprint
Validate and backup-key export are read-only POST exceptions; their schema
does not require a key.

Equal replay resolves the original intent. Pending work may return
`idempotency.in_progress`; settled evidence replays the original public response,
including its Task id. Changed input under the same key returns
`idempotency.mismatch`. Terminal replay evidence is retained for 90 days;
keys are not permanent operation ids.

After a timeout or lost response, preserve the original key, stable target,
body and preconditions. Replay them instead of generating a new mutation.
Request replay resolves acceptance; **Task Retry** creates another eligible
execution attempt after failure.

The CLI generates keys automatically. Most commands have no operator-supplied
replay key, so rerunning them is a new request. Blueprint Apply exposes the
recovery flags in its guide; Image Fetch, Image Remove, Backup Restore and Backup
Point removal expose `--idempotency-key`. Supply and retain that key before
Restore or Point removal when you need to resolve uncertain acceptance. See
[Backups](features/backups.md) for Point-deletion safety and Restore source
selection, overwrite, downtime and Retry restrictions. Use the API when automation
needs explicit replay control for other implemented operations.

## Blueprint requests

For CLI commands and replay, see [Blueprint authoring](features/blueprints.md#read-edit-validate-and-apply).
For YAML syntax and decoded bundle limits, see the [Blueprint reference](blueprint.md).

Show is `GET /api/v1/environments/{id}/blueprint`. It returns `environment_id`,
`revision` and canonical YAML `document`, plus an `ETag`. Keep the exact quoted
ETag for `If-Match` on both Validate and Apply. Revision `"0"` means no desired
head. An unquoted revision, `*` or a list of revisions is invalid. A stale
revision requires rereading and reconciling the edit.

Validate uses `POST /api/v1/environments/{id}/blueprint/validate`; Apply uses
`PUT /api/v1/environments/{id}/blueprint`. Both consume the same closed multipart
bundle. Validate returns the compared revision and changes (`create`, `update`,
`remove`, `retain`), without a Task or desired revision. Apply additionally
requires an idempotency key and returns a reconcile Task.

### Upload encoding

The multipart stream begins with a `manifest` part of type `application/json`,
then one `application/octet-stream` part per declared file, in manifest order.
Parts have form names and **no filename parameter**; with curl use `<`, not `@`.
Do not add a charset parameter to either part's content type.

The manifest identifies the root path, ordered `compose_sources` beginning with
the root, explicit non-secret `interpolation`, and each file's path, part name,
byte size and lowercase SHA-256. The `files` array must be sorted by path with
no duplicates; this order is independent of Compose layer precedence.
An optional `service` names the one Compose Service to apply. Omit it for full
Apply. Validate accepts the same selector and returns it with the reviewed
changes and Service release strategies. Selection is part of request identity;
an uncertain Apply must be replayed with the same selector. See
[scope restrictions](features/blueprints.md#apply-one-service).
File parts are named `file-000001`,
`file-000002`, and so on. Bytes must match their size and digest; undeclared,
missing or trailing parts are rejected. The entire body is bounded to 2 MiB;
the manifest to 256 KiB.

For a single file at `blueprint/blueprint.yaml`, write the manifest outside the
bundle directory:

```sh
python3 -c 'import hashlib, json, pathlib
b = pathlib.Path("blueprint/blueprint.yaml").read_bytes()
print(json.dumps({"root": "blueprint.yaml", "compose_sources": ["blueprint.yaml"],
"interpolation": {}, "files": [{"path": "blueprint.yaml", "part": "file-000001",
"size": len(b), "sha256": hashlib.sha256(b).hexdigest()}]}))' > blueprint-manifest.json

curl -sS -X POST \
  http://127.0.0.1:8080/api/v1/environments/env_01J00000000000000000000000/blueprint/validate \
  -H 'If-Match: "0"' \
  -F 'manifest=<blueprint-manifest.json;type=application/json' \
  -F 'file-000001=<blueprint/blueprint.yaml;type=application/octet-stream'
```

Use `"0"` only when Show returned that revision. To Apply, use PUT on the
Blueprint path, keep the reviewed bundle and `If-Match`, and add a new
`Idempotency-Key`. Do not change files after hashing them. The CLI builds this
stream for you; the [multipart decoder](../internal/controller/handlers/blueprint_multipart.go)
enforces its encoding.

## Pagination and streams

Paginated API collections return `items` and an optional `next_cursor`.
Use the cursor unchanged on the next request with the same scope, filters and
page size. Ordinary collection limits default to 50 and allow 1 through 200.
The cursor fixes the read snapshot; an expired snapshot needs a new first page.
Non-page responses such as live image inventory have their own shape.

Task, Activity and Release CLI lists expose `--limit` and `--cursor`.
`backup points` exposes `--cursor` with a server-selected page size and no
`--limit`. Other CLI lists may only show the first page and have no continuation
flag; consult their help
and use the API for complete traversal. Task journal filters select one
hierarchy scope or `--workspace`, not both. Configured scope also counts when
checking that exclusivity.

Task events use Server-Sent Events (`text/event-stream`):

```sh
curl -N -sS \
  http://127.0.0.1:8080/api/v1/tasks/task_01J00000000000000000000000/events \
  -H 'Accept: text/event-stream' \
  -H 'Last-Event-ID: 42'
```

`Last-Event-ID` is the last received decimal sequence. Omission or `0` replays
retained events. Keep each SSE `id` for reconnect; a trimmed cursor requires a
fresh Task snapshot. The CLI event command starts from the retained journal and
has no resume flag. Workload logs offer no durable replay. See
[Tasks and logs](features/tasks-and-logs.md).

## Errors

API errors use `application/problem+json` with `type`, `title`, `status`,
`detail` and stable `code`. A representative response is:

```json
{
  "type": "about:blank",
  "title": "service.not_found",
  "status": 404,
  "detail": "Service was not found",
  "code": "service.not_found"
}
```

Branch on `code` and status rather than diagnostic wording. Malformed input
can return 400 and semantically invalid input 422 under `validation.failed`.

| Code | Operator response |
| --- | --- |
| `state.conflict` | Reread state or revision and reconcile the change |
| `resource.in_use` | Inspect dependencies or retained operation authority |
| `cursor.expired` | Start a fresh list or Task-event snapshot |
| `idempotency.in_progress` | Follow original work or replay later with the same inputs |
| `idempotency.mismatch` | Restore original replay inputs; use a new key only for a deliberate new intent |
| `task.not_retryable`, `task.not_abortable` | Read current Task and feature restrictions |
| `strategy.not_implemented` | Choose a supported strategy or source |

The CLI writes errors to stderr. A transport failure with no valid API Problem
does not establish whether a mutation happened. Use protected replay where
available; do not blindly retry every conflict or connection error.

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
