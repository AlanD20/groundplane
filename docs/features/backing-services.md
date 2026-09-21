# Backing services and Attaches

Backing services provide shared datastores and caches without creating a second
resource model. A Backing Service is the facade over one Platform-owned
hierarchy:

```text
backing Project -> Environment named main -> adapter-backed Service
```

Backing Services run shared workloads. Built-in adapters add convenient
provisioning; provisioning is not what makes a workload a Backing Service.
The supported choices are PostgreSQL 16, Valkey 9, and Custom. Custom runs an
operator-selected container image under GP. Without hooks, an Attach only
connects its consumer Service to the backing network. It creates no credentials
or facts. Custom does not inherit database grants or managed Backup support.

Backing creation does not accept an uploaded Blueprint, an existing Zone, an
operator-selected PostgreSQL image, or runtime plugins. Existing-Zone selection
is deferred post-MVP; [Backing Service provisioning](../decisions/backing-service-provisioning.md#network-selection-is-intentionally-narrow)
records the constraints that a future decision must resolve without making
that path current behavior.

The exact public operations are owned by [the API and CLI
contract](../api-cli.md). Desired-state Attach syntax is owned by [the
Blueprint contract](../blueprint.md).

## Operating a backing service

### Creation and lifecycle

Backing creation is one protected atomic operation. The operator supplies the
Project slug, name and optional description, one adapter key,
the `main` Environment network pool, and one new Zone's name, subnet, and
`internal` decision. Valkey also requires an explicit immutable authentication mode.
Custom requires an image. It does not invent a data Volume, credentials, exposed
ports, or a healthcheck for an arbitrary image. Its Service name is the Project
slug; the display name remains independent.

The Controller publishes exactly one backing Project, one `main` Environment,
one dedicated backing-owned Zone, one adapter Service, one adapter-defined data
Volume for built-in database adapters, and one Agent Task. The Zone subnet must be canonical IPv4, inside the
new Environment pool, and globally unreserved. A conflict commits none of the
aggregate; protected replay returns the original ids and response.

Start, Stop, and Destroy change only the adapter Service's runtime intent under
the current MVP and API contract. Destroy removes runtime, not the Project,
Environment, Zone, Volume, Entries, Attach history, credentials, or data. There
is no Backing Service DELETE endpoint.

Permanent Backing deletion is outside the current MVP, by owner decision on
2026-09-12. Start recreates runtime using the retained configuration and data.
A future permanent Delete needs a separate Console action, CLI command and API
operation with impact preview, confirmation and explicit approval. The
[deferred safeguards](../decisions/resource-deletion.md#permanent-backing-deletion-remains-deferred)
do not authorize a route or a change to Destroy.

### Unified provisioning and optional Custom hooks

Built-in adapters and Custom hooks implement the same logical input/output
contract. Compiled adapters consume typed inputs; commands receive their
environment-variable representation. Results are declared string facts with
sensitivity metadata, not text scraped from diagnostic logs.

Custom hooks are optional: `attach`, `detach`, `before-stop`, and `after-start`.
Restart uses before-stop followed by after-start. These belong to GP-requested
operations, not autonomous container restarts or host boot. An omitted hook is
a no-op. A hook failure blocks its operation. The operator owns command
correctness and idempotency; GP owns targeting, time bounds, recording, and
result validation. An interrupted custom command must not be silently executed
again; retry is an explicit operator action.

Configuration uses `attach`, `detach`, `before_stop`, and `after_start` fields,
each with a literal `command` argument array and an explicit `timeout_seconds`
from 1 to 900. The image must provide `/bin/sh`, `timeout` with kill-after
support, `mktemp`, `chmod`, `cat`, and `rm` for bounded execution and private
result-file handling. These requirements do not install software into the image.

`inputs` declares keyed plain values, Secret references, or generated Attach
passwords; exactly one source is required per key. GP supplies `HOST` as the
stable backing endpoint, so operators cannot override it. Generated inputs are
Attach-scoped; configurations with lifecycle hooks cannot declare them.
`facts` declares the exact output keys and their `secret` classifications.
There is no inferred custom port, username, database or connection URL.
Secret references resolve in the backing Project's scope, not the consumer's.
Their exact values follow the existing [Secret pin and deletion
rules](secrets-and-connectors.md): a hook cannot introduce an untracked retained
copy that survives permitted deletion of its source Secret.

Inputs use `GP_EVENT` and `GP_BACKING_SERVICE_ID`. Attach/detach also receive
`GP_ATTACH_ID`, `GP_TENANT_ID`, `GP_PROJECT_ID`, `GP_ENVIRONMENT_ID`, and
`GP_SERVICE_ID`. These are stable consumer identities, not mutable slugs.
Lifecycle hooks have no consumer context. Resolved provisioning inputs use
`GP_INPUT_<KEY>`; previous facts use `GP_FACT_<KEY>`. GP must not inject the
consumer's complete configuration or secrets. Values enter the process
environment, never interpolated command source. Hook commands run inside the
selected backing container, not on the host.

The command writes its result to `GP_RESULT_FILE`. GP reads each line and
splits at its first `=`. The key precedes it; every remaining character is the
literal value. Blank lines are skipped, empty values are allowed, and quotes,
comments, substitutions, and escapes have no special interpretation. Keys must
be declared and unique; missing separators, missing declared facts, and
undeclared keys fail the result. Values are single-line. Result size is bounded
to 64 KiB. The result file is never sourced as shell code.

Fact declarations, not command output, determine sensitivity. Only successful
attach provisioning may publish new facts, after the complete result validates.
Secret values remain outside public Task events and diagnostic output; the
Controller encrypts stored fact values. Detach receives the saved provisioning
facts. Lifecycle hooks cannot rotate consumer credentials by returning facts.
GP removes its private temporary result files on success and failure.

For this implementation, create new hook-based credential owners through
standalone Attach, then reference their ready facts or reuse that owner from a
Blueprint. Blueprint creation of a new custom Attach requiring an attach or
detach hook is deferred. A single Blueprint apply cannot both produce new
custom-hook facts and use them in configuration files:
the files' contents are sealed before execution, while hook output is known
only after execution. Reject that combination before publishing changes, with
guidance to finish the Attach first. Existing ready Attach facts remain usable.
Single-apply support is deferred; it requires a separately designed change to
Task execution and interruption handling, not a mutable-plan workaround.

### Attach identity and credential reuse

An Attach connects exactly one consumer Service to exactly one Backing Service
network. Each consumer Service needs its own Attach record, even when several
Services intentionally share a credential. Create makes an explicit choice:

- `credential.mode: new` creates one credential-owning Attach and may include
  up to eight grants;
- `credential.mode: existing` names a ready credential owner in the same
  consumer Environment and Backing Service, rejects grants, and cannot point
  through another dependent.

The credential owner stores adapter identity, encrypted fact values, grants,
and any eligible database Backup-source identity. A dependent stores a direct
owner reference, resolves facts through that owner, and contributes only its
own Service/network edge. A dependent detach removes only that edge. An owner
cannot detach while a dependent references it; after dependents are gone, its
detach revokes grants and deprovisions through the adapter.

Every Backing Service owns a dedicated Zone and physical network. PostgreSQL
and Valkey do not share one backing network. A Service attached to both joins
both. Rendered network membership is a sorted, deduplicated union, but the
Attach records remain distinct durable facts.

Managed backing creation publishes a stable network alias: `gp-` plus the
lowercase backing Service id with `_` replaced by `-`. Owner and grant HOST/URL
facts use this alias, not the adapter-wide Service name. Joining two same-name
backing networks must not make endpoint selection ambiguous. Slug changes do
not change the endpoint.

### Facts, grants, and reveal

Attach facts are not reusable Secret resources and are never injected
automatically. A managed credential owner stores its fact schema separately
from an encrypted fact-value envelope. Its provisioned database and role
identity is the exact Service name plus `_` and the first six characters of
the Attach id's random tail. A credential-backed Attach rejects Service names
longer than 56 bytes rather than truncating the resulting 63-byte-bounded
adapter identity.

A grant exposes an additional prefixed fact set for another owner-selected
Attach. Omitting a grant in an Entry fact reference selects the credential
owner's own set; naming a grant selects that granted set. Credential reuse and
grants are independent relationships.

Attach list results expose stable backing ownership and fact keys with their
secret classification, never fact values. Fact values are returned only by the
explicit fact-reveal operation. The Controller derives Environment scope from
the durable Attach and requires ready facts; the caller cannot override scope.
Non-secret database and role facts may be used for labels. Password and
credential-bearing URL facts remain masked until explicit reveal. Plaintext
facts must not enter Attach primaries, Tasks, Activity, list responses, or
desired-state projections.

### Adapter behavior and Valkey authentication

The accepted managed create keys are `postgres:16` and `valkey:9`. Adapter
defaults are compiled product behavior and the operator cannot override their
managed image, mount, bootstrap, health, or procedure decisions through the
Backing Service create request.

Valkey authentication is required, explicitly selected, immutable instance policy:

- `username_password` gives each credential owner a named
  ACL identity and password;
- `password` gives each owner an independent password on the shared `default`
  user;
- `none` explicitly enables access without AUTH and produces HOST, PORT, and a
  credential-free URL, but no ROLE, PASSWORD, or consumer credential.

No mode is selected by default. The Console starts unselected; API and CLI
creation reject omission. Missing, empty, null or unknown values cannot publish
resources or Tasks. Every Attach inherits the selected mode. No Attach can weaken it. All modes
share one instance keyspace and Pub/Sub channels; an ACL identity is not tenant
or data isolation. The Console must explain the reachability risk before a
no-auth instance is created.

ACL state is persistent data. Management must use authenticated, discrete
commands, carry the final secret token through stdin rather than argv or the
environment, save ACL changes to the owned data Volume, and prove success
before publishing ready facts. Existing ACL state is retained on restart; it
must not be regenerated in a way that erases Attach users. Consumer identities
must not receive administrative commands. The exact accepted modes and fact
forms remain in [Networks, Connectors, and shared access](../decisions/network-and-shared-access.md#valkey-authentication).

## Limits and design

Valkey shares one instance keyspace across Attaches. Removing a password blocks
future authentication with it; it does not terminate already established
application connections. Grants are a PostgreSQL provisioning feature, not
generic Custom or Valkey access controls.

Valkey Backup/Restore has no accepted safe per-Attach artifact and restore
contract. A live data-directory archive is not a substitute; unsupported requests
fail with `strategy.not_implemented`. Custom has no managed Backup support.
Backup/Restore work is currently deferred.

[Backing-service provisioning](../decisions/backing-service-provisioning.md)
explains why credential ownership, network edges and hook results are separate.
[Network decisions](../decisions/network-and-shared-access.md) explain dedicated
backing networks and authentication. [Resource deletion](../decisions/resource-deletion.md)
records the deferred permanent-deletion boundary.

Custom hooks are implemented but not freshly runtime-qualified. See
[current limitations](../capabilities.md) and the [QA matrix](../qa-matrix.md).
