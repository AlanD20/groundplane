# Product scope

Groundplane lets a trusted operator organization run multiple projects on one
Linux host through a Console, CLI and API. The Controller owns decisions and
durable state; its local Agent executes planned workload operations.

This document owns shared product rules. The [feature guides](README.md#features)
own feature-specific behavior; [capability status](capabilities.md) distinguishes
implementation from qualification. This documentation cleanup changes neither
product behavior nor the approval status of incomplete features.

## Supported deployment and trust boundary

The MVP targets `linux/amd64` and `linux/arm64` without emulation or multi-host
placement. Installation targets Ubuntu 24.04/26.04 and Debian 13; each combination
still needs its own qualification.

The human API has no authentication. It defaults to loopback and may bind
explicitly configured trusted private interfaces. Public addresses, wildcard
listeners and application-Tunnel exposure are forbidden until an authentication
design is accepted.

Tenant and credential ownership is enforced, but shared backing bridges permit
peer reachability. The MVP makes no hostile-tenant network-isolation guarantee.
Runner isolation is a separate, stronger requirement described in its guide.

The native Controller is the only persistent Groundplane systemd service. It owns
the local Agent and private etcd containers without depending on Agent Tasks to
bootstrap them. A normal Controller restart may restart the Agent, but must not
restart etcd or interrupt application requests, held connections or data.
Host reboot necessarily interrupts the host; recovery must restore operation
without manual repair. GP software upgrades must preserve application service.

## Resource model

| Resource | Meaning |
| --- | --- |
| Tenant | Ownership and credential scope for a group of applications |
| Tenant Project | An application belonging to one Tenant |
| Environment | One instance of a Project, with its own desired state, network pool and storage |
| Service | A declared workload with separate desired configuration, runtime intent and observed health |
| Backing Service | A Platform-owned Project containing one `main` Environment and shared Service |
| Attach | One Service's connection to a Backing Service, optionally owning or reusing provisioning facts |
| Zone | An Environment-owned Docker network; not a DNS zone |
| Route | An Environment's declared HTTP match and Service target |
| Volume | Managed persistent storage with stable identity |
| Entry | An explicitly exposed environment variable or file |
| Secret | Reusable Project- or Platform-owned confidential input |
| Connector | An Environment-owned S3-compatible destination and credential selection |
| Script | A Service-associated manual action or deployment hook |
| Release / Release Group | Immutable deployment history / an explicitly ordered group operation |
| Task | Durable asynchronous work, including progress, failure, Retry and Abort |
| Runner | A Tenant- or Project-owned isolated GitHub Actions runtime |
| Component | A compiled integration using granted GP capabilities; not the Controller or Agent |

Stable ids are references. Slugs and Environment names are scoped human labels;
rename must not move data, rewrite descendants or retain old-label aliases.
Project display-name editing is separate from slug rename. Project description
is supplied at creation; Tenant description may be edited.

A backing Environment has no instance-wide Backup Policy. Backups belong to
consumer Environments. Destroy removes runtime while retaining desired state and
data; it is not permanent Backing deletion.

## Shared behavior

### One backend and three surfaces

Every operator-facing Controller capability has one Console action, CLI command
and API endpoint. [Surface conventions](api-cli.md) owns the closed exceptions.
Clients display Controller decisions rather than inventing health, permissions or
operation success. Loading, failures and unavailable observations stay visible.

### Desired, applied and observed state

Blueprints contain operator decisions only. They do not contain generated
credentials, filesystem paths derived by GP, runtime observations or execution
history. Canonical export reconstructs desired state rather than preserving
uploaded formatting.

Saving Service configuration does not deploy it. Runtime intent is separate:
Start selects running, Stop selects stopped, and Destroy selects absent while
retaining configuration. Blueprint edits preserve existing runtime intent.
Remove is an explicit dependency-checked operation.

Omitting an existing resource from a Blueprint retains it. Omission never deletes
or renames. Validation is read-only. Accepted work captures its exact inputs;
Retry and recovery cannot silently use later desired state.

Resource-level latest-wins reconciliation is an accepted design, not a completed
runtime capability. Until connected and qualified, its design must not be
advertised as active automatic cancellation. See [Blueprints](features/blueprints.md).

### Execution and failure

Workload effects run through typed Agent Tasks. Native Controller Tasks own the
closed bootstrap, Runner and persistence-only operations that cannot depend on
the Agent. An executor never derives permission from a Task label alone.

Protected retries preserve operation identity and captured inputs. A lost response
or cancellation is not proof that an external effect did not occur. Unknown
effects retain their ownership restrictions until resolved. Recovery preserves
the original failure and restores only explicitly captured authority; it does not
restore databases or reverse arbitrary migrations.

Workload Deploy resolves an image already present in the host Docker daemon.
GP neither pulls nor builds workload images during Deploy. Historical operations
use the captured local image id, not a mutable tag resolved again. Managed GP
images are installation/release assets with their own immutable selection.

Services publish no host ports. Communication uses explicit Zone memberships,
backing Attaches and declared Routes. Application DNS/provider policy and
certificate issuance remain operator responsibilities.

### Credentials and configuration

Secrets are encrypted at rest and excluded from public metadata and Task events.
Attach facts are not reusable Secrets and are never injected automatically;
operators choose Entry mappings and exposure. A Task that needs an exact Secret
for recovery blocks deletion until that use is released.

Valkey creation requires an explicit immutable authentication choice:
`username_password`, `password` or `none`. There is no default.
Shared keyspace and Pub/Sub are not isolated by those credentials.

Custom backing hooks and Scripts are explicit operator-authored execution, not
runtime plugins. Their declared context, timeout and output rules limit GP's
authority. Authors remain responsible for safe commands and migration semantics.

## Acceptance gates

Gate A qualifies hosting: bootstrap; resource ownership; first Blueprint Apply
and exact reapply; supported backing authentication and facts; configuration and
storage; ordered hooks; deployment and rollback; current Service observations;
logs; HTTP and held WebSocket routing; restart/reboot recovery; and GP upgrades
without application interruption. Failure, Abort, reconnect and unknown-outcome
cases must preserve data and unrelated resources.

Recreate must reach the declared replica count but does not promise zero downtime.
Blue-green supports one replica; replicated blue-green and rolling are rejected.
Release Groups must preserve declared order, hook selection and failure behavior.

Gate B adds source-specific Backup, verified Restore to original surviving targets,
retention and recovery. PostgreSQL Attach, Environment config and Volume are the
accepted source kinds. Valkey recovery still needs a safe source/artifact decision;
reject unsupported sources before Task publication. Backup/Restore qualification
is explicitly deferred for the current hosting effort, not declared complete.

Applicable CI, generated artifacts, security and surface parity remain release
requirements. The [QA matrix](qa-matrix.md) owns behavioral cases; historical
evidence does not qualify the post-restructuring build.

## Outside the MVP

Multi-host scheduling, remote-Agent identity, human-API public authentication,
hostile-tenant network isolation, registry hosting/integration, build-on-deploy,
rolling and replicated blue-green deployment, runtime Component plugins,
automatic provider DNS/certificate setup, scheduled Scripts, permanent Backing
deletion, Runner autoscaling/in-place updates and automatic empty-host disaster
recovery are outside the current scope. Accepted-but-unimplemented Backup and
Runner requirements remain documented; absence of code is not a scope reduction.
