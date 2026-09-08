# ADR 0015: Component runtime boundary and deterministic CoreDNS apply

- Status: Superseded by ADR 0061
- Date: 2026-08-25

## Supersession notice

ADR 0061 supersedes this decision in full. The body below is retained only as
historical design context and is non-normative. In particular, its former
requirements and rejected alternatives concerning container recreation,
service interruption, reload, interruption recovery, managed-config handling,
or technology-specific Component procedures are not current contracts. ADR
0061's closed Component capabilities and generic typed Agent lifecycle are the
sole authority for those concerns.

## Context

Groundplane's MVP has one Component resource spanning five compiled-in kinds:
`caddy`, `cloudflare-tunnel`, `coredns`, `controller`, and `agent`. Caddy and
Cloudflare Tunnel belong to an Environment. CoreDNS, Controller, and Agent
belong to the Platform workspace. The public noun, stable identity rules,
human surfaces, Task journal, and extension registry are shared even though
their runtime ownership differs.

The implementation scaffold already contains useful pieces of this model:

- one compiled Component registry with owner and apply-strategy metadata;
- stable Environment Component records and owner/kind indexes;
- separate desired replacement and Controller-owned runtime replacement helpers;
- one durable active Environment reconciliation fence;
- retry-stable Component candidates and Caddy address reservations;
- deterministic Environment Component rendering into generated Services and files;
- a closed Caddy validate-before-recreate Agent procedure; and
- a Cloudflare Tunnel planner that consumes a reusable Secret reference.

The product and implementation contracts nevertheless remain incomplete and
contradictory:

- Component `healthy` is currently stored in the primary even though accepted
  persistence rules require health to be projected from separate observed state;
- the public Component DTO is Environment-only while the resource explicitly
  spans Environment and Platform owners;
- Component routes remain placeholders and the CLI sends an empty config replacement;
- the Console changes desired and observed fields together and manufactures Task history;
- the Console represents the Tunnel token as a platform Secret and invents
  Tunnel hostname config, although the accepted model requires a same-Environment
  Entry referenced by stable id;
- the Console renders Corefiles itself with one hardcoded Caddy address,
  invented internal hostnames, editable listen state, relative timestamps,
  and ambient `/etc/resolv.conf` reads;
- a `200` Component config replacement is documented separately from the Task
  that must validate and apply an enabled Component; and
- CoreDNS preflight, activation proof, resolver ownership, rollback evidence,
  restart reconciliation, and failure behavior are not closed.

This ADR defines the complete C12 Environment/platform Component and C13
CoreDNS MVP contract. It does not add multi-host placement, runtime plugins,
Cloudflare API integration, DNS automation, wildcard records, metrics,
enterprise policy, or any sixth Component kind.

## Accepted constraints

This proposal preserves the following already accepted constraints:

- the Controller decides and the Agent applies every host mutation;
- the MVP runs on one host and has at most one enrolled local Agent;
- etcd is host-level bootstrap state in a Controller-managed OCI container,
  not a Component or systemd unit;
- the native Controller is the only Groundplane systemd unit;
- stable ids own references and API entity routes; mutable labels never do;
- each Component kind declares its allowed owner and apply strategy in the
  compiled registry;
- Caddy and Cloudflare Tunnel are off when an Environment is created;
- Tunnel lifecycle is independent of Caddy and other HTTP routers;
- Tunnel DNS, public hostnames, ingress rules, origin targets, and protocol
  remain provider-managed work outside Groundplane;
- CoreDNS uses host networking and listens only on `127.0.0.1:53`;
- CoreDNS applies a candidate only after validation and keeps the last-known-good
  resolver serving on failure;
- the Agent mounts a read-only Groundplane resolver file over
  `/etc/resolv.conf` only after a healthy initial CoreDNS apply and unmounts
  only that exact owned mount on successful disable;
- exact Environment hostnames resolve to that Environment's applied Caddy private IPv4;
- generated Corefiles and Caddyfiles are derived artifacts, not desired-state records;
- Tasks and observations are durable Controller records while high-volume
  process output is not;
- Environment Component Tasks use Environment journal ownership and Platform
  Component Tasks use Platform journal ownership;
- Platform Activity and the Platform Components Tasks section are the same
  unfiltered `workspace=platform` journal page; and
- no compatibility record, fallback parser, legacy route, alias kind, dual
  write, or scan fallback is introduced.

The ADR 0021 replacement is a registered hybrid Task mutation. A synchronous
desired-state response may be `200` or `201` while the same transaction creates
one Task. Its marker is still `kind=task`, carries the Task id, follows the
ordinary pending and terminal transitions, returns `idempotency.in_progress`
while pending, and replays the exact original synchronous response after the
Task is terminal. This is not a `direct` marker with a hidden Task.

The ADR 0034 replacement changes only which half of Component state is a
candidate. Operator-authored `desired` commits synchronously. The Task owns one
private immutable candidate for `applied` state and its generated identities.
Terminal success promotes that applied candidate; every other terminal result
retains the prior applied snapshot. Address reservation and replay behavior are
unchanged.

## Superseded historical decision

The following proposal records the design evaluated at the time. It must not be
used to resolve an implementation or review question against ADR 0061.

### 1. Closed Component catalog and action scope

The MVP catalog is exactly:

| Kind | Owner | Config | Operator actions |
| --- | --- | --- | --- |
| `caddy` | Environment | Caddy config | config show/set, enable, disable, update |
| `cloudflare-tunnel` | Environment | Tunnel config | config show/set, enable, disable, update |
| `coredns` | Platform | resolver config | config show/set, enable, disable, update |
| `controller` | Platform | none through Component | update |
| `agent` | Platform | none through Component | read-only Component projection |

Controller config remains the local startup document. Agent lifecycle, Agent
config, and Agent update remain exclusively under the `agent` noun. The Agent
Component does not create a second write path to those capabilities.

Calling a valid Component endpoint for an action outside the kind's catalog
returns `component.action_unsupported` with HTTP 409. It creates no
idempotency marker and no Task. There is no Component create, edit, rename,
remove, or delete operation.

Environment Caddy and Tunnel updates apply the Controller's compiled image
pin. CoreDNS update applies the compiled CoreDNS pin. Controller update uses
the existing staged-binary procedure. No Component update request accepts a
tag, image, URL, checksum, command, or arbitrary argument.

### 2. Stable identity, owner addressing, and Task target

Every Component has one globally stable `cmp_...` id. Owner and kind are
immutable. Exactly one Component exists for each allowed owner/kind pair:

```text
/v1/records/components/<component-id>
/v1/indexes/components/by-owner/environment/<environment-id>/<component-id>
/v1/indexes/components/by-owner/platform/-/<component-id>
/v1/indexes/components/by-kind/environment/<environment-id>/<encoded-kind>
/v1/indexes/components/by-kind/platform/-/<encoded-kind>
```

The owner and kind indexes are written, compared, and removed atomically with
the primary. Missing, duplicate, or owner-mismatched indexes are corruption;
readers never scan or repair them.

Addressing rules are exact:

- REST detail, config, and action paths always take the stable Component id.
- A direct Component operation records that Component id as the Task target.
- An Environment Blueprint, Route, or general reconciliation Task retains the
  Environment id as its target even when it changes multiple Components.
- The Console may use `/platform/components/<kind>` as a presentation route,
  but resolves the owner/kind record before making an id-addressed request.
- The CLI accepts a Component kind by default and resolves it within exactly
  one selected owner. `--id` treats the target as a stable Component id.
- A CLI kind target requires `--platform` or a resolved Environment scope. An
  id target requires no owner flag; if a scope is supplied it must match.
- `GET /components` requires exactly one of `environment=<env-id>` or
  `platform=true`. Optional `kind=` is a filter, not a second identity.

Task ownership remains independent from target. Environment Component Tasks
freeze the Environment owner tuple. Platform Component Tasks freeze the
Platform workspace. A cascade keeps the initiating Task's owner even when a
step touches a dependency in the other workspace.

### 3. Durable desired and applied state

The Component primary separates operator intent from the last successfully
applied snapshot:

```text
ComponentRecord
  id                         stable Component id
  owner                      environment | platform
  owner_id?                  required exactly for environment
  kind                       one accepted kind
  created_at                 Controller-authored UTC
  updated_at                 Controller-authored UTC

  desired                    exact oneof below
  applied?                   exact matching oneof below
  last_attempt?              exact matching attempt record below
```

The persisted unions are inhabitable and do not force container fields onto
native kinds:

```text
ManagedDesired                         ControllerDesired
  generation                            generation
  enabled                               compiled_artifact_ref
  config_state unconfigured|configured  compiled_artifact_sha256
  config?                               updated_at
  updated_at

AgentDesired
  generation
  enrolled
  authoritative_agent_id?             present exactly when enrolled
  authoritative_agent_generation?     present exactly when enrolled
  image_state                          unconfigured | configured
  compiled_image_index_ref?            present exactly when configured
  updated_at

ManagedApplied                        ControllerApplied
  desired_generation                   desired_generation
  enabled                              native_artifact_ref
  config?                              native_artifact_sha256
  resolved_dependency_sha256           applied_at
  input_sha256
  artifact_sha256?
  image_index_ref
  image_child_digest
  render_generation
  generated_service_ids
  pinned_ipv4?                        AgentApplied
  trust_generation?                   Caddy enabled only
  trust_proof_sha256?                 Caddy enabled only, exactly 32 bytes
  baseline_generation?                CoreDNS enabled only
  ownership_generation?               CoreDNS enabled only
  mount_proof_sha256?                 CoreDNS enabled only, exactly 32 bytes
  dns_full_proof_sha256?               CoreDNS enabled only, exactly 32 bytes
  applied_at                           desired_generation
                                       agent_id
                                       agent_generation
                                       image_index_ref
                                       image_child_digest
                                       applied_at

ManagedAttempt                        NativeAttempt
  desired_generation                   desired_generation
  base_applied_generation?             task_id
  base_dependency_sha256               status
  task_id
  status
```

`ManagedDesired` is written synchronously by an operator mutation.
`ControllerDesired` and `AgentDesired` are Controller projections from their
native authoritative records and have no Component write surface.
`ManagedApplied` advances only in the same transaction as successful terminal
Task acknowledgement. Failure, timeout, or abort updates the matching attempt,
releases the active-operation fence, and leaves the applied union unchanged.

The desired and applied unions are exact by kind:

| Kind | Desired | Applied artifact |
| --- | --- | --- |
| Caddy, Tunnel | `ManagedDesired`; config only when configured | `ManagedApplied` |
| CoreDNS | `ManagedDesired`; always configured with complete resolver config | `ManagedApplied` |
| Controller | `ControllerDesired`; no enablement or config member | `ControllerApplied` |
| Agent | `AgentDesired`; no Component enable/config member | `AgentApplied` while enrolled |

Only newly created disabled Caddy and Tunnel records may have an unconfigured
`ManagedDesired`. An unenrolled Agent may also have
`AgentDesired.image_state=unconfigured`; this means the Controller startup
document has no Agent image and the image is non-applicable, not that an empty
image reference is valid. `enrolled=true` requires `image_state=configured`
and a non-empty immutable index reference. Agent enrollment resolves and
stores that reference before publishing the lifecycle Task.

CoreDNS's persisted config contains this exact bootstrap-only union in addition
to its public resolver fields:

```text
TailnetDefaultDecision
  oneof
    pending
      bootstrap_schema               exactly 1
    decided
      enabled                        boolean, equal to tailnet_delegation
      source                         operator | tailscale_observation
      observation_generation?        present exactly for tailscale_observation
      observation_sha256?            exactly 32 bytes, present exactly for tailscale_observation
      decided_at                     Controller-authored UTC
```

`pending` is valid only on the clean-start CoreDNS record and forbids render or
apply. The first operator config replacement changes it to `decided` with
`source=operator` in the same mutation. Otherwise the first baseline handoff
changes it once to `decided` with `source=tailscale_observation`. No later
observation can change a decided value. Public config continues to expose only
the selected boolean, and reports `tailnet_default_pending=true` while the
internal union is pending; that field is false after either decision.

Newly created disabled Caddy and Tunnel records with unconfigured desired state
have no applied snapshot, are publicly `disabled`, accept config replacement,
and reject enable with `component.config_required` until configured. No renderer,
candidate output, dependency lookup, generated identity, or Task is created for
an unconfigured record. Controller startup publishes its native applied snapshot
after validating the running binary. Agent join, update, removal, and Ready
transactions update the read-only Agent Component projection from the
authoritative Agent record; Component endpoints never mutate it.

The applied `config` is an intentional successful source snapshot, not a
rendered artifact or a second desired record. It makes drift and rollback
comparisons independent of later desired replacements. The Task retains its
immutable resolved procedure and step checkpoints under the ordinary Task
retention contract.

Applied generated identities are exact:

- enabled Caddy has one generated Service id and one pinned IPv4;
- enabled Tunnel has one generated Service id and no pinned address;
- enabled CoreDNS has one Platform generated Service id;
- applied-disabled Caddy, Tunnel, and CoreDNS have no generated Service ids;
- Controller and Agent have no generated Service ids in this record; and
- Caddy's address remains reserved across apply retries and successful
  updates, and is released only after successful disable.

The proof fields are equally exact. Enabled Caddy requires both trust fields
and enabled CoreDNS requires all four resolver/DNS fields; every other kind and
every applied-disabled snapshot requires them absent. Successful promotion
compares the local proof digest reported by the Agent and commits it with the
applied snapshot. Recovery accepts only the corresponding local generation and
digest, never Task checkpoint presence by itself.

The current `healthy` field is removed from the primary. A schema cutover is
clean; there is no dual reader for the old Component record.

### 4. Latest Component observation

Runtime truth is one latest typed observation separate from the primary:

```text
/v1/observed/environments/<environment-id>/component/<component-id>
/v1/observed/platform/components/<component-id>
```

The common envelope is:

```text
ComponentObservation
  schema
  component_id
  owner
  owner_id?
  kind
  source                     agent | controller
  state                      absent|starting|running|healthy|unhealthy|stopped
  reported_at                source-authored UTC
  received_at                Controller-authored UTC
  evidence                   exact oneof below
```

The observation evidence union is:

- Caddy: Agent id/generation, desired and render generations, generated Service
  id, pinned IPv4, image index reference and verified platform-child digest,
  Caddyfile digest, HTTP proof time, and installed Environment root-CA digest
  when present.
- Tunnel: Agent id/generation, desired and render generations, generated
  Service id, image index reference and verified platform-child digest, and
  continuous-running proof time. It never claims Cloudflare edge, account,
  token validity, or route health.
- CoreDNS: Agent id/generation, desired and render generations, generated
  Service id, image index reference and verified platform-child digest, fixed
  listen endpoint, Corefile digest, resolver ownership generation, latest cheap
  observation-check time/result, and the immutable full-proof generation,
  digest, Task lineage, time, static result, catch-all result, and forwarder
  proof counts from the apply or repair that promoted the current snapshot.
- Controller: desired generation, running native artifact ref and SHA-256,
  build version, and native process health. It has no Agent, image, render, or
  generated-Service fields and is reported by the Controller.
- Agent: desired generation, authoritative Agent id/generation, verified image
  index and child digest, version, and authenticated Ready state. It has no
  Component render or generated-Service fields and is reported by the
  Controller from the channel.

Only the currently enrolled local Agent generation may submit Agent-sourced
observations. The Controller compares its Agent identity/generation and the
expected render generation in the same transaction as the observation write.
Stale or mismatched reports cannot overwrite current observation.

Cadence, freshness, and expiry are source-specific:

- the native Controller publishes its self-observation immediately after
  validating the running binary and every 10 seconds thereafter; it is stale
  after 30 seconds and expires 24 hours after becoming stale;
- the Controller publishes Agent Component channel observations at
  authenticated Ready and every 15-second authenticated heartbeat; they are
  stale after 45 seconds and expire 24 hours after becoming stale or when the
  Agent generation is superseded;
- the Agent publishes managed Caddy and Tunnel observations in its 15-second
  observation cycle. For CoreDNS, this cycle performs only Moby identity and
  label inspection, hashes the at-most-96-KiB active Corefile, and performs one
  UDP-first root-NS query plus the selected static query when one exists, under
  a four-second total deadline. It does not scrape counters, query every
  forwarder, or contact upstreams directly. CoreDNS observations are stale
  after 45 seconds; the immediate post-apply observation carries the complete
  Task proof, while periodic observations bind the cheap result to the retained
  full-proof digest. Managed observations expire 24
  hours after becoming stale, owner deletion, or Agent-generation
  supersession; and
- expiry deletes only the latest observation record and never desired,
  applied, Task, or checkpoint state.

Each Controller process has a random 128-bit `controller_instance_id` in its
self-observation. On startup, a persisted observation from any prior instance
is stale regardless of timestamp. The Controller must commit one fresh,
binary-matching self-observation within five seconds and before opening the
external API listener; failure fails startup closed. Native Controller
freshness never depends on Agent pull or connectivity. Observation records are
capped at 64 KiB and are not a history log.

The public Component status is an authoritative Controller projection with
closed conditions by persisted union:

| Projection | Status | Exact condition |
| --- | --- | --- |
| Managed | `disabled` | `ManagedDesired.enabled=false`, `ManagedApplied` is absent or disabled, and no Component or graph operation is active |
| Managed | `pending` | an operation is active, or desired generation/current resolved-dependency digest differs from `ManagedApplied` without a terminal failure for that exact desired/dependency pair |
| Managed | `healthy` | desired generation and current resolved-dependency digest equal an enabled `ManagedApplied`, and a fresh observation matches its render, artifact, image-child, and Agent generations and is healthy |
| Managed | `degraded` | the latest attempt for the exact desired/dependency pair failed, or a fresh matching-generation observation is unhealthy or has mismatched artifact/image evidence |
| Managed | `unknown` | `ManagedApplied.enabled=true` but no fresh matching observation exists |
| Controller | `pending` | a Controller update Task is active, or `ControllerDesired` artifact ref/SHA differs from `ControllerApplied` without a terminal failure for that desired generation |
| Controller | `healthy` | `ControllerDesired` exactly equals `ControllerApplied` and a fresh Controller-sourced observation reports the same native artifact healthy |
| Controller | `degraded` | the latest Controller update attempt for the desired generation failed, or a fresh Controller-sourced observation is unhealthy or artifact-mismatched |
| Controller | `unknown` | `ControllerApplied` exists but no fresh matching Controller-sourced observation exists |
| Agent | `disabled` | `AgentDesired.enrolled=false`, `AgentApplied` is absent, and no authoritative Agent lifecycle operation is active |
| Agent | `pending` | an authoritative Agent lifecycle operation is active, or enrolled/id/generation/image-child desired projection differs from `AgentApplied` without a terminal failure for that projection generation |
| Agent | `healthy` | enrolled `AgentDesired` exactly equals `AgentApplied` and a fresh Controller-sourced observation reports that Agent generation authenticated and Ready |
| Agent | `degraded` | the latest authoritative Agent lifecycle attempt for the projection generation failed, or a fresh Controller-sourced observation reports that matching Agent generation unhealthy, disconnected, or not Ready |
| Agent | `unknown` | enrolled `AgentApplied` exists but no fresh matching Controller-sourced observation exists |

No Controller Component can be `disabled`. No native projection reads managed
enablement, render, generated-Service, or Agent-sourced observation fields that
its union does not contain.

Clients do not infer status from Docker state, timestamps, Task titles, or fixture fields.

### 5. Public Component and config projections

`GET /components` and `GET /components/{id}` return the same Component item
shape, with list pages using the repository's fixed-revision id-ascending
cursor contract:

```text
Component
  id
  owner
  owner_id?
  kind
  desired
  applied?
  observation?
  status
  active_task_id?
  last_task_id?
```

No raw etcd revision, Docker container id, Agent path, token value, relative
time, or locally inferred health crosses the public interface.

`GET /components/{id}/config` returns:

```text
ComponentConfig
  component_id
  kind
  desired_generation
  config
  tailnet_default_pending?  present exactly for CoreDNS
  reconcile_task_id?         last Task for this desired generation
  candidate_output
    state                    ready | blocked | none
    path?
    sha256?
    content?
    code?
```

`candidate_output` is rendered on demand by the Controller from current
desired state and authoritative dependency inputs. It is never stored. Caddy
and CoreDNS may return a complete candidate file. Tunnel, Controller, and
Agent return `none`. Missing auto-upstream input returns `blocked` with
`component.upstream_unavailable`; the config read itself remains successful.

The active digest comes only from `applied` and matching `observation`, so a
client can distinguish desired candidate output from the serving generation.
The Console removes its local Corefile renderer and displays this projection.

Candidate content is returned only for configured Components. A complete
encoded `ComponentConfig` response is capped at 128 KiB so ADR 0021 exact replay
always fits; each candidate file is capped at 96 KiB. Exceeding either limit is
`component.artifact_too_large` with HTTP 422 before marker claim or Task
publication.

### 6. Exact kind config schemas

Caddy config is a complete singleton replacement:

```json
{
  "zone": {"slug": "frontend"},
  "caddyfile_template": "{routes}\n"
}
```

`zone` is required. `caddyfile_template` defaults to `{routes}\n`, contains
exactly one `{routes}` marker, and rejects `{host}`, `{slot}`, and NUL. The
selected Zone belongs to the Environment. A resolved Zone change while Caddy is
applied enabled returns `component.config_locked` with HTTP 409; the operator
disables Caddy, replaces config, and enables it again.

The normalized Cloudflare Tunnel config is:

```json
{
  "secret_id": "sec_01ARZ3NDEKTSV4RRFFQ69G5FAV"
}
```

There is no Tunnel hostname list, origin field,
Cloudflare account, zone id, API credential, or managed DNS field.

CoreDNS config is:

```json
{
  "upstream_auto": true,
  "upstream_resolvers": [],
  "forwarders": [],
  "tailnet_delegation": false
}
```

That value is the exact empty-store bootstrap config, not an illustrative LAN
default. The Controller never invents a forwarder or resolver address.

There is no listen field. The fixed MVP endpoint is `127.0.0.1:53`.

Resolver endpoints are canonical IPv4 or IPv6 literals with an optional
explicit decimal port in `1..65535`. Hostnames, URI schemes, DNS-over-TLS
tokens, filesystem sources, interface zone suffixes, and arbitrary CoreDNS
tokens are excluded. Port 53 normalizes away. A non-default IPv6 port uses
brackets. Each resolver list has at most 15 unique endpoints. Loopback,
unspecified, multicast, the fixed CoreDNS listener, and Docker's embedded
resolver are unusable.

The modes are mutually exclusive. `upstream_auto=true` requires
`upstream_resolvers=[]` and uses the complete usable pre-Groundplane Resolver
baseline in canonical endpoint order. `upstream_auto=false` requires 1..15
configured endpoints and never reads or merges the baseline for forwarding.
There is no explicit-as-fallback hybrid and no silent public-DNS fallback. Auto
without a usable baseline blocks render/apply with
`component.upstream_unavailable`.

Forwarder domains are canonical lowercase exact DNS names without a trailing
dot and occupy at most 220 DNS wire octets, leaving room for the fixed proof
label. Each has 1..15 resolvers. Duplicate domains, duplicate resolver endpoints
within a directive, the catch-all domain, wildcard names, and an operator
`ts.net` forwarder while managed delegation is enabled are invalid.

`tailnet_delegation=true` adds the ordinary managed forwarder
`ts.net -> 100.100.100.100`. Tailscale observation only selects the initial
toggle default on first bootstrap; it never overwrites the operator's stored value.

CoreDNS config canonical JSON is at most 64 KiB and contains at most 8
forwarders. The rendered input contains at most 1,024 distinct static hostnames,
at most 128 host-address groups, and at most 15 endpoints in any directive.
Canonical DNS names retain the DNS wire limits. Caddy config canonical JSON is
at most 64 KiB and `caddyfile_template` is at most 32 KiB. These limits apply to
API and Blueprint input before mutation.

Controller and Agent have no Component-config replacement schema. Their
ordinary read projections may link to their authoritative startup or Agent
surfaces, but Component config PUT remains unsupported.

Blueprint input uses the existing `x-gp-components` mapping as a sparse,
authoritative set of Component decisions. Absence of the whole mapping or of
one kind preserves that kind's current desired state; disabling or clearing
configuration is therefore explicit, never inferred from omission. A present
item is a complete replacement containing `kind`, `enabled`, and optional
`config`. `config` is required when enabled, a disabled item with config is
configured-disabled, and a disabled item without config becomes unconfigured.
On a newly created Environment, omitted Caddy and Tunnel remain the
unconfigured-disabled records created by the Environment transaction. Stable
Component ids and derived generations, addresses, artifacts, digests,
observations, or resolver data are never authored in a Blueprint.

Public authored surfaces use one closed `ResourceSelector` grammar:
`{"slug":"<zone-slug>"}` or `{"key":"<entry-key>"}` by default, and
`{"id":"<stable-id>"}` only as an explicit id-mode alternative. Exactly one
member is present. Zone slugs and Entry keys use their authoritative grammar
and scoped unique indexes; ids use the primary and owner index. Blueprint
accepts only `slug`/`key`. REST accepts the same default selectors and the
explicit `id` branch. CLI default flags `--zone <slug>` and
`--token-entry <key>` serialize the default branch. CLI `--id` switches the
Component target and every entity-valued flag in that invocation to the id
branch; for example,
`component config set cmp_... --zone net_... --id`. Console selectors display
and submit the current slug/key branch, while entity routes continue to use
stable ids.

The Controller resolves every selector from its exact owner-scoped index at the
mutation's fixed revision, rejects zero, duplicate, wrong-kind, wrong-owner, or
deleted matches, and admits only the resolved stable id and resource revision.
It never scans or defers resolution to the Agent. The normalized durable Caddy
config contains `zone_id`; the normalized durable Tunnel config contains
`secret_id`. `ManagedApplied`, Task input, and reverse-reference indexes
use those same ids. A retained authored Blueprint is opaque source/audit input,
not referential state and is never consulted after admission.

GET projections may resolve the current human label for display, but never
persist that label back into Component state. A separately accepted label
rename changes no Component config, generation, Task, artifact, or reverse
index. In particular, this ADR does not add Zone edit or rename: the current
Zone contract remains immutable. There is no rename-triggered selector rewrite,
slug/key compatibility field, dual read, or dual write.

### 7. Cloudflare Tunnel Secret contract

The request selects an authorized Project or platform env-var Secret by stable
id, or supplies `{mode:new, secret_name, token}`. The new form creates a Project
Secret whose key is `secret_name`. The durable Tunnel config stores only
`secret_id`; it never stores the token or a mutable selector.

The Controller validates Secret ownership and kind and freezes the Secret id,
metadata revision, ciphertext SHA-256, generated Tunnel Service id, output kind,
destination, uid, gid, and mode in the durable Task reference. The sealed Agent
plan repeats only authenticated non-secret output metadata, length, and digest.
Token bytes
travel through the existing bounded transient Entry materialization channel
and are written only into the generated Tunnel Service's `TUNNEL_TOKEN`
environment file. Retry and restart resolve the pinned generation, never the
Entry's later current generation. Token bytes and every plaintext-derived
digest are excluded from Component config, public `input_sha256`, Compose YAML,
rendered non-secret files, public Task parameters, observations, logs, API
responses, and the Console store.

Deleting the Entry returns `resource.in_use` while any desired-enabled or
applied-enabled Tunnel references it. A disabled un-applied reference does not
retain the Entry; a later enable must fail validation until config points to a
valid Entry.

Creating or editing the current generation of an Entry referenced by a
desired-enabled Tunnel recomputes that Tunnel's resolved dependency digest.
If it changes, the Entry mutation uses the same hybrid contract as a
Route/Service mutation: the Entry generation, Tunnel applied base, immutable
candidate graph, Environment fence, Task, protected marker, and exact
`200`/`201` response with `reconcile_task_id` commit together. The Task pins
the new Entry generation and recreates Tunnel; it never follows a later current
generation. A referenced Tunnel that is desired-disabled and applied-disabled
publishes no Task. Entry removal retains its existing `202` Task and folds
Tunnel reconciliation into that Task.

The Console Router surface selects an existing matching Environment Entry or
links to Environment Entries to create one. It never creates a platform
Secret, invents token bytes, or calls Cloudflare.

### 8. Component dependency graph

The dependency rules are part of validation and Task planning:

- every Agent-applied Component waits for the enrolled local Agent to become
  Ready before assignment; the pending Task remains durable while no Agent is
  available;
- Caddy requires its selected Zone, address reservation, reachable target
  Services, exposed target ports, and valid rendered Caddyfile;
- Tunnel enable/apply requires only its authorized Secret reference and does
  not read or constrain the HTTP-router graph;
- Tunnel uses no Groundplane-authored DNS, public hostname, ingress rule,
  origin target, or protocol and never requires CoreDNS;
- Caddy and Tunnel may be enabled or disabled independently;
- Caddy remains usable without CoreDNS through direct, LAN, or public-DNS paths;
- an enabled CoreDNS consumes successful applied Caddy snapshots, not
  uncommitted candidates or fixture health;
- CoreDNS host inputs are exact host-valued Routes from the same successful
  applied Environment projection; Tunnel contributes no separate hostname grammar;
- Caddy enable applies and verifies Caddy before adding its CoreDNS records;
- Caddy disable removes and verifies its CoreDNS records before removing Caddy; and
- a failure in either half restores the complete prior applied graph before
  the initiating Task fails.

A cross-workspace cascade retains the initiating Task's journal owner. A
Caddy action therefore remains Environment-owned even when its final steps
reload the Platform CoreDNS Component. Locks are acquired in stable id order;
one active Environment reconciliation fence and one active Platform Component
fence prevent conflicting renders.

#### Closed OCI catalog and Compose procedures

The compiled MVP image catalog is exactly:

| Kind | Immutable index reference | `linux/amd64` child | `linux/arm64/v8` child |
| --- | --- | --- | --- |
| Caddy | `docker.io/library/caddy:2.11.4-alpine@sha256:5f5c8640aae01df9654968d946d8f1a56c497f1dd5c5cda4cf95ab7c14d58648` | `sha256:98eb57d882ccd5213d1688764db10c1ca2c58a1ca3a6717a3411ad798f7a423a` | `sha256:1172d4213087d3fc30bafc7ff2c2896180eb0c41ff7f75f315568fb36cabdcba` |
| cloudflared | `docker.io/cloudflare/cloudflared:2026.7.2@sha256:4f6655284ab3d252b7f28fedb19fe6c8fc82ee5b1295c20ac74d475e5398a52d` | `sha256:18626b1baac4450214535cd5bc40ef44c0635244d585ebf707749c22b6f3408f` | `sha256:a85d5a3d6f22cb3c7e78b2f0d05b0f0daeb72566e9426f656c60b357b7b89c95` |
| CoreDNS | `docker.io/coredns/coredns:1.11.3@sha256:9caabbf6238b189a65d0d6e6ac138de60d6a1c419e5a341fbbb7c78382559c6e` | `sha256:f0b8c589314ed010a0c326e987a52b50801f0145ac9b75423af1b5c66dbd6d50` | `sha256:31440a2bef59e2f1ffb600113b557103740ff851e27b0aef5b849f6e3ab994a6` |

The index and child descriptors above were resolved on 2026-08-25 from
<https://hub.docker.com/v2/repositories/library/caddy/tags/2.11.4-alpine>,
<https://hub.docker.com/v2/repositories/cloudflare/cloudflared/tags/2026.7.2>,
and
<https://hub.docker.com/v2/repositories/coredns/coredns/tags/1.11.3>.
Packaging stores the index and this closed platform map. The only MVP runtime
platforms are `linux/amd64` and `linux/arm64/v8`; a different Ready Agent
platform is `component.platform_unsupported` before claim.

The Controller places the index reference and expected platform-child digest
in the immutable intent and uses `repository@<child-digest>`, without a tag,
as the Compose `image`. The Agent pulls that exact child, then requires Docker
to report the expected OS, architecture, and exact child `RepoDigest` before
preflight or `ComposeApply`. The index digest, child digest, and platform are
managed labels and proof fields. A pull or observation resolving to any other
descriptor is post-claim `component.image_mismatch`; a tag is never resolved
at apply time.

The canonical Caddy service key is `caddy`. Its exact Compose service has the
selected Environment Zone as its only network with the pinned private IPv4;
no public address or host `ports`; `expose` entries `80/tcp`, `443/tcp`, and
`443/udp`; the child-digest image; `pull_policy: always`; image entrypoint with
command `["run","--config","/etc/caddy/Caddyfile","--adapter","caddyfile"]`;
the generation-specific trusted Caddyfile mounted read-only at
`/etc/caddy/Caddyfile`; Component-owned `data` and `config` directories mounted
read-write at `/data` and `/config`; read-only root filesystem;
`no-new-privileges`; all capabilities dropped except `NET_BIND_SERVICE`;
restart `unless-stopped`; and local logs `max-size=10m,max-file=3`. Its Compose
healthcheck is exec-form
`["CMD","caddy","validate","--config","/etc/caddy/Caddyfile","--adapter","caddyfile"]`
with interval 5 seconds, timeout 3 seconds, 6 retries, and no start period.
After `WaitHealthy`, the Agent sends
`GET / HTTP/1.1\r\nHost: groundplane.invalid\r\nConnection: close\r\n\r\n`
to the pinned IPv4 port 80 and requires within 10 seconds a syntactically valid
HTTP response, status `200..599`, and `Server: Caddy`.

After Caddy HTTP proof and before a successful enable/update acknowledgement,
the Agent reads the fixed Caddy state location
`/data/caddy/pki/authorities/local/root.crt` through the Component-owned data
mount. It accepts at most 32 KiB containing exactly one PEM certificate whose
DER parses as a currently valid self-signed CA with `BasicConstraints CA=true`
and certificate-signing key usage. The certificate SHA-256 and DER length are
Task result fields and observation evidence; private-key material is never
read.

`HostTrustTransition` is a closed task-scoped helper operation with
`install`, `remove`, and `verify` variants. Its only destination is
`/usr/local/share/ca-certificates/groundplane-caddy-<component-id>.crt`,
derived by the helper from a validated Component id. Install atomically writes
the validated certificate as uid/gid `0:0`, mode `0644`; remove first requires
the exact owned path, metadata, certificate digest, Task lineage, and trust
generation. Both mutation variants execute exactly
`/usr/sbin/update-ca-certificates` with no arguments, cwd `/`, stdin closed,
and exactly `LANG=C`, `LC_ALL=C`, and
`PATH=/usr/sbin:/usr/bin:/sbin:/bin`. Success requires exit zero within 30
seconds, combined stdout/stderr of at most 32 KiB containing no certificate
bytes, and a descriptor-relative scan of
`/etc/ssl/certs/ca-certificates.crt` proving the exact DER certificate present
once after install or absent after remove. A pre-existing matching certificate
without Groundplane ownership proof, a different file at the owned path, an
unexpected updater executable type/owner, or an ambiguous bundle fails closed.

The trust helper is the exact pinned Agent platform-child image, user `0:0`,
network `none`, PID mode `host`, read-only container root, no Docker socket or
host bind, restart `no`, `no-new-privileges`, and a task-specific seccomp
profile allowing only the fixed descriptor, namespace, filesystem, process,
and exec syscalls. It drops all capabilities and adds only `SYS_ADMIN`,
`SYS_CHROOT`, `DAC_OVERRIDE`, `FOWNER`, and `CHOWN`. The Agent creates it
through the Docker socket; the helper opens `/proc/1/root`, joins only PID 1's
mount namespace, chroots to that descriptor, and accepts one at-most-40-KiB
protobuf request on stdin. Its argv is exactly
`["/groundplane-agent","host-integration-helper","--request-fd=0"]`. No
operator path, executable, argument, environment name, or certificate
destination enters the request.

Trust recovery never depends on a retained Task. The Agent stores the accepted
DER certificate at
`/var/lib/groundplane/agent/components/<component-id>/trust/<trust-generation>/root-ca.der`
and its proof at the sibling `proof.json`. The DER file is exactly the validated
certificate and is capped at 32 KiB. The proof names schema 1, Component id,
trust generation, certificate SHA-256 and length, owned destination metadata,
bundle multiplicity, Agent id/generation, originating Task/step/attempt, and
the transition result. Both are root-owned mode `0600`.

Both privileged `HostTrustTransition` and `ResolverMountTransition` helpers use
one exact Agent lifecycle. The name is
`gp-hi-<first-20-lowercase-hex(SHA-256(task-id || NUL || step-id || NUL ||
assignment-attempt-u64be))>`. The complete labels are
`com.groundplane.managed=true`,
`com.groundplane.role=host-integration-helper`, the exact
`com.groundplane.task-id`, `com.groundplane.step-id`,
`com.groundplane.assignment-attempt`, `com.groundplane.agent-id`,
`com.groundplane.agent-generation`, and
`com.groundplane.helper-schema=1`; no other label is permitted. The Agent calls
Docker create with stdin open and the complete fixed config, attaches stdin and
bounded stdout/stderr before start, records the immutable returned container id,
inspects that id to prove name, labels, image child digest, argv, mounts,
capabilities, namespace modes, and creation time, starts by id, writes one
big-endian uint32-length-prefixed deterministic protobuf request, closes the
write half, waits by id, accepts exactly one bounded framed result, and removes
by id. Name and labels support collision and orphan discovery but never replace
the returned id as mutation authority.

Every exit path removes the helper. Cancellation, timeout, Agent shutdown, or a
lost attach first stops it, then force-removes it under a separate 30-second
cleanup deadline; cleanup failure is Internal. On restart, before assignment
redelivery, the Agent lists only the exact role and Agent labels, inspects each
immutable id, recomputes its expected name/config proof, and force-removes an
exact orphan under the same deadline. A same-name container with incomplete or
foreign proof is `component.recovery_required` and is not mutated. A helper is
never reattached or adopted after restart; the fenced step replays only after
orphan cleanup.

Trust checkpoints are `root_ca_captured`, `trust_install_started`,
`trust_install_verified`, and `trust_observation_reported`. Caddy disable runs
`trust_remove_started` and `trust_remove_verified` before Compose removal.
Failure after trust removal reinstalls and proves the prior certificate before
reapplying/proving the previous Caddy artifact. Initial install failure removes
the owned certificate, reruns the updater, proves absence, then removes the
candidate Caddy service. Update with an unchanged owned digest is a verified
no-op; a rotated candidate records the prior certificate and rollback restores
and proves it. Helper or updater interruption is classified from the exact
owned file/bundle proof before replay and never inferred from a checkpoint
alone.

The canonical Tunnel service key is `cloudflared`. Its exact Compose service
has the selected Caddy Zone as its only network; no address reservation,
public address, `ports`, or `expose`; the child-digest image;
`pull_policy: always`; image entrypoint with command
`["tunnel","--no-autoupdate","run"]`; the generation-specific service-only
mode-`0600` environment file consumed through Compose long-form
`env_file` with `required: true` and `format: raw`; `depends_on` Caddy
with `service_started`; image-defined user unchanged; read-only root
filesystem; `no-new-privileges`; all capabilities dropped; restart
`unless-stopped`; disabled Compose healthcheck; and the same bounded local
logging. The raw environment file exists only from the immediately preceding
materialization through the return of its final consumer,
`ComposeApply(candidate,[tunnel-service-id],false)`. Each such consumer is one
closed Tunnel token session with the durable checkpoints
`tunnel_token_materialized`, `tunnel_compose_returned`, and
`tunnel_token_cleanup_proven`. The first checkpoint privately binds Task id,
step id, assignment attempt, Environment id, render generation, exact canonical
volume-relative raw-env path, and the materialized regular file's device,
inode, length, and SHA-256. The second records the bounded Compose return,
including failure, timeout, or cancellation; it is not a terminal Task result.
Every Compose return enters cleanup, and no Compose result can be acknowledged
until the third checkpoint records a matching cleanup proof.

`TunnelTokenCleanup` is an explicit typed, post-Compose helper step. The Agent
starts a fresh helper from the exact verified Agent platform-child image with
user `0`, restart `no`, network `none`, a read-only container root, no Docker
socket, no devices, no environment variables, `no-new-privileges`, the default
seccomp policy, every capability dropped, and exactly one mount: the authorized
host `environment.volume_dir` read-write at
`/run/groundplane/materialize`. Root owns the generated mode-`0600` file and
its mode-`0700` ancestors, so cleanup adds no capability. The helper argv is
exactly
`["/groundplane-agent","tunnel-token-cleanup-helper","--request-fd=0"]`.
It accepts exactly one big-endian uint32-length-prefixed deterministic protobuf
request of at most 8 KiB on stdin, returns exactly one framed proof of at most
4 KiB on stdout, and limits stderr to 8 KiB of stable-code-only output.

The request schema is closed to schema version 1; Task id; step id; assignment
attempt; Environment id; render generation; exact canonical relative raw-env
path; expected device, inode, length, and SHA-256; and the SHA-256 of the
matching private `tunnel_token_materialized` checkpoint. It carries no token
bytes, host path, executable, free-form argument, or alternate destination.
The helper opens `/run/groundplane/materialize` once as its only filesystem
root and applies ADR 0020's descriptor-relative
`RESOLVE_BENEATH|RESOLVE_NO_SYMLINKS|RESOLVE_NO_MAGICLINKS|RESOLVE_NO_XDEV`
policy to the exact supplied relative path. If the destination exists, it must
be the same single-link regular file with exact device, inode, uid/gid `0:0`,
mode `0600`, length, and SHA-256. Any mismatch fails without mutation. The
helper overwrites the exact length with zero octets and syncs the file when the
filesystem mechanically supports that operation; an unsupported zeroing
operation instead truncates to zero and syncs. Any other write, truncate, or
sync error fails closed. It then unlinks the verified entry descriptor-
relatively, fsyncs its parent directory, and returns a proof binding the request
digest, observed identity, cleanup method, and absence. This is logical
best-effort clearing, not a claim of physical erasure on copy-on-write,
journaled, flash, or remote storage.

An absent destination is idempotent success only when the Agent has already
authenticated the exact matching private materialization checkpoint and the
request repeats its digest; the proof records `already_absent`. An existing
object that differs in any requested identity, metadata, length, or digest is
never interpreted as partial cleanup and is not mutated. The persistent Agent
has no Environment-volume mount, retained file descriptor, or cleanup authority
outside this fresh helper session.

The cleanup runner uses the materializer lifecycle exactly: create with stdin
open, attach bounded stdin/stdout/stderr before start, record and inspect the
returned immutable container id, start by id, write the one frame, close the
write half, wait by id, accept the one bounded proof, and remove by id. Its name
is
`gp-ttc-<first-20-lowercase-hex(SHA-256(task-id || NUL || step-id || NUL ||
assignment-attempt-u64be))>`. Its complete labels are
`com.groundplane.managed=true`,
`com.groundplane.role=tunnel-token-cleanup-helper`, the exact
`com.groundplane.task-id`, `com.groundplane.step-id`,
`com.groundplane.assignment-attempt`, `com.groundplane.agent-id`,
`com.groundplane.agent-generation`, and
`com.groundplane.helper-schema=1`; no other label is permitted. Inspection
proves the name, labels, image child digest, argv, sole mount, capabilities,
network mode, restart policy, root-filesystem mode, and creation time before
start. Name and labels are discovery evidence, never mutation authority.

Every helper exit removes by immutable id. Task cancellation is already frozen
in `tunnel_compose_returned` and does not cancel the separate cleanup session.
Cleanup timeout, Agent shutdown, or lost attach first stops and then
force-removes the helper under the separate 30-second cleanup deadline; removal
failure is Internal and takes precedence over the Compose result. Before
assignment redelivery after restart, the Agent lists only the exact cleanup
role and Agent labels, inspects every returned immutable id, recomputes its
expected name and complete config proof, and force-removes an exact orphan under
that deadline. A same-name container or listed container with incomplete or
foreign proof is `component.recovery_required` and is not mutated. A cleanup
helper is never reattached or adopted; its fenced step replays only after exact
orphan removal.

Crash recovery resumes from the durable session checkpoints. From only
`tunnel_token_materialized`, it resolves the existing ADR 0022 Compose step to
a bounded return and records `tunnel_compose_returned`; from that checkpoint it
runs or reruns cleanup; from `tunnel_token_cleanup_proven` it may continue to
health proof, rollback, or terminal acknowledgement. A crash after unlink but
before proof is recovered by the checkpoint-bound `already_absent` result.
Cleanup failure is Internal and cannot be hidden by success, failure, timeout,
or cancellation from Compose. No later proof, observation, stop, or remove step
depends on the file. Every later recreate, repair, or rollback rematerializes
the Task's pinned Entry generation into a new session immediately before its
own `ComposeApply` and performs the same bounded cleanup afterwards. Health
means only that the expected container remains running with unchanged restart
count through observations 10 seconds apart and has matching image/labels.
Logs or Cloudflare APIs are not health inputs and no edge/account/route claim
is made.

A Tunnel-token Entry generation is valid only when its plaintext is 1..8192
octets and every octet is visible non-whitespace ASCII `0x21..0x7e`. NUL, CR,
LF, space, tab, non-ASCII, and empty values fail `validation.failed` before a
Tunnel claim; the Agent independently repeats the length, SHA-256, and byte-set
check after authenticated materialization and uses the existing ADR 0020
materialization-failure diagnostic on mismatch. The raw env file is exactly
the 13 ASCII octets `TUNNEL_TOKEN=`, the token octets unchanged, and one LF, so
its length is 15..8206 bytes and the cleanup request's expected length uses
that same bound. Compose raw format performs no interpolation
or quote removal, and `=` or `$` inside the token remains literal. cloudflared
receives the resulting `TUNNEL_TOKEN` environment variable; the token is never
placed in `command`, Compose YAML, a label, or proof output.

The canonical CoreDNS service key is `coredns` in project
`groundplane-infra`. It has host networking; no Compose network, address,
`ports`, or `expose`; the child-digest image; `pull_policy: always`; user
`0:0`; command
`["-conf","/etc/groundplane/coredns/Corefile"]` under the image entrypoint;
one generation-specific trusted Corefile mounted read-only at that exact path;
read-only root filesystem; `no-new-privileges`; all capabilities dropped
except `NET_BIND_SERVICE`; restart `unless-stopped`; disabled Compose
healthcheck; and the same bounded local logging. Component trusted files are
under `/var/lib/groundplane/agent/components/<component-id>/generations/<render-generation>/`;
the path is derived only by the Agent and never supplied by a plan.

The managed Component transition, rollback, and interruption-recovery
sequences formerly specified here are superseded. ADR 0061 owns enable,
update, disable, compensation, observation, and recovery through the generic
typed Agent procedure; this historical ADR neither requires nor rejects
recreation, reload, or any technology-specific mutation sequence.

The ADR 0022 plan extension is closed to
`ComponentImageVerify`, `ComponentConfigPreflight`, `ComponentHTTPProof`,
`ComponentRunningProof`, `ComponentDNSProof`,
`PlatformArtifactMaterialize`, `ResolverBaselineCapture`,
`ResolverMountTransition`, `HostTrustTransition`, and `TunnelTokenCleanup`.
`ExecutionPlan.operation.component_apply` gains exactly one
`coredns_config_apply` payload branch. These steps have the typed fields and
fixed behavior in this ADR. `TunnelTokenCleanup` mutates only its verified
Task-owned relative file through the authorized Environment-volume mount; the
two host transitions retain their separately closed host mutations. No step
accepts an executable, argument vector, host path, network target, or query
supplied by an operator.

### 9. Config replacement and reconciliation Task semantics

`PUT /components/{id}/config` remains HTTP 200. Its exact meaning is that one
complete desired config replacement was durably committed. It does not claim
that host convergence is complete.

When the replacement leaves desired enablement enabled, the same transaction:

1. validates the decoded request and current dependencies;
2. increments desired generation;
3. replaces desired config;
4. publishes one immutable reconciliation intent;
5. creates the Task, owner indexes, queue record, active-operation fence, and
   protected idempotency marker; and
6. stores the exact `200` response for replay.

The response is `ComponentConfigMutationResult`; its `resource` is the normal
post-commit `ComponentConfig` projection and it carries the new
`reconcile_task_id`. When desired enablement is disabled, config is saved
without host mutation and `reconcile_task_id` is null even if a failed disable
left the prior applied generation serving. A conflicting active enable,
disable, update, or reconciliation returns `component.in_flight`. An identical
complete replacement is a no-op with no new generation or Task and uses a
terminal direct `200` marker for its exact response.

This is the narrow hybrid Task-marker replacement of ADR 0021 described above,
not an untracked exception to `202 TaskAccepted`. The
operator capability is singleton replacement, so `component-config.set` is
still exactly one Console action, one CLI command, and one REST operation.
The Task is its durable convergence result, not a second apply action.

Synchronous mutation responses use closed wrappers; GET projections are
unchanged:

```text
ComponentConfigMutationResult { resource: ComponentConfig, reconcile_task_id: TaskID|null }
EntryMutationResult           { resource: Entry,           reconcile_task_id: TaskID|null }
RouteMutationResult           { resource: Route,           reconcile_task_id: TaskID|null }
ServiceMutationResult         { resource: Service,         reconcile_task_id: TaskID|null }
```

`component-config.set` returns `200 ComponentConfigMutationResult`.
`entry.create`, `route.create`, and `service.create` return their existing
`201` status with the matching wrapper; `entry.edit`, `route.edit`, and
`service.edit` return their existing `200` status with the matching wrapper.
`reconcile_task_id` is non-null exactly when the same transaction publishes a
Component graph Task and is null when normalized applied input is unchanged or
all affected managed Components are desired-disabled. A non-null wrapper uses
the hybrid Task marker and terminal replay rules; a null wrapper uses the
terminal direct marker. The `resource` is the post-commit fixed-revision
projection. Removal, Component enable/disable/update, and Blueprint apply keep
their existing `202 TaskAccepted`; GET endpoints never return a mutation
wrapper.

The operation manifest remains exactly one entry for each operation id named
above, with one REST endpoint, CLI command, and Console action. A wrapper is a
response schema, not an apply endpoint or second action. OpenAPI operation ids,
CLI commands, and Console actions do not gain `component.apply`,
`reconcile`, or any other alias.

If Agent-side binary validation fails, desired config remains saved and
visible, the prior applied snapshot remains serving, and status becomes
`degraded`. Generic Task retry reuses the same desired generation and
procedure. A later config replacement creates a new generation; retrying the
superseded Task returns `state.conflict` without host mutation.

Every publication and retry CAS pins the complete applicable base: Component
record revision and desired generation; prior applied generation, resolved
dependency digest, render generation, and artifact digest; owner and Agent
generations; resolver baseline and ownership generations; Entry id and
`cfg_...` generation; Route, Service, and Zone revisions; Caddy address
reservation; image index and platform-child digests; previous and candidate
Compose artifact hashes; active fences; and protected marker revision. A new
retry Task copies the immutable candidate and all base values. It may proceed
only when every base still matches, or when same-Task checkpoints prove the
exact candidate side effect already occurred. Any later dependency generation
or base revision returns `state.conflict` before assignment; no retry silently
rebases or follows a current Entry generation.

Enable and disable are explicit operational actions and return
`202 {task_id}`. They update desired enablement and publish the Task atomically.
Failure does not roll desired intent back. Update returns `202 {task_id}` and
does not change config or enablement.

### 10. Exact 1:1 human operations

The operation identities are:

| Operation id | REST | CLI | Console action |
| --- | --- | --- | --- |
| `component.list` | `GET /components` | `component list` | load owner-scoped Components |
| `component.show` | `GET /components/{id}` | `component show <target>` | open Component detail |
| `component-config.show` | `GET /components/{id}/config` | `component config show <target>` | load settings/output |
| `component-config.set` | `PUT /components/{id}/config` | `component config set <target>` | Save settings |
| `component.enable` | `POST /components/{id}/enable` | `component enable <target>` | Enable |
| `component.disable` | `POST /components/{id}/disable` | `component disable <target>` | Disable |
| `component.update` | `POST /components/{id}/update` | `component update <target>` | Update |
| `router.show` | `GET /environments/{id}/router` | `router show` | load Router projection |

All mutating operations require and honor `Idempotency-Key`. Config
replacement replays its exact `200` projection and Task id. Task-backed
actions replay the original `202` and Task id. An in-flight duplicate,
mismatched request, and unknown write outcome retain the repository-wide
idempotency contract.

For a hybrid marker, an identical request while its Task is pending or running
returns `idempotency.in_progress` rather than replaying premature success.
After any terminal Task state, replay returns the exact originally stored
`200` or `201` body and its original reconciliation Task id; it never
re-serializes current state. The encoded response is validated against the
128-KiB limit before the atomic claim.

Component config CLI flags are closed by kind:

- Caddy requires `--zone` and accepts `--template-file PATH|-`;
- Tunnel requires `--token-entry`;
- CoreDNS requires explicit `--upstream-auto=true|false`, repeatable
  `--upstream`, repeatable `--forward DOMAIN=RESOLVER[,RESOLVER...]`, and
  `--tailnet-delegation=true|false`; and
- Controller and Agent reject config set before making an HTTP request when
  their kind is known.

The Router remains a read-only projection of the same two Environment
Components. It carries stable Component ids, desired/applied enablement,
status, applied Caddy IPv4, exact public Route hostname checklist, Tunnel
origin guidance, token Entry readiness, and warnings. It has no mutable
Router record and no Tunnel hostname config.

### 11. Pure CoreDNS render input

The Controller renderer accepts one normalized, already-resolved value:

```go
type CoreDNSRenderInput struct {
	Hosts      []CoreDNSHost
	Forwarders []CoreDNSForwarder
	CatchAll   []ResolverEndpoint
}

type CoreDNSHost struct {
	Address   netip.Addr
	Hostnames []string
}

type CoreDNSForwarder struct {
	Domain    string
	Resolvers []ResolverEndpoint
}
```

Listen is not input. The renderer always emits `127.0.0.1:53`.

The input builder, not the renderer:

- selects the accepted pre-Groundplane Resolver baseline for auto mode;
- uses configured upstreams only under the exact fallback rules above;
- gathers host-valued Routes from successful applied Caddy projections;
- maps each hostname to that Environment's applied pinned Caddy IPv4;
- adds managed tailnet delegation when enabled;
- canonicalizes endpoints and DNS names;
- rejects conflicting hostname addresses, collisions, and self-forwarding; and
- calculates the normalized input SHA-256 placed in the Task and successful
  applied snapshot.

Public `input_sha256` covers only canonical non-secret control input. For
Tunnel it includes Component id, desired generation, Entry id, immutable
generation id, generated Service id, image ref, and non-secret output metadata;
it excludes token bytes, plaintext length, plaintext digest, ciphertext, and
every reversible or guessable derivative. The private materialization step
retains the ADR 0020 length and digest needed to authenticate the stream.

The renderer never reads a file, queries an Agent, walks Environments, chooses
a fallback, checks observations, allocates an address, or emits observed
metadata. Equal normalized input always produces byte-identical output.

### 12. CoreDNS validation and canonical Corefile

Input validation is exact:

- every host address is canonical IPv4;
- every hostname is an exact canonical DNS name accepted by Route validation;
- wildcard and path-only Routes create no host record;
- one hostname maps to exactly one address in one render;
- a forwarder domain is canonical and is not the root catch-all;
- each resolver directive has 1..15 unique usable endpoints;
- the catch-all has 1..15 endpoints after auto resolution;
- no resolver is a self, loopback, unspecified, multicast, or Docker embedded endpoint;
- duplicate domains fail;
- conflicting hostname mappings fail; and
- managed `ts.net` conflicts with operator `ts.net` and fails rather than silently winning.

Ordering is deterministic:

- host groups sort by numeric IPv4;
- hostnames within a group sort lexicographically;
- forwarders sort by descending DNS-label count, then canonical domain;
- resolver endpoints sort by canonical address and port; and
- managed tailnet delegation participates in the same forwarder ordering.

The Corefile uses an ASCII subset of UTF-8, LF line endings, four-space
indentation, and exactly one final LF. Its order is:

1. one `.:53` server block;
2. `bind 127.0.0.1`;
3. one inline `hosts` block when static records exist;
4. `no_reverse`, then `fallthrough` as the final two items of a present `hosts` block;
5. sorted domain forwarders;
6. one `forward .` catch-all;
7. `prometheus 127.0.0.1:9153`;
8. `log`; and
9. `errors`.

Example:

```corefile
.:53 {
    bind 127.0.0.1
    hosts {
        10.200.30.4 api.example.com app.example.com
        10.200.40.7 admin.example.com
        no_reverse
        fallthrough
    }
    forward lab.home.arpa 10.0.0.53
    forward home.arpa 192.168.1.1
    forward ts.net 100.100.100.100
    forward . 1.1.1.1 8.8.8.8
    prometheus 127.0.0.1:9153
    log
    errors
}
```

The complete `hosts` block is omitted when empty. No blank presentation
section, revision, timestamp, reload age, Environment label, record count,
health, arbitrary comment, `router.groundplane`, or `caddy.groundplane` is
emitted.

The enabled directive set is exactly `bind`, `hosts`, `forward`, `prometheus`,
`log`, and `errors`. Other plugins compiled into the official binary are not a mismatch by
their presence, but the renderer and preflight reject any other configured
directive. The exact CoreDNS image and platform-child verification remain
historical context. Managed-config publication and runtime transition semantics
are owned exclusively by ADR 0061.

### 13. Pre-Groundplane Resolver baseline

Auto mode never reads Groundplane's active `/etc/resolv.conf`.

After authenticated Agent Ready and before bootstrap initial apply, and before
every explicit re-enable following a successful disable, the Controller
publishes one Platform-owned Controller orchestration Task targeting the
CoreDNS Component. The operator enable action returns this parent Task id;
bootstrap uses actor `system`. The parent creates one Agent child Task with
operation `resolver_baseline_capture`, waits for its immutable result, commits
that result, publishes one `component_apply` Agent child, and remains running
until the apply child is terminal. Both children carry `parent_task_id`, share
the parent's owner, and use actor `system`; the parent retains the initiating
operator actor and mirrors child failure diagnostics. Parent deadline is 540
seconds, capture child deadline is 30 seconds, and apply child deadline is 480
seconds.

The capture child payload is exactly:

```text
ResolverBaselineCapture
  component_id
  expected_component_desired_generation
  agent_id
  agent_generation
  expected_groundplane_owned false
  helper_schema 1
```

It contains no path, link, resolver bytes, executable, or host-selected value.
Its checkpoints are exactly:

```text
baseline_claimed
resolver_helper_started
resolver_source_captured
baseline_manifest_published
baseline_observation_reported
```

The capture result extracts canonical usable nameserver endpoints, excludes
all rejected self/loop endpoints, and reports:

```text
ResolverBaseline
  component_id
  agent_id
  agent_generation
  baseline_generation
  ownership_generation
  document_sha256
  resolvers
  resolver_count
  captured_at
  received_at
  groundplane_owned           false
  path_kind                  regular | symlink
  local_manifest_sha256
```

More than 15 usable unique nameserver directives is
`component.baseline_invalid`; the Agent does not truncate or reorder by
availability. At most 15 are canonicalized for renderer input. The Agent stores
only a root-owned mode-`0600` local manifest at
`/var/lib/groundplane/agent/resolver/<component-id>/baseline/<baseline-generation>/manifest.json`.
It contains aggregate proof of the descriptor-resolved underlying path:
top-level type, up to eight link texts and uid/gid pairs, terminal regular-file
uid/gid/mode/length/digest, and parsed resolver endpoints. The captured
document and link bytes are bounded transient helper output and are discarded
after manifest construction; there is no backup to restore because Groundplane
never writes or renames the underlying chain.

Capture and mount ownership use one closed task-scoped host-integration helper.
The Agent creates it through its Docker socket from the verified Agent
platform-child image. The helper has user `0:0`, network `none`, PID mode
`host`, read-only container root, restart `no`, `no-new-privileges`, no Docker
socket and no host bind mounts. It drops all capabilities and adds only
`SYS_ADMIN` and `DAC_READ_SEARCH`. Its task-specific seccomp profile permits
only the fixed descriptor, `setns`, modern mount API, `umount2`, stat/read, and
framed-I/O syscalls. Its argv is exactly
`["/groundplane-agent","host-integration-helper","--request-fd=0"]`; one
length-prefixed protobuf request of at most 8 KiB selects
`capture|mount|unmount|verify`, and the response is at most 96 KiB with stderr
capped at 8 KiB stable-code-only output.

The helper opens `/proc/1/root` and `/proc/1/ns/mnt`, joins only PID 1's mount
namespace, and performs every lookup from the pre-opened root descriptor. It
accepts no path. Capture starts at fixed `/etc/resolv.conf`, resolves at most
eight symlink hops with `openat2`/`readlinkat` using
`RESOLVE_IN_ROOT|RESOLVE_NO_MAGICLINKS`, and permits only an exact regular-file
terminal. NUL, escape, magic link, device, FIFO, socket, directory terminal,
dangling link, cycle, link text over 4 KiB, aggregate link text over 16 KiB, or
document over 64 KiB fails closed.

The Groundplane resolver source is a Controller-specified constant containing
exactly `nameserver 127.0.0.1\n`, materialized by the Agent as uid/gid `0:0`,
mode `0644`, beneath the fixed trusted path
`/var/lib/groundplane/agent/resolver/<component-id>/owned/<ownership-generation>/resolv.conf`.
After descriptor-validating the underlying baseline against the capture
manifest, `mount` bind-mounts that source file over fixed
`/etc/resolv.conf`, then applies read-only, nosuid, nodev, and noexec mount
attributes. It never writes, chmods, chowns, unlinks, or renames any `/etc` or
`/run` entry, and the Agent retains no persistent host `/etc` mount.

The durable/local mount proof is stored at
`/var/lib/groundplane/agent/resolver/<component-id>/owned/<ownership-generation>/mount-proof.json`
beside the Groundplane-owned `resolv.conf`. It contains source and target
canonical fixed paths,
source and mounted-target `statx` device/inode pairs, mount id, parent mount id,
mountinfo major:minor, root, mount point, read-only option set, filesystem type
and source, Component id, Task id, step id, Agent generation, baseline
generation, ownership generation, and resolver source digest. `verify` rereads
`/proc/self/mountinfo`, requires exactly one mount at `/etc/resolv.conf`, and
requires every field plus mounted bytes/metadata to match. A same-content mount
without exact proof is foreign.

`ResolverMountTransition` has two replay-safe branches. `mount` accepts the
exact unmounted underlying manifest or the exact already-owned mount and
same-Task checkpoint; any other mount or baseline fails
`component.resolver_mount_mismatch`. `unmount` first verifies the exact owned
mount/proof, calls `umount2` only on fixed `/etc/resolv.conf`, proves that mount
id absent, then descriptor-validates the newly revealed current underlying
baseline and performs the non-CoreDNS DNS proof. Replay accepts an already
absent owned mount only with the same Task's unmount checkpoint and a matching
revealed baseline. It never restores captured bytes or link metadata.

Resolver and trust JSON files use one codec: UTF-8 canonical JSON with sorted
keys, no insignificant whitespace, and one final LF; duplicate or unknown
members, invalid UTF-8, non-integer numbers, non-canonical ids/timestamps/hex,
and trailing bytes are rejected. A baseline manifest is capped at 96 KiB, a
mount proof at 32 KiB, and a trust proof at 64 KiB before allocation. Binary
certificate and resolver-source files are length-bounded before allocation and
verified by SHA-256. Publication uses an exclusive mode-`0600` temporary in the
same directory, write/hash, `fsync`, rename, and parent-directory `fsync`;
creating a generation directory also fsyncs its parent. Every proof carries
Component ownership, its exact proof generation, Agent id/generation, and
originating Task/step/assignment-attempt lineage.

The current applied proof generation and any rollback predecessor named by
`last_attempt` are retained independently of Task/event pruning. Recovery reads
only these fixed generation paths and verifies the persisted Component proof
digest before host mutation. A superseded proof tree is removed only after a
later successful applied promotion no longer references it, no attempt retains
it, and deletion plus parent-directory fsync succeeds. Resolver proof storage
never contains captured resolver document bytes or a copied-content backup;
the only resolver document stored is Groundplane's constant owned source.

While `groundplane_owned=true`, no refresh may read `/etc/resolv.conf` because
that path points to CoreDNS. Controller and Agent restart reuse the durable
baseline manifest and exact mount proof. There is no Agent backup of the
underlying resolver content.

Successful CoreDNS disable unmounts the exact owned mount, reveals the
untouched current underlying resolver chain, directly queries at least one
revealed resolver and requires a matching root NS response, clears ownership,
and retains only non-secret audit metadata. The next enable always creates a
new parent and capture child; no prior baseline is reused. If a later disable
step fails, rollback revalidates the still-revealed baseline and remounts the
same exact Controller source before re-proving CoreDNS.

Crash reconciliation classifies only: exact owned mount with matching desired
or applied ownership generation; exact unmounted baseline with a completed
same-Task unmount checkpoint; or foreign/mismatched state. The first is
adopted and verified, the second resumes disable or remount rollback according
to the durable phase, and the third is
`component.resolver_mount_mismatch` with no host mutation. If desired/applied
requires ownership but the exact mount is absent, a repair Task first proves
CoreDNS, then mounts a new generation; it never adopts by content alone.

If auto mode has no usable baseline and no configured resolver, candidate
construction is blocked with `component.upstream_unavailable`. Groundplane
never substitutes a public provider.

The parent handoff transaction verifies the capture child's terminal result
and immutable result hash, stores the baseline generation, records the child
id, and publishes exactly one apply child plus Platform fence atomically. If
that transaction cannot fit or commit, the capture child remains terminal but
the parent remains retryable and no apply assignment exists. Redelivery
replays the same handoff by result hash.

A clean-start CoreDNS `TailnetDefaultDecision.pending` union starts at
`bootstrap_schema=1`. The
capture child pins one bounded authenticated Tailscale-present observation
generation and digest. The handoff transaction consumes that exact observation
once, writes the selected boolean into the durable CoreDNS desired config,
increments its desired generation, writes `decided` with
`source=tailscale_observation` and the exact generation/digest, and only then
renders the apply child. An operator config replacement instead writes
`decided` with `source=operator` and can never be overwritten. The handoff CAS
requires the still-pending union; restart and retry reproduce stored desired
input and never consult ambient observation during render.

### 14. Typed CoreDNS Agent procedure

Add one closed `CoreDNSConfigApply` execution payload. It carries only:

- stable Component and generated Service ids;
- exact index reference, expected platform-child digest, and platform;
- desired, render, Agent, resolver baseline, and ownership generations;
- previous and candidate Compose artifact ids and SHA-256 values;
- candidate Corefile SHA-256;
- normalized input SHA-256;
- at most one static proof: the lexicographically smallest canonical hostname
  and its expected IPv4 address, absent when the render has no static records;
- the fixed recursive proof `{name:".", type:NS}`;
- every canonical forwarder domain and its normalized resolver group; and
- whether this is initial resolver materialization.

It carries no executable name, arbitrary argument, host path, candidate bytes,
token, shell fragment, or user-selected query command. Candidate bytes use the
existing bounded transient materialization mechanism.

The exact ADR 0022 branch is:

```protobuf
message ComponentApply {
  oneof component_payload {
    CoreDNSConfigApply coredns_config_apply = 1;
  }
}

message CoreDNSConfigApply {
  string component_id = 1;
  string service_id = 2;
  uint64 desired_generation = 3;
  uint64 render_generation = 4;
  string agent_id = 5;
  uint64 agent_generation = 6;
  string image_index_ref = 7;
  string image_child_digest = 8;
  string platform = 9;
  string candidate_artifact_id = 10;
  bytes candidate_compose_sha256 = 11;
  string previous_artifact_id = 12;
  bytes previous_compose_sha256 = 13;
  bytes corefile_sha256 = 14;
  uint32 corefile_length = 15;
  bytes normalized_input_sha256 = 16;
  uint64 baseline_generation = 17;
  uint64 ownership_generation = 18;
  CoreDNSApplyMode mode = 19;
  CoreDNSStaticProof static_proof = 20;
  repeated CoreDNSForwardProof forward_proofs = 21;
}

message CoreDNSStaticProof {
  bool present = 1;
  string hostname = 2;
  bytes canonical_ipv4 = 3;
}

message CoreDNSForwardProof {
  string domain = 1;
  repeated string resolver_endpoints = 2;
}

enum CoreDNSApplyMode {
  COREDNS_APPLY_MODE_UNSPECIFIED = 0;
  COREDNS_INITIAL_ENABLE = 1;
  COREDNS_RECREATE = 2;
  COREDNS_DISABLE = 3;
  COREDNS_REPAIR = 4;
}
```

`previous_artifact_id` and its digest are empty only for initial enable;
baseline and ownership generations are zero only when the selected mode does
not change resolver ownership. `static_proof.present=false` requires its other
fields empty. Unknown enum values, absent mode-required fields, duplicate
proof domains, digest lengths other than 32, or field/config disagreement
reject the assignment before mutation.

`PlatformArtifactMaterialize` uses this independent schema-1 protobuf stream;
it does not reuse ADR 0020's Entry header, credit state, acknowledgement, or
reconnect semantics:

```protobuf
message CoreDNSArtifactFrame {
  uint32 schema = 1;                 // exactly 1
  bytes transfer_id = 2;            // exactly 32 bytes
  uint64 sender_sequence = 3;
  oneof body {
    CoreDNSArtifactOpen open = 10;
    CoreDNSArtifactChunk chunk = 11;
    CoreDNSArtifactCredit credit = 12;
    CoreDNSArtifactEOF eof = 13;
    CoreDNSArtifactAck ack = 14;
    CoreDNSArtifactCancel cancel = 15;
  }
}

message CoreDNSArtifactOpen {
  string task_id = 1;
  uint64 assignment_attempt = 2;
  string step_id = 3;
  string component_id = 4;
  uint64 desired_generation = 5;
  uint64 render_generation = 6;
  CoreDNSArtifactDestination destination = 7;
  uint64 byte_length = 8;
  bytes sha256 = 9;                   // exactly 32 bytes
}

message CoreDNSArtifactChunk {
  uint64 offset = 1;
  bytes content = 2;                 // 1..32768 bytes
}

message CoreDNSArtifactCredit {
  uint64 next_offset = 1;
  uint64 next_controller_sequence = 2;
  uint32 bytes = 3;                  // 1..65536
}

message CoreDNSArtifactEOF {
  uint64 byte_length = 1;
  bytes sha256 = 2;                  // exactly 32 bytes
}

message CoreDNSArtifactAck {
  uint64 byte_length = 1;
  bytes sha256 = 2;                  // exactly 32 bytes
  uint64 render_generation = 3;
  uint64 device = 4;
  uint64 inode = 5;
}

message CoreDNSArtifactCancel {
  CoreDNSArtifactCancelReason reason = 1;
}

enum CoreDNSArtifactDestination {
  COREDNS_ARTIFACT_DESTINATION_UNSPECIFIED = 0;
  PLATFORM_ARTIFACT_COREDNS_COREFILE = 1;
}

enum CoreDNSArtifactCancelReason {
  COREDNS_ARTIFACT_CANCEL_REASON_UNSPECIFIED = 0;
  COREDNS_ARTIFACT_ABORTED = 1;
  COREDNS_ARTIFACT_TIMED_OUT = 2;
  COREDNS_ARTIFACT_DISCONNECTED = 3;
  COREDNS_ARTIFACT_PROTOCOL_ERROR = 4;
}
```

`transfer_id` is SHA-256 over length-prefixed Task id, assignment attempt,
step id, Component id, desired generation, render generation, length, and
digest. Controller and Agent sender sequences are independent and start at
zero. Controller sequence zero is the sole `open`; it carries those exact
inputs plus destination enum `PLATFORM_ARTIFACT_COREDNS_COREFILE`. Agent
sequence zero is `credit {next_offset, next_controller_sequence, bytes}` on a
new or incomplete transfer, or the exact final `ack` on reconnect to an
already durable transfer.
Credit is positive and at most 64 KiB. A Controller `chunk` is at most 32 KiB,
has the exact credited offset, consumes credit by its byte length, and advances
Controller sequence by one. The temporary is exactly
`generations/<render-generation>/.coredns-transfer-<first-20-hex(transfer-id)>.tmp`
and its state is exactly the sibling
`.coredns-transfer-<first-20-hex(transfer-id)>.state.json`. Before publishing more credit the Agent
writes the chunk, `fdatasync`s the temporary, atomically publishes and fsyncs
the at-most-8-KiB state file with next offset/sequence and prefix SHA-256,
and fsyncs the directory. Thus reconnect never acknowledges volatile progress.

After exactly the declared bytes, the Controller sends one `eof` at the next
sequence repeating length and SHA-256. The Agent hashes and fsyncs the
temporary, renames it to
`generations/<render-generation>/Corefile`, fsyncs that directory, and sends
one `ack` at its next sequence containing final length, digest, device/inode,
and render generation. Either side may send `cancel` only at its next sequence;
it contains a closed stable reason code and no diagnostic text. Cancel closes
the stream, clears owned buffers, unlinks and syncs an unpublished temporary,
and never removes an already acknowledged final file.

Within one connection, a repeated frame with the same sender sequence and
byte-identical deterministic encoding is a duplicate: it causes no second
write and replays the last applicable credit or ack. Reusing a sequence with
different bytes, skipping a sequence, an unknown tag/field, a second open/EOF,
chunk after EOF, extra or early bytes, credit overrun, or digest disagreement
is `component.artifact_transfer_invalid`. On authenticated reconnect the
Controller repeats the identical open. The Agent verifies `transfer_id` and
the current assignment, then returns either ack for an already durable exact
final file or credit with its fsynced temporary/state pair's exact
`next_offset` and next
Controller sequence. A mismatched temporary is unlinked and synced before
returning offset zero; a mismatched final is recovery-required and is never
overwritten.

Controller and Agent each cap all transfer-state objects at 8 KiB and all
aggregate owned chunk buffers at 64 KiB per transfer. At most one transfer is
open per assignment and at most `max_concurrent_tasks` globally. Buffers have
single ownership and are cleared immediately after send/write and on every
error, cancel, disconnect, or timeout. No request field selects a host path.

Candidate bytes are at most 96 KiB. Resolver source bytes are at most 64 KiB,
one link is at most 4 KiB, all link text is at most 16 KiB, Caddy/Tunnel
generated files are each at most 96 KiB, a durable Component intent is at most
256 KiB, a deterministic `ExecutionPlan` is at most 4,194,304 bytes, a
canonical Compose artifact is at most 1,048,576 bytes, assignment fields
outside the plan are at most 65,536 bytes, and the complete protobuf assignment
is at most 4,259,840 bytes. One
graph contains at most three managed Components, three generated Services, 16
Compose artifacts, 8 CoreDNS forwarders, 1,024 hostnames, and 96 Task steps.
The sum of all artifact encodings, steps, and plan metadata must remain within
ADR 0022's 4,194,304-byte serialized plan ceiling even though one individual
artifact may reach its 1-MiB ceiling.
Publication and terminal mutation plans must pass ADR 0021's 96-operation and
1-MiB etcd transaction ceilings before claim; there is no post-claim batching
of an atomic Controller mutation.

The Controller performs one deterministic size gate after normalization,
rendering, deterministic protobuf encoding, and exact response/transaction
planning, but before any desired write, marker claim, fence, or Task
publication. Authored config, Corefile, response, intent, Compose artifact,
plan, assignment, graph-count, operation-count, or transaction-byte overflow
returns `component.artifact_too_large` with HTTP 422 and the violated bound in
non-secret details. The Agent checks the sealed declared sizes before helper or
file creation; a post-claim encoder, transfer, or local artifact exceeding or
disagreeing with those bounds ends the Task with
`component.artifact_size_mismatch`. Neither side truncates, batches, or retries
with a smaller representation.

The Agent independently validates the sealed plan, trusted platform path,
fixed image, ids, digests, generations, and step order before mutation.

CoreDNS does not provide a documented validation-only command. Preflight
therefore uses the exact pinned serving image in an isolated container:

- `--network none`, so its loopback listener cannot contend with live host CoreDNS;
- candidate Corefile mounted read-only at the fixed container path;
- read-only root filesystem;
- all capabilities dropped except `NET_BIND_SERVICE` in the bounding set;
- no published port, host path beyond the candidate, environment input, or
  restart policy; and
- fixed `/coredns -conf <candidate>` entrypoint and arguments derived by the Agent.

Positive preflight means the container completes setup and remains running
through two state probes within five seconds. Early exit, setup error, timeout,
or image/plugin mismatch is `component.config_rejected`. The Agent stops and
removes the isolated container on success, failure, cancellation, and restart
recovery.

The enable and update mutation sequence formerly specified here is superseded.
ADR 0061 alone determines candidate publication, runtime transition,
compensation, and proof ordering through the generic typed Agent procedure.

The following live proof is the immediate full Task proof. It runs once for
each enable, recreate, update, repair, rollback, and disable rollback before
promotion or acknowledgement; it is not the 15-second observation procedure.
Live proof uses a fresh randomized DNS message id, recursion desired, UDP first,
and TCP retry only when the UDP response is truncated. Each response must have
QR set, match id and question, and return before the step deadline. It requires:

- `NOERROR` and exactly the expected A address for the selected static proof
  when present;
- `NOERROR`, recursion available, and at least one NS answer for the root query
  through the catch-all;
- for the catch-all and every forwarder directive, one counter-attributed
  query through `127.0.0.1:53` plus direct reachability of the exact upstream
  identified by that counter; and
- the exact generation file digest, candidate Compose labels, image digest,
  render generation, read-only mounted artifact ownership, and the latest
  CoreDNS startup/reload log SHA-512 for the effective parsed configuration.

The effective configuration SHA-512 is computed exactly as CoreDNS 1.11.3:
parse the mounted Corefile with its exact container path, JSON-encode the
resulting Caddy server blocks, and hash those encoded bytes. The startup log is
mandatory and its latest reported digest must match. The
`coredns_reload_version_info` metric is absent on first startup; when present
after a reload, its `hash="sha512"` and `value` digest must also match.

The private Prometheus endpoint is fixed at `127.0.0.1:9153`, has no Compose
port publication, and may be queried only by the Agent. For a forwarder domain,
attempt `n` uses type A and the deterministic name
`gp-<first-20-hex(SHA-256(TaskID || stepID || n))>.<domain>`. Before and after
the query, the Agent fetches at most 256 KiB of metrics with a one-second
deadline and snapshots
`coredns_proxy_request_duration_seconds_count{proxy_name="forward",to,rcode}`.
It requires the DNS response to match id/question and have `NOERROR` or
`NXDOMAIN`, exactly one configured `to` series for that response rcode to
increase by one, and every other configured endpoint series for the group to
remain unchanged. It then directly sends the same question to that normalized
`to` endpoint and requires the same accepted rcode. More than one delta,
counter reset, missing/duplicate label, scrape truncation, or concurrent delta
on a group endpoint makes the attempt indeterminate, not healthy.

Catch-all uses the fixed root NS question with the same before/after counter
rule and additionally requires `NOERROR`, recursion available, and at least
one NS answer. Each group gets at most three attempts; the nonce is derived,
not random. DNS/direct attempts have a two-second deadline, at most one
counter window is open at a time, and the complete catch-all plus at most eight
forwarder proofs has a 60-second deadline. Static proof must cause no forward
counter delta. Matching Corefile SHA-256, image child, render labels, and a
freshly created container are required with the latest startup/reload log
SHA-512 of the effective parsed configuration; when the reload metric is
present, its digest must also match. `healthy` proves
the exact configured routing block and one reachable selected upstream per
group at proof time; catch-all success alone is insufficient.

Image evidence keeps the selected child and config digests distinct. With
classic Docker, the config digest is derived from matching container and image
inspect IDs. With the containerd image store, both inspect IDs and their typed
manifest descriptors must match the exact selected child, and the image's
reported platform must match the sealed platform. The proof's config digest
then denotes the sealed catalog config authority committed by that verified
child; it is not an independently observed hash of runtime config bytes.
The Controller still requires exact equality with the candidate's sealed config
digest. An index descriptor, inconsistent descriptor/platform, or mismatched
runtime ID cannot supply this binding.

Only after healthy initial apply does the Agent mount and verify the exact
read-only Groundplane resolver source over `/etc/resolv.conf`, retaining the
underlying baseline manifest and exact mount ownership proof.

On full-proof success the Agent atomically publishes
`/var/lib/groundplane/agent/resolver/<component-id>/dns/<render-generation>/full-proof.json`
with the same strict codec and a 64-KiB limit. It binds every proof result and
counter to Component, render, artifact, image, resolver ownership, Agent, and
Task/step/attempt lineage. The Controller persists its SHA-256 in
`ManagedApplied`; Task pruning does not remove it. The immediate observation is
written from that proof. Later cheap periodic observations can refresh runtime
freshness only while the exact retained full-proof digest still matches the
applied snapshot.

### 15. CoreDNS checkpoints and rollback

CoreDNS enable/config apply exposes these durable step checkpoints in order:

```text
candidate_materialized
candidate_preflight_started
candidate_preflight_passed
last_good_captured
candidate_compose_applied
static_query_verified        omitted when no static records exist
recursive_query_verified
forwarder_queries_verified
resolver_mount_verified      initial enable only
observation_reported
```

Each checkpoint is generation-, digest-, Task-, assignment-, and attempt-
fenced. A retry proves an existing checkpoint before skipping its side effect.
It never infers completion from a file or container that lacks matching
Groundplane labels, generation, mounted-artifact ownership, image digest, and
the latest CoreDNS startup/reload log SHA-512 of the effective parsed candidate
configuration. When the reload metric is present, its digest must also match.

The former update/reload paragraph was internally inconsistent with earlier
recreation text and is superseded. ADR 0061's generic typed lifecycle is the
sole current transition contract.

If Compose apply or live proof fails, the same attempt runs:

```text
rollback_candidate_stopped
rollback_candidate_removed
rollback_previous_applied
rollback_static_verified     omitted when old config had no static record
rollback_recursive_verified
rollback_forwarders_verified
rollback_observation_reported
```

A verified rollback leaves the previous applied snapshot and healthy
observation active, then fails the Task with `component.health_failed`. The
public Component is `degraded` because desired still differs from applied.

Rollback failure fails the Task with `component.rollback_failed`, records an
unhealthy or unknown observation with exact phase evidence, retains all
forensic checkpoints, and suppresses automatic retry. Operator retry remains
available after inspecting the Task. Operator retry starts with a typed recovery
probe that classifies the serving container, active bytes, resolver ownership,
image digest, and last-good digest; it may resume only from one exact recognized
state and otherwise fails `component.recovery_required` without mutation.

Initial-install failure removes the failed CoreDNS Service, deletes candidate
active bytes, preserves the baseline, and leaves `/etc/resolv.conf` unchanged.

CoreDNS disable uses:

```text
resolver_unmount_started
resolver_unmount_verified
previous_service_stopped
previous_service_removed
generated_service_removed
observation_reported
```

Resolver unmount and revealed-baseline proof occur before CoreDNS stops. If any
later disable step fails, the same attempt applies the exact prior Compose
artifact, proves its image/config/DNS state, and runs
`ResolverMountTransition.mount` against the exact revealed baseline before
failing.
Its rollback checkpoints are `disable_service_restored`,
`disable_live_verified`, and `disable_resolver_remounted`. If that rollback
fails, the Task ends with `component.rollback_failed`, retains ownership
evidence, and suppresses automatic retry. A failure before resolver restore
changes nothing and keeps the last-known-good Service and ownership.

CoreDNS config/apply/disable children have a 480-second deadline. Isolated
preflight is 30 seconds, each Compose transition is 45 seconds, immediate full
DNS proof is 60 seconds, each resolver transition is 30 seconds, immediate
observation publication is four seconds, and the complete rollback budget is
180 seconds within that total. These are per-step maxima, not additional
periodic work; the success path and the longest failure-plus-rollback path both
fit the child deadline. CoreDNS image update has the same 480-second deadline
and uses the same preflight, exact
previous/candidate Compose replacement, and proof before advancing `applied`.
Failure removes the replacement, applies the prior child digest and Corefile,
proves DNS, and only then fails the Task. A disabled managed Component update returns
`component.not_enabled` before marker or Task creation; Agent Component update
remains exclusively under the `agent` noun.

### 16. Environment Component procedure and checkpoints

Direct Environment Component actions, dependent resource reconciliation, and
Blueprint reconciliation share one
procedure. The Controller pins the complete current applied graph and one
candidate graph before Task publication. It allocates generated Service ids
and any candidate Caddy address in the publication transaction.

The ordered logical checkpoints are:

```text
dependency_snapshot_pinned
generated_identity_reserved
candidate_rendered
candidate_materialized
tunnel_token_materialized    immediately after each Tunnel raw-env materialization
tunnel_compose_returned       after each Tunnel Compose consumer returns
tunnel_token_cleanup_proven  before interpreting that Compose result
compose_applied_or_removed
caddy_config_applied         when Caddy config is active
component_health_verified
caddy_root_ca_captured       when Caddy is enabled
host_trust_verified          when Caddy is enabled or disabled
coredns_dependency_applied   when Caddy host records change and CoreDNS is enabled
observation_reported
```

Successful acknowledgement atomically advances every affected Component's
applied snapshot, releases obsolete Caddy reservations, updates the Task, and
releases active fences. Failure restores host mutations in reverse dependency
order, retains the prior applied snapshots and reservations, records
observations, and releases the active fences only at terminal acknowledgement.

Direct Caddy disable never silently changes Tunnel desired state. Blueprint
reconciliation may carry both candidates because the Blueprint explicitly
authored both enablement decisions.

Any successful direct Route create/edit, or Service edit, whose candidate
changes an enabled Caddy render or CoreDNS host input uses the registered hybrid
mutation: the desired resource mutation, immutable graph candidate, Task,
indexes, fences, protected marker, and exact `200`/`201` response commit
together. The resource response carries `reconcile_task_id`. Route, Service, or
Zone removal already returning `202` folds the graph procedure into that same
Task rather than creating a child apply Task. Mutations that do not change the
normalized graph return their ordinary synchronous response with a direct
marker and null reconciliation id. Blueprint apply retains its one existing
`202` Task. No `apply component`, hidden endpoint, or second Console action is
introduced, so 1:1 parity remains one operator mutation per capability.

### 17. One-host bootstrap and reconciliation

The startup sequence is:

1. systemd starts the dedicated etcd unit.
2. systemd starts the native Controller.
3. Controller validates startup config, storage schema, and compiled Component catalog.
4. On a genuinely empty product store, the Controller performs one linearizable
   range of `/v1/` before enabling any product writer or external listener. Any
   key fails with `storage.schema_incompatible`. It allocates three Component
   ids and one UTC timestamp, then uses one transaction whose compares require
   etcd `Version=0` for the marker and each of the three primaries, three
   Platform owner indexes, and three Platform kind indexes. The successful arm
   puts exactly those nine records plus this marker key:

   ```text
   key     /v1/meta/component-catalog-bootstrap
   Version exactly 1 after the transaction; it is never rewritten
   value   canonical UTF-8 JSON plus one LF:
   {"agent_component_id":"<agent-cmp-id>","catalog_schema":1,"coredns_component_id":"<coredns-cmp-id>","controller_component_id":"<controller-cmp-id>","record_schema":1}\n
   ```

   The ids in the marker exactly equal the primary/index targets. Each primary
   has `record_schema=1`, generation 1, no applied snapshot or attempt, and the
   common timestamp. Controller desired names the validated running native
   artifact. Agent desired is unenrolled, has no authoritative id/generation,
   and has `image_state=unconfigured` when startup `agent.image` is empty or
   `configured` with its validated immutable index reference otherwise.
   CoreDNS desired is enabled/configured with the section-6 empty resolver
   fields, provisional `tailnet_delegation=false`, and
   `TailnetDefaultDecision.pending{bootstrap_schema:1}`.
5. The only other accepted start state is that exact marker at etcd Version 1,
   byte-identical schema/value shape, plus exactly the
   three matching Platform primaries and owner/kind indexes. Any product key
   without the final marker, any Component primary/index from a scaffold or
   older schema, a partial set, unknown field/version, duplicate, wrong owner,
   or Environment Component created before final Platform bootstrap fails
   startup with `storage.schema_incompatible`. The Controller never migrates,
   repairs, dual-reads, or allocates a replacement stable id.
6. Controller records its own fresh native observation and only then marks the
   API and Environment-creation capability ready.
7. Controller reconciles the enrolled local Agent container through its native
   lifecycle manager.
8. After authenticated Ready, the Controller resumes and redelivers existing
   assignments before creating new reconciliation work.
9. Controller compares desired, applied, active Tasks, checkpoints, and latest
   observations for every Platform and Environment Component.
10. Controller publishes at most one repair Task per distinct drift episode.

CoreDNS is created desired-enabled in the initial Platform record. Before
Agent enrollment it is `pending`; no Controller-side host mutation occurs.
Its exact bootstrap config is the empty value in section 6 and its
`TailnetDefaultDecision` is the pending branch above. After Ready, the Controller parent,
capture child, and apply child are the ordered Task chain defined above. Caddy
and Tunnel are
created desired-disabled and unconfigured atomically with each Environment.

Reconciliation rules are:

- pending and running Tasks resume; they are not duplicated;
- a matching completed applied generation is not replayed because the
  Controller restarted;
- stale or missing observation for an applied-enabled Component creates at
  most one system repair Task for that drift fingerprint;
- a terminal failure for the same desired generation is not retried in a
  periodic loop;
- a new desired generation, explicit Task retry, or a distinct later drift
  episode may create new work;
- Agent disconnect preserves queue and assignment state; ordinary timeout and
  retry rules apply;
- if `/etc/resolv.conf` is Groundplane-owned while CoreDNS observation is
  absent or unhealthy, CoreDNS recovery is the first workload repair after
  Agent Ready; and
- the Controller never repairs host state directly.

A desired-enabled Component whose desired and applied generations differ and
has neither an active Task nor a terminal failure for that desired generation
publishes exactly one reconciliation Task. Config replacement normally does so
in its own transaction; startup recovery covers a committed pre-existing
divergence. Drift identity is the SHA-256 of Component id, desired generation,
applied artifact/image digests or absence, observation generation/state/digests,
and Agent generation. One durable repair claim per fingerprint prevents restart
duplication. A fresh matching observation or new desired generation retires the
claim; a terminal failure suppresses only that exact fingerprint.

All Component record, Task, idempotency, active-fence, address-reservation,
and successful applied-snapshot mutations use atomic revision comparisons.

### 18. Error and failure contract

HTTP errors use the repository-wide RFC 7807 shape. Relevant stable codes are:

| Code | Status | Meaning |
| --- | --- | --- |
| `component.not_found` | 404 | stable Component id was not found |
| `validation.failed` | 422 | decoded config or owner/kind input is invalid |
| `component.platform_unsupported` | 422 | Ready Agent platform has no pinned child image |
| `component.artifact_too_large` | 422 | normalized preclaim config, artifact, plan, response, or transaction exceeds its exact bound |
| `component.action_unsupported` | 409 | kind does not expose that action |
| `component.config_locked` | 409 | applied state forbids this config change |
| `component.config_required` | 409 | enable requires configured kind config |
| `component.not_enabled` | 409 | image update requires an applied-enabled managed Component |
| `component.in_flight` | 409 | conflicting Component/Environment reconciliation is active |
| `component.dependency_unsatisfied` | 409 | required Component or resource is not applied and healthy |
| `component.upstream_unavailable` | 409 | auto has no usable baseline and no explicit upstream |
| `resource.in_use` | 409 | dependency or Entry is retained by active desired/applied state |
| `state.conflict` | 409 | revision, desired generation, or retry base changed |
| `storage.unavailable` | 503 | durable state could not be committed |

Agent Task terminal diagnostics are:

- `component.config_rejected`: isolated binary preflight rejected candidate;
- `component.artifact_transfer_invalid`: framed CoreDNS transfer violated its exact state machine;
- `component.artifact_size_mismatch`: post-claim bytes or encoding disagreed with the sealed exact bounds;
- `component.image_mismatch`: Docker did not provide the pinned platform child;
- `component.compose_rejected`: canonical Compose validation or mutation failed;
- `component.health_failed`: apply failed live proof and rollback succeeded;
- `component.rollback_failed`: apply and rollback proof both failed;
- `component.resolver_mount_mismatch`: exact resolver mount ownership or transition proof failed;
- `component.recovery_required`: observed host state matched no safe replay branch;
- `component.observation_mismatch`: reported identity, generation, or digest
  did not match the assignment;
- `component.resolver_reveal_failed`: owned unmount did not reveal and prove the
  untouched current underlying resolver; no copied document is restored; and
- `agent_removed`, timeout, and abort retain their existing Task meanings.

Preclaim HTTP diagnostics are disjoint from postclaim Task diagnostics.
Malformed transport input uses the repository's existing HTTP decoder problem;
well-formed schema/reference/dependency failures use the table above.
Controller-side decoding, schema, reference, dependency, platform, fence,
revision, and storage failures discovered before atomic claim create no desired
mutation, protected marker, or Task. Once a Task/marker transaction commits,
the initiating HTTP response is its stored `200`/`201`/`202`; every later
validation, image, Compose, resolver CAS, health, observation, cleanup,
rollback, timeout, or recovery result is recorded only on that Task and is
never retroactively returned as an HTTP problem for the claimed request.

### 19. Acceptance contract

C12 and C13 remain incomplete until automated checks and the one-host L2
scenario prove all of the following:

- the catalog contains exactly the five accepted kinds and owner scopes;
- owner/kind indexes reject duplicates and owner mismatch atomically;
- clean bootstrap creates exactly three stable Platform records once;
- clean bootstrap accepts an unconfigured unenrolled Agent image, publishes the
  exact Version-1 marker/CAS, and consumes the exact pending tailnet union once;
- startup fails closed for a corrupt bootstrapped catalog;
- Environment creation publishes disabled Caddy and Tunnel stable records;
- desired replacement preserves applied and observed state;
- successful acknowledgement alone advances applied state;
- failed, timed-out, and aborted Tasks retain the prior applied snapshot;
- stale Agent generations cannot write observations;
- public status follows the exact desired/applied/observation table;
- API actions use Component ids, while CLI kind resolution selects the same id;
- public slug/key selectors resolve at admission and every normalized durable
  Component reference is a stable id, with no rename rewrite or Zone rename;
- every operation has one Console action, CLI command, and OpenAPI operation;
- the Console creates no local ids, observations, Task history, Corefile, or
  reload claims;
- config save returns 200 and atomically publishes exactly one Task when the
  replacement leaves desired enablement enabled;
- identical config replacement is a no-op and idempotency replay returns the
  exact stored response and Task id;
- unsupported actions and pre-claim validation create no Task or marker;
- Tunnel enable rejects every token source except the authorized Secret-store
  contract;
- token bytes never appear in public/durable control metadata or logs;
- the raw Tunnel env file exists only across its final Compose consumer; every
  Compose return enters the bounded fresh-helper cleanup session, and durable
  materialized, Compose-returned, and cleanup-proven checkpoints recover it;
- Tunnel cleanup proves the exact Task/generation/path/device/inode/size/digest,
  clears or truncates, unlinks, and directory-syncs the matching file; absence
  after matching materialization is idempotent and an existing mismatch fails;
- direct Caddy disable is independent of Tunnel lifecycle;
- Blueprint dual-disable imposes no ordering between Tunnel and Caddy;
- equal normalized CoreDNS input produces byte-identical output;
- Corefile directive, whitespace, ordering, plugin, and final-LF snapshots are exact;
- no invented internal hostname, wildcard, relative time, label, count, or
  comment enters Corefile bytes;
- every Environment hostname maps to its own applied Caddy IPv4;
- conflicting hostname/address mappings fail before Task publication;
- auto upstream uses only a usable pre-Groundplane baseline;
- no public resolver is silently selected;
- Groundplane-owned `/etc/resolv.conf` is never an auto-upstream source;
- isolated validation uses the exact pinned serving image without contending
  with the live listener;
- CoreDNS artifact transfer proves exact tags, sequence, credit, EOF,
  duplicate, ack, cancel, reconnect, fsync, and buffer bounds without falling
  back to ADR 0020's Entry protocol;
- invalid candidate bytes never replace active bytes;
- baseline capture completes as a separate durable Task before initial apply;
- trust, baseline, mount, and full-DNS proof files use the exact bounded local
  paths/codecs and remain recoverable independently of Task pruning;
- privileged host helpers and the volume-scoped Tunnel cleanup helper follow
  their exact create-attach-start-wait-remove lifecycle, immutable-id proof,
  cancellation cleanup, and restart orphan scan;
- regular and symlink resolver baselines remain untouched beneath the exact
  owned read-only mount;
- resolver mount proof prevents unmounting a foreign or mismatched mount;
- initial enable mounts over `/etc/resolv.conf` only after live proof;
- the ADR 0061 generic lifecycle proves managed-config transition,
  compensation, and interruption recovery without relying on this historical
  ADR's recreate-or-reload clauses;
- immediate apply uses the complete bounded DNS proof while feasible 15-second
  observations use only the four-second cheap check bound to that proof;
- disable unmounts only the exact owned mount and proves the revealed current
  resolver before stopping CoreDNS;
- disable failure after unmount proves Service and resolver-remount rollback;
- CoreDNS update replaces and, on failure, restores the exact digest-pinned Service;
- Controller and Agent restart resume checkpoints without duplicate mutation;
- an unhealthy Groundplane-owned resolver is prioritized after Agent Ready;
- Caddy/CoreDNS cross-dependency failure restores the prior complete applied graph; and
- dependent synchronous Route and Service mutations publish their graph Task and
  exact hybrid response atomically;
- every accepted config, candidate, response, Task, plan, and transaction fits
  its declared cardinality, byte, operation, and etcd ceilings;
- every preclaim oversize fails deterministically as HTTP
  `component.artifact_too_large`, while postclaim disagreement fails the Task
  as `component.artifact_size_mismatch`; and
- Platform Components Tasks and Platform Activity return identical Platform
  Task pages and cursors without Component filtering.

## Proposal and mirror boundary

This ADR remains Proposed. Its proposed supersessions, schemas, steps, helper
permissions, public projections, and error codes are not implementation
authority until owner acceptance. The current higher-authority product and
human-surface documents also do not authorize selectively implementing this
proposal merely where their prose happens to resemble it; accepted ADRs and the
current synchronized contracts remain the implementation boundary.

The pending synchronized cutover must resolve these known mirror differences
in one acceptance change:

- Historical mirror differences listed here predate ADR 0061 and are not a
  lifecycle contract. ADR 0061 and its synchronized current mirrors determine
  managed-config transition and compensation semantics.
- `docs/api-cli.md` currently presents id-only Component CLI targets and older
  request/response summaries. This proposal adds owner-scoped kind lookup for
  CLI presentation, admission-time public selectors, hybrid mutation wrappers,
  and the exact config flags while retaining stable-id REST entity paths and
  durable references.
- `docs/blueprint.md`, `docs/architecture.md`, the Console store, OpenAPI, and
  generated clients still describe their current accepted selector, Component,
  healthcheck/reload, helper, and machine-contract shapes. None may be treated
  as an implicit partial adoption of this proposal.

Until acceptance those differences stay explicit and unresolved. Acceptance
requires changing this ADR's status and synchronizing the complete dependency
set below in the same change; implementation may begin only from that accepted,
coherent contract.

## Clean replacements required

Acceptance requires removing, not preserving or adapting, all superseded shapes:

- Component `healthy` in the primary record;
- the Environment-only public Component DTO;
- Environment-targeted Task identity for direct Component actions;
- placeholder Component HTTP handlers and handwritten raw CLI transport;
- the CLI's empty Component config replacement;
- Console-local Component desired/observed mutation and manufactured activity;
- editable CoreDNS listen state;
- Console-local Corefile rendering and relative reload metadata;
- hardcoded shared Caddy addresses;
- `router.groundplane` and `caddy.groundplane` fixture records;
- Tunnel `hostnames`, `origin`, and `tokenRef` config;
- platform Secret or project Secret storage for the Tunnel token;
- random Console-generated Tunnel tokens and `secrets/.env.edge` behavior;
- ambient `/etc/resolv.conf` reads while Groundplane owns the resolver;
- any Agent helper with a persistent host `/etc` bind or arbitrary host-writer
  path, command, or argument;
- copied resolver baseline restoration in place of exact mount/unmount ownership;
- the MVP text that makes render time read ambient `/etc/resolv.conf`;
- lifecycle claims that conflict with ADR 0061's generic typed Agent procedure;
- tag-only or index-only Caddy, cloudflared, and CoreDNS runtime selection;
- any compatibility parser or alias for the superseded config shapes;
- ADR 0021 implementations that assume every Task marker response is `202`; and
- ADR 0034 implementations that candidate the whole Component primary or require
  the Environment id as every direct Component Task target; and
- ADR 0022 implementations only insofar as they reject the closed
  non-container operations and steps explicitly named by this ADR.

The exact synchronized mirror dependency set is `docs/mvp.md` Component,
Router, DNS, trust, bootstrap, and mutation-result text; `docs/api-cli.md`
operation manifest, request/response schemas, flags, statuses, and examples;
`docs/blueprint.md` `x-gp-components` selector/omission grammar;
`docs/architecture.md` Component, execution-plan, OpenAPI, and host-helper
boundaries; the OpenAPI source and all regenerated API/CLI/Console clients;
the CLI parser/help and operation manifest; the Console store types, fixtures,
Component settings, Router, Entry/Route/Service mutations, Task links, and
status projections; Component/Entry/Route/Service persistence and indexes;
the CoreDNS/Caddy/Tunnel renderers; ADR 0022 plan validation and Compose
artifacts; Agent assignment/materialization/observation/host-helper protocols;
and the C12/C13 unit, contract, and one-host L2 acceptance fixtures. Acceptance
requires one clean synchronized cutover of every item; none is a compatibility
mirror.

The authoritative product, Blueprint, human-surface, generated-client, Console
store, persistence, execution-plan, Agent, and acceptance contracts must be
updated together if this proposal is accepted.

## Alternatives considered

### Return `202` from config replacement

Rejected in this proposal. The primary mutation is a complete singleton
desired replacement, and the established human contract gives it `200`.
Returning only TaskAccepted would hide the successfully stored desired config.
The response instead carries the exact reconciliation Task for the new
generation through the explicitly registered hybrid Task marker.

### Save config without scheduling reconciliation

Rejected. It contradicts DNS-save-to-Corefile-reload behavior and leaves no
operator action whose meaning is "apply current config". `component update`
updates the compiled runtime artifact; it is not a config-apply alias.

### Roll desired state back when Agent validation fails

Rejected. A successful config `200` must remain true, and desired intent must
not be confused with last applied state. The separate applied snapshot and
degraded status make failure explicit without lying about either value.

### Address REST actions by kind

Rejected. Kind is owner-scoped lookup metadata; stable ids own references,
Task targets, idempotency scope, and entity routes. The CLI and Console may
resolve kind before making the stable-id request.

### Store Tunnel token in a platform Secret

Rejected. The token belongs to one Environment deployment and must be exposed
only to its generated Tunnel Service. A same-Environment Entry supplies the
existing encrypted generation and transient materialization boundary without
creating a second Secret resource.

### Emit `forward . /etc/resolv.conf`

Rejected. Groundplane rewrites that path to CoreDNS, which creates a forwarding
loop after the first successful apply or restart.

### Silently use public DNS when auto observation is unavailable

Rejected. Resolver selection changes where host DNS queries are disclosed.
Groundplane uses an observed pre-ownership baseline or explicit operator input
and otherwise fails with `component.upstream_unavailable`.

### Invent `coredns -validate`

Rejected. The pinned binary documents no validation-only command. The isolated
serving-image preflight proves parse and setup without contending with the live
host listener.

### Choose recreation or reload in this ADR

Superseded. ADR 0061's generic typed Agent lifecycle is the sole authority for
managed-config publication, runtime transition, compensation, and recovery.
This historical ADR does not reject or require either mechanism.

### Persist rendered Corefile bytes in etcd

Rejected. Corefiles remain derived artifacts. Durable config, applied source
snapshot, normalized input digest, Task checkpoints, and Agent-owned
last-known-good bytes provide reproducibility and rollback without making
render output desired state.

### Add health, ready, loop, cache, or metrics plugins

Rejected for the MVP. Each changes runtime behavior or public operational
surface. The closed plugin set and explicit Agent queries are sufficient for
this contract.

## Consequences if accepted

- One Component interface covers all accepted kinds without erasing their
  ownership or action differences.
- Desired intent, successful applied state, and runtime observation cannot
  overwrite one another.
- Config `200` responses remain honest while every enabled change has a
  durable, retryable convergence Task.
- Human surfaces use stable Component identity even when the CLI and Console
  present owner-scoped kind labels.
- Cloudflare Tunnel keeps its accepted outbound-only exposure and manual DNS
  workflow without a Cloudflare control-plane integration.
- Tunnel token bytes stay inside the Secret store and transient Agent
  materialization boundary.
- CoreDNS output is pure and byte-deterministic.
- Auto upstream cannot loop into Groundplane or silently disclose queries to a
  public resolver.
- Invalid CoreDNS candidates leave the current resolver untouched.
- Runtime transition and interruption behavior is determined by ADR 0061, not
  by this historical decision.
- Live apply failure has explicit rollback proof and a distinct catastrophic
  rollback-failure state.
- Controller and Agent restarts converge from durable intent, applied
  snapshots, observations, Tasks, and checkpoints without duplicate work.
- The cost is a broader Component persistence cutover, typed Platform records
  and observations, a new closed CoreDNS Agent procedure, and synchronized
  replacement of current placeholder and fixture surfaces before C12/C13 can
  be accepted.
