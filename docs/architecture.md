# Architecture

Groundplane is a modular monolith with three Go binaries: Controller, Agent and
CLI. The Console is a React/Vite SPA embedded in the Controller. One product
decision has one owner, regardless of which client invokes it.

[Product scope](mvp.md) defines behavior. [Coding standards](standards.md) defines
dependency and implementation rules. [Technical decisions](decisions/README.md)
explains the reasons for the important boundaries below.

## Processes and data flow

The [Task execution replacement](decisions/task-execution-redesign.md) is implemented
and deployed to disposable QA, with partial [qualification](qa-matrix.md).
The [scheduler](../internal/controller/workdispatch)
owns assignment and capacity; operation handlers own typed effects and recovery.
[Execution storage](../internal/infra/etcd/workqueue), resource state and disposable
diagnostics have separate owners. Assignments bind to the selected host and Agent
generation. This does not qualify remote-Agent operation.

Console or CLI → typed Controller API → capability-owned planning and publication
→ durable Task and resource work → host-bound attempt → checked result → committed state.

The Controller selects work, validates resource ownership and captures immutable
inputs. The Agent runs only its assigned typed procedures. Native Controller
execution owns bootstrap, Runner, connection-check and persistence orchestration.
Bootstrap and persistence-only work can run without an Agent. Backing access
checks delegate a fixed consumer-network helper through the authenticated Agent
stream while the Controller retains the captured input, result and cleanup
ownership. These executors share scheduling, not execution permission.

DNS checks capture the acknowledged resolver configuration rather than reconstruct
authority from a previous Task's retained history. The compiled Component catalog
selects the fixed read-only recipe; the authenticated Agent verifies that exact
configuration before and after querying. The native Controller Task owns result
settlement and retention, separate from configuration activation. Interruption
records failure, not an automatic query replay. See the
[DNS check owner](../internal/controller/dnsresolver/connection_check_service.go).

Runner diagnostics reuse completed registration authority and the existing
Runner container. The Controller captures that authority once, records execution
before invoking the fixed read-only helper, and persists results under its native
Task claim. Interruption is not permission to register or probe again. The runtime
owner serializes checks with maintenance and removal; refreshed observations alone
do not invalidate a result. See the
[Runner check owner](../internal/controller/runner/connection_check_service.go).

Tunnel diagnostics share one captured-input executor and cleanup owner for two
origins: a managed public Route or an ordinary Service's acknowledged stable
proxy. The operator supplies the public target for a standalone Tunnel; an opaque
provider token cannot establish ingress policy. Private connector-network results
remain separate from Controller-outbound public results. Interrupted checks retain
only exact helper cleanup authority and do not replay probes. See the
[Tunnel check owner](../internal/controller/edgechecks).

The Controller runs natively so recovery can start without a working Agent.
It manages the private etcd and Agent containers directly. etcd is not an
Agent-applied Component. Ordinary Controller shutdown closes clients without
stopping etcd. Application runtime must remain independent of Controller restart.

Start at [application composition](../internal/app),
[Agent execution](../internal/agent) and
[local Agent lifecycle](../internal/controller/localagent).

## Module ownership

| Boundary | Responsibility and entry point |
| --- | --- |
| Composition | [app](../internal/app) constructs concrete modules and connects their lifetimes; [host commands](../internal/app/hostcommands) bind short-lived execution protocols separately from daemon startup. No feature policy or persistence transactions. |
| Controller capabilities | [controller](../internal/controller) owns use cases, typed API adapters, scheduling and immutable plan preparation |
| Domain | [core](../internal/core) owns pure domain values and decisions without infrastructure |
| Agent capabilities | [agent](../internal/agent) owns assigned execution, cancellation and reports; no desired-state selection |
| Technology adapters | [infra](../internal/infra) owns Docker, etcd, filesystems and other external effects |
| Capability persistence | [etcd](../internal/infra/etcd) and its capability modules own atomic state changes and storage constraints |
| Shared values and behavior | [common](../internal/common) contains narrowly named leaves used across binaries, not a generic utility package |
| Human clients | [cli](../internal/cli) and [Console features](../console/src/features) own presentation and request state |
| Registered integrations | [Component SDK](../component-sdk) and [registered Components](../registered-components) isolate pure integration planners |

Group code by the decision it hides, not by a shared suffix such as `manager`,
`helper` or `types`. A module should own a cohesive operation and keep its
private storage or runtime details out of callers. Splitting a file without
reducing caller knowledge is not a module boundary.

Concrete types are the default. Consumers declare narrow interfaces only for
actual variation, side effects, process ports or standard-library contracts.
Constructors return concrete implementations. No interface-to-concrete recovery,
compatibility facade or parallel legacy implementation.

## Persistence and execution

etcd is the durable control-plane store. Capability repositories own record
meaning; shared storage code owns reads, transactions and encoding mechanics.
Coordination that must atomically commit a Task, desired state and resource
ownership remains one transaction, even when preparation comes from several
capabilities. A file move does not justify weakening that boundary.

Desired input, prepared execution, acknowledged runtime and live observation are
separate facts. A prepared value cannot become applied state merely because
publication succeeded. Terminal publication needs evidence from the assigned
execution and compares the sources used during preparation.

Immutable Blueprint revisions retain one cleanup owner. Apply revisions use the
native parent Task; synchronous metadata edits retain their publication descriptor
instead of creating a no-op Task. Both use the same
[retention guards](../internal/infra/etcd/blueprintretention/guards.go): current
desired state, acknowledged runtime, unresolved omissions and active executions
keep their captured revision available. Cleanup retires companions before the
immutable root, after replay retention permits it. A durable scan cursor prevents
held revisions from starving other cleanup. This implementation is locally compiled;
interrupted cleanup and concurrent publication still require behavioral qualification.

Current code entry points include
[Blueprint preparation](../internal/infra/etcd/blueprintplanning),
[Component preparation](../internal/infra/etcd/componentplanning),
[network reservations](../internal/infra/etcd/networkreservations),
[hierarchy deletion planning](../internal/infra/etcd/hierarchydeletionplanning),
[execution](../internal/infra/etcd/hierarchydeletionexecution) and
[finalization](../internal/infra/etcd/hierarchydeletionfinalization).
These are navigation pointers, not an exhaustive package catalogue.

Task replay, failure recovery and resource deletion retain exact operation
ownership. Unknown external effects remain restricted until accounted for.
Recovery may use captured predecessor runtime and authorized pinned configuration;
it cannot select the latest desired files or reinterpret a database migration.
See the corresponding [decisions](decisions/README.md).

## API contracts (locked)

The human API uses REST/JSON. Editable typed handlers and
[public API models](../pkg/api) generate [OpenAPI](../openapi.json), which
generates the CLI Go client and Console TypeScript transport types.
The Agent uses [protobuf](../proto/agent.proto) over its local authenticated gRPC
stream. These are different boundaries with different authorities.

Edit the source definition and regenerate derived artifacts when implementing
a wire change. Do not maintain a second handwritten schema in Markdown or the
root Console store. Accepted missing protocols must be explicitly labelled as
unimplemented in their design owner.

Generation detects artifact drift; it does not prove Console action wiring or
operator-surface parity. [API usage](api-cli.md) defines those requirements.

## Console

[Console UX standards](console-ux.md) govern navigation, capability placement,
language, interaction and [workspace presentation](console-ux.md#workspace-presentation)
across resource scopes. Apply them to new and changed Console UI using shared
presentation primitives. They describe the target design;
the [operator guide](features/console-and-api.md) describes current behavior.

[UI primitives](../console/src/components/ui) own controls and interaction.
[Shared presentation](../console/src/components/common) composes them.
[Feature modules](../console/src/features) own requests, state and product
presentation. [Routes](../console/src/routes) stay shallow.

The [workspace store](../console/src/lib/store.tsx) composes feature providers;
it does not define product behavior or wire schemas. Environment operation state
and observation are owned by their [feature](../console/src/features/environment).
The existing plum/lavender light and dark design system remains shared.

Production serves embedded Vite assets without Node or adjacent files. API and
operational routes take precedence over SPA fallback. Path validation and asset
cache policy belong to the serving boundary, not individual pages.

## The Component Capability seam

Registered integrations compile only against the public SDK and approved
standard library. They receive immutable scoped inputs and return typed intents.
They receive no repositories, Docker/filesystem handles, secret plaintext,
arbitrary execution or private Controller APIs.

Groundplane validates grants and ownership, prepares the shared capability
operations and publishes them atomically. The Agent executes compiled generic
recipes. A new integration must not add implementation-named backend branches.
Controller and Agent are processes/resources, never registered Components.

Backing provisioning adapters are a different boundary. Built-ins use typed
inputs and facts; Custom hooks use the explicit bounded command contract.
That feature does not authorize runtime Component plugins.

## Maintenance and qualification

Cross-layer integration journeys live in application composition and construct
repositories through their public APIs. Storage tests retain private fixtures
where they check repository internals. Production APIs are not expanded merely
to expose test fixtures. Local gate results do not establish live qualification.

[Delivery](delivery.md) owns checks and release criteria. Toolchain versions,
file limits and import checks are sourced from repository manifests and gates;
this page does not duplicate their implementation.
