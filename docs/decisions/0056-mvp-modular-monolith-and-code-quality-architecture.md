# ADR 0056: MVP modular monolith and code-quality architecture

Status: Accepted for the MVP

## Context

Groundplane's product topology is intentionally small: one native Controller,
one Controller-managed Agent container, one host, one human API, and three
frontends. The implementation has nevertheless accumulated several structural
risks before the MVP is complete:

- `internal/app` contains application behavior even though its documented job
  is dependency injection and process assembly;
- `internal/infra/etcd` contains many unrelated aggregates and several very
  large persistence and lifecycle files;
- the Console store and Environment page have become cross-feature modules;
- the architecture says both "one task pipeline" and that every host mutation
  belongs to the Agent, while accepted contracts require separate Agent and
  native Controller executors;
- declarative adapter and component contracts may import infrastructure under
  the current import matrix;
- interfaces, concrete types, and conversion boundaries do not have a closed
  design rule; and
- the delivery gate checks formatting and behavior but does not mechanically
  reject dependency, module-growth, unsafe-conversion, or file-growth drift.

These are maintainability problems, not reasons to introduce microservices,
runtime plugins, a generic orchestration framework, or enterprise-scale
machinery. The required correction is a conventional Go modular monolith with
deeper modules, concrete types, explicit execution authority, and enforceable
fitness checks.

## Scope

This decision closes the MVP implementation architecture for:

- process and task-executor authority;
- desired, durable, observed, and ephemeral state ownership;
- Go package and Console module placement;
- interfaces, concrete types, and boundary conversion;
- persistence adapter placement;
- documentation ownership;
- structural quality gates; and
- the refactoring sequence for unfinished MVP work.

It does not change the product nouns, public actions, Blueprint grammar,
single-host topology, adapter catalog, or accepted lifecycle behavior. It does
not introduce multi-host placement, high availability, runtime plugin loading,
service discovery, an event bus, a dependency-injection framework, or a second
backend.

## Decision

ADR 0061 amends the Component extension seam. Registered Component code no
longer lives under the root module's `internal/components` or imports
Groundplane domain types. The closed MVP integrations compile in the separate
`registered-components` module against the pure `component-sdk` module, and
only `internal/app` imports concrete registrations. The modular monolith,
explicit composition, state ownership, concrete-first, and two-executor rules
below remain authoritative.

### 1. Groundplane remains one modular monolith

The shipped process topology remains exactly:

```text
groundplane-controller.service
    -> Controller-managed etcd OCI container
    -> embedded Console and REST API
    -> scheduler and native Controller executor
    -> Controller-managed Agent OCI container
        -> Agent executor and workload Docker/Compose operations
    -> Controller-managed isolated Runner runtimes
```

The repository continues to build the Controller, Agent, and CLI from one Go
module. Packages are internal unless they are an intentional wire contract.
No package is split into a separately deployed service for the MVP.

The architecture uses two useful upstream ideas without copying their scale:

- Go packages remain small, named, internal modules with concrete exported
  implementations and consumer-owned interfaces.
- Reconciliation compares durable desired state with observed external state
  and moves reality toward the desired state through an explicit executor.

Kubernetes controllers, informers, generic resource machinery, dependency
injection containers, and Docker's engine-scale plugin architecture are not
adopted. Groundplane has one host and a closed resource catalog.

### 2. One Task journal has exactly two executors

There is one durable Task model, journal, event stream, idempotency boundary,
retry lineage, abort action, and terminal protocol. Execution authority is an
explicit immutable Task field. Task kind, target, or queue position never
implies authority.

| Executor | Concurrency | Owns |
| --- | --- | --- |
| `agent` | bounded by the durable Agent config | Workload and platform Compose operations, materialization through constrained helpers, backup/restore data-plane steps, adapter procedures, scripts, and observed-state collection |
| `controller` | one serial recoverable native queue in the MVP | Agent container lifecycle, isolated Runner runtime lifecycle, persistence-only finalizers and no-op operations, and other procedures that must remain available before or without the Agent |

The closed authority rules are:

1. An HTTP handler, scheduler, or Console action never performs a host mutation
   directly.
2. Every asynchronous operation atomically publishes its intent, immutable
   execution authority, durable Task, and required fences before side effects.
3. An Agent cannot claim a Controller Task. The native executor cannot claim
   an Agent Task.
4. Controller execution is not a generic escape hatch. Adding a native
   procedure requires an accepted ADR or an amendment to an existing one.
5. Agent execution receives a sealed typed plan. It does not interpret the
   Blueprint, select policy, infer ownership, or repair a missing Controller
   decision.
6. Both executors acknowledge through the same durable terminal transaction
   model and public Task projection.

The MVP statement that "everything host-level is actioned by the Agent" is
narrowed accordingly. The Agent owns workload and platform data-plane host
effects. The native Controller owns only its closed bootstrap and isolation
substrate: the Agent container, Runner runtimes, and persistence-only native
procedures.

### 3. State has one owner per category

| State category | Authority | Durability |
| --- | --- | --- |
| Product desired state | normalized Controller records derived from accepted human input and Blueprint decisions | etcd |
| Operational intent | Controller-owned lifecycle intent, policy, locks, allocation claims, and immutable Task inputs | etcd |
| Task state | Task primary, ownership, executor, claims, events, replay evidence, checkpoints, and terminal receipts | etcd |
| Compiled execution | canonical immutable metadata, plan digest, artifact identities, and reconstructable non-secret inputs | etcd while required for replay |
| Transient execution material | secret bytes, short-lived tokens, rendered secret files, streaming logs, and helper frames | memory or constrained runtime files only for their bounded operation |
| Observed state | typed evidence reported by the Agent or collected by a closed native observer | etcd only where the product contract requires durable evidence; otherwise bounded memory |
| Docker, systemd, filesystem, and remote object state | external reality, never desired-state authority | external systems |
| Console state | a view and request cache over the human API | browser memory only |

Docker labels, container presence, task output, Console state, and historical
events never become alternate desired-state stores. A retry either reconstructs
the same authorized plan or fails closed; it never reconstructs intent from
current external state.

### 4. Packages are layered and owned by capability

The dependency direction remains layered, but behavior is placed in named
capability modules inside those layers. The target shape is:

```text
cmd/*                              process entrypoints only
internal/app                       composition roots and process lifecycle only
internal/core/<capability>         pure domain values and transition decisions
internal/controller/<capability>   Controller use cases and orchestration
internal/controller/handlers       typed Huma boundary adapters
internal/agent/<capability>        Agent execution modules
internal/infra/etcd                shared etcd client/CAS/key primitives only
internal/infra/etcd/<capability>   concrete aggregate persistence adapters
internal/infra/<technology>        concrete Docker, systemd, age, S3 adapters
internal/adapters/<kind>           declarative backing-service contracts
component-sdk                      public pure Component Capability contract module
registered-components             closed build-time Component planner module
internal/common/<name>             narrowly named cross-binary leaf modules
pkg/api                            the intentional human wire contract
proto                              the intentional Agent wire contract
console/src/features/<capability>  feature state, actions, and presentation
```

A capability module owns a coherent behavior such as Tasks, Environments,
Backups, Runners, Agent lifecycle, or Attachments. It is not a directory for
every type sharing a noun. Its interface must hide orchestration and invariants
rather than expose each internal step to callers.

The placement rules are:

- `internal/app` constructs concrete implementations, registers routes, starts
  processes, and shuts them down. It contains no validation, persistence
  transaction, product transition, rendering, or task-sequencing behavior.
- `internal/core` contains domain values and pure decisions. It does not import
  HTTP, Huma, protobuf, etcd, Docker, systemd, filesystem, or Agent packages.
- `internal/controller/<capability>` owns one human intent from validated input
  through durable publication. It does not expose persistence keys or wire
  models as its interface.
- `internal/infra/etcd` becomes a small technology module. Aggregate records,
  keys, encoders, and transactions move into capability subpackages as those
  areas are refactored. The parent never imports its capability children.
- adapter packages remain declarative root-module contracts. Registered
  Component planners import only `component-sdk` and approved stdlib, never any
  root-module package, and execute no side effect.
- `internal/common` is a namespace, not a `common`, `util`, `helpers`, or
  `types` grab-bag package. New shared code goes into a meaningful leaf package
  only when at least two binaries genuinely share the behavior.
- `pkg/api` and `proto` contain wire types, not product decisions. Boundary
  adapters convert them into validated domain/application input.

The current top-level layer packages are migration sources, not permanent
homes for additional capability logic. New MVP work must use the target
placement when it touches a hotspot.

### 5. Concrete types are the default

Go interfaces are permitted only for one of these reasons:

1. A consumer needs a behavior with at least two real implementations.
2. A side-effecting external dependency needs a deterministic test adapter.
3. The standard library already defines the correct interface, such as
   `io.Reader`, `io.Writer`, `fs.FS`, or `http.RoundTripper`.
4. A process boundary requires a deliberately small port whose implementation
   is selected in `internal/app`.

Every repository or runtime port must satisfy all of these rules:

- the interface is declared by the consuming package;
- it contains only methods that consumer calls;
- its methods describe behavior, not transport mechanics or a concrete
  implementation's complete method set;
- constructors in implementing packages return concrete types;
- the interface never appears in a persisted, JSON, protobuf, Blueprint, or
  public API model; and
- a compile-time assignment may prove conformance, but runtime type assertions
  may not recover implementation-specific behavior.

Interfaces are rejected when they exist only to mock a struct, group unrelated
methods, avoid choosing a type, hide a circular dependency, or permit later
type assertions. An interface with one production implementation and a fake is
acceptable only at a side-effect seam; pure logic is tested through concrete
types and results.

There is no central `Repository`, `Service`, `Manager`, `Provider`, or
`Registry` interface covering unrelated capabilities. Narrow capability names
must state the behavior callers receive.

### 6. Boundary parsing replaces unsafe conversion

External values are untrusted until one boundary parser constructs a valid
internal value. The boundaries are HTTP/OpenAPI, CLI arguments, protobuf,
etcd records, YAML/Compose, Docker, systemd, filesystem input, registry
content, and remote object storage.

The type rules are:

- stable identifiers, digests, CIDRs, image references, owner variants,
  lifecycle states, and secret-bearing inputs use validated concrete types at
  the layer that owns their invariants;
- semantically different identifiers are not passed through one unqualified
  string parameter list when swapping them would be valid Go but invalid
  Groundplane;
- closed variants use typed constants plus exhaustive validation and switches;
- contradictory bags of optional fields are replaced by constructors or
  explicit variants;
- wire structs remain wire structs and domain structs remain domain structs;
  mapping is explicit at the boundary;
- JSON round trips, `map[string]any`, reflection, `unsafe`, double assertions,
  and `interface{}` conversion are never mapping mechanisms;
- a type assertion is allowed only at a true language or standard-library
  boundary where the dynamic type is the contract and the failure is handled;
  it is not allowed to recover a concrete implementation hidden behind a local
  interface; and
- generated OpenAPI and protobuf types are authoritative. Handwritten parallel
  wire models are removed rather than converted through casts.

Type safety must reduce downstream branches. A wrapper that merely renames a
string without validating or preventing misuse does not justify itself.

### 7. The Console follows the same module ownership

The generated OpenAPI client is the only transport contract. The Console does
not add a generic repository or interface layer over it.

The root store owns only cross-feature workspace selection, shared navigation
identity, and composition of feature providers. Each feature owns its request
state, actions, and projections under `console/src/features/<capability>`.
Routes remain shallow parameter resolution and composition.

A feature page is split by independently understandable behavior, not by
arbitrary visual fragments. Components receive concrete generated or
feature-owned types. `any`, `unknown as T`, double casts, non-null assertions
used as validation, and duplicated handwritten API response types are
prohibited outside a named parsing boundary.

The Console remains a frontend. It never derives lifecycle authority,
permissions, health, task completion, subnet allocation, or defaults that the
Controller has not returned.

### 8. Persistence adapters preserve domain transactions

An etcd capability adapter owns its keys, encoders, fixed-revision reads, CAS
transactions, and replay classification. The Controller capability owns the
use-case ordering and product decision. Pure transition calculations are
extracted from transaction assembly when that makes them independently
testable; transaction invariants are not split across packages merely to make
files shorter.

Repository methods return concrete domain results and typed Groundplane
errors. They do not return raw etcd responses, keys, revisions without a named
meaning, or partially decoded maps to callers.

Cross-aggregate transactions remain in one module chosen by the aggregate
that owns the invariant. A generic transaction builder may provide mechanics,
but it may not encode product policy through callbacks, untyped operation
lists, or reflection.

### 9. Documentation has the same ownership discipline

The source-of-truth order remains unchanged. Each document has one job:

| Document | Owns |
| --- | --- |
| `mvp.md` | product behavior, vocabulary, topology, and acceptance outcome |
| `blueprint.md` | authored desired-state grammar only |
| `api-cli.md` | human wire and CLI surfaces |
| `architecture.md` | process, module, state, and dependency architecture |
| `standards.md` | mechanically enforceable coding rules |
| ADRs | expensive decisions, invariants, consequences, and rejected alternatives |
| `capabilities.md` | implementation status and evidence, never future authority |
| package docs and tests | local mechanics and executable invariants |

An ADR is not a second complete product specification. Wire schemas belong in
their source files, key grammars beside persistence code, and acceptance
transcripts under `docs/acceptance`. Mirrored text states the decision and
links to its authority instead of copying entire algorithms into several
documents.

### 10. Quality is enforced by ratchets and fitness checks

The foundational quality slice adds one `make architecture-check` gate and a
pinned Staticcheck release compatible with the repository's Go version. It is
then included in `make ci`.

`architecture-check` must reject:

- import-matrix violations;
- new capability behavior in `internal/app`;
- adapter imports of infrastructure or a registered Component import outside
  `component-sdk` and approved stdlib;
- a concrete Component registration imported outside `internal/app`;
- backend-shaped planner parameters, untyped Component config/intents,
  Component I/O, secret reveal, CLI execution, or arbitrary command authority;
- new imports of a catch-all `common`, `util`, `helpers`, `types`, or
  `interfaces` package;
- constructors in implementation packages that return a local interface when
  a concrete type can be returned;
- local interface values followed by implementation type assertions;
- reflection, `unsafe`, unapproved `any` model fields, JSON-based type
  conversion, and double TypeScript casts;
- hand-edited generated artifacts;
- a new handwritten production file over 600 physical lines;
- a new handwritten test file over 1,000 physical lines; and
- growth of an existing file already above its applicable limit.

Generated artifacts are excluded from size checks and remain protected by
their generation-drift checks. A large immutable fixture may receive a narrow
path-specific exception. Every exception is explicit, justified, and cannot
match a directory or future file.

The size rule is a reviewability ratchet, not a reason to create pass-through
files. A split is accepted only when each resulting module has a coherent
interface and ownership. Moving methods without reducing caller knowledge is
not a refactor.

Existing oversized files are a finite migration set. They may shrink without
reaching the target in one change, but they may not grow. A security or
correctness hotfix may receive a one-change exception only when extracting
first would increase risk; the same change records the exact debt and the next
bounded extraction.

Every change is reviewed on correctness, readability, architecture, security,
and performance. Passing tests is necessary but does not override a structural
failure. Refactoring and new behavior are separate commits unless the old
shape makes the behavior impossible to add safely.

### 11. Refactoring is incremental and precedes new growth

There is no repository-wide rewrite. The migration is ordered:

1. **Q0: Contract and fitness foundation.** Synchronize the contract
   corrections, add architecture checks, pin Staticcheck, and capture the
   finite oversized-file baseline.
2. **Q1: Composition root.** Move use-case behavior out of `internal/app` and
   leave process construction and shutdown only.
3. **Q2: Task execution modules.** Establish the explicit shared Task journal
   plus closed Agent and Controller executor modules before adding more
   lifecycle behavior.
4. **Q3: Persistence extraction on touch.** Move Runner, Backup, Environment,
   and other aggregate adapters from the flat etcd package as their remaining
   MVP slices are implemented. Shared etcd mechanics stay in the parent.
5. **Q4: Console decomposition.** Split the root store and Environment page by
   capability before wiring their remaining live actions.
6. **Q5: Remaining MVP verticals.** Deliver Backups, Runners, Release Groups,
   remaining Blueprint reconciliation, and Console parity through the new
   modules.

Each step preserves behavior, passes focused tests, and leaves one
implementation. Old files, types, routes, wrappers, and adapters are deleted
when callers move; no compatibility layer survives before the first release.

Remaining MVP work may proceed in parallel only when file ownership and module
interfaces are already closed. A swarm does not decide architecture and does
not merge its own work.

## Contract corrections required by this decision

Acceptance of this ADR requires one synchronized documentation change that:

- replaces the stale `agent.yaml` wording with the Controller-owned Agent
  runtime document;
- replaces the absolute Agent-only host-mutation statement with the closed
  Agent-versus-Controller executor authority above;
- defines the human API default as loopback and permits additional explicitly
  configured trusted private listen addresses, while forbidding public or
  Cloudflare-Tunnel exposure without a future authentication decision;
- describes one Task journal with two executor queues rather than one executor
  path;
- replaces the privileged root-module Component registry with ADR 0061's
  compiler-enforced SDK and registered-planner module boundary;
- describes `internal/common` as named leaf packages rather than a shared
  catch-all module; and
- adds the concrete-first interface, boundary conversion, and quality-ratchet
  rules to `standards.md` and `delivery.md`.

These are contract repairs. They do not add a Console action, CLI command, API
endpoint, or Blueprint field.

## Consequences

- The existing process topology and user experience remain unchanged.
- New capability work cannot silently enlarge the current god modules.
- Persistence remains transactional without making etcd the owner of product
  orchestration.
- Interfaces become smaller and rarer; fakes remain available at real
  side-effect seams.
- Type conversion becomes visible and testable at boundaries instead of
  failing later through assertions.
- The Controller/Agent split becomes internally consistent with native Agent
  and Runner lifecycle work.
- Some unfinished MVP features must first extract the module they would
  otherwise make worse.
- CI becomes stricter and initially requires a bounded cleanup slice before
  all new gates can pass.

## Rejected alternatives

### Keep the layer packages and only split large files

This reduces individual file size but leaves ownership and caller knowledge
unchanged. `internal/app`, the flat etcd package, and the Console store would
continue accumulating unrelated behavior.

### Reorganize the entire repository by vertical feature

A full feature-package rewrite would mix domain, HTTP, persistence, and Agent
execution concerns and create a large migration before the MVP. Capability
ownership inside strict layers provides locality without losing dependency
direction.

### Define an interface for every repository and implementation

Implementor-owned interfaces and constructors returning interfaces make APIs
harder to evolve and encourage runtime assertions. Interfaces remain
consumer-owned and exist only at genuine behavioral seams.

### Use `any`, maps, reflection, or serialization for generic orchestration

Groundplane's resource and step catalogs are closed for the MVP. Generic
conversion weakens compile-time proof without buying a required extension
surface.

### Adopt Kubernetes-style generic controllers

Groundplane benefits from desired-versus-observed reconciliation, not from
generic resources, informers, admission chains, or independently scheduled
controllers. Typed capability modules are smaller and clearer for one host.

### Adopt a dependency-injection framework

Explicit constructors in `internal/app` make dependencies visible and remain
easy to test. A container or service locator would hide the graph and replace
compile-time wiring with runtime lookup.

### Enforce size limits without a baseline

The repository already contains oversized files. An immediate blanket limit
would either block every change or require broad suppressions. A finite,
non-growing baseline creates an enforceable path to zero exceptions.

## Acceptance evidence

Decision acceptance requires:

- synchronized review against `mvp.md`, `api-cli.md`, `blueprint.md`,
  `architecture.md`, `standards.md`, `agents.md`, and `delivery.md`;
- an explicit inventory of every corrected contradiction;
- review of the package target against the current import graph;
- review of the interface rules against official Go guidance; and
- confirmation that no public capability or Blueprint decision changed.

Implementation acceptance additionally requires:

- `make architecture-check` in `make ci`;
- pinned Staticcheck execution over production tags;
- a committed finite oversized-file baseline with no directory wildcards;
- tests proving each architecture rule detects one violating fixture and
  accepts one valid fixture;
- `internal/app` containing composition only for migrated capabilities;
- no new adapter/component infrastructure import;
- no new unsafe conversion or implementation-recovery assertion;
- focused behavior tests for every extraction; and
- the full delivery gate before any refactoring slice is declared complete.

No implementation evidence is claimed by this accepted architecture decision.
Its Q0 fitness-gate implementation and the subsequent refactoring slices remain
delivery work tracked by `docs/capabilities.md`.

## Upstream guidance

- Go Code Review Comments, especially consumer-owned interfaces and concrete
  implementation returns: https://go.dev/wiki/CodeReviewComments
- Effective Go, especially small behavioral interfaces and package naming:
  https://go.dev/doc/effective_go
- Go module organization, especially `internal` packages for server projects:
  https://go.dev/doc/modules/layout
- Go package naming, especially avoiding catch-all API/types/util packages:
  https://go.dev/blog/package-names
- Kubernetes' desired-versus-current controller pattern, adopted only as a
  reconciliation principle: https://kubernetes.io/docs/concepts/architecture/controller/
- Moby contribution guidance, especially internal packages, Go community
  conventions, and avoiding utility/helper packages:
  https://github.com/moby/moby/blob/master/CONTRIBUTING.md
- Staticcheck 2026.1, the pinned Go 1.26-compatible static-analysis baseline:
  https://github.com/dominikh/go-tools/releases/tag/2026.1
