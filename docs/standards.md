# Go and interface standards

These rules define allowed dependencies and implementation boundaries. They are
not a duplicate build recipe. [Architecture](architecture.md) explains ownership;
[delivery](delivery.md) owns testing and release gates.

## Import boundaries

| Path | Purpose | Imports from | NEVER imports |
| --- | --- | --- | --- |
| `cmd/groundplane` | CLI entry — thin: init ctx, init logging, run root | `internal/app`, `internal/cli` | anything else |
| `cmd/controller` | Controller entry — root daemon | `internal/app` | anything else |
| `cmd/agent` | Agent entry — the Agent container | `internal/app` | anything else |
| `internal/app` | Composition roots only: construct concrete modules, register transports, run and shut down each binary | everything its binary needs | product validation, persistence transactions, rendering, task sequencing, or feature behavior |
| `console/` | Build-tagged stdlib-only embedded Vite assets | stdlib only | every internal or external package |
| `internal/cli/` | Cobra commands — parsing, transport and presentation; `common/` = error handler + output | `pkg/api`, `pkg/errs` (error plumbing only), `internal/cli/common`, `internal/common` | `internal/controller`, `internal/agent`, `internal/infra`, `internal/adapters`, registered Component modules |
| `internal/controller/` | API server, task sequencing, serialization locks, renderers, secret store, scheduler | `internal/core`, `internal/adapters`, `internal/infra`, `internal/common`, `component-sdk`, `pkg/api`, `pkg/errs` | `internal/cli`, concrete registered Component implementations |
| `internal/agent/` | gRPC client, worker pool, compose applier, materializers | `internal/core`, `internal/infra`, `internal/common`, `pkg/errs` | `internal/controller`, `internal/cli`, `internal/adapters`, `internal/components` |
| `internal/core/` | Domain model + desired-state schema (Blueprint) — pure | `internal/common`, `component-sdk`, `pkg/errs` | `internal/infra`, `internal/controller`, `internal/agent`, `internal/adapters`, concrete registered Components, `internal/cli` |
| `internal/adapters/` | Declarative registry and one typed contract package per backing kind | `internal/core`, narrowly named pure `internal/common/*` leaves, `pkg/errs` | `internal/infra`, `internal/cli`, `internal/controller`, `internal/agent`, registered Component modules |
| `component-sdk/` | Separate public module containing only typed Component Capability descriptors, grants, immutable planning snapshots, and intents | approved stdlib only | root-module `internal/**`, `pkg/api`, protobuf, Huma, persistence, Docker, filesystem/process/network I/O |
| `registered-components/` | Separate public module containing the closed build-time Caddy, Cloudflare Tunnel, and CoreDNS planners | `component-sdk`, approved stdlib only | root module, `pkg/api`, protobuf, Huma, etcd, Docker, filesystem/process/network I/O, CLI |
| `internal/infra/` | Concrete technology adapters; `infra/etcd` owns mechanics and `infra/etcd/<capability>` owns aggregate persistence | stdlib, external deps, named `internal/common/*` leaves, `component-sdk`, `pkg/errs`, and `proto/agentpb` only for the Agent helper machine contract | `internal/cli`, `internal/controller`, `internal/agent`, `internal/adapters`, concrete registered Components, `pkg/api` |
| `internal/common/` | Namespace of narrowly named cross-binary leaf packages; never an importable catch-all common/util/helpers/types/interfaces module | stdlib, necessary external deps, `pkg/errs`, and `proto/agentpb` only for the closed shared machine contract | upward feature packages and public human API models |
| `pkg/api/` | Public API types + stable error codes; the OpenAPI spec is generated from `internal/controller` handlers (code-first) | `internal/common`, `pkg/errs` | `internal/infra` |
| `pkg/errs/` | The single error type + codes | stdlib only | everything — leaf package |
| `proto/` | The agent channel contract — generated Go from `.proto`, never hand-edited | — | — |


Go rejects import cycles, but not every forbidden dependency. Repository
architecture gates enforce additional rules; compilation alone does not prove
compliance. Named common packages are leaves, not an importable catch-all.
Cross-domain transactions stay with their explicit coordinator rather than being
split into independent commits merely to move code.

## Modules, interfaces and types

- Composition roots construct and connect modules; they do not validate product
  input, sequence Tasks, render configuration or own transactions.
- The Controller decides and publishes work. The Agent executes sealed work and
  reports evidence; it does not select new desired state or recovery targets.
- Use concrete types and concrete constructor returns by default. An interface
  needs real varying behavior, a side-effect seam, process port or standard
  library contract. Declare it in the consuming package with only needed methods.
- Do not create interfaces just for mocks, mirror a concrete implementation or
  recover one through an assertion. Pure logic takes concrete inputs and results.
- Persisted, JSON, protobuf, Blueprint and public API models have concrete fields.
  Parse external values into validated types at their boundary. Use semantic
  identifiers and closed variants where interchange would otherwise compile.
- Conversion is explicit: no reflection, unsafe conversion, JSON round trips,
  untyped maps, double assertions or TypeScript double casts. Dynamic boundary
  assertions must handle failure and cannot recover an internal implementation.
- Add named operations for actual needs, not generic extension points for imagined
  future use. Components remain pure public-SDK planners with no direct I/O,
  plaintext Secrets, raw Agent payloads or arbitrary command declarations.

## Errors

Use [pkg/errs](../pkg/errs) as the single error taxonomy. Constructors take a
closed Kind; its descriptor owns public code, class and HTTP status. Do not add
bespoke error types, status overrides or string matching. Public codes are stable
API values, not internal constructor choices.

Wrap private causes for diagnosis without exposing provider or secret text.
Internal/request-failure details remain generic. Compare Kind or use semantic
helpers; distinct Kinds can intentionally share a public code. CLI error handling
and Controller problem responses each use their shared adapter rather than
formatting errors in feature code. Cancellation and broken-pipe behavior remain
with those adapters.

## Context, processes and concurrency

Every side-effecting repository or infrastructure function takes `context.Context`
first. Fixed standard-library signatures may instead capture the operation context
or use bounded cleanup. Propagate cancellation and join goroutines; do not leak
unbounded workers. Agent concurrency follows its configured task bound. Controller
operation owners provide serialization, not scattered ad hoc locks.

Subprocesses use [the shared Runner](../internal/common/runner) for deadlines,
logging and cancellation. Raw execution exceptions are limited to:

- the infrastructure Docker SDK/CLI bridge;
- infrastructure systemd unit control;
- validated staged-binary handoff;
- the fixed [PostgreSQL helper](../internal/postgres16helper) and its command,
  where closed child execution and Linux process-control operations cannot be
  represented by Runner.

These exceptions do not extend to callers. A new exception needs an explicit
reason recorded with the change, not an undocumented bypass.

## Shared utilities and input handling

Use [structured logging](../internal/common/logging) and `log/slog`, never secret
values or interpolated diagnostic strings. CLI presentation is separate from
logging. Logging precedence is debug, verbose, environment, config, then WARN;
the log file retains DEBUG. No alternate logging framework or setup per feature.

Use [the config reader](../internal/common/config) with strict keys and
fail-fast validation. Precedence is defaults, file, environment, flags. Defaults
belong in one owner and must be part of the contract, not silent fallbacks added
in executors. Controller and Agent do not add independent YAML readers.

Use [the ID package](../internal/common/ids) for stable prefixed ULIDs, not ad hoc
random/time formatting. Use `time.RFC3339` for timestamps. CLI commands perform
parsing, scoped resolution, transport and presentation; they never import host
infrastructure. Empty tables still show headers. Command names and aliases are
owned by the command source and [API/CLI conventions](api-cli.md).

## Code style and prohibited patterns

- Package and directory names agree; use descriptive names without underscores
  or implementation-recovery aliases. Nullable values with meaningful zero values
  use pointers. Error messages start lowercase.
- No `goto`, implicit `init()` wiring, or reflection without an accepted decision.
  No untyped model/validator fields; unavoidable standard-library boundaries are
  not permission to propagate `any` internally.
- Use address literals or a pointer helper, not `new(T)` for pointer construction.
  Do not discard errors without an explicit reason.
- Do not validate compile-time constants with runtime regexes or copy records
  merely to restate the same fields. Copies needed to isolate mutable input or
  protect immutable ownership are necessary, not redundant transformations.
- New handwritten production files are limited to 600 physical lines and test
  files to 1,000; generated files are excluded. Existing oversized files may
  shrink but not grow without a specific approved exception. A pass-through split
  is not a coherent module.

## Testing and enforcement

Follow [delivery's testing policy](delivery.md). Straightforward wiring and static
text/schema/declaration checks do not justify tests. Behavioral tests must name a
[QA matrix](qa-matrix.md) case and explain the concrete failure they detect.
A missing rationale comment alone does not make a useful regression disposable.
Coverage percentages are diagnostic, not product readiness.

Pure behavioral checks, hermetic side-effect checks and real operator journeys
have different evidence boundaries. Formatting, generation, compilation and
architecture gates are separate from product cases. The Makefile and workflows
own executable gate commands; do not duplicate them here.

The production restructuring was implementation-only. Existing tests and frozen
gate metadata were not migrated or run; current compliance is not established.
See [remaining qualification](issues/runtime-qualification.md#architecture-and-tests).
