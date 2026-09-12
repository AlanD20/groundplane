# Groundplane Go Coding Standards

Coding standards, conventions, and architectural rules for the Groundplane
Go codebase (Controller, Agent, CLI). Companion to `architecture.md` (the
layout and principles) and `mvp.md` (the product contract). Adapted from
the mvmctl codebase's proven rules.

## 1. Package structure and import matrix

| Path | Purpose | Imports from | NEVER imports |
| --- | --- | --- | --- |
| `cmd/groundplane` | CLI entry — thin: init ctx, init logging, run root | `internal/app`, `internal/cli` | anything else |
| `cmd/controller` | Controller entry — root daemon | `internal/app` | anything else |
| `cmd/agent` | Agent entry — the ECS-agent container | `internal/app` | anything else |
| `internal/app` | Composition roots only: construct concrete modules, register transports, run and shut down each binary | everything its binary needs | product validation, persistence transactions, rendering, task sequencing, or feature behavior |
| `console/` | Build-tagged stdlib-only embedded Vite assets | stdlib only | every internal or external package |
| `internal/cli/` | Cobra commands — one file per noun, zero logic; `common/` = error handler + output | `pkg/api`, `pkg/errs` (error plumbing only), `internal/cli/common`, `internal/common` | `internal/controller`, `internal/agent`, `internal/infra`, `internal/adapters`, registered Component modules |
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

**Key rule:** named `internal/common/*` packages, `pkg/errs`, and `pkg/api` are
leaves. The
CLI never imports `internal/infra` (it must stay buildable without etcd,
docker, systemd, age). Go's circular-import detection enforces the matrix:
a violation is a compile error, not a review finding.

## 2. Layer rules

- **CLI layer** — argument parsing, output, user-facing text ONLY here.
  Commands call `pkg/api`-generated clients, never core/infra directly.
  `PersistentPreRunE` on root sets up logging level (the `--debug` /
  `--verbose` chain from architecture.md).
- **Controller layer** — the single backend (locked): validates, sequences,
  renders, schedules, serializes. Every handler is typed; request
  validation happens at the API boundary.
- **Agent layer** — dumb executor (locked): pulls tasks, runs the worker
  pool (`max_concurrent`), applies Compose, materializes files at 0600,
  acks. No decision authority.
- **Core layer** — pure model + Blueprint schema. No infra imports, so it
  tests in isolation.
- **Adapters layer** — contract shapes (see architecture.md). One package
  per kind. Core never switches on kind; the registry is the only entry.
- **No generic extension points.** "This is extensible for future use" is a
  smell. Prefer typed named methods that say exactly what they do
  (`ProvisionDatabase`, `Dump`, `Restore`). Add methods when needs arise.

## Interface and type rules (locked: ADR 0056)

- Concrete types are the default. Implementing constructors return concrete
  structs or pointers, not locally declared interfaces.
- An interface requires a real varying behavior, a side-effect test seam, a
  process port, or an existing standard-library contract.
- Interfaces are declared by the consuming package and contain only methods
  that consumer calls. Do not mirror an implementation's full method set.
- Persisted, JSON, protobuf, Blueprint, and public API models never contain
  local interface fields.
- Never hide a concrete implementation behind an interface and recover it with
  a type assertion. Add the required behavior to the owning seam or keep the
  concrete type.
- Do not introduce interfaces solely for mocks. Pure logic uses concrete
  inputs/results; fakes belong only at real side-effect seams.
- External data is parsed into validated concrete values at HTTP, CLI,
  protobuf, etcd, YAML, Docker, systemd, filesystem, registry, and object-store
  boundaries.
- Semantic identifiers and closed variants use types that prevent accidental
  interchange where that error would otherwise compile.
- Conversion is explicit. Reflection, `unsafe`, JSON round trips,
  `map[string]any`, double assertions, and TypeScript `unknown as T` or double
  casts are prohibited conversion strategies.
- A type assertion is allowed only at a true dynamic language or
  standard-library boundary, must handle failure, and must not recover a local
  implementation choice.

## 3. Error handling (locked: one error type)

`pkg/errs` is the **only** error type in the codebase. No multi-type
assertion chains, no bespoke error structs.

```go
// Simple — public Code, Class, status, and Op derive from closed internal Kind
errs.New(errs.KindServiceNotFound, "service not found: app-api")

// Wrapping a cause
errs.Wrap(errs.KindDeployInFlight, err)

```

- Kinds are the closed internal constructor vocabulary. Each Kind maps in one
  descriptor to exactly one public Code, Class, and HTTP status. Constructors
  accept Kind only; no Code constructor, status override, or Class override
  exists. `KindMalformedRequest` and `KindValidationFailed` intentionally share
  `validation.failed` while remaining distinct 400 and 422 errors.
- Codes are dot-namespaced constants, grouped by domain:
  `service.not_found`, `deploy.in_flight`, `strategy.not_implemented`,
  `attach.not_found`, `zone.not_found`, `slug.conflict`, … The
  `code` string **is** the RFC 7807 `code` the API returns, verbatim.
- `Class` categorizes semantically: `bad_request`, `validation`, `not_found`,
  `conflict`, `retryable`, `internal`, and the narrow
  framework request classes. Retry decisions derive from Class; HTTP status is
  fixed by the Kind descriptor.
- `errs.Wrap` preserves a private cause for logging and `errors.Is`, while the
  public detail remains safe and Kind-owned.
- `KindInternal` and `KindRequestFailed` constructor messages are private
  diagnostics: problems always expose `Internal Server Error` as title and
  detail. `KindNotImplemented` remains an explicit HTTP 501 response.
- Checking: `errors.Is(err, errs.New(errs.KindServiceNotFound, ""))`,
  `errs.KindOf`, or semantic helpers (`errs.IsNotFound`,
  `errs.IsRetryable`). `errors.Is` compares Kind, never public Code. Never
  string-match error messages.
- One shared handler: the CLI's `common.HandleErrors` (BrokenPipe →
  silent exit, `context.Canceled` → propagate, DomainError → code+message
  display, unexpected → generic message, exit 1). The Controller maps the
  taxonomy to RFC 7807 problem+json. Nothing else formats errors.

## 4. Context propagation

Every repository method and every infrastructure function with side
effects takes `ctx context.Context` as its **first parameter** — no
exceptions. Signal handling (SIGINT/SIGTERM) in `cmd/*` cancels the root
context; task aborts and in-flight step cancellation thread through it.

Implementations of fixed standard-library interfaces such as `io.Reader.Read`
and `io.Closer.Close` are the only signature exception. They must capture the
operation context at construction or use a bounded independent cleanup context;
private side-effect helpers still take `ctx` first.

## 5. Subprocess execution (locked: the Runner)

ALL subprocesses go through `internal/common`'s Runner
(`Runner.Run(ctx, opts)` / `Runner.Stream(ctx, opts)` with `RunCmdOpts`) —
never raw `os/exec` outside of it. This is the Agent's heart: docker
exec, pg_dump, psql, systemctl, staged-binary swaps all flow through one
abstraction with uniform logging, timeouts, and testability (FakeRunner).

Documented exceptions (must be listed here before being written):
- the docker driver inside `internal/infra` (SDK/CLI bridge),
- systemd unit control inside `internal/infra`,
- the staged-binary swap handoff (validate-before-exec, documented in
  the controller update task),
- the fixed PostgreSQL 16 image helper at `cmd/postgres16-helper` and its sole
  implementation package `internal/postgres16helper`, solely for raw `os/exec`
  and Linux process-control syscalls (`execve`, pidfd, `PDEATHSIG`, process
  groups, and `wait4`) that the common Runner cannot express. Its public
  protocol and PostgreSQL child argv remain closed; the exception does not
  extend to callers or other PostgreSQL subprocesses.

If a new raw-exec site is needed, add it to the exceptions with a reason
in the same commit, or route it through the Runner.

## 6. Logging

- Only `log/slog` — never `log.Printf`, `fmt.Fprintf(os.Stderr, …)`, or a
  third-party framework. The CLI is the only layer that prints user-facing
  output.
- Everything goes through the unified setup in `internal/common` (one
  `SetupLogging` per entry point; level chain `--debug` > `--verbose` >
  `GROUNDPLANE_LOG_LEVEL` > config `log.level` > WARN; file + stderr,
  file always DEBUG).
- Never log secret values — consistent with the locked reveal rule.
- Structured attributes (key=value), never interpolated strings.

## 7. Configuration

- One config reader in `internal/common`. `internal/controller` and
  `internal/agent` never parse YAML themselves.
- Merge order everywhere: defaults < config file < env < flags.
- Strict keys, fail-fast validation at load (see architecture.md
  "Configuration").

## 8. ID generation

ULID ids via `internal/common`: `common.NewID("env")` — no raw
`rand`/`time.Now`/`fmt.Sprintf` ad-hoc generation anywhere. One
implementation, all three binaries. (`<kind>_` prefix + ULID body, per
the stable-identifiers lock — the canonical prefix table lives in
`mvp.md`'s "Stable identifiers" section; a new entity gets a row there
in the same commit that adds it, never a guessed abbreviation.)

## 9. CLI patterns

- Cobra; one command file per noun (`service.go`, `zone.go`, …); the tree
  matches api-cli.md verbatim (flat nouns, verbs last, scope resolved
  once).
- Aliases for humans: `ls`+`list`, `rm`+`remove`+`delete`+`del`.
- Table commands always show headers, even when empty.
- Every `RunE` wraps `common.HandleErrors`. User-facing output lives in
  `internal/cli/common` — nowhere else.
- `completion bash|zsh|fish` from Cobra; root `PersistentPreRunE` owns
  verbose/debug and config loading.

## 10. Concurrency

- Agent worker pool: bounded by `max_concurrent_tasks`, never unbounded
  goroutines; every task gets a context that can be cancelled (abort).
- Controller: deploy/rollback serialization per service (locked) — one
  lock, one place, no ad-hoc mutexes.
- No goroutine leaks: every spawned goroutine must be joinable (WaitGroup/
  errgroup) or tied to a context lifetime. Races are CI failures
  (`-race` in tests).

## 11. Banned patterns

- `reflect` — banned unless approved via ADR (use `errors.As`, type
  switches, interfaces, generics).
- `goto` — banned.
- `interface{}` / `any` — banned for model fields and validators
  (required by stdlib where unavoidable, e.g. `json.Decoder.Decode`).
- `log.Printf` / `fmt.Fprintf` below the CLI.
- `init()` globals — everything wired explicitly in `internal/app`.
- registered Component I/O, CLI/subprocess use, backend-shaped parameters,
  secret reveal, raw Task/Agent payload construction, and arbitrary command
  declarations — Components are pure capability planners.
- `new(T)` for pointer types — use `&Type{}` or a `ptr` helper.
- Implicit defaults — `if x == "" { x = default }` banned unless
  approved via ADR; pass values explicitly.
- 1:1 deep copies — return repo results directly; copy only when
  transforming fields or changing types.
- Cargo-cult validation — don't validate compile-time constants with
  regex.
- Discarded errors — `_ =` only with a comment or an explicit reason.

## 12. Code style

- Directory name = package name. No underscores, no `Xcore` aliases; bare
  names (`controller`, not `controllercore`).
- Timestamps: `time.RFC3339` constant — no hardcoded format strings.
- Error messages start with lowercase (Go convention).
- `*T` for every nullable field where the zero value has meaning.
- Overridable defaults in ONE place (`internal/common`), never duplicated.
- Every side-effecting function: `ctx` first, then inputs.
- New handwritten production files are at most 600 physical lines; new
  handwritten test files are at most 1,000. Generated files are excluded.
- A pre-existing file above its applicable limit may shrink incrementally but
  may not grow. Narrow correctness/security exceptions require an exact
  path-specific record and cannot cover a directory or future file.
- File splitting must create coherent modules. Pass-through files that only
  relocate methods do not satisfy the size rule.

## 13. Testing (tiered)

- **L0 — pure function tests**: renderers, the YAML serializer, ULID
  generation, config validation. Table-driven, no mocks.
- **L1 — hermetic tests**: FakeRunner + fake docker driver + in-memory
  etcd store — the Agent's step executor and the Controller's task
  sequencing run against doubles; no host side effects.
- **L2 — real-host system tests**: CLI against a real Controller+Agent on
  a host; capability acceptance journeys exercise deploy, rollback, Backup and
  recovery through the supported operator surfaces.
- Every test carries a `// Rationale:` header comment explaining *why* the
  case exists (from the mvmctl practice) — the rationale is the
  documentation, not the assertion.
- Tests run with `-race -count=1`; coverage profile collected.

## 14. CI gate (mirrors the mandatory local checks)

```bash
test "$(node --version)" = "v24.19.0" && test "$(npm --version)" = "11.17.0"
cd console && npm ci && npm test && npm run build && cd ..
make console-verify
make generate
git diff --exit-code openapi.json internal/cli/apiclient/generated/client.gen.go console/src/lib/api.generated.ts proto/agentpb
go mod tidy && git diff --exit-code
make format-check
make architecture-check
go tool staticcheck -tags groundplane_console ./...
go vet -tags groundplane_console ./...
go test -tags groundplane_console ./... -count=1 -race -coverprofile=coverage.out -covermode=atomic
make backupstage-host-acceptance
go build -tags groundplane_console -o bin/controller ./cmd/controller
make console-release-smoke
make agent-image-smoke
```

Integrated-journey and release qualification require this complete local gate,
matching CI. Bounded changes follow the scoped proof and completion definitions in
`delivery.md`; an implementation commit alone is not qualification. Generated
contracts (proto Go, OpenAPI clients) are regenerated when their sources change,
never hand-edited.

## 15. Verification checklist (before declaring code complete)

- [ ] `go build ./...` passes
- [ ] `go vet ./...` passes; `gofmt -l .` empty; lines ≤ 120
- [ ] `ctx context.Context` is the first param in every side-effecting function
- [ ] No `reflect`, `goto`, `log.Printf`, `init()`, `new(T)`, `os.Exit()` in handlers
- [ ] No `_ =` discarded errors without reason
- [ ] Error messages start with lowercase; timestamps use `time.RFC3339`
- [ ] Every new error uses `errs.New/Wrap/WrapMsg` with a dot-namespaced code; code added to the RFC 7807 surface
- [ ] All subprocesses through the Runner (or listed in the exceptions)
- [ ] New config keys go through the one reader and are documented in the
      Controller config or Controller-owned Agent runtime contract
- [ ] New ids via `common.NewID(kind)`, never ad-hoc
- [ ] New registered Component implementation imports only `component-sdk` and
      approved stdlib, consumes only declared capabilities, and adds no core,
      persistence, API, or Agent implementation branch
- [ ] Component configuration and intents are closed typed variants; no
      `any`, raw JSON, reflection, or generic map crosses the boundary
- [ ] Tests: tiered (L0/L1/L2), `// Rationale:` headers, `-race` clean
- [ ] Generated contracts regenerated, diff checked in
- [ ] The 1:1 rule holds: every new Console action has a CLI command and an API endpoint (the OpenAPI diff proves it)
- [ ] Interfaces are consumer-owned, justified by a real seam, and never
      followed by implementation-recovery assertions
- [ ] External values are parsed once at their boundary; no unsafe, reflective,
      JSON-round-trip, untyped-map, or double-cast conversion was added
- [ ] `internal/app` contains composition only; capability behavior lives in
      its owning module
- [ ] `make architecture-check` and pinned Staticcheck pass
