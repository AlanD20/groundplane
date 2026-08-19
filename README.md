# Groundplane

Self-hosted control plane for running many projects on one machine without
hand-maintaining Docker Compose. Desired state is a **Compose-compatible
document plus a namespaced `x-gp-*` extension grammar** — not a bespoke
YAML schema (see `blueprint.md`). This is the **implementation
boilerplate**: package layout, entry points, the shared "common" project,
the domain model, the adapter/addon registries, and the full CLI command
tree, wired but not yet backed by real etcd/docker/gRPC calls. Every
`TODO` marks where real logic replaces a stub.

Companion docs (authoritative — keep these next to the repo, not
duplicated inside it): `architecture.md` (this layout, why), `mvp.md`
(the product contract), `api-cli.md` (CLI tree + REST resource map),
`blueprint.md` (the authored Compose+`x-gp-*` desired-state format),
`standards.md` (the Go-level rules this repo follows — see
`docs/README.md` for a summary and two deliberate reconciliations
between it and architecture.md's original sketches). Code comments
reference all five by name.

## Layout

```
cmd/groundplane      CLI entry point — imports ONLY internal/app + internal/cli
cmd/controller       Controller entry point — imports ONLY internal/app
cmd/agent            Agent entry point — imports ONLY internal/app

console/             React 19 + Vite + Tailwind v4 static SPA
                      (go:embed'd into the Controller binary)

internal/app         DI wiring, per binary: config load, logging setup,
                      explicit adapter AND addon registration,
                      store/server/scheduler construction. The only thing
                      cmd/* is allowed to import.
internal/cli         Cobra commands — one file per noun, zero logic
internal/cli/common  CLI-only: the shared error handler (HandleErrors) and
                      the --output TABLE|JSON|YAML writer
internal/cli/apiclient  The CLI's Go client for the human API
internal/controller  API server, task sequencing, the render pipeline
                      (renderer.go) and the typed ExecutionPlan (plan.go),
                      scheduler
internal/agent       gRPC client, worker pool (executes steps via the
                      Runner), materializers
internal/core        Domain model (model.go) + the authored Blueprint
                      envelope/x-gp-* types (envelope.go) + validation
                      (blueprint.go) — pure, no infra imports
internal/adapters    Backing-service adapter registry — one package per
                      kind (postgres16, valkey9, manual), each exporting
                      an explicit Register()
internal/addons      Environment-addon registry — architecture.md's third
                      extension seam, mirrors internal/adapters exactly
                      (caddy = ingress.caddy, cloudflaretunnel =
                      edge.cloudflare-tunnel)
internal/infra       Server-side platform integrations (etcd, age,
                      systemd, docker) — ONLY the daemons import this;
                      the CLI never does
internal/common      Logging, the ONE config reader, ULID ids, and the
                      locked subprocess Runner — imported by every binary
                      including the CLI, zero heavy dependencies

pkg/api              Public API DTOs (independent of internal/core's
                      domain model — see pkg/api/types.go) + re-exported
                      pkg/errs codes
pkg/errs             THE single error type in the codebase: Code
                      (dot-namespaced, grouped by domain), Class
                      (auto-derived, drives HTTP status + retry
                      decisions), New/Wrap/Is. A leaf: stdlib only.
proto/                agent.proto — the Controller↔Agent gRPC contract,
                      including the ExecutionPlan's plan_id/plan_hash/
                      render_generation carried over the live channel

config/               Example config files (controller.yaml, agent.yaml, …)
docs/                 Pointer to the companion docs (kept outside the repo)
```

## What changed from the product's earlier drafts

Two structural shifts are worth knowing about before reading the code:

- **Desired state is Compose, not a parallel grammar.** Earlier drafts
  described a bespoke `tenants.yaml`/`projects.yaml`/`environments/…`
  document tree. The current contract (`blueprint.md`) is a real
  Compose document (parsed by a real Compose library, never
  reimplemented — see `internal/core/envelope.go`'s `TODO`) plus
  `x-gp-*` extensions for everything Compose has no opinion on
  (releases, attachments, entries, requires, routes, addons, backups).
  `internal/core/model.go` is the Controller's *typed* internal
  representation the Controller compiles a Blueprint into — the two are
  deliberately different shapes; see `envelope.go`'s package comment for
  the layering.
- **The Router is a projection, not a resource.** Caddy and Cloudflare
  Tunnel used to be bespoke on/off toggles. They're now addon *kinds*
  under a generic `internal/addons` registry, managed by `addon
  enable|disable|config`; `GET /environments/{id}/router` is a read-only
  view grouping whichever ingress addons happen to be enabled.

## The import matrix (locked)

See `standards.md`, section 1 for the authoritative table. The
load-bearing rules:

- `cmd/*` imports **only** `internal/app` (and `internal/cli`, for the
  CLI binary specifically) — nothing else. `go list -deps ./cmd/...` is
  the whole story of what a binary depends on.
- `internal/common` and `pkg/errs` are leaves: stdlib + external deps
  only, imported by everything, importing nothing internal of their own.
- `internal/cli` never imports `internal/infra`, `internal/controller`,
  `internal/agent`, or `internal/adapters` — it must stay buildable
  without etcd, docker, systemd, or age.
- `pkg/api` never imports `internal/core` — the public API contract and
  the internal domain model are independent shapes on purpose;
  `internal/controller`'s handlers translate between them.
- Go's circular-import detection enforces the rest: a violation is a
  compile error, not a review finding.

## Building

Requires Go 1.22+, Node 20+ (for the Console), and `protoc` +
`protoc-gen-go` + `protoc-gen-go-grpc` (for `proto/agent.proto`).

```sh
go mod tidy                     # resolve the dependencies listed in go.mod
make proto                      # regenerate proto/*.pb.go
make build                      # builds bin/groundplane, bin/controller, bin/agent
make console                    # npm install && npm run build in console/, embedded via go:embed
make ci                         # the full local gate: tidy, gofmt, vet, race tests — mirrors CI exactly
```

This has been built, `go vet`'d, `gofmt`'d, and exercised end-to-end
(the CLI binary really does round-trip HTTP requests against the
Controller binary for every noun — including `release-group`, `addon`,
and the discriminated `entry` model — and decode its RFC 7807 error
responses) — but `internal/infra`'s etcd/docker/age/systemd
integrations, and `internal/addons`' Render/Healthy implementations,
all still return `errs.CodeNotImplemented`. Start there;
`internal/agent/worker.go`'s `runStep` and
`internal/controller/server.go`'s `acceptTask` are the two places that
light up once they're wired.
