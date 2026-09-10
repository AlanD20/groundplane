# Groundplane

Self-hosted control plane for running many projects on one machine without
hand-maintaining Docker Compose. Desired state is a **Compose-compatible
document plus a namespaced `x-gp-*` extension grammar** — not a bespoke
YAML schema. This repository contains the Go implementation, production
Console, packaging, and authoritative product and engineering contracts.
Implementation and acceptance remain incremental; use
[`docs/capabilities.md`](docs/capabilities.md) for the delivery ledger and
[`docs/head.md`](docs/head.md) for the compact continuation checkpoint.

The authoritative documents live in [`docs/`](docs/). Start with
[`docs/README.md`](docs/README.md), which routes product, API, Blueprint,
architecture, delivery, and implementation-status questions to their single
source of truth.

## Layout

```
cmd/                   Controller, Agent, CLI, and repository tool entry points
console/               Production React/Vite Console embedded by the Controller
internal/              Application, Controller, Agent, domain, and infrastructure code
component-sdk/         Closed interface available to registered Components
registered-components/ Caddy, Cloudflare Tunnel, and CoreDNS implementations
pkg/                   Public API and shared error contracts
proto/                 Controller↔Agent gRPC contract
packaging/             Host installation and service packaging
docs/                  Authoritative contracts, decisions, delivery ledger, and evidence
```

Controller and Agent are processes and product resources, not registered
Components. See [`docs/architecture.md`](docs/architecture.md) for the enforced
module boundaries and [`docs/mvp.md`](docs/mvp.md) for the product model.

## What changed from the product's earlier drafts

Two structural shifts are worth knowing about before reading the code:

- **Desired state is Compose, not a parallel grammar.** Earlier drafts
  described a bespoke `tenants.yaml`/`projects.yaml`/`environments/…`
  document tree. The current contract ([`docs/blueprint.md`](docs/blueprint.md)) is a real
  Compose document (parsed by a real Compose library, never
  reimplemented) plus
  `x-gp-*` extensions for everything Compose has no opinion on
  (releases, attachments, entries, requires, routes, components, backups).
  The Controller's typed internal representation and the authored document
  are deliberately different shapes.
- **The Router is a projection, not a resource.** Caddy and Cloudflare
  Tunnel used to be bespoke on/off toggles. They're now component *kinds*
  under the registered Component catalog, managed by `component
  enable|disable|config`; `GET /environments/{id}/router` is a read-only
  view grouping whichever ingress components happen to be enabled.

## The import matrix (locked)

[`docs/standards.md`](docs/standards.md) and
[`docs/architecture.md`](docs/architecture.md) own the import and module
rules. `make architecture-check` enforces their machine-checkable boundaries;
do not infer current boundaries from this overview.

## Building

Use the repository-pinned toolchains and generators. `go.mod`, `.node-version`,
the Makefile toolchain checks, and [`docs/standards.md`](docs/standards.md) are
the executable references; do not substitute an older compatible-looking
Node/npm or Go version.

```sh
go mod tidy                     # resolve the dependencies listed in go.mod
make proto                      # regenerate proto/*.pb.go with repository-pinned Go tools
make build                      # builds bin/groundplane, bin/controller, bin/agent
make console                    # npm ci && npm run build in console/, embedded via go:embed
make ci                         # full local gate: tidy, gofmt, golines, vet, race tests; mirrors CI exactly
```

A successful focused test does not establish MVP acceptance. Follow the
verification ladder in [`docs/delivery.md`](docs/delivery.md) and the active
closure order in [`docs/capabilities.md`](docs/capabilities.md).

For first installation and guarded Controller/Agent updates, use the
[`deployment runbook`](docs/deployment.md). Routine deployment follows the
Controller update Task; `--bootstrap` is only the explicit legacy transition.
