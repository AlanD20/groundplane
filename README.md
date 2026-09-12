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

## Product and feature documentation

The [feature index](docs/README.md#features) explains each feature's purpose,
requirements, implementation and acceptance. The shared contracts define product
scope, Compose/Blueprint grammar, API surfaces and module boundaries.
Read current requirements there; Git retains earlier designs and work logs.

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

Make initializes validated repository-local temporary and cache paths. For direct
commands, use the [repository environment wrapper](docs/agents.md#supported-tooling-invocation).

```sh
bash scripts/repo-env.sh go mod download  # download dependencies with repo-local temporary paths
make proto                      # regenerate proto/*.pb.go with repository-pinned Go tools
make build                      # builds bin/groundplane, bin/controller, bin/agent
make console                    # npm ci && npm run build in console/, embedded via go:embed
make ci                         # full integration/release gate; see docs/delivery.md
```

A successful focused test does not establish MVP acceptance. Follow the
verification ladder in [`docs/delivery.md`](docs/delivery.md) and the active
closure order in [`docs/capabilities.md`](docs/capabilities.md).

For first installation and guarded Controller/Agent updates, use the
[`deployment runbook`](docs/deployment.md). Routine deployment follows the
Controller update Task; `--bootstrap` is only the explicit legacy transition.
