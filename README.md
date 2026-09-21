# Groundplane

Groundplane is a self-hosted control plane for running multiple projects on one
Linux machine. It manages desired configuration, deployments, networking and
shared services through one Console, CLI and API.

## Install

```sh
curl -fsSL https://gp.aland20.com/install.sh | sudo bash
```

This runs the downloaded installer as root and selects published stable releases.
To inspect it first, install a specific version, or build a Git ref locally, see
[installation and upgrades](docs/deployment.md). A source checkout does not imply
that a release has been published.

The MVP assumes trusted operators. Its human API has no authentication: keep it
on loopback or explicitly selected trusted private interfaces.
Read [product scope](docs/mvp.md) and [current limitations](docs/capabilities.md)
before selecting it for production. Backup/Restore and full runtime qualification
remain incomplete.

## Use Groundplane

Start with the [operator guides](docs/README.md#operators).
The [Blueprint reference](docs/blueprint.md) describes Compose-compatible desired
state with Groundplane extensions. The [changelog](CHANGELOG.md) records release
changes.

## Work on Groundplane

Start with [architecture](docs/architecture.md), [technical decisions](docs/decisions/README.md)
and [delivery](docs/delivery.md). The Controller owns decisions; the Agent executes
sealed work; Console and CLI are clients of the same API.

Use the toolchain versions in the repository manifests and the build targets in
the [Makefile](Makefile). Run direct development commands through
`bash scripts/repo-env.sh COMMAND [ARG...]` so temporary files and caches stay
inside the checkout. See [repository tooling](docs/agents.md#supported-tooling-invocation).

Production restructuring is integrated but has not been build- or runtime-verified
after that migration. A clean working tree is not release qualification.
