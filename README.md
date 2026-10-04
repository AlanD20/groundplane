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
before selecting it for production. Config, Volume and PostgreSQL Backup/Restore
are implemented, with selected recovery journeys verified; full production
qualification remains incomplete. See [Backups](docs/features/backups.md) for
supported sources, overwrite behavior and recovery limits.

## Use Groundplane

After installation, [connect to the Console and CLI](docs/deployment.md#connect-after-installation).
Use [CLI and API usage](docs/api-cli.md) for configuration, resource selection,
requests and Task results, and the [Blueprint reference](docs/blueprint.md) for
Compose-compatible desired state with Groundplane extensions.
The [documentation index](docs/README.md) links the remaining guides.
The [changelog](CHANGELOG.md) records release changes.

## Work on Groundplane

Start with [architecture](docs/architecture.md), [technical decisions](docs/decisions/README.md)
and [delivery](docs/delivery.md). The Controller owns decisions; the Agent executes
sealed work; Console and CLI are clients of the same API.

Use the toolchain versions in the repository manifests and the build targets in
the [Makefile](Makefile). Run direct development commands through
`bash scripts/repo-env.sh COMMAND [ARG...]` so temporary files and caches stay
inside the checkout. See [repository tooling](docs/agents.md#supported-tooling-invocation).

Documentation in a source checkout describes that revision. An installed release
may expose different commands or fields; use its command help and Controller's
`/openapi.json` when checking compatibility. See [current limitations](docs/capabilities.md)
for implementation and qualification boundaries.
