# Changelog

Release entries describe implemented features and known limits, not a guarantee
that every deployment or failure scenario has been qualified.

## [Unreleased]

## 0.0.1 — pending publication

Initial single-host release.

### Application hosting

- Manage Tenants, Projects and Environments through the web Console, CLI and
  REST API.
- Validate and apply Compose-compatible Blueprints with Groundplane extensions
  for managed resources and application configuration.
- Deploy Services, inspect serving Releases, roll back and coordinate ordered
  deployments through Release Groups.
- Configure Zones, Routes, Caddy routing and Environment DNS through managed
  Components.
- Provision PostgreSQL and Valkey backing services and attach application
  consumers. Valkey requires an explicit choice of username/password,
  password-only or no authentication.
- Manage persistent Volumes, configuration Entries and Secrets. Materialize
  configuration for workloads and run Scripts and ordered deployment hooks.

### Operations

- Inspect Host, Controller, Agent and Service health, workload logs and Activity.
- Track durable Tasks and their events, with Retry and Abort where the operation
  permits them.
- Perform guarded Controller and Agent updates with recorded update Tasks and
  recovery checks.

### Installation and distribution

- Prebuilt Linux amd64 and arm64 bundles contain the Controller with embedded
  Console, CLI, installation helpers, systemd configuration and manifests.
- One `install.sh` handles initial installation and guarded upgrades on
  Ubuntu 24.04/26.04 and Debian 13. Rerunning an already healthy, matching
  installation does not start another update.
- Agent and Runner container images are distributed through GHCR. Release
  bundles pin image digests and include file checksums.
- Tagged GitHub releases publish both platform bundles, checksums and installer.

### Limits and deferred work

- Backup/Restore is not a complete, qualified feature in this release.
  Privileged Backup staging acceptance is separate from default CI and deferred.
- GitHub Runner management and its image are included, but complete token-handoff,
  runtime and real-job qualification remain incomplete.
- Packaging checks do not establish fresh-install qualification on every supported
  OS/architecture or guarantee uninterrupted upgrades for every workload.
- This is a single-host release, not a multi-host or high-availability platform.
- Existing structural architecture debt is explicitly deferred for 0.0.1;
  new findings remain blocking.

See the [capability status](docs/capabilities.md),
[QA matrix](docs/qa-matrix.md) and [deployment guide](docs/deployment.md)
for feature-specific limits, evidence and installation instructions.
