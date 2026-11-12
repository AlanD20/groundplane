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
- Deploy Services with recreate, blue-green or rolling strategies and configurable
  healthchecks. Edit Service configuration or apply selected Services from a Blueprint.
- Inspect serving Releases, roll back the image and coordinate ordered
  deployments through Release Groups.
- Configure Zones, Routes, Caddy HTTP or Envoy HTTP/TCP/UDP routing and scoped
  Environment DNS through managed Components.
- Expose application Routes through Cloudflare Tunnel. Operators supply the
  Tunnel token and configure Cloudflare hostnames, DNS and origin policy.
- Provision PostgreSQL, Valkey and MySQL backing services and attach application
  consumers. Valkey requires an explicit choice of username/password,
  password-only or no authentication.
- Share PostgreSQL through separate consumer databases and credentials, with
  explicit access grants and default cross-database access restrictions for
  newly provisioned databases.
- Run Custom backing services from an operator-selected image, with optional
  provisioning and lifecycle hooks that receive inputs and return declared facts.
- Manage persistent Volumes, configuration Entries and Secrets. Materialize
  configuration for workloads and run Scripts and ordered deployment hooks.

### Operations

- Expose the Console/API on an explicit Tailscale IPv4 address while retaining
  loopback and a separate internal Runner API/registry endpoint. Tailnet
  enrollment and access policy remain operator-managed.
- Configure separate Platform and Environment Discord destinations for Task
  failures and needs-attention transitions, with recorded delivery and bounded retries.
- Use multiple host-terminal tabs with configurable input inactivity.
- Edit Service-specific environment variables in bulk and filter/sort Release history.
- Inspect image tags, immutable identities and container usage; fetch and remove
  images through protected Tasks.
- Inspect Host, Controller, Agent and Service health, workload logs and Activity.
- Track durable Tasks and their events, with Retry and Abort where the operation
  permits them.
- Perform guarded Controller and Agent updates with recorded update Tasks and
  recovery checks.
- Capture and verify Environment configuration, managed Volumes and
  credential-owning PostgreSQL and MySQL Attach databases to S3-compatible storage.
  Restore a verified Recovery Point to its original surviving target, with
  age encryption/key rotation, scheduled runs and per-source retention.
- Retain Backup cleanup and Attach deprovisioning through hierarchy-deletion
  failures; remote Backup absence is proved before Volume destruction or
  credential revocation.
- Show each Recovery Point's original Connector and storage destination in the
  Console, CLI and API. The Console refreshes Backup results and encryption-key
  metadata automatically, with a linked Task for key rotation.
- Delete an individual Recovery Point through the Console, CLI or API. Protected
  cleanup verifies archive absence before releasing its references and leaves
  source data unchanged.
- Group Recovery Points by their recorded Backup run in the Console, with
  readable sources, per-source Restore/Delete and a protected sequential
  deletion queue for the remaining archives. Retention remains per source.

### Runner CI/CD

- Manage persistent GitHub Actions Runners owned by a Tenant, Project or
  Environment. Each uses a dedicated rootless Docker daemon, not the host
  Docker socket. Trusted jobs can use the normal GP CLI/API with full operator
  authority; Runner ownership is not an API permission boundary.
- Build and push images to a private authenticated TLS registry on the host.
  CoreDNS resolves its internal name for the Runner and host Docker.
- Fetch a selected registry image explicitly through the Console, CLI or API,
  then use ordinary Service Deploy. Fetch retains the selected image identity
  for retries; it does not edit or deploy a Service itself.
- Preserve the serving Release and data when build, push or Fetch fails.
  Registered Runners resume without a new registration token; local Runner
  removal preserves shared registry content and application images.

### Installation and distribution

- Prebuilt Linux amd64 and arm64 bundles contain the Controller with embedded
  Console, CLI, installation helpers, systemd configuration and manifests.
- One `install.sh` handles initial installation and guarded upgrades on
  Ubuntu 24.04/26.04 and Debian 13. Rerunning an already healthy, matching
  installation does not start another update. With no version specified, it
  resolves the latest stable release once; `--version` pins a chosen release.
- Install a branch, tag or commit with `install.sh --ref`, without a published
  release. Toolchains run in disposable Docker containers; temporary build
  files are cleaned up, and installation uses the same guarded upgrade path.
- Release Agent and Controller independently with `agent/vX.Y.Z` and
  `controller/vX.Y.Z`, using `--agent-only` or `--controller-only` where needed.
  A combined `vX.Y.Z` release also publishes both matching component releases
  from the same artifacts. Agent-only updates leave the Controller unchanged.
- Agent and Runner container images are distributed through GHCR. Release
  bundles also select upstream PostgreSQL 16 Alpine and independent capture/restore
  tools. Installation preloads these without replacing existing databases;
  source installations build only GP's tools into the local registry. Backing
  image updates retain data Volumes and allow PostgreSQL 16 patch images without
  rebuilding a GP database image. Bundles pin image
  digests and include file checksums.
- Tagged GitHub releases publish both platform bundles, checksums and installer.
- GitHub Pages deploys independently on every push to `main`, serving a dark
  violet installation page, the source installer and its SHA256 checksum.

### Limits and deferred work

- Config, Volume, PostgreSQL and MySQL Backup/Restore have selected live capture,
  restore, key-era, retention and interruption proof on Ubuntu amd64. Full Gate B,
  other provider/archive/checkpoint and arm64 variants remain unqualified.
  Valkey and Custom Attach sources are unsupported. Privileged Backup staging
  acceptance remains separate from default CI.
- A Project-owned Runner's real build/push/Fetch/Deploy, delivery-failure
  preservation, same-boot listener restart and local cleanup passed on Ubuntu
  amd64. Full isolation, token-failure, concurrency, reboot and other platform
  variants remain unqualified. Runners are for trusted workflows, not hostile
  jobs; GitHub-side deregistration remains manual.
- Blueprint Apply supports resource-level reconciliation, protected omissions,
  latest-wins settlement and same-Apply Custom hook facts. Persistent Volumes
  and backing Zones with external consumers require separate protected removal.
  Selected interruption/recovery journeys are qualified; not every fault variant.
- Tailscale Console/API/CLI and root-terminal access passed a selected Ubuntu
  amd64 QA journey. Tailnet policy failures and host reboot remain unqualified.
  Native Discord test delivery was confirmed on the disposable QA installation.
- Packaging checks do not establish fresh-install qualification on every supported
  OS/architecture or guarantee uninterrupted upgrades for every workload.
- This is a single-host release, not a multi-host or high-availability platform.
  Host reboots and recreate deployments interrupt application service.

See the [capability status](docs/capabilities.md),
[QA matrix](docs/qa-matrix.md) and [deployment guide](docs/deployment.md)
for feature-specific limits, evidence and installation instructions.
