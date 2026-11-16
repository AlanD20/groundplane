# Changelog

Release entries describe implemented features and known limits, not a guarantee
that every deployment or failure scenario has been qualified.

## [Unreleased]

## 0.0.1 — 2026-11-16

Initial single-host release.

### Console, hosting and operator refinements

- Queue operator actions behind up to 16 whole Tasks with **Run after** in the
  Console, CLI and API. Capture execution inputs only after every prerequisite
  succeeds and fully settles, without occupying Agent capacity while waiting.
  Show prerequisite links and explicit preparation, cancellation and failure reasons.
- Configure **Concurrent Tasks** and **Parallel operations per Task** independently
  on each Agent, defaulting to three of each, with bounded total capacity and
  scheduling across ready Tasks.
- Use consistent Configuration labels for variables and files, and shared form
  shortcuts: Enter to submit single-line forms, Ctrl/Cmd+Enter from editors,
  and Escape to cancel while respecting open menus and dialogs.
- Protect credentials selected by queued requests and authored Components from
  deletion. Missing inputs fail clearly instead of waiting indefinitely, and
  deleting and recreating a key cannot silently retarget an accepted request.
- Advance ready Blueprint work without the periodic scheduler delay, and release
  large queued request payloads after capture while retaining compact Task history.
- Retain known deployment readiness failures across Agent reconnects, showing
  container exits, failed healthchecks and missing probe tools in Task steps.
  Keep genuinely unobserved outcomes explicitly unknown.
- Isolate the Agent execution journal from temporary Script workspaces so
  concurrent Scripts cannot prevent execution or reconnect recovery.
- Resume Attach preparation after a concurrent state change without carrying
  an earlier waiting diagnostic into its executable state.
- Keep disabled Cloudflare Tunnels valid without a token throughout Component
  storage and Blueprint preparation; enabling a Tunnel still requires a valid
  Secret reference.
- Preserve the applied configuration receipt for disabled authored Components,
  so Blueprint Service preparation can proceed after their runtime stops.
- Show bounded input-preparation issues in Task details and CLI output instead
  of leaving waiting work unexplained. Repeated identical issues do not add
  writes or manufacture execution attempts.
- Open container shells from Service and Backing Service details with an exact
  container selector, user, command arguments and working directory, sharing the
  existing terminal tabs and bounded session lifetime.
- Establish storage epoch 1 and Agent channel schema 1 for the replacement
  0.0.1 clean-install baseline; older development installations require a reset.
- Identify Entry ownership, Service exposure and value references consistently
  in Environment and Service Variables & files, and keep Secret owner labels
  visible on narrow screens.
- Render only visible container log rows while retaining the full rolling buffer
  for search and copy. Reuse timestamp formatting, preserve row styling and full
  Service names, and distinguish received lines from the newest 1,000 retained.
- Group Environment deployments by their triggering Task, with complete
  expandable Service releases, Task outcomes and direct execution links.
- Allow manual and rollback Scripts alongside deploy hooks during Blueprint
  Service preparation without treating them as missing or extra deploy hooks.
- Use one shared horizontal scrollbar for unwrapped container logs instead of a
  separate scrollbar on every log line.
- Keep router request-policy edits from redeploying unchanged application
  Services. Acknowledge already-applied Blueprint work without Agent execution,
  and prevent router health reports from falsely rejecting Route edits.
- Export topology diagrams from a simple SVG/PNG menu for all Services or those
  selected directly on the diagram with Shift/Ctrl/Cmd-click,
  cropped to the diagram with 16 px margins. Share view controls with the empty-space
  context menu and preserve resource-specific node menus.
- Give Service card footer actions consistent padding and click areas.
- Use the shared strict Caddy observation codec for continuous Service health,
  preventing false degraded status after trusted forwarding is deployed.
- Verify live and restart Caddy routing with the sealed configuration encoding,
  so admin API key ordering cannot falsely time out a trusted proxy promotion.
- Resolve captured Service network names through the frozen Zone identities
  when selecting trusted ingress networks for native Caddy forwarding.
- Accept exact captured serving-proxy configurations during blue-green and
  rolling rendering, validating their hash, engine, Release and generation.
- Preserve trusted ingress HTTPS headers through native Caddy Service proxies
  using Release-captured, shared Environment Zone networks, with strict routing
  validation and exact predecessor configuration during rollout and recovery.
- Capture strict HTTP forwarding policy in matching Controller and Agent artifacts;
  both components must be updated together when the channel format changes.

- Authorize Group preflight Script checkpoints through the same captured
  membership checks used for delivery and standalone Script execution.
- Settle cancelled or expired Group preflight Scripts before assignment using
  their retained membership and no-execution evidence.
- Publish Group preflight Scripts using their captured Environment and immutable
  projection fences, preserving authority across durable capture and resume.
- Deliver captured network prerequisites for direct Service and Group releases
  using their own resource identity, while retaining parent-scope and ensure-only guards.
- Settle expired Network assignments that never obtained a Start grant, using
  captured failure evidence and atomic checks against a concurrent start.
- Size Group publication batches against the actual atomic byte and operation
  budgets, including the Task progress write, so larger environments can deploy.
- Read retained Blueprint declarations by their sealed identity and common
  snapshot rather than assuming staged records share a physical write revision.
- Publish Blueprint Release Groups in bounded, guarded batches before opening
  execution, preserving group identities across publication retries and restarts.
- Keep deployment and recovery stages visible after embedded Scripts by
  appending to their shared diagnostic sequence without reopening completed stages.
- Preserve literal runtime variables in Blueprint exports and direct edits,
  so reapplying authoring input cannot expand protected Entry references to empty.
- Preserve failed Release and recovery execution causes in private Agent logs,
  correlated with their Task, work item and attempt identifiers.
- Allow pre-deploy Scripts on Services with phase-specific dependencies by
  consuming recognized dependency metadata before closed runner validation.
- Complete Blueprint reapply for unchanged DNS records after confirming the
  existing resolver acknowledgement.
- Stage immutable Blueprint acceptance records under their existing revision
  owner, keeping Task and desired-state publication within the atomic operation
  limit. Keep staged prerequisites usable during runtime preparation and retire
  abandoned captures without activating work or relaxing guards.
- Keep Runner registration files private while using normal file permissions
  for workflow jobs and Docker build contexts.
- Include a pending deployment candidate in Service and Environment logs, using
  its captured workload ownership alongside the serving Release.
- Fail deployment readiness when a candidate exits, restarts or exhausts its
  healthcheck retries, instead of waiting for the whole Blueprint deadline.

- Allow Entry creation, edits and bulk updates before runtime configuration is
  prepared when no acknowledged workload exists; retain prior files until Deploy.
  Read selected values consistently from authored state and retain normal host
  execution for existing configuration.
- Preserve an existing key-only secret Entry's protected value when Blueprint
  adoption or an exposure edit changes its metadata.
- Finish captured work revoked before assignment using a durable no-assignment
  receipt, preserving its cancelled or superseded outcome and normal retention.
- Show embedded Script preparation, command outcome and cleanup in parent Task
  details. Read retained hook outcomes without requiring the Agent's local
  delivery journal or storing Script output.

### Lifecycle cleanup repairs

- Reconcile omitted connections from retained Blueprint declarations, with native
  cleanup ordering, captured hook/Secret inputs and explicit resource receipts.
- Remove omitted Entry files through the existing native file-cleanup and recovery
  path; keep loaded process environments until the next Deploy.
- Preserve completion evidence for work cancelled before input creation, and
  allow DNS withdrawal with a stopped or never-published resolver.
- Use captured cleanup receipts with a clean cutover from development state;
  no compatibility reader is provided.

- Allow Detach before first Deploy and while initial Blueprint runtime rendering
  is pending, preserving concurrent deployment and desired-state checks.
- Keep Attach provisioning, Release, Script and Environment-removal checkpoint
  conflicts local to their operation; retry atomic publication contention without
  disconnecting unrelated Agent work.
- Preserve Volume execution failure causes in private Agent logs, with the Task,
  work item and attempt identifiers needed for diagnosis.
- Settle interrupted initial Volume creation when recovery proves both the
  directory and Docker Volume absent, retaining the failed action and its history.

### Update and configuration repairs

- Qualify combined Controller/Agent updates as one prepared pair, including
  channel-format changes, with predecessor-pair rollback on failed readiness.
  Reject mismatched single-component updates before activation. This replacement
  uses storage epoch 1; earlier development installations require a clean cutover.
- Retry contended native execution checkpoints without disconnecting unrelated
  Agent work, and record refused checkpoints in Task execution details.
- Edit Project and Platform file Secret contents in the Console, preserving
  stable references and guarding concurrent updates.

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
- Set CPU/RAM limits before a Backing Service's first startup. Custom Backings
  can also create an explicitly selected persistent data Volume at startup.
- Edit shared Backing Service display names without changing consumer references.
- Manage persistent Volumes, configuration Entries and Secrets. Materialize
  configuration for workloads and run Scripts and ordered deployment hooks.
- Import and edit Platform and Project Secrets from dotenv text or files, with
  per-key results, stable references and protected execution-pinned values.
- Reveal file Secrets in a searchable, full-width editor view with line numbers.

### Operations

- Select an existing GitHub Runner group at registration through the Console,
  CLI or API. GP ownership is independent of GitHub organization/repository scope;
  captured group selection is retained for Retry and shown in Runner details.
- Set an optional GitHub-visible Runner name and a combined CPU/RAM budget for
  the listener, rootless Docker daemon and builds, with kernel enforcement checks.
- Retain readable local tags for installed platform images, using each artifact's
  own release or commit identity while keeping execution pinned to digests.
- Keep dhcpcd lease renewals from overwriting GP's active host DNS. Restore the
  captured DNS baseline on CoreDNS disable and remove the owned guard on uninstall,
  without restarting the host network or changing DHCP address allocation.
- Close Environment creation forms when their background Task is accepted;
  progress remains available in Task notifications.
- Use consistent empty, loading and error views across Environment collections,
  hide unused pagination, and omit status summaries when nothing exists to observe.

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
- Verify backing-network membership for stopped consumers and safely settle
  connection work when no consumer container exists.
- Verify and remove application-owned Volume directories without changing their
  ownership or weakening captured path and identity checks.
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
- Select automatic upstream Runner updates or an exact manual version, separate
  from the GP Runner image. Updates verify official checksums, wait for idle
  execution and retain captured recovery state without re-registering the Runner.
- Expose the automatic-update checkbox under **Runner settings**. Retry a cleaned
  registration failure with a fresh token without rejecting its accepted Task response.
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
- Reset GP with the standalone `uninstall.sh`, retaining Docker and host
  dependencies. Repeated cleanup tolerates image aliases already removed by Docker.
- Mark Runner and temporary helper resources as GP-managed. Reset discovers owned
  networks and Volumes independently of surviving containers and removes isolated
  GP build caches while retaining unrelated Docker resources.
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

- Runner upstream updates, reusable Secret bulk editing and create-time Backing
  limits/storage are implemented but have not received live qualification.
- This replacement establishes storage epoch 1 and Agent channel schema 1.
  Existing development installations require an explicit clean reinstall;
  no automatic data migration or compatibility path is provided. Reset deletes
  prior GP-managed local state only when the operator explicitly requests it.
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
