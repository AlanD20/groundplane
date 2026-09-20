# Controller installation and native updates

This runbook implements ADR0074's machine-bootstrap exception. Normal native
activation remains the same Console/CLI/API Controller update Task. It does not
authorize a production target or changes to ingress, host firewall or other hosts.

## Prebuilt releases

Local release scripts build artifacts without implicitly publishing or installing.
The tagged GitHub workflow below publishes them. Use an explicit version, never a moving `latest` selection.
Build from the intended source commit with the repository-pinned toolchains.

### Publish through GitHub Actions

`.github/workflows/release.yml` runs on a pushed `vMAJOR.MINOR.PATCH` tag whose
commit belongs to `main`. All external Actions use supported major-version tags
(such as `@v7`), not patch-version pins or commit hashes. Check upstream releases
and migration requirements when adopting a new major.
The first selected release is `0.0.1`. After local `make ci` passes, commit the
reviewed changes on `main`, push that commit, then create and push `v0.0.1`.
Review the matching [changelog entry](../CHANGELOG.md) before publication;
replace its pending-publication label with the actual release date when published.

### Prepare a version

`VERSION` records the intended GP release. Console package and root lockfile
versions mirror it. Development Go builds still report `dev`; release binaries
receive their explicit version through the existing build flags. `.runner-version`
is the upstream GitHub Actions Runner dependency version, not the GP version.

Record changes under `## [Unreleased]` in the root changelog. Then prepare the
next chosen stable version, for example:

```sh
bash scripts/repo-env.sh python3 scripts/bump_version.py 0.0.2 --dry-run
bash scripts/repo-env.sh python3 scripts/bump_version.py 0.0.2
bash scripts/repo-env.sh python3 scripts/bump_version.py --check
```

The script updates all four metadata files and moves Unreleased notes into a
new pending-publication section, retaining earlier sections. With no notes it
creates empty Added/Changed/Fixed headings; fill them before validation passes.
It rejects malformed, repeated or older versions and inconsistent metadata before
writing. Dry-run prints the proposed diff without edits. It never commits, tags,
pushes, publishes or changes package visibility. Review and commit the changes,
pass CI, then create and push the matching `vVERSION` tag separately.

Default CI checks metadata consistency and nonempty release notes. The release
workflow also checks that its tag matches `VERSION`, and publishes only that
version's changelog body as GitHub release notes. Existing 0.0.1 architecture
deferrals do not automatically authorize debt exceptions for a later release.

### Published artifacts

The workflow reuses the full CI gate before publication. It builds and smoke-tests
Agent and Runner images on native amd64/arm64 GitHub runners, pushes those children
to `ghcr.io/aland20/groundplane-agent` and `ghcr.io/aland20/groundplane-runner`,
then publishes and reads back their two-platform indexes. It builds native bundles
with those immutable index digests and publishes both archives, both checksums and
`install.sh` to the tagged GitHub Release. Runtime image digests and file checksums
remain required; using Action tags does not remove artifact integrity checks.

GitHub's workflow token needs package-write and release-write permissions, scoped
to the relevant jobs. The packages must allow public anonymous pulls: newly created
GHCR packages may need their visibility changed in GitHub package settings. The
workflow checks anonymous access and stops before publishing installer assets if
either image is private. No personal access token is embedded or required by the
workflow. The repository's release assets must also be publicly downloadable for
the installer, which does not accept GitHub credentials.

The owner approved public visibility for `groundplane-agent` and
`groundplane-runner` on 2026-09-20. This does not authorize changing repository
visibility. Approval is not proof that the remote package settings have changed.
After the packages exist, an account with package-admin access must select
**Package settings → Change visibility → Public** for each package. Follow
[GitHub's package visibility instructions](https://docs.github.com/en/packages/learn-github-packages/configuring-a-packages-access-control-and-visibility).
Rerun the failed anonymous-access job after that change. Do not remove its check
or claim public distribution before anonymous pulls succeed.

Failed jobs do not deploy to hosts. Already pushed image children can remain after
a later failure. Rerun failed jobs for the same unchanged tag; do not move a release
tag or overwrite an existing GitHub Release. Manual dispatch is available against
an existing release tag, not an arbitrary branch. Publishing is not proof of the
still-unrun fresh-install OS/architecture or uninterrupted-upgrade journeys.

### Public installation site

The owner selected public release assets and GHCR images, with
`https://gp.aland20.com` as the primary website and `groundplane.aland20.com`
redirecting to it. This workflow does not change repository visibility. Its current
same-repository release URLs must be public; a private source repository would
need a separate public release location before these URLs could work.

The release workflow calls `.github/workflows/pages.yml` after publication.
It can also be dispatched manually with an existing stable release version.
Before deployment it requires publicly downloadable amd64/arm64 bundles and
checksums, and compares the published installer with the tagged source bytes.
Only the landing page, installer, installer checksum and `.nojekyll` are uploaded;
the repository, private evidence and internal docs are not exported to Pages.
No site is published for a draft, prerelease or inaccessible release.

One-time setup, performed by an account with GitHub and DNS administration access:

1. Enable Pages with **GitHub Actions** as its source and set its custom domain to
   `gp.aland20.com`. Verify domain ownership through GitHub's account settings.
   Ensure the `github-pages` environment permits deployments from the release tags.
2. Create `CNAME gp → aland20.github.io` in the `aland20.com` DNS zone. Do not put
   a scheme, repository name or URL path in the target.
3. Enable **Enforce HTTPS** in Pages after DNS verification and certificate issuance.
4. Configure an HTTPS 301 redirect from `groundplane.aland20.com` to
   `https://gp.aland20.com`, preserving path and query. This requires the DNS/edge
   provider's URL-forwarding service; a CNAME alone does not perform a redirect.
   The redirect service must also cover TLS for the source hostname.

See [GitHub's custom-domain instructions](https://docs.github.com/en/pages/configuring-a-custom-domain-for-your-github-pages-site/managing-a-custom-domain-for-your-github-pages-site)
and [multiple-domain limits](https://docs.github.com/en/pages/configuring-a-custom-domain-for-your-github-pages-site/troubleshooting-custom-domains-and-github-pages).
With an Actions deployment, a tracked CNAME file does not configure the custom domain.

After successful publication, download and inspect the installer before running:

```sh
curl -fsSLO https://gp.aland20.com/install.sh
curl -fsSLO https://gp.aland20.com/install.sh.sha256
sha256sum --check install.sh.sha256
sudo sh install.sh --version 0.0.1
```

The release builder already generates bundle SHA256 files. The installer computes
the downloaded bundle's checksum and compares it with the expected checksum before
extracting executable files; it also verifies the internal manifest. Pages adds
an installer checksum. Checksums from the same distribution channel detect changed
bytes but are not independent publisher signatures. Acceptance requires HTTPS,
exact installer bytes, working public downloads and path-preserving redirect checks.

### Build the Agent image

```sh
bash scripts/release-agent.sh --version 1.0.0 \
  --image ghcr.io/aland20/groundplane-agent:1.0.0
```

The default only builds. Add `--push` when publication is intended and registry
credentials are configured on the builder. Use the returned registry digest in
the bundle command, not the local Docker image id. Runner remains a separate
release input built with `make runner-image`; publish it explicitly and supply
its registry digest too. These images contain no installation credentials.
The final Agent image uses `scratch`, retaining the Agent, static Docker CLI,
Compose and CA bundle. The measured amd64 image is 147.5 MB uncompressed versus
276.0 MB before (46.6% smaller); required Docker/Compose binaries account for much
of the remaining size. This is a measured reduction, not an absolute minimum claim.

### Build the native bundle

```sh
bash scripts/repo-env.sh python3 scripts/release_bundle.py --version 1.0.0 \
  --agent-image ghcr.io/aland20/groundplane-agent@sha256:AGENT_DIGEST \
  --runner-image ghcr.io/aland20/groundplane-runner@sha256:RUNNER_DIGEST
```

Replace both digest markers with the actual 64-character registry hashes.
Build natively on each supported Linux architecture. The script builds the
production Controller with embedded Console, CLI and source-derived compatibility
descriptor. It bundles the existing bootstrap, release stager, protected update
client, unit, tmpfiles and startup example; it does not create another updater.
Image bytes are not in the archive: installation requires access to their registry.
The supplied image digests must contain the matching platform; the publisher owns
registry readback and two-platform qualification under ADR0060.

Outputs are `.tmp/releases/groundplane-VERSION-linux-ARCH.tar.gz` and the adjacent
`.tar.gz.sha256`. Existing archives are never overwritten. Archive headers are
stable; this is not a claim that independent compiler runs are byte-reproducible.
After qualification, upload these exact files and the reviewed root `install.sh`
to GitHub Release `vVERSION`, or use the tagged workflow above. The local bundle
command itself uploads nothing.

### Install or upgrade on a host

Download the reviewed `install.sh` from the selected release, inspect it, then run:

```sh
sudo sh install.sh --version 1.0.0
```

The installer targets Ubuntu 24.04/26.04 and Debian 13 on native amd64/arm64
and needs root. Ubuntu 22.04 and other distribution versions are rejected. It
downloads the architecture-specific bundle from this repository's GitHub Release
and verifies its SHA256 before executing bundled code. It rejects incorrect
version/platform, incomplete or altered members, duplicate paths, links and path
traversal. Downloads use HTTPS only and bounded sizes/timeouts. The trust root is
the reviewed installer plus GitHub's HTTPS release assets; a checksum from the
same release is integrity checking, not an independent publisher signature.
Supply `--sha256 HEX` to pin an independently obtained archive hash.

For a previously downloaded bundle:

```sh
sudo sh install.sh --version 1.0.0 \
  --bundle /path/to/groundplane-1.0.0-linux-amd64.tar.gz --sha256 ARCHIVE_SHA256
```

This avoids the archive download, not registry or package-manager access. A fresh
host provisions the existing Docker/Compose, registry and Runner prerequisites,
installs Controller/CLI and the recovery guard, creates the age identity and
enrolls the Agent. No Go, Node or source checkout is needed on the host. The
Controller listens on loopback by default; use `--listen-ip PRIVATE_IPV4` for a
trusted private listener. Use `--config /path/to/controller.yaml` for initial
startup settings, including address pools appropriate for the host. Bootstrap
fills the pinned Agent/Runner images into that configuration. Public/wildcard
listeners, public ingress and firewall changes are not part of this installer.

On an existing guarded installation the same command stages and follows a normal
Controller update Task. It does not run prerequisite setup, replace CLI/Runner,
rewrite configuration/keys/units or independently restart etcd/applications.
`--config` is refused on that path. `--stage-only` requires an existing guarded
installation and stages without activation; use the Console or the existing CLI
command below to activate the printed release digest. Legacy and partial
installations are refused, not silently converted or repaired.

Failures retain the invocation's private files when recovery needs them. Follow
the printed resume instructions; never delete the update journal to retry.
The new entrypoint's real fresh-host and native-update journeys remain unrun;
host-free safety checks and earlier `deploy.py` QA do not qualify them.

### Repeating installation

Repeating installation is safe for an already completed healthy release. After
staging, the installer checks the running and installed Controller digest, native
recovery availability, platform health and the enrolled Agent's actual container
ownership, pinned image and image bytes. An exact match prints `already installed
and healthy; skipped` without another update Task or process restart. A changed
version follows the normal protected update. Downloads and staging may repeat;
this guarantee concerns installed runtime, not an absence of file reads/writes.

An unresolved deployment receipt takes precedence over that shortcut: the client
resumes its original acceptance key or Task instead of submitting another update.
A failed update stays failed in history. Matching binary bytes alone never hide
an unhealthy Agent, active recovery, changed disk bytes or foreign container.

Prerequisite setup reuses a complete working Docker installation and an existing
matching registry; it does not reinstall/restart them on a successful repeat.
Ubuntu uses `docker.io` plus `docker-compose-v2`; Debian 13 explicitly installs
`docker.io`, `docker-cli` and `docker-compose` (Compose v2), since the daemon's
client dependency is only a recommendation. Package sources:
[Ubuntu 24.04](https://packages.ubuntu.com/noble/docker-compose-v2),
[Ubuntu 26.04](https://packages.ubuntu.com/resolute/docker.io), and
[Debian 13](https://packages.debian.org/trixie/docker.io).
Partial/mixed Docker
installations are refused rather than replaced. No upstream Docker APT repository
or convenience installer is added.

This is not an automatic repair tool for every interrupted bootstrap. Partial
recovery layouts and an unfinished initial Agent enrollment require the retained
diagnostic/recovery instructions. Existing configuration is preserved; `--config`
remains initial-install-only and is refused on a native installation. Repeating
an install must not become an implicit configuration change or destructive reset.

## Prerequisites

Use the repository-pinned Go, Node and npm toolchains, local Docker and SSH,
an existing Ubuntu 24.04/26.04 or Debian 13 host of the same architecture, a private root
SSH identity and an independently verified, pre-populated known-hosts file.
The target needs Docker/Compose, the loopback registry, curl, Python3, flock and
systemd. Optional `--setup` provisions the documented host prerequisites; use
it only with explicit host provisioning authority.

All examples use operator-supplied paths and a target address. The Controller
remains private; `--ip` configures its LAN listener only at initial bootstrap.
The native update route does not change listeners or host networking.

## Initial installation or one-time legacy transition

```sh
python3 scripts/deploy.py --key /path/to/key --known-hosts /path/to/known_hosts \
  --ip HOST_IP --version RELEASE_VERSION
```

For an existing legacy Controller, add `--bootstrap` during a maintenance window.
Stop new operator/scheduled work and wait for existing Tasks to finish; the
installer checks all retained Task pages and refuses nonterminal work. An old
binary cannot provide an admission fence retroactively. No active Task is aborted
by this check. A host already using native recovery refuses `--bootstrap`.

The first installation backs up existing files, creates the private release root,
installs the Controller and predecessor recovery executable, then installs the
unit's `ExecStartPre` guard before enabling the service. Controller/recovery
executables are root-owned0500, matching native immutable input validation.
It installs the CLI,
bootstrap Agent/Runner image configuration and age identity only on this path.
Agent enrollment/update must settle before the first candidate is made selectable.
On failure, restoration requires a successful Controller stop. Unresolved Agent
work, native evidence or failed restoration retains the private recovery files;
follow the reported state before another deployment. Never remove recovery data
to bypass a failed preflight.

## Normal release deployment

Run the same command without `--bootstrap`. On a guarded host it:

1. Builds release assets and source-derived Controller compatibility metadata.
2. Publishes/resolves the immutable Agent OCI digest and verifies the transferred
   Controller bytes against the build descriptor.
3. Publishes an immutable manifest/binary under
   `/var/lib/groundplane/controller-updates/releases/<sha256>` and atomically
   selects it in `candidate.json`.
4. Persists a protected request receipt, submits `POST /controller/update` and
   follows that exact Task through the brief Controller disconnect.

Distribution reuses an image only after the local and target Docker content ids
match exactly, then creates its invocation-owned transport tag. Missing images
use a4MiB/s paced archive. Runner build/transfer is bootstrap-only: an initial
read-only recovery-guard presence hint selects artifacts, while the remote
installer still performs authoritative complete-layout validation and rejects
partial or changed layouts before lifecycle effects. This
transport optimization does not authorize a local image id as runtime identity:
the remote publisher still resolves the registry-reported Agent RepoDigest.

Compilation is deliberately modest on a shared builder: native deployment sets
build-process `GOMAXPROCS=2`, and the Agent Docker build uses `GOMAXPROCS=2` and
`go build -p=2`. These are build-only settings, not deployed runtime limits.
[Go's build documentation](https://pkg.go.dev/cmd/go) defines `-p` as parallel
build programs; [the runtime documentation](https://pkg.go.dev/runtime#GOMAXPROCS)
defines the per-process execution limit. Neither is a hard memory or whole-host
resource guarantee. Provision separate build capacity when application traffic
cannot tolerate shared CPU, storage or network pressure.

The checkout filesystem must have at least10GiB free before target access and
before each uncached build stage; at least2GiB must remain before image transfer
or publication. Deployment refuses insufficient or unreadable capacity and does
not delete anything automatically. These are conservative admission checks, not
reservations or a whole-host guarantee: independently provision and monitor
Docker, Go cache, target and VM backing storage when they use other filesystems.
Never fill a filesystem that also backs an active application's virtual disk.

Only a `completed` Task is success. A recovered failed update remains failed.
The Controller owns drain, activation, Agent replacement and recovery. Deployment
does not overwrite executables, YAML, keys or systemd units, abort work, or
redeploy application Services. CLI and Runner distribution/configuration are not
part of the native Controller/Agent release; they remain unchanged on this path.

To stage for later operator review, add `--stage-only`. It requires an already
guarded host and never submits an update. Review the candidate in the Console's
Controller page, or activate the printed digest explicitly:

```sh
bin/groundplane --host PRIVATE_CONTROLLER_URL controller update --release sha256:DIGEST
bin/groundplane --host PRIVATE_CONTROLLER_URL task show TASK_ID
```

Compatibility is enforced again by the running Controller before activation.
An incompatible or modified candidate cannot be activated. Staging never executes
candidate code and never updates the qualified Agent selection.

## Interrupted deployment client

The private `deployment.json` receipt retains one release, idempotency key and
accepted Task id. A lost POST response is resolved with the same key/body. Once
the Task id is known, polling never submits another update. The client waits up
to720seconds; unresolved outcome is exit2, failed/rejected update is exit1.

On failure, deployment prints the exact retained helper path. Resume that helper
on the target with `python3 PRINTED_HELPER_PATH --resume`; it uses fixed loopback
HTTP and the existing receipt, not a new key. Repeating deployment for the same
unresolved release also reuses its receipt. A different release is refused until
the earlier request settles. After terminal failure and completed native recovery,
a fresh explicit deployment is a new update attempt.

Do not manually replace the Controller, remove `journal.json`, or delete the
retained bundle to make an uncertain operation look complete. Host and Task
surfaces remain the operator evidence. The receipt contains no credentials.

## Local verification

`make deployment-check` exercises staging safety, partial bootstrap rejection,
empty-only rollback, idle preflight, durable acceptance replay, same-Task resume
and the installer native/legacy branch boundary. `make controller VERSION=...`
builds embedded Console assets, Controller and its matching build descriptor.
These checks do not substitute for live A→B, bad-candidate, drain, interruption
and workload-traffic qualification or the required complete `make ci` gate.
