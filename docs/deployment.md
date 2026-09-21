# Install and update Groundplane

The installer targets Ubuntu 24.04, Ubuntu 26.04 and Debian 13 on native
amd64/arm64. It requires root. Supported targets are not proof that every fresh
installation and upgrade combination has passed QA; see [current limitations](capabilities.md).

## Install a published release

```sh
curl -fsSL https://gp.aland20.com/install.sh | sudo bash
```

This runs the downloaded script as root. Use only a trusted installer and source.
To inspect the script first and choose a version:

```sh
curl -fsSLO https://gp.aland20.com/install.sh
curl -fsSLO https://gp.aland20.com/install.sh.sha256
sha256sum --check install.sh.sha256
sudo sh install.sh --version 0.0.1
```

The website being available does not mean a release has been published. Without
`--version`, installation selects the highest published stable version in the
required namespace, excluding drafts and prereleases. Selection happens once.

The installer verifies the downloaded archive checksum and manifest before
executing bundled code. It rejects incorrect version/platform, missing or altered
members, duplicate paths, links and path traversal. A checksum delivered by the
same channel detects changed bytes; it is not an independent publisher signature.
Use `--sha256` to supply an independently obtained archive hash.

For a local bundle:

```sh
sudo sh install.sh --version 0.0.1 \
  --bundle /path/to/groundplane-0.0.1-linux-amd64.tar.gz --sha256 ARCHIVE_SHA256
```

Replace the path and hash with the actual archive and checksum. This skips the
archive download, not package-manager or runtime-registry access.

## What initial installation changes

Bootstrap provisions Docker/Compose and the loopback registry prerequisites,
installs Controller/CLI and the native recovery guard, creates the age identity,
configures the pinned Agent/Runner images and enrolls the Agent. No Go or Node
toolchain is installed on the host. Python, curl and CA certificates are installed
when needed. A partial or mixed Docker installation is refused, not replaced.

The unauthenticated Controller listens on loopback by default. To expose it on a
trusted private interface, add `--listen-ip PRIVATE_IPV4`. Use
`--config /path/to/controller.yaml` for initial configuration, including suitable
non-overlapping address pools. Bootstrap fills its pinned image selections.
The installer does not configure public ingress, wildcard listeners or host
firewalls. Do not expose the API to an untrusted network.

## Build and install a Git ref

```sh
curl -fsSL https://gp.aland20.com/install.sh | sudo bash -s -- --ref main
```

Replace `main` with a branch, tag or commit. The installer resolves it once,
downloads that exact commit and builds locally in containers. This does not need
a published GP release and does not publish per-commit artifacts. The selected
commit must contain the ref installer and build Dockerfile. Its deployment helpers
execute as root, so selecting a ref is a code-trust decision.

Docker Engine must already be running. Go, Node, npm, Git and Buildx are not
installed on the host; build tools run in containers. Internet access and at least
10 GiB of free build space are required. This is a preflight minimum, not a bound
on peak disk or memory use.

Each invocation owns its builder, cache, source checkout, temporary tags and newly
pulled build-tool images. Normal completion and handled failures clean those up
without pruning existing images or unrelated caches. SIGKILL or a machine crash
cannot run cleanup. Installed runtime images and files needed by unresolved
recovery are intentionally retained.

Existing installations skip the Runner build and retain their CLI, Runner, etcd,
configuration and keys. Builds use the resolved commit's timestamp for image
metadata; the displayed version includes its commit. Activation still checks exact
image and binary identities.

`--ref` cannot combine with `--version`, `--bundle` or `--sha256`. Scope flags,
`--stage-only`, `--listen-ip` and initial-only `--config` retain their normal roles.

## Independent release scopes

| Release | Intended use |
| --- | --- |
| `vX.Y.Z` | Combined fresh installation; publication also creates matching component releases. |
| `controller/vX.Y.Z` | Controller/Console/CLI bundle and Runner dependency; no Agent build. |
| `agent/vX.Y.Z` | Agent image, metadata and small installer archives. |

Updates select only the component namespaces, never plain `v` releases. With no
version specified, each selected component resolves its own latest stable version.
Runtime images use immutable digests. OCI repositories use `vX.Y.Z` image tags
because `/` is not valid in an image tag; embedded component versions retain their
Git namespace.

The installer accepts mutually exclusive `--controller-only` and `--agent-only`.
A Controller-only update keeps the existing Agent; a Controller-only fresh install
selects the latest Agent release when none exists. Agent-only requires the
Controller and an enrolled Agent and does not replace the Controller.

Updating both runs the guarded Controller update first, preserving the Agent,
then its independent update. A failed second operation does not undo a completed
Controller update. Inspect each Task's result.

## Updates, staging and repeat runs

On an existing guarded installation, the installer stages and follows normal
protected update Tasks. It does not rerun bootstrap, replace CLI/Runner, rewrite
configuration or keys, or independently restart etcd and application workloads.
`--config` is rejected on this path. Partial or legacy layouts require explicit
diagnosis; the installer does not silently repair them.

`--stage-only` requires an existing guarded installation. It prepares a candidate
without activation so an operator can review it. Activate the printed digest
through Host's Controller view or the CLI:

```sh
groundplane --host PRIVATE_CONTROLLER_URL controller update --release sha256:DIGEST
groundplane --host PRIVATE_CONTROLLER_URL task show TASK_ID
```

Replace those values with the private endpoint and actual candidate/Task identity.
Compatibility is checked again by the running Controller. Staging is not update
success and does not authorize a changed Agent selection.

A repeat of an already completed healthy installation can skip activation after
checking exact Controller bytes, recovery availability, platform health and Agent
ownership/image. It may repeat downloads and staging. Matching a version string
alone cannot hide unhealthy or foreign runtime.

An unresolved receipt takes priority over that shortcut. Lost request responses
reuse the original acceptance key, body and Task. Follow the printed retained
helper/resume path; never delete the update journal or bundle to force a new
request. A recovered failed update stays failed. Only a completed Task is success.

See [update safety](features/upgrade-safety.md) for busy-work, interruption and
application-continuity guarantees. Current source has not been freshly qualified.

## Maintainer deployment helper

[scripts/deploy.py](../scripts/deploy.py) builds and transfers from a checkout over
SSH for explicitly authorized development hosts. Its help owns the command
options. Supply a private identity and independently verified known-hosts file;
provisioning is not implied by a documentation example.

The helper reuses the same native stager and update operation. Bootstrap or a
one-time legacy transition is separate, requires an idle host and may need a
maintenance window. It refuses retained nonterminal work and does not abort it.
Guarded hosts cannot use the legacy bootstrap path. Unresolved recovery files
remain until the reported outcome is resolved.

Build-space checks do not reserve capacity: do not fill a filesystem that also
backs an application's virtual disk. Release production and Pages publication
are described in [releasing](releasing.md), not performed by host installation.
