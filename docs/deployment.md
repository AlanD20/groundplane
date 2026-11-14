# Install and update Groundplane

The installer targets Ubuntu 24.04, Ubuntu 26.04 and Debian 13 on native
amd64/arm64. It requires root. Supported targets are not proof that every fresh
installation and upgrade combination has passed QA; see [current limitations](capabilities.md).

The host must use systemd. Published-bundle installation checks for at least
2 GiB of free space before downloading and again before installation; Git-ref
builds require at least 10 GiB. These are admission checks, not total storage
estimates. Package installation and runtime image downloads also need network
access, including when the GP archive is supplied locally.

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
configures the pinned Agent/Runner images, preloads upstream PostgreSQL 16 Alpine
and enrolls the Agent. PostgreSQL Backup/Restore tools are a separate verified
artifact extracted to a digest-owned host directory and mounted read-only into
the unmodified database image. No Go or Node toolchain is installed on the host.
Python, curl and CA certificates are installed
when needed. A partial or mixed Docker installation is refused, not replaced.

The unauthenticated Controller listens on loopback by default. To expose it on a
trusted private or Tailscale IPv4 interface, add `--listen-ip OPERATOR_IPV4`. Use
`--config /path/to/controller.yaml` for initial configuration, including suitable
non-overlapping address pools. Bootstrap fills its pinned image selections.
The installer does not configure public ingress, wildcard listeners or host
firewalls. Do not expose the API to an untrusted network.

## Uninstall or reset

To erase this host’s Groundplane installation and start again:

```sh
curl -fsSL https://gp.aland20.com/uninstall.sh | sudo bash -s -- --yes
```

This permanently deletes GP-managed containers, networks, Volumes and their local
data, including databases, Secrets, configuration, Task history, registry storage,
Runner accounts and GP binaries. It works without a healthy Controller or API.
If host DNS used GP’s resolver, its saved baseline is restored before CoreDNS
is removed. A missing baseline requires repairing host DNS before continuing.
On dhcpcd hosts, uninstall also removes GP's DNS guard so DHCP can resume normal
resolver updates. DHCP, host packages and the network configuration remain installed.
Docker, Compose, host packages, unrelated Docker resources and shared upstream
image caches remain installed. Remote backups, GitHub registrations and Cloudflare
configuration are outside this host reset and remain untouched.

Replace `--yes` with `--dry-run` to inspect the selected resources without changing
them. The script selects Docker resources by GP ownership labels, not application
names. Runner allocation labels and GP Compose ownership remain discoverable even
after their containers are gone. GP's invocation-owned Buildx daemons and cache
Volumes use their reserved generated builder identities; the shared Docker builder
and cache are retained. Cleanup stops GP execution before re-reading child resources
and tolerates only confirmed absence when a resource disappears during removal.
It refuses to erase mounted directories or traverse symlink ancestors.
If Docker is installed but unavailable, restore access to its local daemon and
rerun; an unavailable daemon is not evidence that its resources are absent.
Operator files stored outside GP-managed directories and Volume paths are not
guessed or deleted. The shared installer lock inode remains for safe coordination;
system journal records remain under the host’s normal retention.

After successful removal, run the normal install command again. No package
reinstallation or manual etcd cleanup is needed. The script and its checksum are
published independently of release tags at `/uninstall.sh` and
`/uninstall.sh.sha256`.

## Connect after installation

With the default configuration, the Console is served at `http://127.0.0.1:8080`
on the installed host. The CLI is installed as `groundplane`:

```sh
groundplane version
groundplane host show
```

For a remote host that retains the loopback default, use an SSH tunnel from your
workstation:

```sh
ssh -N -L 18080:127.0.0.1:8080 user@your-host
```

Keep that connection open and visit `http://127.0.0.1:18080`. Replace the SSH
user and host with your own. If the CLI is also installed on your workstation,
use `groundplane --host http://127.0.0.1:18080 host show` through the same tunnel.
A configured private listener can be reached directly from its trusted network.
Custom startup YAML may select a different HTTP port.
For Tailscale operator access, enroll the host separately and add its exact
Tailscale IPv4 address to `listen.http`, retaining loopback:

```yaml
listen:
  http:
    - 127.0.0.1:8080
    - 100.101.102.103:8080
  internal_http: 172.17.0.1:8080
```

Use the host's actual Tailscale address and Docker bridge gateway, not these
example addresses. Save startup configuration, then restart the Controller.
Visit `http://TAILSCALE_IP:8080`; the CLI uses that same origin with `--host`.
[Tailscale encrypts the network transport](https://tailscale.com/docs/concepts/tailscale-ip-addresses). This direct HTTP listener does not
configure Tailscale Serve, HTTPS certificates or tailnet access policy.

Only exact IPv4 addresses in Tailscale's `100.64.0.0/10` range are accepted;
public, wildcard and IPv6 listeners remain rejected. The Controller can bind its
configured Tailscale address before the interface appears, so local operation
does not depend on Tailscale startup order. Remote access requires the enrolled
host and client to be connected with an allowing tailnet policy. Every client
allowed to reach GP is a trusted operator with full API and host-terminal access.

`listen.internal_http` is independent of operator listeners. Fresh installation
sets it to the default Docker bridge gateway on port 8080 for Runner API access;
the private registry uses that address on port 5000. Neither selects a Tailscale
address. Manually configured installations default this endpoint to loopback;
set an explicit private, container-reachable address before provisioning Runners.
Existing installations must set this field explicitly when adopting this
configuration; it is not inferred from their operator listeners.


The Console and API share the Controller's HTTP listener. A successful host read
checks control-plane access; it does not prove that an application is serving.
Continue with [CLI/API usage](api-cli.md) and [Blueprint authoring](blueprint.md).

## Build and install a Git ref

```sh
curl -fsSL https://gp.aland20.com/install.sh | sudo bash -s -- --ref main
```

Replace `main` with a branch, tag or commit. The installer resolves it once,
downloads that exact commit and builds locally in containers. This does not need
a published GP release and does not publish per-commit artifacts. The selected
commit must contain the ref installer and build Dockerfile. Its deployment helpers
execute as root, so selecting a ref is a code-trust decision.

The default local Docker Engine must already be running. Remote `DOCKER_HOST`
values and a nonempty `DOCKER_CONTEXT` are rejected. Go, Node, npm, Git and Buildx
are not installed on the host; build tools run in containers. Internet access and at least
10 GiB of free build space are required. This is a preflight minimum, not a bound
on peak disk or memory use.

Each invocation owns its builder, cache, source checkout, temporary tags and newly
pulled build-tool images. Normal completion and handled failures clean those up
without pruning existing images or unrelated caches. SIGKILL or a machine crash
cannot run cleanup. Installed runtime images and files needed by unresolved
recovery are intentionally retained.

Existing installations skip the Runner build and retain their Runner, etcd,
configuration and keys. A successful automatic Controller update also installs
the matching CLI after its protected Task completes. Builds use the resolved
commit's timestamp for image metadata. Branch and commit refs produce a
`VERSION-ref.COMMIT` version; stable `vX.Y.Z`, `controller/vX.Y.Z` and
`agent/vX.Y.Z` refs retain a component release version instead. Activation still
checks exact image and binary identities, not just that version label.

`--ref` cannot combine with `--version`, `--bundle` or `--sha256`. Scope flags,
`--listen-ip` and initial-only `--config` retain their normal roles.
`--stage-only` requires an existing guarded installation. Agent-only installation
rejects both `--stage-only` and `--config`, including with `--ref`.

## Independent release scopes

| Release | Intended use |
| --- | --- |
| `vX.Y.Z` | Combined fresh installation; publication also creates matching component releases. |
| `controller/vX.Y.Z` | Controller/Console/CLI bundle and Runner dependency; no Agent build. |
| `agent/vX.Y.Z` | Agent image, metadata and small installer archives. |

Updates select only the component namespaces, never plain `v` releases. With no
version specified, each selected component resolves its own latest stable version.
Pass numeric `--version X.Y.Z`, without a `v` or component prefix. On a combined
update this pins both component releases to that version; use a component scope
when their desired versions differ.
Runtime images use immutable digests. OCI repositories use `vX.Y.Z` image tags
because `/` is not valid in an image tag; embedded component versions retain their
Git namespace.

The installer accepts mutually exclusive `--controller-only` and `--agent-only`.
A Controller-only update keeps the existing Agent; a Controller-only fresh install
selects the latest Agent release when none exists. Agent-only requires the
Controller and an enrolled Agent and does not replace the Controller.
Pulls from GP's managed registry select its installer-managed Docker credentials
automatically, including updates of ref-built Agents. No `DOCKER_CONFIG` override
or global Docker login is required for that registry.

Updating both stages the selected Controller and Agent together, then uses one
guarded handoff. The candidate Controller replaces the Agent and must prove the
pair ready before the update succeeds. Failed qualification restores the previous
Controller and Agent. A channel-format change requires Both; a persistent-format
change still rejects an in-place update. Follow the same update Task through
Controller restart and recovery.

Controller installation or staging also preloads the selected upstream database
and installs its independent backup tools. It does not replace an existing
Backing's database or retained tools. Agent-only updates do not patch databases.
Source installation builds the backup-tool artifact locally and publishes it to
the host's managed registry before building the Controller; allow space and memory
for that build as well as the Controller, Console, Agent and fresh-install Runner.

## Updates, staging and repeat runs

On an existing guarded installation, the installer stages and follows normal
protected update Tasks. Once a Controller update completes, it atomically replaces
the local CLI with the bundled version. A failed or unresolved Controller Task
leaves the old CLI in place. If CLI replacement itself fails, the installer
reports failure and retains the candidate for a repeat run; it does not claim
that the completed Controller Task was rolled back. The installer does not rerun
bootstrap, replace Runner, rewrite configuration or keys, or independently
restart etcd and application workloads.
`--config` is rejected on this path. Partial or legacy layouts require explicit
diagnosis; the installer does not silently repair them.

Controller upgrades require matching storage epochs. A mismatch is refused before
replacing the Controller, leaving the installed software and application data
intact. A fresh install is for an empty host; moving data between storage formats
requires a separately qualified migration, not an installer compatibility path.

The Runner-group revision of `0.0.1` changes the stored Runner format. Earlier
`0.0.1` candidates cannot use the normal update path to this revision. For an
installation with no data to retain, explicitly uninstall and reinstall, then
recreate the configuration. Existing workloads or data require a reviewed
migration; rerunning the installer does not authorize deleting them.

`--stage-only` requires an existing guarded installation. It prepares a candidate
without activating the Controller or replacing the CLI. Activate the printed
digest through Host's Controller view or the CLI:

```sh
groundplane --host PRIVATE_CONTROLLER_URL controller update --release sha256:DIGEST
groundplane --host PRIVATE_CONTROLLER_URL task show TASK_ID
```

Replace those values with the private endpoint and actual candidate/Task identity.
Compatibility is checked again by the running Controller. Staging is not update
success and does not authorize a changed Agent selection. A manual Controller
update does not install a CLI; rerun the installer for that release afterward if
the host CLI should match it.

A repeat of an already completed healthy installation can skip activation after
checking exact Controller bytes, recovery availability, platform health and Agent
ownership/image. It may repeat downloads and staging. Matching a version string
alone cannot hide unhealthy or foreign runtime.

An unresolved receipt takes priority over that shortcut. Lost request responses
reuse the original acceptance key, body and Task. Follow the printed retained
helper/resume path; never delete the update journal or bundle to force a new
request. A recovered failed update stays failed. Only a completed Task is success.

See [update safety](features/upgrade-safety.md) for busy-work, interruption and
application-continuity guarantees. [Capability status](capabilities.md) separates
recorded qualification from cases still needing proof for the selected release.

## Maintainer deployment helper

[scripts/deploy.py](../scripts/deploy.py) builds and transfers from a checkout over
SSH for explicitly authorized development hosts. Its help owns the command
options. Supply a private identity and independently verified known-hosts file;
provisioning is not implied by a documentation example.

The helper initializes fresh hosts and uses the native stager and guarded update
operation for existing installations. An existing Controller without native
recovery is rejected; there is no unguarded conversion or replacement option.
Partial recovery installations are also rejected. Unresolved recovery files
remain until the reported outcome is resolved.

Build-space checks do not reserve capacity: do not fill a filesystem that also
backs an application's virtual disk. Release production and Pages publication
are described in [releasing](releasing.md), not performed by host installation.
