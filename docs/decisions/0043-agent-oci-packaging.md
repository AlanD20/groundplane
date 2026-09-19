# ADR 0043: Ship one pinned Agent image with Docker CLI and Compose v2

- Status: Accepted
- Date: 2026-08-23

## Context

ADR 0016 makes the Agent a Controller-owned OCI container. ADR 0022 makes the
same image the execution environment for short-lived Compose and
materialization helpers and requires Docker Compose v2 as the workload apply
runtime.

The repository previously built only a host Agent binary. Its real-host C05
acceptance image was based on scratch and therefore contained neither the
Docker CLI nor the Compose plugin. Agent enrollment succeeded, but the first
real Compose task failed before it could validate or apply its artifact. A
temporary image assembled from test-host binaries proved the execution path,
but copying host packages is not a production build contract.

The owner approved proceeding with the recommended MVP decisions without a
separate approval round. The exact upstream images below were inspected on the
minimal Ubuntu 24.04 acceptance host before this decision was recorded.

## Decision

Groundplane ships one `Dockerfile.agent`. It builds the Agent with the module's
Go 1.26 language contract and these immutable upstream inputs:

| Purpose | Version | OCI reference |
| --- | --- | --- |
| Go builder | Go 1.26.6 on Alpine 3.23 | `golang:1.26.6-alpine3.23@sha256:e57c41c1d5864341031181b0db34b9a537bb5773eb6428e4e5bdaea0f9135406` |
| Docker CLI runtime | 29.1.3 | `docker:29.1.3-cli@sha256:4fa0ee1f3a7e4354c4ea34558b6d4ee32859baf4973d4c8ccc8e7fe3dd730c04` |
| Compose plugin | 2.40.3 | `docker/compose-bin:v2.40.3@sha256:e39da8206cc48c6e2c99ce371935eef9d7eff7db600a993fd357de8b75621edf` |

The final runtime uses `scratch`, copying only the static Docker CLI and CA
bundle from the pinned Docker input, the accepted Compose 2.40.3 binary at
Docker's standard CLI-plugin path and the built Agent. Explicit PATH/HOME and
empty runtime directories replace inherited base-image defaults. The Docker
input's Compose 5 plugin, Buildx, shell and package manager are not copied.
Groundplane therefore has one Compose implementation and does not silently
cross the ADR 0022 major-version seam. Unlike the earlier incomplete scratch
image, this image retains the tools required by the closed helper procedure.

The Agent binary is built with `CGO_ENABLED=0`, `-trimpath`, disabled VCS
probing, and an empty Go build id. `AGENT_VERSION` is embedded into the shared
version value and the OCI version label. Its default is `dev`; release
automation supplies the actual release version rather than deriving a version
from a mutable tag or dirty checkout.

The final image contract is:

- user `0:0`, as required by the accepted Agent runtime;
- work directory `/`;
- `DOCKER_HOST=unix:///var/run/docker.sock`;
- entrypoint `/usr/local/bin/groundplane-agent`;
- no OCI image command (`CMD []`, normalized to `Config.Cmd=null`), so the
  an inherited `sh` command cannot become an unsupported Agent
  argument; and
- Docker CLI and Compose available only as tools invoked by the validated
  Agent helper procedure.

The persistent Agent and all short-lived Agent helpers use this same image.
There is no helper tag, host-package copy, bootstrap Compose file, Agent
systemd unit, or second workload executor.

`scripts/release-agent.sh` requires explicit version/image arguments, runs focused
runtime checks and publishes only with `--push`; its output then identifies the
registry digest. `make agent-image` builds a local development tag.
`make agent-image-smoke` additionally
asserts the entrypoint, empty command, root user, Docker CLI version, and
Compose version. `make ci` includes that smoke. A local or registry tag is only
a build handle: deployment still requires the resulting immutable RepoDigest
in `controller.yaml`.

The pinned manifests support the platform selected by the Docker builder. MVP
acceptance currently proves `linux/amd64`; no other architecture is claimed
until the same image and host scenario pass there.

## Rejected alternatives

### Keep the scratch image and use the host CLI

Rejected because the helper executes inside the Agent image and receives only
the narrow accepted mounts. Adding host executable/library mounts would make
runtime behavior host-package-dependent and enlarge the security boundary.

### Copy Docker and Compose from the build host

Rejected because the resulting image cannot be reconstructed from repository
and immutable upstream inputs. This was acceptable only as a disposable
acceptance diagnostic.

### Use the Compose 5 plugin bundled by Docker CLI 29.1.3

Rejected because ADR 0022 explicitly accepts Compose v2. A major-version
change requires its own compatibility evidence and decision.

### Ship separate persistent-Agent and helper images

Rejected because Controller configuration owns one digest and every helper is
a mode of the same trusted binary. Multiple images would add version skew and
another delivery surface without an MVP capability.

## Consequences

- A release now has a reproducible, pinned Agent runtime build graph.
- CI and production image builds require Docker Engine and registry access for
  missing immutable base layers.
- Updating Go, Docker CLI, Compose, or an upstream manifest digest is an
  explicit reviewed contract change.
- ADR 0010 uses the digest-pinned configured image directly for Controller-owned
  replacement. Offline update bundles and release-signature policy are
  post-MVP.
