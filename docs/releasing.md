# Build and publish releases

Host installation is described in [deployment](deployment.md). This guide is for
maintainers producing public artifacts. Build, publication and deployment are
separate actions; none implies authorization for the next.

## Prepare a version

`VERSION`, Console package metadata and its lockfile must agree. Go development
builds report `dev`; release builds receive explicit versions. `.runner-version`
selects the upstream GitHub Actions Runner dependency, not the GP release.

Add operator-visible changes under `Unreleased` in [CHANGELOG.md](../CHANGELOG.md).
Then prepare the chosen version, for example:

```sh
bash scripts/repo-env.sh python3 scripts/bump_version.py 0.0.2 --dry-run
bash scripts/repo-env.sh python3 scripts/bump_version.py 0.0.2
bash scripts/repo-env.sh python3 scripts/bump_version.py --check
```

The script updates the release metadata and moves notes to a pending-publication
section without committing, tagging or pushing. Empty notes need meaningful
content before consistency checking succeeds. Review the diff and follow
[delivery](delivery.md) before publication. A previous release's approved debt
exception is not permission to carry it into a new version.

## Tags and workflow

[release.yml](../.github/workflows/release.yml) publishes pushed stable tags whose
commit belongs to `main`:

- `vX.Y.Z` builds the combined fresh-install release and creates
  `controller/vX.Y.Z` and `agent/vX.Y.Z` at the same commit, reusing its artifacts.
- `controller/vX.Y.Z` builds the Controller bundle and Runner dependency without
  building an Agent image.
- `agent/vX.Y.Z` builds only Agent artifacts and may use an independent version.

Generated component tags do not trigger duplicate builds. Existing tags and
published assets are not overwritten. A failed workflow can leave image children
already pushed; rerun the unchanged tag rather than moving it. Manual dispatch
uses an existing release tag, not an arbitrary branch.

External Actions use supported major-version tags, not commit hashes or patch
pins. Toolchain/dependency versions remain owned by their manifests and workflow.
Artifact checksums and runtime image digests are still required.

## Artifacts and visibility

Publication builds native amd64/arm64 images and reads back their multi-platform
indexes. Native archives contain Controller with embedded Console, CLI,
compatibility metadata and installation/update helpers. Images are registry
dependencies, not embedded archive layers. Each archive gets a SHA256 file.

Releases and the `groundplane-agent`/`groundplane-runner` GHCR packages are intended
for public anonymous download. The installer carries no GitHub credentials.
Workflow permissions allow the relevant release and package writes. New packages
may require an administrator to set public visibility; an approval or successful
push alone is not proof of anonymous access. The publication check must not be
bypassed if a package is private.

## Local builders

[scripts/release.py](../scripts/release.py) coordinates both builds;
`--controller-only` and `--agent-only` select one. A combined build requires
`--push` so its bundle can pin the built Agent's registry digest. GitHub Release
publication is separate.

For an Agent image:

```sh
bash scripts/release-agent.sh --version agent/v0.0.1 \
  --image ghcr.io/aland20/groundplane-agent:v0.0.1
```

This builds locally unless `--push` is supplied. The final image uses `scratch`
but still includes the Agent, static Docker CLI, Compose and CA certificates;
`scratch` does not make those required binaries disappear.

For a native combined bundle:

```sh
bash scripts/repo-env.sh python3 scripts/release_bundle.py --version 0.0.1 \
  --agent-image ghcr.io/aland20/groundplane-agent@sha256:AGENT_DIGEST \
  --runner-image ghcr.io/aland20/groundplane-runner@sha256:RUNNER_DIGEST
```

Replace digest markers with actual registry hashes containing the target platform.
Build on each supported native Linux architecture. Runner is a separate dependency
built with `make runner-image`; publish it explicitly when needed.

Archives and checksums go under `.tmp/releases/`. Existing outputs are not
overwritten. Stable archive headers do not prove bit-identical independent compiler
runs. The [packaging decisions](decisions/release-packaging.md) explain integrity
and compatibility boundaries.

## Installation website

[pages.yml](../.github/workflows/pages.yml) runs separately on pushes to `main`
and manual dispatch on `main`. It publishes the landing page and that commit's
installer/checksum, not repository contents or private evidence. A successful
Pages deploy does not prove that a stable release exists.

The primary address is `https://gp.aland20.com`. Pages uses GitHub Actions as its
deployment source. DNS is `CNAME gp → aland20.github.io`; GitHub Pages must also
have that custom domain configured and HTTPS enabled. Domain and environment
administration remain manual account operations.

`groundplane.aland20.com` needs an HTTPS redirect to the primary address that
preserves path and query. A CNAME alone is not a redirect; its serving provider
must handle the redirect and source hostname's certificate.

Publication readiness still requires [the release gate](delivery.md) and the
relevant [operator QA](qa-matrix.md). This documentation cleanup performed neither.
