# Runners

Trusted workflow delivery is implemented. A Project-owned Runner's build, push,
Fetch, Deploy, failure preservation, same-boot restart and removal passed the
selected Ubuntu amd64 journey in [H75](../acceptance.md#runner-delivery). The full
isolation and lifecycle matrix remains unqualified; this is not a blanket
production-CI or hostile-workflow safety claim.

A Runner is a persistent GitHub Actions self-hosted runner owned by a Tenant,
Project or Environment. It is not a Project or an ordinary Agent workload. The native Controller
owns its local lifecycle and registration.

The [GitHub build workflow example](../../examples/github-actions/groundplane-build.yml)
shows the workflow contract for a GitHub-connected Service. It requires a scoped
Runner and the private job credential endpoint. Review it on the repository's
trusted workflow ref before approving it for use. Its GitHub-triggered deployment
journey still requires qualification.

## Creation and limits

Creation selects one Tenant, Project or Environment owner, a canonical GitHub repository or
organization URL, a GitHub Runner group, a slug, optional custom labels and a fresh short-lived GitHub
registration token. Obtain the token through GitHub settings; GP does not create
or fetch it.

GP ownership and GitHub registration scope are independent. A Project-owned Runner
can register against an organization URL to serve multiple repositories. For an
organization registration, enter the exact name of an existing GitHub Runner group;
GitHub controls that group’s repository and workflow access. GP does not create
groups or fetch their names with a registration token. Repository registrations
use `Default`; a custom group requires an organization URL.

The Console registration form exposes **GitHub Runner group**. The CLI uses
`runner add ... --runner-group "Build runners"`; the API creation body requires
`runner_group`. Console and CLI initially select `Default`. Names retain their
case and internal spaces. Registration, Retry and diagnostics use the captured
selection. A different group requires removing and registering the Runner again.

An optional **GitHub Runner name** replaces the generated stable name. It must be
unique within the GitHub registration scope and use at most 64 ASCII letters,
digits, dots, underscores or hyphens. It is independent of GP’s renamable slug.
The CLI uses `--github-name`; the API uses optional `github_name`.

Creation also selects a combined CPU/RAM budget. Console and CLI start at 2 CPUs
and 4 GiB; the API requires `cpu_millis` and `memory_bytes` explicitly. The CLI
flags are `--cpus` and `--memory-gib`. A CPU is 1,000 millicores; one GiB is
1,073,741,824 bytes. Accepted limits are 0.1–1,024 CPUs and 512 MiB–1 TiB.
These are ceilings, not reserved host capacity. Leave room for applications and
the control plane when selecting them.

One cgroup v2 systemd slice contains the listener, rootless Docker daemon and
its build processes. They share the CPU quota and memory maximum; the slice
cannot use swap. An out-of-memory build can fail or terminate this Runner’s own
processes. GP verifies actual kernel limits and requires host Docker’s systemd
cgroup driver; unsupported hosts fail rather than run unbounded. Removing a
Runner releases its empty slice. Persistent Docker cache consumes disk outside
the RAM budget; automatic cache cleanup is not implemented.

A Tenant can have at most five Runner records across all three ownership scopes.
Provisioning, failed and deleting records count until cleanup releases them.
Host identity slots, subordinate UID/GID ranges and network space are also finite;
exhaustion reports `resource.in_use`, not automatic pool expansion.

The slug and automatic upstream-update policy are editable. Owner, GitHub URL, GitHub Runner group, GitHub-visible name, resource budget, labels and the
release-selected image are immutable. Renaming does not re-register the Runner.
There is no in-place GP Runner image/tooling update, autoscaling, ephemeral pool, multi-host
placement or GitHub Enterprise support in the MVP contract.

## GitHub runner application updates

This capability is implemented separately from GP software upgrades. Live
automatic/manual-update and interruption qualification has not been run.

Creation exposes **Automatically update GitHub runner software**, initially enabled
in Console/CLI (`runner add --auto-update`). The API requires `auto_update`.
Change the policy through Runner settings, `runner edit --auto-update=false`, or
the existing Runner edit endpoint. The policy is Controller-managed: hourly checks
select newer verified upstream releases and submit the same durable update path
used manually. GitHub’s independent updater remains disabled to avoid competing
writers. Automatic checks require the Controller to be running and network access
to GitHub; errors do not enable another update path.

With automatic updates disabled, use the Runner software panel to load available
versions, select an exact version and choose **Update when idle**. Equivalent
commands are `runner versions`, `runner update --version` and `runner update-result`;
the API uses `/runners/{id}/versions` and `/runners/{id}/updates`. Uncertain
acceptance retries the original version/revision and idempotency key. The response
links the update Task; acceptance is not completion. The version list contains
checksummed official releases for this host architecture, not a promise that
GitHub still accepts every listed release or that a downgrade is application-safe.

GP verifies downloaded bytes before activation, preserves the registration,
workspace and rootless Docker storage, then observes the installed version and
GitHub reconnection. A busy Runner waits within the bounded Task deadline; an idle
race cannot authorize killing an observed active worker. Captured host receipts
support interruption reconciliation and exact predecessor restoration. Unproved
restoration remains unresolved rather than being reported as a successful update.

**Installed upstream version** is observed from the registered Runner binary.
The GP image version shown elsewhere describes its packaged CLI, Docker tools and
base OS. Updating the upstream application does not patch those tools or the OS,
change resource budgets, or require a new registration token. A future upstream
version needing different system libraries may fail and require a separately
reviewed GP image/tooling update.

## Tokens and Retry

The registration token is transient. GP does not save it in Tasks, logs, replay
records or durable runtime configuration. A queued Create does not allocate a
Runner until its prerequisite Tasks succeed and settle. The transient token is
held for at most one hour; queuing does not extend GitHub token validity. If the
Controller restarts or the token expires before capture, preparation fails
without creating a Runner. Submit a fresh Create with a fresh token.

A failure after creation has captured its native work retains the Runner and its allocations. Use Runner-specific
Retry with a **fresh token**; generic Task Retry cannot retry Runner creation.
Retry keeps the same Runner ID, quota claim, host slot and subnet. Replaying the
original request does not replace the token in an existing attempt.

## Isolation

Each Runner owns a dedicated rootless Docker daemon, unprivileged host identity
and distinct subordinate UID/GID ranges. It receives that daemon's socket, never
the host Docker socket. Jobs and service containers stay inside its user namespace.
Jobs use ordinary Docker commands directly; no custom Docker proxy filters them.
Docker privileges and networking options are bounded by the rootless daemon's
namespace, not host-root authority. Only trusted workflows may run here.

Each Runner gets a `/29` from the separate `runner.network_pool`, configured as
a `/24`, `/25` or `/26`. It joins no Environment, backing, platform or other
Runner network. Egress policy blocks direct access to GP private pools except the
private Controller API and local registry endpoints while allowing DNS and ordinary internet access. Same-Tenant
ownership does not permit communication with another Runner.

## GP command access and image delivery

A legacy Runner executes ordinary GitHub Actions
workflows. Its jobs may use the existing GP CLI or private API with full operator
authority. Tenant, Project and Environment ownership organizes Runners and their
lifecycle; it does not limit their GP commands or accessible resources.

Normal validation, deletion approvals, concurrency and replay checks apply.
Only trusted workflows should run here: they can manage other projects, shared
Backing Services, Secrets and host-wide GP operations. Direct network isolation
does not prevent actions that a workflow requests through GP.

The image includes the GP CLI. Jobs receive `GROUNDPLANE_HOST` pointing to
the Controller's explicit `listen.internal_http` endpoint. Installation sets
this to the default Docker bridge gateway. A manual loopback-only internal
endpoint cannot provision a Runner: configure a private container-reachable
endpoint first. Operator LAN/Tailscale listeners do not select Runner API or
registry addresses. No public listener or API relay is created automatically.

The API remains private; Runner access does not require public exposure.

### Scoped Runner migration and GitHub deployments

This opt-in path requires installed upgrade and live GitHub qualification before
production use. The Controller reads the dedicated Runner API and registry
addresses and pinned scoped-capable Runner image from the local
`<controller-config>.github-delivery.yaml` companion. These listeners use
separate ports on the private internal address. The scoped registry requires
its TLS certificate and key in `/etc/groundplane/scoped-runner-registry`,
with the listener IPv4 address in the certificate's subject alternative name.

Register the Runner once at Tenant, Project or Environment scope. With the
scoped registry configured, new Runners use Groundplane DNS and public IPv4
HTTPS until verification. To grant scoped Groundplane API and registry access,
open the Runner's details and select **Verify GitHub registration** or **Move to
scoped access**. Choose a GitHub App connection and repository that can verify
the registration. The migration Task waits for active work, then enables scoped
access while preserving the Runner identity and workspace. Check the Task
before using the Runner for a GitHub deployment.

In the Service's **GitHub deployment** page, select the repository, reviewed
workflow and source branch. Inherit a Runner selector from the repository
settings or choose an eligible label and optional group for this Service.
The build captures the source commit and publishes a digest to a dedicated
registry repository. Groundplane verifies the job and digest before it creates
the native Fetch and Deploy Tasks. Approval, when enabled, holds the image
until an operator approves deployment. Deployment follows the captured Service
rollout strategy.

For automatic deployments, pin the approved workflow to a protected ref
separate from the moving source branch. Branch polling works on private
installations. An optional dedicated listener accepts signed GitHub App
webhooks through an operator-configured HTTPS endpoint. Both inputs use the
same deployment deduplication.

After scoped migration, **Runner CLI access** lets an operator grant specific
Service, Task and image operations within one Environment for up to an hour.
A Runner can check the status of Tasks admitted under its own grant.
Revoking the grant removes those CLI permissions while preserving the Runner's
ability to authenticate a verified GitHub build.

#### Image delivery with legacy Runner access

Jobs build and push with ordinary Docker commands
to a private registry on the same host. CoreDNS supplies its internal name to both
the Runner's rootless daemon and host Docker. An explicit GP image-fetch operation
then loads the selected image into host Docker before ordinary Service Deploy.
No external registry or public DNS configuration is required.

The working implementation exposes **Host → Images → Fetch image** in the Console and
`groundplane image fetch <reference>` in the CLI. Use an explicit tag or digest
under `registry.groundplane.internal:5000` for Runner builds. Public images also
use the [host image workflow](platform.md#images). Acceptance returns a Task ID;
it does not resolve a tag or make the image available. After prerequisite Tasks
succeed and settle, Fetch captures the selected immutable image.
Wait for the Task to complete, then inspect Host Images for the resulting
reference before ordinary Deploy. The image's `config_digest` identifies its OCI
configuration, rather than Docker's store-specific local image ID.
Once preparation succeeds, Task Retry keeps that captured selection even if the
tag moves. A new Fetch makes a new selection. The CLI's `--idempotency-key` lets automation resolve uncertain
acceptance without creating another operation. H75 covers the selected live
delivery path; moved-tag and replay behavior has local, not live, proof.

The two Docker daemons have separate image stores: a successful build or push
alone does not make an image available for GP Deploy. Fetch must complete first.
Failed build, push or fetch leaves the serving Release unchanged. Removing a
Runner preserves shared registry data and application images. See the
[runtime design](../decisions/runner-isolation.md) for identity and access rules.

## Status and removal

### Connection checks

The Console Runner inspector offers **Test connections**. Equivalent CLI actions
are `runner check <slug>` and `runner check-result <slug>`; the result command
accepts `--task` to inspect a particular check rather than the latest retained one.
The successful four-dependency diagnostic on a registered Project Runner passed
[H115](../acceptance.md#historical-evidence-register). Dependency-failure and
interruption variants remain unqualified.

The check runs inside the existing registered Runner. It reports GitHub HTTPS
reachability, the dedicated Docker daemon's rootless status, private GP API access
and an authenticated registry read separately. It uses the Runner's existing
registry credentials and trust configuration; no registration token is requested
or consumed. It does not restart the Runner, build an image or publish anything.

A GitHub response establishes a network path, not GitHub registration or job
readiness. A registry read does not establish push permission. Passing is evidence
at the recorded time, not a CI/CD qualification or permanent availability guarantee.

Results retain the exact Runner configuration and runtime generation tested.
Changed configuration, replacement of the container or daemon, and removal mark
the result stale. Routine observation refreshes do not. Interruption records a
failure; generic Task Retry cannot replay the probe. Start a new check instead.
After uncertain acceptance, reuse the same CLI `--idempotency-key` and `--revision`;
the Console retains the exact unresolved submission when its inspector is reopened.

### Runtime lifecycle

Online status comes from current local runtime observation, not an editable flag.
Missing, stale or mismatched evidence reports offline. Reboot recovery must prove
new runtime ownership rather than reuse stale process identities.

Installed software details retain the accepted image, image configuration digest
and reported GP package version/source commit. They survive a Controller restart
and are separate from online status. Source-built packages show their short commit;
packages without recorded version metadata show a compact digest. A pending or
failed registration does not advertise its candidate as installed software.

Removal cleans up only ownership-proved local resources. Failed cleanup retains
the record and allocations needed for retry. **It does not deregister the Runner
in GitHub**; remove that separate registration manually.

## Design and qualification

[Runner isolation decisions](../decisions/runner-isolation.md) explain the process,
network, token and cleanup boundaries. The [QA matrix](../qa-matrix.md) retains
the behavioral cases needed before this isolation can be relied on. See
[current limitations](../capabilities.md) for the release boundary.
