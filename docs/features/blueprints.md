# Blueprint authoring and application

A Blueprint describes desired Environment configuration using Compose plus GP
extensions. It is not a record of running containers.
[The Blueprint reference](../blueprint.md) owns syntax and accepted fields.

## Read, edit, validate and apply

Use the Environment Blueprint editor in the Console, or the CLI workflow below.
The Tenant, Project and Environment must already exist. For a first Blueprint,
start with the [complete example](../blueprint.md#start-with-one-service).

For an existing Environment, export its desired configuration into a dedicated
bundle directory:

```sh
mkdir -p blueprint
groundplane environment blueprint show production \
  --tenant acme --project storefront > blueprint/blueprint.yaml
```

Show takes the Environment name; it writes YAML directly, without `--output`.
Edit the exported file, then preview and submit it:

```sh
groundplane environment blueprint validate production \
  --tenant acme --project storefront \
  --bundle-dir ./blueprint --root blueprint.yaml

groundplane environment blueprint apply production \
  --tenant acme --project storefront \
  --bundle-dir ./blueprint --root blueprint.yaml
```

`--bundle-dir` and `--root` are required for both commands. The CLI uploads every
regular file in that directory, including hidden and nested files; keep unrelated
source, private files and export/review output outside it.

Export reconstructs normalized desired state. It does not preserve comments,
anchors, aliases or source-file boundaries. Script bodies use literal YAML
blocks. It is one YAML document, not a full archive: any referenced companion
files must still be supplied in the import bundle. Drafts stay local until
explicitly submitted.

The API requires the exact last-read quoted revision in `If-Match`; see
[Blueprint requests](../api-cli.md#blueprint-requests). Revision zero means no
desired head. A stale revision fails without writes. The Console retains its
loaded revision. The CLI automatically reads a revision separately for each
Validate and ordinary Apply invocation; it does not pin Apply to a previous
Validate result. If another operator changes the Environment between those
commands, review it again. Use the API when automation needs to enforce the
exact reviewed revision.

Validate creates no Task, desired revision or host effect. Its diff uses
`create`, `update`, `remove` and `retain`; JSON output also identifies new empty
secret values. It shares Apply's parser but does not run all of Apply's
preparation checks, including local image resolution and executable resource
preparation. A successful preview is not a reservation or proof that Apply
will be accepted. This is a current implementation gap, not a reduced
validation contract.

Apply publishes one reconcile Task with immutable inputs. Follow its returned id:

```sh
groundplane task show task_01J00000000000000000000000
groundplane task events task_01J00000000000000000000000
```

Replace the illustrative id with the actual result. Acceptance does not imply
successful application. Required workload images must already exist in the host
Docker daemon. While the Apply Task is pending or running, a second Apply to
that Environment is refused; wait for settlement.

Current Apply rolls out native Service candidates with recreate. A declared
blue-green default belongs to the explicit Service Deploy workflow; Apply
does not promise its traffic-switch behavior. See the
[release fields](../blueprint.md#x-gp-release).

## Multi-file bundles

A bundle can contain native Compose layers and companion files:

```text
blueprint/
  blueprint.yaml
  production.yaml
  config/
    app.env
```

The root contains the Groundplane envelope and root extensions. For example,
an additional `production.yaml` can override the root's API image:

```yaml
services:
  api:
    image: ${API_IMAGE}
    env_file:
      - path: config/app.env
```

`config/app.env` contains only non-secret configuration, for example
`LOG_LEVEL=info`. Submit the layer and its explicit interpolation value:

```sh
groundplane environment blueprint validate production \
  --tenant acme --project storefront \
  --bundle-dir ./blueprint --root blueprint.yaml \
  --compose-file production.yaml \
  --var API_IMAGE=registry.example/storefront-api:2026-09-29
```

Use the same bundle options for Apply. Repeat `--compose-file` for additional
layers in order; the root is already first. Repeat `--var` for different keys.
There is no implicit `.env`, host environment, undeclared file or remote lookup.
Bundle paths and limits are defined in the [reference](../blueprint.md#closed-bundle-input).

## Resolve an uncertain Apply

If the CLI reports an unknown Apply outcome, keep the bundle unchanged. Repeat
the Apply against the same Environment ID with the reported `--retry-key` and
`--retry-revision`; this replays the original request without reading a newer
revision or creating a second intent. If the bundle changed, the Controller
rejects the replay. The Console retains its original request in the current
browser tab and offers **Resolve original Apply** after an uncertain response.

For example, using the Environment id, revision and key printed by the error:

```sh
groundplane environment blueprint apply env_01J00000000000000000000000 --id \
  --bundle-dir ./blueprint --root blueprint.yaml \
  --retry-key 01J000000000000000000000000 \
  --retry-revision 0
```

Replace all three recovery values with the reported values. Both retry flags
are required together; the key is the original Apply ULID and the revision is
`0` or its original `task_…` revision. Preserve additional layer and interpolation
options too. Request replay resolves the original acceptance; Task Retry is a
separate action after an eligible execution failure.

## Removal and identity

The Blueprint is the Environment's foundational intention. Adding or removing an
operator-managed Entry changes its Environment configuration when Apply succeeds.
Directly created Entries also appear in canonical export and are reconciled by
the next Apply. Existing secret-literal values remain selected when their key
and source are unchanged; export does not reveal those values. A new key-only
secret literal starts with an empty value. Set or rotate its value through the
Entry action, never by putting plaintext into a Blueprint.
The Console review marks a newly declared key-only secret Entry as empty.

Directly created Attaches also appear in canonical export. A later Blueprint
can keep a ready Attach by declaring the same name, consumer Service, Backing
Service, credential owner and grants. Validate rejects an in-place change to
that binding; detach it separately before replacing it.

Persistent Volume omission is rejected: use its impact-checked Remove action
before applying a Blueprint without it. If a Backing Zone serves external
Attaches, use its impact-approved Remove action before omitting it from the
Blueprint; this Environment's `If-Match` does not approve disruption to those
consumers.

Current Validate rejects omission of existing Services, Zones, Attaches and
Routes because protected removal through that preview path is unavailable.
Use their explicit dependency-checked Remove/Detach actions before exporting
and editing the next Blueprint. Other omission paths can still retain or reject
resources; see [implementation limits](../issues/runtime-qualification.md#incomplete-features).
The Console must not promise removal that the Controller retains or rejects.

Blueprint Entry keys identify their reconciliation identity, separately from
destination env keys or file paths. Source/exposure edits retain the Entry id;
type, destination, file ownership and secret-storage class are not in-place
identity edits. Generated Component configuration is not an operator-authored Entry.

Existing Service runtime intent remains separate: applying a Blueprint must not
restart a deliberately stopped or destroyed Service merely because its definition
is still present.

## Hooks and partial failure

Setup and migration hooks follow [Script rules](setup-scripts.md).
The accepted Apply behavior can create a Custom backing Attach, wait for its
hook to produce facts, then build dependent Entry files and Service work from
those facts in the same Apply. The authored input remains fixed; only facts
produced by that Apply may extend its effective plan. A failed hook leaves its
dependent work pending or failed rather than substituting later values.

This same-Apply path is not implemented yet. Today, create the Custom Attach
separately and wait until it is ready before a Blueprint uses its facts or
credential owner. See [Custom backing hooks](backing-services.md).

Retry/recovery use the captured input, not the latest desired document.
Partial failure must remain visible. Pinned configuration recovery is limited to
files that the failed operation was authorized to replace; it is not database
restoration or migration reversal.
After a child fails and GP proves its effects were restored, the visible Apply
fails. GP does not retry that child automatically; the operator submits a new
Apply after correcting the cause. Unknown effects keep their claims and prevent
premature completion.

## Latest-wins design: not yet available end to end

The accepted design compares resource-level effective inputs and cancels obsolete
work when a newer valid input supersedes it. Conflicting work waits for proven
executor stop and accounted effects; unrelated work may continue.

Only a superseded Blueprint shared-configuration write may hand off forward
repair from accounted-but-diverged effects. Unknown effects remain restricted.
This does not authorize replaying Scripts or superseding ordinary Deploy,
Backup, upgrade or destructive operations.

The selector exists, but unit persistence, late plan preparation, safe handoff
and full runtime integration remain incomplete. Do not depend on automatic
supersession in production.

## Design and qualification

[Desired-state publication](../decisions/desired-state-publication.md) explains
immutable inputs, bounded staging, atomic visibility and the supersession design.
[Capability status](../capabilities.md) and the [QA matrix](../qa-matrix.md) own
implementation limits and behavioral proof.
