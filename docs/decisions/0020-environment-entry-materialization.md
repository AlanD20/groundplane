# ADR 0020: Constrain environment Entry materialization

- Status: Accepted
- Date: 2026-08-20
- Accepted: 2026-08-22

## Context

The Controller renders deterministic generated environment files and resolves
plain and secret file Entries. The MVP requires the Agent to publish those
files below the selected environment's stable `environment.volume_dir` without
placing plaintext in desired state, a durable Task, a log, or Compose YAML.

Accepted ADR 0016 deliberately gives the persistent Agent container no host
root, generic host-runtime mount, or workload-materialization root. It mounts
the Docker socket so the Agent can execute Controller-issued workload
procedures, but that socket is itself root-equivalent authority over the host.
A task-scoped helper can narrow accidental filesystem exposure and make the
write path mechanically auditable; it cannot contain a malicious or compromised
Agent that can issue arbitrary Docker API calls.

The filesystem boundary must also close several independent concerns:

- a canonical-looking `/var/lib/groundplane/vol/<tenant-id>/<project-id>/<environment-id>`
  string is not authorization to mutate that environment;
- a helper bind mounted at `/run/groundplane/materialize` cannot safely call a
  writer that opens `/` and traverses the original host path;
- `os.Root` prevents `..` and escaping symlinks, but does not reject nested
  mount points, procfs magic links, devices, or every same-identity substitution
  race on Linux;
- process-local mutexes do not serialize separate helper containers;
- generated env files, non-secret file Entries, and secret file Entries need
  exact and reproducible ownership and mode rules; and
- best-effort temporary cleanup is insufficient when the temporary contains
  plaintext.

The Controller decides the destination, bytes, metadata, and order. The Agent
validates and executes that closed plan. The helper performs only one
descriptor-relative file replacement.

## Existing constraints

Any accepted decision must preserve these contracts:

- every output remains below the one environment volume authorized by the
  durable render plan;
- the all-services env-file name is Environment-id-based, service-specific
  files append the stable service name, and their bytes are deterministic;
- a file Entry path is canonical and relative to the environment volume;
- the persistent Agent container receives no host root or persistent workload
  volume mount;
- the Controller owns the persistent Agent container's complete lifecycle;
- secret outputs are regular files at mode `0600`;
- plaintext is never logged, placed in arguments or environment variables,
  persisted in a Task or event, or retained after its final consumer; and
- there is no compatibility writer, arbitrary bind path, or runtime fallback.

## Recommended contract

### 1. Authorize one materialization from durable state

The durable render plan contains, for every materialization step:

- `task_id`, immutable `step_id`, stable `environment_id`, and monotonic
  `render_generation`;
- the exact `environment.volume_dir` derived from the durable tenant, project,
  and environment ids;
- one canonical volume-relative destination;
- for a service-specific generated env file, the stable `service_id` and exact
  Compose service name that the sealed artifact maps to that id;
- `output_kind`, exactly one of `generated_env`, `plain_file`, or
  `secret_file`;
- exact numeric `uid`, `gid`, and mode;
- exact plaintext length and SHA-256 digest; and
- durable references needed by the Controller to reproduce the bytes, never
  the bytes themselves.

Those references live in the durable Task beside their `step_id` and
`environment_id`, never in string params or the Agent protobuf. They form one
closed tagged union:

- `blueprint_file` names an immutable Blueprint `revision_id` and normalized
  bundle path;
- `entry_value` names an Entry id, immutable `cfg_...` value generation, and
  explicit `plain` or `secret` storage; and
- `generated_environment` names format version `1` and a uniquely sorted list
  of environment keys, each bound to one typed Entry value generation.

The Task codec rejects an unknown kind, multiple populated union members,
unknown or duplicate step binding, cross-Environment ownership, an unversioned
generated format, unsorted keys, and invalid stable ids. Retries deep-copy the
same references. The Agent sees only the independently authenticated output
metadata, length, and digest; it never learns Controller storage topology.

Immediately before dispatch, the Controller rereads the environment and render
plan at the recorded revision. It rejects stale ownership, a mismatched derived
volume directory, or a superseded generation before resolving any plaintext.
The Agent independently checks that the assignment metadata matches the signed
plan projection it received. A syntactically valid stable id or path is never
sufficient authorization.

The Controller permits at most one active materialization writer for an
environment. The active durable Task index records the generation and owning
task. A newer generation waits for the previous writer to reach a terminal
state; an older or duplicate assignment may run only when its `task_id`,
`step_id`, generation, destination, length, and digest all match the active
record. A retry of the same step therefore writes the same bytes to the same
path. A process-local mutex is neither required nor considered ordering.

Compose-only Attach and detach Tasks use the same durable Environment writer
index with a distinct `mutation_environment_id` parameter. This serializes
their pinned network reconciliation against materialization without declaring
or authorizing file-value references.

### 2. Execute through one narrow helper runner

The Agent owns a `MaterializerRunner` port with one operation: create one
helper, stream one request through its stdin, wait for its terminal result, and
remove it. The Docker implementation is separate from the persistent Agent
container lifecycle and cannot create or replace that container.

For each invocation the runner creates the helper from the exact
Controller-approved Agent image digest with:

- user `0`, restart policy `no`, network mode `none`, a read-only root
  filesystem, no Docker socket, and no additional devices or mounts;
- all Linux capabilities dropped except `CHOWN` and `FOWNER`, which are needed
  to publish a file with the declared numeric identity;
- `no-new-privileges`, the default seccomp policy, and no environment variable,
  label, or argument containing request data; and
- exactly one read-write bind: the authorized host `environment.volume_dir` at
  `/run/groundplane/materialize`.

The helper command is a closed materializer entrypoint in the Agent image. It
does not invoke a shell or accept a host path. Container creation, stdin attach,
start, wait, and forced removal are one runner operation. Once creation returns
a container id, every error and cancellation path attempts terminal helper
cleanup and explicit removal. The runner reports success only after a zero exit
status and successful removal; cleanup failures are Internal failures.

### 3. Transfer plaintext only through a transient framed stream

The durable Task and execution plan store metadata, value references, length,
and digest only. Immediately before dispatch the Controller resolves values
into owned byte buffers, renders the exact output, and streams a transient
materialization message over the authenticated Agent channel. It never converts
secret material to immutable Go strings.

The transient message is keyed by `task_id` and `step_id`. It sends the closed
non-secret header first, followed by exactly the declared number of content
bytes in bounded chunks. The header repeats the authorized environment id,
generation, relative destination, output kind, uid, gid, mode, length, and
SHA-256 digest. Unknown fields, duplicate headers, extra bytes, early EOF,
out-of-order chunks, or a digest mismatch reject the step.

The Agent writes the same closed header and content framing to the helper's
private stdin pipe. The Agent hashes the stream while forwarding it; the helper
hashes the temporary file while writing it. Both must observe the declared
length and digest. The helper never renames a file before its independent check
succeeds.

Every Controller, protobuf, Agent, runner, and helper byte buffer is owned by
exactly one stage and cleared immediately after that stage forwards or consumes
it. Cancellation and error paths clear buffers too. Stream chunks, content, and
derived plaintext never enter Task events, structured error details, logs,
argv, environment variables, labels, image layers, or durable storage.

### 4. Root the writer at the fixed helper mount

The helper opens `/run/groundplane/materialize` once and treats that descriptor
as its only filesystem root. Its writer accepts only the authorized relative
destination and metadata. It never receives, reconstructs, opens, or logs the
host `environment.volume_dir`.

Linux path resolution is descriptor-relative. Every existing destination
ancestor is opened beneath the fixed root with no symlink, magic-link, or mount
transition allowed. The implementation uses `openat2` resolution equivalent to
`RESOLVE_BENEATH | RESOLVE_NO_SYMLINKS | RESOLVE_NO_MAGICLINKS |
RESOLVE_NO_XDEV`; unsupported kernels fail closed. Missing parent directories
are created one component at a time as `uid: 0`, `gid: 0`, mode `0700`, opened
with the same policy, verified, and their parents synced. An existing ancestor
with a different owner, group/other write bit, symlink, nested mount, or
non-directory type is an Internal reconciliation failure.

The final destination must be absent or a regular file with the exact declared
uid, gid, and mode. Devices, FIFOs, sockets, directories, symlinks, hard-linked
files, and mismatched metadata fail closed. The reserved temporary prefix is
`.groundplane-materialize-`; an Entry destination may not use it.

The helper creates one unpredictable, exclusive temporary in the destination
directory as `0:0` mode `0600`, writes while hashing, syncs it, applies the
declared uid and gid with `fchown`, reapplies the exact mode, and verifies type,
link count, owner, group, mode, length, and digest through the open descriptor.
It then atomically renames the temporary over the destination and syncs the
destination directory.

Cancellation before rename closes and unlinks the temporary, syncs the
directory when an entry was removed, and leaves the old destination unchanged.
Cancellation concurrent with or after rename cannot interrupt the final
directory sync; the helper completes durability and then reports cancellation.
An unlink, close, metadata, rename, or sync failure is never discarded when it
can leave plaintext or an ambiguous publication result.

Before each write, the helper reconciles orphaned files using the reserved
prefix in the selected destination directory. Because durable ordering permits
only one writer for the environment, no live helper can own such a file.
Regular single-link orphan files are unlinked descriptor-relatively and the
directory is synced. An orphan with an unexpected type or link count fails
closed for operator inspection. This reconciliation covers a helper killed
after creating a temporary but before normal cleanup.

### 5. Fix ownership and mode by output kind

The render plan and helper enforce this closed table:

| Output kind | Destination | Required uid:gid | Required mode |
| --- | --- | --- | --- |
| `generated_env` | Controller-generated `secrets/.env.<environment-id>` or `secrets/.env.<environment-id>.<service-name>` | `0:0` | `0600` |
| `plain_file` | the file Entry's declared volume-relative path | the Entry's explicit numeric `uid:gid` | `0444` |
| `secret_file` | the file Entry's declared volume-relative path | the Entry's explicit numeric `uid:gid` | `0600` |

Every file Entry carries explicit numeric `uid` and `gid` desired state. The
Console may prefill `0:0`, but the submitted request and Blueprint must contain
the accepted values; there is no inferred image user, container inspection,
filesystem-owner fallback, or silent default. One Entry may target multiple
services only when the same declared identity is valid for all of them.

Generated env files are consumed by the privileged Compose apply path. Plain
files use Compose config-style read-only visibility and mode `0444`. Secret
files retain the locked `0600` contract. Every workload mount of a final file is
read-only; host parent directories remain root-owned `0700`, so workload users
do not receive host-directory traversal rights.

### 6. Separate user validation from invariant failures

The Controller API validates Entry paths, uid/gid values, output shape, and
referential ownership before state mutation or Agent dispatch. Invalid operator
input is `validation.failed` with HTTP 422.

After dispatch, a path, ownership, generation, digest, mode, or plan mismatch
means the Controller emitted or the Agent received an impossible procedure. The
Agent and helper classify it as Internal, with no plaintext or host path in the
public detail. Filesystem substitution, unsafe metadata, cleanup failure, and
durability failure are also Internal reconciliation failures. Task cancellation
remains cancellation/aborted rather than validation failure.

### 7. Remove only the pinned applied materialization

Protected Entry deletion publishes the tombstone, immutable removal intent,
Task, queue/index records, and replay marker atomically. The Entry remains
readable while that Task is pending or running. If the Entry is present in the
current applied Environment projection, the Task is Agent-owned with a
120-second deadline. If it has never been applied, the Task is Controller-owned
with a 30-second deadline and performs no host mutation.

The removal intent pins the Entry revision, selected immutable value
generation, output metadata, and current applied artifact needed to reproduce
cleanup after Controller restart. A file Entry removes only its exact
volume-relative plain or secret destination through the same constrained
helper boundary. An env Entry rewrites each affected generated file from the
remaining pinned Entry generations. The canonical all-services file is always
published, including as an empty file; a service-specific file is removed when
no remaining Entry contributes a value.

Entry removal has no Compose step and does not restart or recreate a Service.
Environment values already loaded by a running process remain until a later
deploy or reconciliation. For head-backed Entries, the existing desired
publisher seals the candidate and publishes the cleanup Task without advancing
the visible head. The intent binds that candidate and the independent applied
cleanup snapshot. Successful acknowledgement atomically advances the desired
head, removes the Entry owner index and all subordinate generations, updates
the applicable applied snapshot, and releases the tombstone and Environment
ownership while terminalizing the Task. The terminal intent remains bound to
that Task for acknowledgement replay and ordinary retention pruning.
Failure, timeout, or abort retains
the Entry and current applied projection, releases removal ownership, and
allows a protected retry. Component credential reverse references belong to
reusable Secret deletion and do not participate in Entry removal.

The unpublished cleanup candidate remains sealed while its original Task is
retained, including after response expiry. Task publication advances the staging
descriptor in the same transaction, so the existing descriptor CAS fences a
publication racing with the collector's fixed-revision Task-absence read. No
staging transaction ceiling changes. The never-applied Controller finalizer
reserves the existing Environment materialization writer at publication and
releases only its own writer at terminal acknowledgement; queued Agent work
cannot invalidate the no-host-cleanup decision in between.

## Threat boundary

The fixed helper root, closed metadata, descriptor-relative traversal, and
short-lived mount protect against programming errors, confused-deputy path
selection, stale plans, unsafe existing filesystem state, and accidental access
to sibling environments. They reduce how much filesystem is ambiently visible
to an ordinary materialization process.

They do not sandbox a compromised persistent Agent from the host. The Agent's
read-write Docker socket is root-equivalent and can create a different container
with arbitrary mounts. Isolation from a malicious Agent requires a future
Docker authorization proxy, a narrower host execution service, or removal of
the raw socket; that is outside this MVP proposal. No security claim or test may
describe the helper as containment from an Agent-process compromise.

## Rejected alternatives

### Write directly from the persistent Agent container

The accepted mount allowlist makes the host environment volume unavailable.
Adding a persistent workload root would broaden accidental exposure for the
container's full lifetime and silently reverse ADR 0016.

### Pass the host volume path into the helper

A helper rooted at `/` can write its container layer or another valid-looking
environment path. The exact bind is the capability; the helper sees only the
fixed mount and a relative destination.

### Treat `os.Root` as the complete Linux policy

`os.Root` is useful but permits mount traversal and is not an exact no-symlink,
no-magic-link, no-cross-filesystem contract. The helper needs the stricter
descriptor-relative Linux policy above.

### Rely on an in-process mutex

Separate helper containers do not share process memory, and a mutex wait is not
task-context cancellation. Durable per-environment generation ordering owns
concurrency.

### Infer file ownership from a service image

Image users may be named, overridden, or absent, and inspection is observed
state rather than reproducible desired input. Numeric uid and gid are explicit
Entry decisions.

### Ignore cleanup errors to preserve an earlier failure

The earlier error does not erase a plaintext temporary. Cleanup and durability
failures take precedence and remain visible as Internal task failures.

## Accepted owner decisions

The owner accepted every value below on 2026-08-22:

1. One task-scoped helper from the pinned Agent image, with the exact container
   restrictions and narrow create/stdin/wait/remove runner described above.
2. The explicit threat model: the helper constrains accidental and confused-
   deputy access but does not contain a compromised Agent holding the Docker
   socket.
3. Durable per-environment and render-generation writer ordering, with exact
   retry identity over task, step, destination, length, and digest.
4. The transient Controller-to-Agent-to-helper framed byte protocol, independent
   digest verification, and single-owner buffer clearing rules.
5. The Linux-only descriptor policy, including fail-closed unsupported kernels,
   no symlinks, no magic links, and no nested mount transition.
6. Parent directories as `0:0` mode `0700`, generated env files as `0:0` mode
   `0600`, plain file Entries as explicit uid/gid mode `0444`, and secret file
   Entries as explicit uid/gid mode `0600`, with no implicit uid/gid default.
7. Reserved-prefix orphan reconciliation and cleanup/durability failures taking
   precedence over earlier operation or cancellation results.
8. The error ownership split: Controller request defects are HTTP 422;
   Agent/helper plan or filesystem invariant failures are Internal.

## Implementation effects

Accepted ADRs 0047 and 0048 narrow this ADR's Backup plan/transfer boundary and
fix Config restore as two-pass validation followed by read-hidden bounded
roll-forward of the canonical Entry primaries. They do not replace this ADR's
destination, ownership, materialization, secrecy, or helper confinement
invariants, and no Environment-wide active-generation compatibility reader is
introduced.

Implementation requires synchronized replacement changes to `mvp.md`,
`blueprint.md`, `architecture.md`, `api-cli.md`, the Entry Console/store/API/CLI
contract, the typed Agent protobuf payload, durable Task ordering, the Agent
worker, the Docker helper runner, and materializer tests. The current direct
host-root writer and public fail-closed stubs are replaced rather than retained
as compatibility paths.

The framed protocol, descriptor-rooted Linux writer, constrained Docker runner,
Agent helper entrypoint, typed protobuf payload, verified transient Agent inbox,
metadata-only plan step, Controller transient sender, and worker-to-helper
streaming runtime have landed. Claim-time durable per-Environment writer
acquisition, fixed-revision queue scanning, reconnect validation, and exact
terminal release have also landed. The durable Task value-reference union,
codec, retry cloning, and immutable plain/encrypted Entry value-generation
repositories have landed. The production Controller resolver now consumes the
closed union, decrypts only inside the Protector callback, renders generated
env files in clearing byte buffers, and verifies final length and digest.
Protected create, edit, and direct removal now integrate those generations with
atomic Entry state, replay evidence, exact restart-reproducible task plans, and
terminal finalization. Blueprint omission reconciliation remains separate work.
No caller may add a broad mount, dispatch plaintext through the generic step
map, or infer ownership.
