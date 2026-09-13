# Volumes and Entries

## Purpose and scope

Give an Environment persistent host storage and explicit configuration inputs
without putting generated paths or secret plaintext into desired state. Volumes
own storage roots; Entries expose a value as an environment variable or file.
Reusable Project and Platform Secrets are a separate resource.

[mvp.md](../mvp.md) owns behavior, [blueprint.md](../blueprint.md) owns authored
syntax, and [api-cli.md](../api-cli.md) owns the public operations.

## Functional requirements

- A Volume has a stable identity independent of its renamable label. Its managed
  root and directory lifecycle are Groundplane-owned. A desired path cannot
  escape that authority or select arbitrary host storage.
- Volume removal is an explicit impact-checked workflow, not an incidental
  consequence of removing a Service. Host cleanup and successful terminal
  publication must agree before desired ownership disappears.
- Entry metadata binds one immutable current value generation. A plain literal
  remains visible; a secret literal stores no plaintext in the primary. Fact and
  reusable-Secret sources remain live desired references while an execution
  captures exact resolved bytes for retry and restart.
- Entry type, destination, secret storage class and file ownership are immutable
  under edit. Source and exposure may change. Every file Entry has explicit
  numeric `uid` and `gid`, including zero; an env Entry has neither.
- Entry removal keeps metadata and generations on failure, Abort or timeout.
  Successful terminal publication deletes the Entry, indexes and its owned
  generations together. Never-applied removal has no host effect.
- Direct desired mutations and complete Blueprint application share the same
  Environment revision authority. Retry never silently selects later values or
  a different serving workload.

## Non-functional requirements

Use canonical root-relative paths, exact owner checks and bounded filesystem
operations. Secret values are encrypted durably and exposed only through explicit
authorized reveal or transient materialization. Protected replay stores literal
digests, not secret bytes. A lost acknowledgement is not permission to repeat a
destructive filesystem operation without its retained authority.

## Technical design

| Concern | Current technical contract |
| --- | --- |
| Value generations, destinations and Agent materialization | [Entry materialization](../decisions/0020-environment-entry-materialization.md) |
| Entry primaries, replay, deletion and API/YAML conversion | [Entry records](../decisions/0029-environment-entry-durable-record.md) |
| Managed roots, directories and permissions | [Volume filesystem lifecycle](../decisions/0025-environment-volume-root-and-directory-lifecycle.md) |
| Exact identity, removal impact, runtime state and recovery | [Volume removal](../decisions/0049-volume-identity-and-removal.md) |
| Shared desired-revision publication | [Direct desired changes](../decisions/0058-revision-authoritative-direct-desired-mutations.md) and [Blueprint publication](../decisions/0051-blueprint-staged-revision-publication.md) |

### One Volume-removal record format

`internal/infra/volumeremovalrecord` owns validated record values, canonical binary
encoding, digests, physical keys and size limits. It imports no etcd client,
repository, Task model, Agent protocol or process capability. Both the desired
publisher and `internal/infra/etcd/volumeremoval` consume it without a cycle.
It is a narrowly named format package, not a shared-record framework.

The runtime repository owns checkpoint transitions, reads, assignment validation
and Retry. The desired publisher owns the single desired/operation/Task
publication. The format package cannot perform either operation or supply
arbitrary store mutations or callbacks. Its replay locator is a plain value;
the runtime adapter converts it to the existing idempotency API, whose owner
retains lookup, replay and retention. Record consumers share one unchanged binary
format and key contract without aliases or alternate decoders.

### Entry changes capture serving runtime

Create, edit and bulk-upsert capture each serving native Release at the desired
read's fixed revision through the lifecycle capture/renderer. Each retained
workload keeps its exact Release, image, proxy and ownership metadata. Remove
historical Entry decorations before attaching current generations so a deleted
file cannot return from history. The mixed-runtime merger retains persistent
resource authority.

Before merging, remove a predecessor stable proxy's generated config only after
proving its canonical name, sealed content and digest, sole proxy binding and
absence of another Service reference. The captured proxy supplies current config;
authored/shared config and Network/Volume guards are unchanged.

The immutable desired candidate stores the capture. Reconstruction uses that
runtime and the baseline revision's prior Entry decorations, never a later live
slot; the existing plan hash seals the pair. Execution selects only exposed
workload instances, without dependency or stable-proxy restarts. Entry DELETE
remains materialization-only and does not use this capture.

When an Entry update has only materialization steps, terminal publication records
the materialized generations without replacing the independently applied Compose
artifact. Pending desired Volumes must not become applied through an Entry edit.

`entry_runtime_epoch_revision` records the captured Environment mutation epoch.
It must equal the direct publisher's epoch, which final commit also compares.
Ordinary Release publication and terminalization advance that epoch and planning
excludes active Release/Environment operations. Drift before publication or at
commit rejects the capture. This adds no second publisher, per-Service
transaction, storage namespace or higher record/transaction limit. Blueprint
domain fragments remain forbidden on direct mutations. An old unstarted plan
without capture authority is not rewritten; a fresh operation must capture it.

## Acceptance

Prove path and ownership rejection, explicit file permissions, exact generation
replay, atomic primary/value publication and deletion, failed-removal retention,
impact guards, bounded cleanup and restart recovery. Pin Volume record bytes and
key/replay admission; prove the combined publisher uses that same format within
the complete transaction budget.

For Entry capture, model a published native Blueprint followed by Deploy/Rollback
with real immutable codecs. Prove serving selection, reconstruction and rejection
of both pre-publication and commit-time epoch drift; replay must be read-only.
Hermetic storage proof and real Docker/CLI proof remain distinct.

## Current status

Lifecycle and desired-revision integration have recorded bounded qualification.
The Entry serving-capture regressions proved selection, reconstruction and both
epoch fences. Source-specific persistence and restore remain part of production
qualification. See [capabilities.md](../capabilities.md), [acceptance.md](../acceptance.md)
and the open storage/recovery issues; no fresh runtime proof is claimed here.
