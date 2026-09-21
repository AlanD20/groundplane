# Volumes and Entries

Volumes provide persistent Environment-owned storage. Entries provide explicit
configuration values as environment variables or files. Reusable Project and
Platform Secrets are separate resources referenced by Entries.

## Volumes

A Volume has a stable ID, a renamable slug and an immutable Compose key. Mounts and
Backup sources retain the ID; the managed host directory retains the key.
Paths are relative to GP's managed storage, not arbitrary host paths. GP rejects
escapes, unsafe links and ownership mismatches.

Removing a Service does not remove its Volumes. Volume removal is an explicit,
impact-checked operation: review mounts, Backup effects and active operations.
The complete preview supplies the removal token; confirm the immutable key, not
the slug. Changed impact invalidates confirmation, and there is no force shortcut.
The operation publishes its replacement desired state and proves consumers detached
before destroying data. Cleanup must succeed before ownership disappears. A failed or
interrupted removal keeps its identity and recovery information; Retry does not
repeat a physical deletion when its exact directory absence has already been
proved.

## Entries

An Entry can use a literal value, a reusable Secret or an Attach fact. Exposure is
explicit: it applies to all Services or the selected Services. GP does not inject
every backing-service credential automatically.

Entry type, destination, secret storage class and file ownership cannot change
through edit. Source and exposure can change. File Entries require numeric `uid`
and `gid`, including explicit zero; environment-variable Entries have neither.
Generated environment files and secret files use mode `0600`; plain file Entries
use read-only mode `0444`.
See the [Blueprint reference](../blueprint.md) for exact input fields.

A plain literal remains visible. Secret literals are encrypted; ordinary reads
do not reveal plaintext. Desired references can resolve to newer values for a
new operation, but an accepted Task pins exact value generations for its own
execution and Retry. A later edit must not change an in-flight operation.

## Applying changes

Entry create, edit and bulk-upsert capture the acknowledged running workloads
affected by the changed exposure. They materialize configuration and select only
those workload instances, without restarting unrelated Services or stable
proxies. A stopped or absent Service is not started merely because its previous
Release remains in history. With no selected running workload, the operation only
materializes files.

Entry removal is different: it updates or removes the pinned configuration files
without running Compose or restarting a Service. A running process keeps its
already-loaded environment until its next deployment or reconciliation. The
shared environment file is rewritten even when empty; an empty Service-specific
file is removed. Removing a never-applied Entry has no host effect.

On failure, timeout or Abort, Entry metadata and generations remain available for
retry. Successful cleanup removes them together. Entry deletion is not reusable
Secret deletion; the latter also protects Component references and values pinned
by recoverable Tasks.

## Safety and design

File operations use bounded, root-relative paths and exact ownership. A lost
acknowledgement does not authorize another destructive operation. Metadata-only
changes do not acknowledge unrelated file or runtime state.

[Configuration materialization](../decisions/configuration-materialization-and-scripts.md)
explains why execution pins values and workload identity.
[Resource deletion](../decisions/resource-deletion.md) explains cleanup before
publication. Current qualification limits are in [capabilities](../capabilities.md);
behavioral cases remain in the [QA matrix](../qa-matrix.md).
