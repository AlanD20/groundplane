# Backups

**Not operationally complete. Backup/Restore work is deferred.** Policy and
Recovery Point scaffolding is not a qualified backup system. The behavior below
is the accepted target, not an instruction to rely on current GP for data recovery.

## Scope

One tenant Environment owns one Backup Policy and selects an S3-compatible
[Connector](secrets-and-connectors.md) from that same Environment. Backing
Environments cannot own a policy or backup key.

The accepted source kinds are a consumer-owned PostgreSQL 16 Attach, the
Environment's complete Entry configuration and selected values, or one owned
Volume. A policy holds at most 12 sources in explicit order. A run is fail-fast,
with a separate consistency boundary per source—not a cross-source snapshot.

Valkey lacks an accepted isolated artifact and restore contract. Valkey, Custom
Attach and undefined source kinds are rejected with `strategy.not_implemented`;
a live data-directory archive does not fill this gap. Restoring to another
Environment or recreating a missing target is outside the contract.

## Policy and scheduling

Enabling requires a valid whole-second daily or weekly UTC frequency, a
same-Environment Connector, at least one source and a positive retention count
(`keep`, at most 9007199254740991). Encryption is `age` or `none`; Config sources
require `age`. A disabled policy may be unconfigured. Replacing the policy is a
complete replacement, not a merge. See [Blueprint syntax](../blueprint.md).

Sources have stable identities independent of list position or target label.
Removing and re-adding a surviving target reuses its identity while references
remain. Duplicate sources are invalid; Config appears at most once.

The Controller owns scheduling, not host cron. After downtime it considers only
the latest missed occurrence. An overlapping Environment operation records a
skipped occurrence instead of queueing it. Repeated ticks and backward clock
movement must not duplicate runs.

Manual run requires an enabled policy and uses its entire stored source list.
An accepted run pins the policy, sources, credentials' authority, formats and key
era. Eligible Retry uses those inputs, not the latest policy. Already verified
sources are not uploaded again; a retention failure resumes retention only.

## Recovery Points and retention

A point becomes visible only after immutable upload and verification. Earlier
verified sources remain available if a later source fails. Provider errors and
private object locators are not public recovery evidence.

Retention keeps the newest verified points per source. Lowering `keep` takes
effect after its next successful backup. Disabling a policy or removing a source
does not delete points. There is no ordinary operator point-delete action.

Remote cleanup must prove absence of the exact selected object before releasing
its records and Connector references. Uncertain cleanup retains the credentials
and ownership needed to retry. Bucket listing is not ownership proof.

## Restore and downtime

Restore overwrites the point's original surviving target. Omission selects the
latest verified point once; Retry cannot silently select another. Removing a
source from the current policy does not invalidate its surviving target's points.

The complete object must be downloaded, authenticated, decrypted when required
and fully format-validated before live mutation.

- PostgreSQL restore stops consumers, restores the selected database within its
  destructive transaction boundary, verifies it and restarts previously running
  consumers. It does not mutate other databases.
- Config restore replaces the complete captured Entry set and values. It does
  not reread today's Secret or Attach fact values.
- Volume restore stops consumers, prepares and verifies a replacement tree on
  the same filesystem, exchanges it with the target and cleans up the old tree
  before restarting previously running consumers.

Every confirmation must identify the exact target and warn about overwrite;
PostgreSQL and Volume restore also require a downtime warning. A restore outcome
does not change its source Recovery Point. An uncertain destructive step cannot
be blindly replayed.

## Encryption keys

First enabling `age` creates the Environment's first key era. Export downloads
the current identity explicitly; it is not an ordinary metadata read. Rotation
affects future points and **does not retain the old private identity**. Export
and securely store an era's identity before rotating if its points must remain
restorable.

An old-era restore needs the matching identity supplied again. GP does not persist
that request-only value, so this request is not generically retryable. A new
attempt requires a new request with the identity. The input is one UTF-8 identity
line of at most 4 KiB.

## Design and qualification

[Backup recovery decisions](../decisions/backup-recovery.md) explain why target
validation, remote ownership, operation deadlines and non-replayable effects are
separate. That document routes the single retained unimplemented wire reference.
The [QA matrix](../qa-matrix.md) preserves required failure cases; none of this
design text substitutes for their runtime evidence.
