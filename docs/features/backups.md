# Backups

Config, Volume and PostgreSQL capture and Restore are available through the
Console, CLI and API. **Full recovery qualification remains incomplete.** Selected
capture, restore, key-era, retention, scheduling and interruption cases passed on
disposable Ubuntu amd64. Other provider, archive, checkpoint, shared/granted Attach
deletion and arm64 variants remain unqualified. See
[current limits](../capabilities.md) and the bounded
[qualification records](../acceptance.md#source-restore-retention-and-deletion-boundary)
before relying on a particular recovery path.

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

## Configure and run

Open **Environment → Backups** to create an S3-compatible Connector, edit the
policy and choose sources. Connector creation stores settings; the first Backup
checks that those credentials can upload and verify an object.

For the CLI, select Tenant, Project and Environment through your configuration
or `--tenant`, `--project` and `--env`. With an existing Connector named
`r2-backups`, PostgreSQL owner Attach named `api-db` and Volume slug
`storefront-data`:

```sh
groundplane backup policy set --connector r2-backups \
  --frequency '*-*-* 03:15:00' --keep 3 --encryption age \
  --source attach:api-db --source volume:storefront-data --source config
groundplane backup policy show --output json
groundplane backup run
```

Replace these labels with your resources. Omit a `--source` you do not need;
repeated flags select capture order. In `--id` mode, supply stable ids for the
Environment, Connector, Attach and Volume instead. Policy Show returns source
ids (`spt_…`), which Restore uses. Run returns a Task id; follow it with
`groundplane task show TASK_ID` or `groundplane task events TASK_ID` before
checking the resulting Points.

`groundplane backup policy set --off` disables a configured policy while retaining
its settings and sources. By contrast, omitting `x-gp-backup` from a Blueprint
disables and unconfigures it. Neither action deletes existing Points.

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

## Capture limits

Config capture includes the complete operator-managed Entry set and exact selected
values, including secret literals, reusable Secret values and Attach facts. It
does not back up reusable Secret resources themselves, Backing Environments,
Controller startup configuration, the host age key, etcd or platform state.
Its bounds are 4,096 Entries, 256 KiB per selected value and 1 GiB of selected
values in total. Blueprint export, in contrast, redacts secret values.

PostgreSQL capture runs release-selected backup tools through a read-only mount
inside an unmodified upstream PostgreSQL 16 Alpine container. The tool selection
is independent of database patches; both identities are verified against the
acknowledged current runtime. It dumps the credential owner's selected database,
not the
whole Backing instance or a filesystem copy of its live database directory.

Volume capture supports regular files and directories on one filesystem, with
at most 2,048 entries including the root and relative paths up to 4,095 bytes.
Symlinks, hard-linked files, special files, nested mounts and extended metadata
such as ACLs or xattrs are rejected. It preserves contents, permissions and
numeric ownership. Capture does not stop consumers: observed changes during
capture fail it. Quiesce application writes when a stable file tree is required;
this is not an application-consistent snapshot of a live database.

Stored objects are bounded to 5 TiB; encryption overhead reduces the allowed
plaintext size. These are format ceilings, not capacity or throughput promises.
Capture/Restore Tasks have a six-hour deadline and remote prune steps have a
30-minute deadline. Staging needs local free space; Volume Restore also needs
room for its replacement tree alongside the existing data.

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

Environment removal cleans up its owned points and proven orphan objects before
retiring Backup scheduling, policy, key and Connector authority. It preserves
terminal Task history. An unproved orphan, active prune or nonterminal Backup or
Restore prevents removal; unavailable remote cleanup keeps the parent fenced.

List verified Points with `groundplane backup points --output json`. Each includes
its Point id (`rp_…`), source id, original target, capture time, size, encryption
and key era. Follow `next_cursor` with `--cursor` to load older records; the API
supplies 50 per page. The Console's **Load more** action extends its
loaded list; sorting applies to that loaded list.

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
  not reread today's Secret or Attach fact values. It republishes desired Entry
  configuration and updates managed files without restarting Services. A running
  process loads the restored environment on its next Deploy.
- Volume restore stops consumers, prepares and verifies a replacement tree on
  the same filesystem, exchanges it with the target and cleans up the old tree
  before restarting previously running consumers.

Every confirmation must identify the exact target and warn about overwrite;
PostgreSQL and Volume restore also require a downtime warning. A restore outcome
does not change its source Recovery Point. An uncertain destructive step cannot
be blindly replayed.

In the Console, choose **Restore** on a verified Point and review its exact
target, capture time and overwrite warning. In the CLI, pass the source id
from Policy Show or Points, and optionally a specific Point:

```sh
groundplane backup restore spt_01J00000000000000000000000 \
  --point rp_01J00000000000000000000000 \
  --idempotency-key storefront-restore-20261004-001
```

Replace the illustrative ids and key. `restore` takes a stable source id even
without `--id`; it does not resolve an Attach name or Volume slug. Omitting
`--point` selects the latest verified Point once. CLI/API callers must review
the target and arrange any needed downtime themselves; the CLI has no interactive
overwrite prompt. Follow the returned Task, then verify actual Entry values,
files or database contents.

The API action is `POST /api/v1/environments/{id}/restore`, with
`Idempotency-Key` and a JSON body containing `source_id`, optional
`recovery_point_id` and optional write-only `age_identity`. It returns HTTP 202
and `task_id`. After a lost response, replay the same key, Environment, source,
Point-selection input and identity; choosing a new key is a new Restore intent.
See [request replay](../api-cli.md#protected-requests-and-uncertain-outcomes).

Task Retry is available only for a terminal failed-safe Restore whose live
mutation has not started and which needs no request-only old-era identity.
Retry retains the original Point, target, credentials, key era and captured
consumer configuration; changed authority can reject it. A Restore that started
mutation or reports unresolved effects is not generically retryable. Preserve
its Task and recovery evidence rather than issuing another overwrite to clear it.

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

Open **Environment → Settings → Encryption key** for **Export identity** and
**Rotate key**. The current CLI key commands require a stable Environment id
in `--env`; they do not resolve its
name. Use `--id` consistently:

```sh
groundplane backup export-key --env env_01J00000000000000000000000 --id \
  --file ./production-era-1.age
groundplane backup rotate-key --env env_01J00000000000000000000000 --id
```

Replace the Environment id. Export writes a private mode-`0600` file and can
overwrite an existing regular file; choose a separate filename for each era
and keep it securely off-host. The default `--file -` writes plaintext to stdout;
global `--output` is rejected. Rotation returns a Task: wait for completion,
then export the new era separately.

To restore an older era through the CLI, add `--age-identity PATH` to Restore
and retain the original request's replay key. The file contains the identity
exported for that Point's era. It is supplied for that attempt only and cannot
be reconstructed after Controller memory is lost.

## Design and qualification

[Backup recovery decisions](../decisions/backup-recovery.md) explain why target
validation, remote ownership, operation deadlines and non-replayable effects are
separate.
The [QA matrix](../qa-matrix.md) preserves required failure cases; none of this
design text substitutes for their runtime evidence.
