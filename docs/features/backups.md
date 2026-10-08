# Backups

Config, Volume, PostgreSQL and MySQL capture and Restore are implemented through the
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

The accepted source kinds are a credential-owning PostgreSQL 16 or MySQL 8.4 Attach, the
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
  --frequency '15 3 * * *' --keep 3 --encryption age \
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

Enabling requires a valid five-field UTC cron frequency, a
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

The Console offers Hourly, Daily, Weekly and Custom cron controls, a readable
summary and the next three scheduled UTC times. Blueprint/API/CLI use the same
cron expression; the controls do not introduce a second stored format. For example,
`15 * * * *` runs hourly at minute 15 and `15 3 * * SUN` runs Sundays at 03:15.
See [the frequency grammar](../blueprint.md#x-gp-backup) for supported fields.

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

MySQL uses the upstream 8.4 container's `mysqldump` and `mysql` tools. Capture
selects one credential owner's database, excluding accounts and grants. MySQL
Restore is not transactional across all schema changes: interrupted execution
requires proof from the original execution, never a blind second Restore.
MySQL data and interruption qualification have not yet been recorded.

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
does not delete points.

Delete one verified Point explicitly with its stable id:

```sh
groundplane backup remove-point rp_01J00000000000000000000000 \
  --idempotency-key storefront-point-delete-20261006-001
```

The command returns a Task to follow. It permanently removes that recovery
artifact; it does not change the original target, current policy or other Points.
Manual selection launches the same system-owned cleanup Task used by retention.
Deletion is blocked while an Environment Backup, Restore, retention prune or key
rotation is active. The Task removes only the exact object captured by that Point.

The API action is the bodyless
`DELETE /api/v1/environments/{id}/recovery-points/{point}`. It requires
`Idempotency-Key` and returns HTTP 202 `TaskAccepted` with `task_id`. After an
uncertain response, replay the same key, Environment and Point rather than
starting a new deletion intent.

Manual deletion and retention cleanup must prove absence of the exact selected
object before releasing its Point record and Connector references. A remote
cleanup failure retains the Point, locator, references and operation authority.
The cleanup scheduler retries that same selection within its bounded attempt
limit; ordinary Task Retry and Abort are unavailable for prune Tasks. A selected
Point is no longer offered for Restore while cleanup is pending. Bucket listing
is not ownership proof.

Environment removal cleans up its owned points and proven orphan objects before
retiring Backup scheduling, policy, key and Connector authority. It preserves
terminal Task history. An unproved orphan, active prune or nonterminal Backup or
Restore prevents removal; unavailable remote cleanup keeps the parent fenced.

List verified Points with `groundplane backup points --output json`. Each includes
its Point id (`rp_…`), source id, original target, capture time, size, encryption
and key era, plus the captured Connector id, endpoint, bucket and prefix. Those
storage fields belong to that Point, not today's policy. Changing a policy's
Connector does not move existing backups or change their Restore destination.
Follow `next_cursor` with `--cursor` to load older records; the API
supplies 50 per page. The Console's **Load more** action extends its
loaded list; sorting applies to that loaded list. While the Backup page is visible,
it refreshes automatically to reflect completed runs and retention cleanup.
With `keep: 2`, another successful run creates a new Point but the retained count
returns to two per source after the oldest is pruned.

The Console groups Points by their recorded Backup Task. Expand a Backup to see
its databases, Volumes and configuration, with **Restore** and **Delete** on each
source. The available-source count is not a claim that the whole run succeeded:
failed captures, retention and manual deletion can leave only some sources.
Run identity is retained with the Points, independently of Task history. Points
without recorded provenance appear under **Earlier recovery points**; GP never
guesses a run from timestamps.

**Delete backup** confirms the remaining loaded sources, then deletes their
archives one at a time through the same protected Point action. Each deletion
has a linked Task. A failure stops the queue; closing stops further submissions,
but an already accepted Task continues. This is not an atomic multi-source
deletion. Load the rest of a group before deleting it. Grouping does not change
per-source retention or introduce a combined Restore.

API and CLI Point listings include optional `capture` metadata: the producing
`task_id`, run `created_at` and original `source_count`. Its absence means that
provenance was not recorded, not that the Point is unusable.

Database Points also include `database` metadata: family, observed source server
version, backup-tool version and artifact format. These values are captured
from the attested source container and authenticated with the archive; an image
tag is not version evidence.

## Restore and downtime

Restore overwrites the point's original surviving target. Omission selects the
latest verified point once; Retry cannot silently select another. Removing a
source from the current policy does not invalidate its surviving target's points.

The complete object must be downloaded, authenticated, decrypted when required
and fully format-validated before live mutation.

- PostgreSQL restore stops consumers, restores the selected database within its
  destructive transaction boundary, verifies it and restarts previously running
  consumers. It does not mutate other databases.
- MySQL restore stops consumers, restores only the selected database, verifies
  it and resumes previously running consumers. It does not recreate credentials
  or overwrite another consumer's database.
- Config restore replaces the complete captured Entry set and values. It does
  not reread today's Secret or Attach fact values. It republishes desired Entry
  configuration and updates managed files without restarting Services. A running
  process loads the restored environment on its next Deploy.
- Volume restore stops consumers, prepares and verifies a replacement tree on
  the same filesystem, exchanges it with the target and cleans up the old tree
  before restarting previously running consumers.

Every confirmation must identify the exact target and warn about overwrite;
Database and Volume restore also require a downtime warning. A restore outcome
does not change its source Recovery Point. An uncertain destructive step cannot
be blindly replayed.

Database Restore first reads the target server and restore-tool versions from
the exact live container. The Console displays that review separately from the
overwrite warning. Version differences are unverified compatibility: operators
must explicitly accept that risk. Unsupported server/tool families and failed
integrity or ownership checks remain blocked regardless of acknowledgement.

Use `groundplane backup preview-restore SOURCE --point POINT` to obtain the
exact Point and `database.review_sha256`. Pass that digest to Restore with
`--version-review DIGEST`; add `--acknowledge-version-difference` only after
reviewing a reported difference. The API preview is
`POST /api/v1/environments/{id}/restore/preview`; Restore accepts
`version_review_sha256` and `acknowledge_version_difference`. A changed Point,
Attach, runtime, container or observed version requires a new review. The Agent
checks the sealed target again before effects. Acknowledgement does not prove
that the restored application will work, nor authorize a database major upgrade.

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
**Rotate key**. Settings loads the current key metadata from Backup Policy and
updates the recipient and era after rotation. Follow the linked Task to inspect
completion or failure. The current CLI key commands require a stable Environment id
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
