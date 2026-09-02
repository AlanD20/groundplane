# ADR 0046: Backup policy persistence and source identity

- Status: Accepted
- Date: 2026-08-23

## Context

The product contract fixes one Backup Policy per tenant Environment, selectable
sources, an Environment-owned Connector, lazy per-Environment age keys, and
immutable Recovery Points. It does not otherwise define how replacement keeps
source identity stable, how an uncertain PUT is replayed, or what durable
reference prevents deletion of a Connector used by an enabled policy.

Those seams must be closed before either Backup Policy persistence or Connector
deletion is exposed. Inferring source identity from the current policy would
orphan Recovery Points when a source is temporarily removed. Checking policies
by scanning after a Connector deletion transaction would leave a race.

## Decision

### Environment singleton

The durable policy is an Environment singleton keyed by the Environment stable
id. It has no independently addressable policy id and no standalone list route.
Its record contains:

- `environment_id`
- `enabled`
- `frequency`
- `keep`
- `encryption` (`age` or `none`)
- optional `connector_id`
- the ordered active `source_ids`
- `updated_at`

`updated_at` is the UTC instant at which this policy revision commits. The
durable policy record does not store the scheduler's derived `next_run_at`.
Both `GET /environments/{id}/backup-policy` and the successful full-replacement
`PUT` response add `next_run_at` to the existing policy projection. It is not a
replacement input: the Controller derives it from the current policy and its
durable coordination schedule state. It is an RFC3339 UTC timestamp at exact whole-second
precision (`...Z`) and is `null` if and only if the policy is disabled or
unconfigured. A valid enabled policy always returns a value. The CLI
`backup policy show` and Console Environment Backup settings expose this same
field; no endpoint or action is added.

`frequency` has one bounded UTC grammar evaluated directly by the Controller:
daily `*-*-* HH:MM:SS` or weekly
`Mon|Tue|Wed|Thu|Fri|Sat|Sun *-*-* HH:MM:SS`. Numeric fields have exact widths,
the separators are one ASCII space, and ranges, lists, repetitions, timezone
suffixes, and calendar dates are invalid. No systemd timer, `systemd-analyze`,
or other scheduler subprocess exists.

The Environment must belong to a tenant Project. Backing Environments cannot
own a Backup Policy or backup key.

`PUT /environments/{id}/backup-policy` is a full replacement protected by the
normal `Idempotency-Key` contract. It stores and replays the exact `200`
response. A retry never allocates different source ids or another age key.
Concurrent replacements use the policy revision as a compare fence; a losing
writer resolves through its protected idempotency evidence rather than merging.

A disabled replacement may be completely unconfigured. Disabling an existing
policy retains its submitted connector, sources, frequency, retention, and
encryption choices; clients that implement an on/off toggle first read and then
replace the complete policy. Enabling requires a valid frequency, positive
`keep`, one or more sources, and an existing Connector owned by the same
Environment.

Owner-approved public-contract clarification (2026-08-24): `keep` remains a
JSON/YAML integer and is valid only in the inclusive range
`1..9007199254740991` (`Number.MAX_SAFE_INTEGER`). This is the one range for
replacement input, public projection, durable policy state, captured run state,
and retention sweeps. The upper bound prevents JavaScript clients from silently
rounding an accepted operator decision; it does not change Go storage from
`int64` or permit floating-point or quoted-string encodings.

The Environment owns one record at
`/v1/runtime/environment-coordination/<environment-id>`, containing
`environment_id`, a monotonic `schedule_clock_floor`, and optional
`current_backup_schedule_state` with exactly `policy_digest`, `frequency`,
`enabled_at`, `last_evaluated_at`, and `updated_at`. It is bounded to 4 KiB,
contains no collections, and its etcd modification revision is the Environment
mutation fence. Ordinary Environment mutations rewrite the complete encoded
record byte-for-byte; only policy replacement and scheduler evaluation may
transform its schedule fields.

Immediately before the Controller seals the replacement response and replay
marker, after any initial age key preparation, it chooses `F =
max(schedule_clock_floor, current last_evaluated_at, now UTC)`. A disabled
replacement clears the optional state while retaining `F` as the floor.
Create, enable, or frequency change seeds `enabled_at`, `last_evaluated_at`, and
`updated_at` at `F`. A same-enabled-frequency replacement carries the original
`enabled_at`, bounds `last_evaluated_at` and `updated_at` at `F`, and binds the
state to the canonical replacement through `policy_digest`.

`last_evaluated_at` is an exclusive UTC instant, while schedule occurrences are
whole-second UTC instants; comparisons use the instants, not rendered strings.
The scheduler reads policy and coordination at one fixed revision, validates
the policy digest, and makes no mutation when `now <= schedule_clock_floor`.
Otherwise it considers only the latest occurrence in `(last_evaluated_at,
now]`. A no-due evaluation advances the floor and evaluation boundary. A due
dispatch or held-lock `skipped_overlap` atomically advances coordination and
writes the immutable due outcome; only dispatch also publishes a Task and
acquires the operation lock. Each transaction compares the exact policy and
coordination revisions, and the due outcome records the policy's etcd
modification revision. `next_run_at` is the earliest occurrence strictly after
the current durable boundary, including an overdue occurrence before the next
tick; it is `null` only for a disabled or unconfigured policy.

### Stable source catalog

Every resolved source is a separate Environment-owned durable record:

- `id` (`spt_<ulid>`)
- `environment_id`
- `kind` (`attach`, `volume`, or `config`)
- resolved `target_id` for attach and volume; the Environment id for config
- `created_at`

Input names are labels only. Blueprint and Console names are resolved to the
current stable Attach or Volume id before the policy transaction, and the CLI
resolves slugs unless `--id` is used. The API request and response use
`target_id`; each response source is exactly `{id, kind, target_id}`.
Persistence stores only that stable target id. The config source resolves to
the owning Environment id.

The identity tuple is `(environment_id, kind, target_id)`. Replacement reuses
the tuple's existing source id. Active membership is derived only from the
singleton policy's ordered `source_ids`; it is not duplicated on source
records. Removing a source therefore preserves the immutable catalog record,
and re-adding the same surviving target reuses the same source id. A source
record may be garbage-collected only when no current policy and no Recovery
Point references it. Policy response order is the operator's submitted order;
storage indexes are not presentation order.

Duplicate source identity tuples are rejected. Config can therefore occur at
most once. A source must resolve within the same Environment: an Attach must be
consumer-owned there, and a Volume must be owned there. Backing Environment
data directories are never Volume sources.

Encryption is `age` or `none`, but a policy selecting the config source must
use `age` because config contains secret Entry values.

### Connector reference fence

An enabled policy owns exactly one reverse-reference index at:

`/v1/indexes/backup-policies/by-connector/{connector_id}/{environment_id}`

The value identifies the Environment policy. A disabled policy owns no such
index even when it retains a configured `connector_id`.

Direct policy replacement through `PUT /environments/{id}/backup-policy`
idempotently ensures source catalog records before the replacement transaction.
This may leave an unreferenced immutable record after an abandoned request,
which is safe and later garbage-collectable. Direct replacement atomically
compares the Environment, Connector primary, Connector owner index, every
selected source and target, and all relevant deletion tombstones; then it
replaces the policy and old/new Connector reference indexes in one transaction.
Connector deletion compares absence of this prefix in the same transaction
that publishes its tombstone and finalizer Task. Therefore an enabled reference
and Connector deletion cannot both commit.

Deleting a Connector referenced only by disabled policies is allowed. Those
policies retain the now-missing id and fail validation if an operator later
tries to enable them without selecting an existing same-Environment Connector.

### Blueprint-owned policy publication

`x-gp-backup` is part of the Environment desired revision and uses ADR 0051's
single final publication transaction. It does not use direct replacement's
pre-ensure allowance. The Blueprint transaction atomically publishes the
desired head, Environment update Task, ADR 0021 marker, Backup Policy,
enabled-only Connector reference, every missing stable source-catalog tuple,
the lazy current age key when required, and every candidate Attach or Volume
identity needed by a selected source. A failed comparison or transaction
publishes none of them. There is no partial source catalog, orphan candidate
identity, policy-only head, or compatibility publication path.

`MaximumBackupPolicySources` remains 12. Each stable source tuple owns exactly
three durable records: its source primary, Environment ownership index, and
`(environment_id, kind, target_id)` identity index. A maximum policy may
therefore add 36 source-catalog mutations in the final Blueprint transaction;
this is not a three-source limit. Active membership remains only the ordered
`source_ids` in the singleton policy.

`MaximumEnvironmentBlueprintAttachCandidates` is 2. It counts only Attach
identities newly introduced by this Blueprint candidate. Retained Attaches and
pre-existing Attaches selected as sources do not consume the candidate limit.
Every candidate Attach or Volume source target is resolved and validated from
the same sealed candidate publication rather than required to exist before it.
An Attach source must still resolve to the credential-owning Attach; an
existing-credential dependent is never a second Backup source.

Connector creation remains outside Blueprint. `x-gp-backup.connector` must
resolve to a pre-existing Connector owned by the Environment at the fixed
validation revision, and the final transaction fences that exact Connector and
owner index. A missing, concurrently replaced, or deleting Connector rejects
the complete publication.

### Per-Environment age key

The backup key is a separate Environment singleton. Its public projection is
`age_recipient`, `key_era`, `key_created_at`, and `key_rotated_at`. Its private age identity is
an encrypted subordinate value sealed by the Controller key; plaintext never
appears in the policy record, idempotency evidence, Task parameters, logs, or
list responses.

The first successful replacement that enables `encryption: age` creates era 1
in the same transaction as the policy. `encryption: none` does not create a
key. Disabling backups or selecting `none` retains an existing key because old
Recovery Points may still need it. Rotation creates the next era for new
points and does not delete older exported-key requirements.

The same lazy and retention rules apply to `x-gp-backup`: an enabled age policy
creates the absent era-1 key in the final Blueprint publication, exact replay
reuses that identity, and disabled or `none` publication creates no key. Any
existing current or historical key identity remains retained for Recovery
Points even when the candidate disables Backup or selects `none`.

## Consequences

- Connector deletion requires this reverse index before its public DELETE can
  be implemented safely.
- Source ids survive policy edits and temporary removal, so immutable Recovery
  Points never depend on mutable names or the current policy document.
- The MVP needs policy, source, Connector-reference, backup-key metadata, and
  encrypted key-value repositories before scheduler/upload work.
- PUT is replacement, not patch. Console toggle behavior must preserve fields
  by submitting the complete current policy.
- No compatibility record or scan-based fallback is introduced.
