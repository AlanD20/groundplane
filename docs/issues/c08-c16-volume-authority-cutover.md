# C08/C16 Volume authority cutover

Status: resolved.

## Resolution

The normalized Environment desired revision is the sole Volume authority.
Backup policy preparation, replacement, secret resolution, run publication,
restart reconstruction, and deletion impact resolve stable Volume ids through
one fixed normalized projection. There is no compatibility or dual-read path.

The pre-ADR flat Volume primary, repository, codecs, owner indexes, slug
indexes, Compose-key indexes, and hierarchy membership were deleted. The
remaining `etcd.VolumeRecord` is explicitly a projection-only API read model
with no primary key or mutation authority.

Volume removal joins current Backup selection and retained historical Recovery
Points at the impact page's fixed revision. The final impact token binds the
normalized Volume identity, mounted consumers, policy change, retained source
records, and historical point digests. Removing the final selected source
disables scheduling while retaining policy configuration and historical
cleanup authority.

## Evidence

- `internal/infra/etcd/backup_volume_projection.go` owns the fixed-revision
  Backup and Volume join.
- `internal/volume/reads.go` includes Backup sources, policy consequences, and
  Recovery Point history in the bounded deletion-impact sequence.
- `internal/infra/etcd/volume_projection.go` declares the remaining public
  `VolumeRecord` as projection-only.
- Production composition injects the hierarchy repository into the Volume read
  service, so API, CLI, and Console consume the same impact authority.

## Remaining C08 work

- Blueprint `x-gp-entry` reconciliation remains blocked on proposed [Blueprint Entry contract](../features/blueprints.md#entry-identity-and-values).
- Real-host Volume lifecycle acceptance remains pending.
