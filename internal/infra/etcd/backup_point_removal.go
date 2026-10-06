package etcd

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"time"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/infra/etcd/backuppolicy"
	"github.com/AlanD20/groundplane/internal/infra/etcd/backupruntime"
	etcdstore "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/pkg/errs"
)

// PrepareRecoveryPointRemoval selects one immutable object, not a retention
// range. Selection, its tombstone, the Environment lock and Task are published
// together; an unsuccessful publication cannot hide a usable Recovery Point.
func (repository *BackupRuntimeRepository) PrepareRecoveryPointRemoval(
	ctx context.Context, environmentID, pointID, taskID, operationID string, at time.Time,
) (PreparedBackupPrune, error) {
	if ids.Validate(ids.KindEnvironment, environmentID) != nil {
		return PreparedBackupPrune{}, errs.New(errs.KindValidationFailed, "environment id is invalid")
	}
	point, err := repository.GetBackupRecoveryPoint(ctx, pointID)
	if err != nil {
		return PreparedBackupPrune{}, err
	}
	if point.Record.EnvironmentID != environmentID {
		return PreparedBackupPrune{}, errs.New(errs.KindRecoveryPointNotFound, "recovery point was not found")
	}
	policyKey := backuppolicy.BackupPolicyKey(environmentID)
	sweepKey := backupruntime.BackupRetentionKey(point.Record.SourceID, pointID)
	read, err := repository.ReadFixedKeys(ctx, []string{policyKey, sweepKey}, point.ReadRevision)
	if err != nil {
		return PreparedBackupPrune{}, err
	}
	defer etcdstore.ClearValues(read.Values)
	if len(read.Values) != 2 || read.Values[0] == nil {
		return PreparedBackupPrune{}, errs.New(errs.KindStateConflict, "backup policy evidence is unavailable")
	}
	if err := backupruntime.ValidateCompletedBackupRetentionSweep(
		read.Values[1], point.Record.BackupRecoveryPointSnapshot, point.Revision,
	); err != nil {
		return PreparedBackupPrune{}, err
	}
	digest := sha256.Sum256(read.Values[0].Value)
	pending := etcdstore.Versioned[backupruntime.BackupRecoveryPointPruneRecord]{
		Record: backupruntime.BackupRecoveryPointPruneRecord{
			Point: point.Record.BackupRecoveryPointSnapshot, PointRevision: point.Revision,
			PolicyRevision: read.Values[0].ModRevision, PolicySHA256: hex.EncodeToString(digest[:]),
			OperationID: operationID, State: backupruntime.BackupPrunePending,
			CreatedAt: at, UpdatedAt: at,
		}, ReadRevision: point.ReadRevision,
	}
	prepared, err := repository.PrepareBackupPrune(ctx,
		[]etcdstore.Versioned[backupruntime.BackupRecoveryPointPruneRecord]{pending}, taskID, at)
	if err != nil {
		return PreparedBackupPrune{}, err
	}
	prepared.Publication.state.plan.conditions = append(prepared.Publication.state.plan.conditions,
		etcdstore.Condition{Key: policyKey, ModRevision: read.Values[0].ModRevision},
		etcdstore.Condition{Key: sweepKey, ModRevision: read.Values[1].ModRevision},
	)
	return prepared, nil
}
