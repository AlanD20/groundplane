package backupwork

import (
	"context"
	"encoding/hex"
	"time"

	"github.com/AlanD20/groundplane/internal/common/executionplan"
	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/internal/infra/etcd/backupruntime"
	"github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/workqueue"
	"github.com/AlanD20/groundplane/proto/agentpb"
)

// Called only after the recovery report proves executor exclusion and every
// captured source's staging/helper cleanup. These writes join that exact report,
// resource settlement and claim release in one transaction, before retention.
func (repository *InputRepository) prepareCaptureOrphanCleanup(ctx context.Context,
	view workqueue.ResumeView, input *agentpb.BackupWorkInput, current, next backupruntime.BackupRunRecord,
	evidence workexecution.Input, at time.Time,
) (workqueue.Change, error) {
	var change workqueue.Change
	for ordinal, source := range next.Sources {
		if source.State != backupruntime.BackupSourceAttemptOrphaned {
			continue
		}
		orphan := backupruntime.BackupOrphanRecordFromRun(next, uint32(ordinal))
		connector, err := backupruntime.BackupOrphanConnectorIndexKey(orphan.Target.ConnectorID, orphan.Target.ID)
		if err != nil {
			return workqueue.Change{}, err
		}
		environment, err := backupruntime.BackupOrphanEnvironmentIndexKey(orphan.Target.EnvironmentID, orphan.Target.ID)
		if err != nil {
			return workqueue.Change{}, err
		}
		keys := []string{backupruntime.BackupOrphanKey(orphan.Target.ID), connector, environment}
		read, err := repository.readSettlementKeys(ctx, keys, view.Work.ReadRevision)
		if err != nil {
			return workqueue.Change{}, err
		}
		if current.Sources[ordinal].State == backupruntime.BackupSourceAttemptOrphaned {
			if read.Values[0] == nil {
				keyvalue.ClearValues(read.Values)
				return workqueue.Change{}, conflict("Backup cleanup lost its captured orphan")
			}
			saved, err := backupruntime.DecodeBackupOrphanRecord(read.Values[0].Value)
			if err == nil {
				err = backupruntime.ValidateBackupOrphanCompanionEvidence(read.Values, saved)
			}
			if err != nil {
				keyvalue.ClearValues(read.Values)
				return workqueue.Change{}, err
			}
			if !backupruntime.BackupOrphanMatchesRunSource(saved, next, uint32(ordinal)) ||
				saved.State != backupruntime.BackupOrphanInspect ||
				saved.CleanupProof != (backupruntime.BackupOrphanCleanupProof{}) || !at.After(saved.UpdatedAt) {
				keyvalue.ClearValues(read.Values)
				return workqueue.Change{}, conflict("Backup cleanup orphan authority changed")
			}
			orphan = saved
		} else if !backupruntime.BackupRunNeedsTerminalOrphan(current, next) {
			keyvalue.ClearValues(read.Values)
			return workqueue.Change{}, conflict("Backup cleanup cannot manufacture an orphan")
		}
		for index, value := range read.Values {
			guard := keyvalue.Condition{Key: keys[index]}
			if current.Sources[ordinal].State == backupruntime.BackupSourceAttemptOrphaned {
				guard.ModRevision = value.ModRevision
			} else if value != nil {
				keyvalue.ClearValues(read.Values)
				return workqueue.Change{}, conflict("Backup cleanup orphan already exists")
			}
			change.Conditions = append(change.Conditions, guard)
		}
		keyvalue.ClearValues(read.Values)
		step := input.Steps[ordinal]
		digest, err := executionplan.BackupSourceStagingKey(next.TaskID, step.ExecutionId, source.RecoveryPointID)
		if err != nil {
			return workqueue.Change{}, err
		}
		orphan.CleanupProof = backupruntime.BackupOrphanCleanupProof{
			TaskID: next.TaskID, WorkItemID: view.Work.Item.Item.ID, AttemptID: view.Attempt.Attempt.ID,
			Executor: view.Attempt.Attempt.Executor, Input: view.Attempt.Attempt.Input, Result: evidence,
			SourceOrdinal: uint32(ordinal), ExecutionID: step.ExecutionId,
			RecoveryKeySHA256: hex.EncodeToString(digest), RecordedAt: at,
		}
		orphan.UpdatedAt = at
		raw, err := backupruntime.EncodeBackupOrphanRecord(orphan)
		if err != nil {
			return workqueue.Change{}, err
		}
		change.Mutations = append(change.Mutations,
			keyvalue.Mutation{Type: keyvalue.MutationPut, Key: keys[0], Value: raw},
			keyvalue.Mutation{Type: keyvalue.MutationPut, Key: keys[1], Value: []byte(orphan.Target.ID)},
			keyvalue.Mutation{Type: keyvalue.MutationPut, Key: keys[2], Value: []byte(orphan.Target.ID)})
	}
	return change, nil
}
