package etcd

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"time"

	"github.com/AlanD20/groundplane/internal/common/executionplan"
	backupruntime "github.com/AlanD20/groundplane/internal/infra/etcd/backupruntime"
	etcdstore "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/taskjournal"
	"github.com/AlanD20/groundplane/pkg/errs"
)

// CaptureBackupOrphanCleanupProof transfers acknowledged physical stage
// cleanup/absence from a terminal Task into independently retained orphan
// authority. The fixed view and CAS include the Task, delivery and exact
// staging index; Task pruning cannot make an unproved orphan invisible.
func (repository *BackupRuntimeRepository) CaptureBackupOrphanCleanupProof(ctx context.Context,
	current etcdstore.Versioned[backupruntime.BackupOrphanRecord], at time.Time,
) (etcdstore.Versioned[backupruntime.BackupOrphanRecord], bool, error) {
	if current.Revision <= 0 || !backupruntime.ValidBackupRuntimeInstant(at) {
		return current, false, errs.New(errs.KindValidationFailed, "backup orphan cleanup proof request is invalid")
	}
	if current.Record.CleanupProof != (backupruntime.BackupOrphanCleanupProof{}) {
		return current, true, nil
	}
	orphanKey := backupruntime.BackupOrphanKey(current.Record.Target.ID)
	taskKey := taskjournal.TaskStorageKey(current.Record.TaskID)
	initial, err := repository.ReadCurrentKeys(ctx, []string{orphanKey, taskKey})
	if err != nil {
		return current, false, err
	}
	defer etcdstore.ClearValues(initial.Values)
	if len(initial.Values) != 2 || initial.Values[0] == nil || initial.Values[1] == nil ||
		initial.Values[0].ModRevision != current.Revision {
		return current, false, errs.New(errs.KindStateConflict, "backup orphan or terminal Task changed")
	}
	stored, err := backupruntime.DecodeBackupOrphanRecord(initial.Values[0].Value)
	if err != nil || stored != current.Record {
		return current, false, backupruntime.CorruptBackupRuntimeRecord()
	}
	task, err := DecodeTaskRecord(initial.Values[1].Value)
	if err != nil || task.ID != current.Record.TaskID || task.Type != taskjournal.TaskBackup ||
		!taskjournal.IsTerminalTaskStatus(
			task.Status,
		) || task.OperationID != current.Record.Reconciliation.OperationID ||
		task.Owner.EnvironmentID != current.Record.Target.EnvironmentID || task.TerminalAssignment == nil ||
		len(task.Steps) == 0 || len(task.Steps) > executionplan.MaximumBackupSources {
		return current, false, errs.New(errs.KindStateConflict, "backup orphan terminal Task authority is unavailable")
	}
	assignment := task.TerminalAssignment
	connectorIndex, err := backupruntime.BackupOrphanConnectorIndexKey(
		current.Record.Target.ConnectorID,
		current.Record.Target.ID,
	)
	if err != nil {
		return current, false, err
	}
	environmentIndex, err := backupruntime.BackupOrphanEnvironmentIndexKey(
		current.Record.Target.EnvironmentID,
		current.Record.Target.ID,
	)
	if err != nil {
		return current, false, err
	}
	keys := []string{orphanKey, connectorIndex, environmentIndex, taskKey,
		backupruntime.BackupStagingDeliveryKey(assignment.AgentID)}
	recoveryKeys := make([][]byte, len(task.Steps))
	for i, step := range task.Steps {
		recoveryKeys[i], err = executionplan.BackupStagingRecoveryKey(task.ID, step.ID, current.Record.Target.ID)
		if err != nil {
			return current, false, err
		}
		keys = append(keys, backupruntime.BackupStagingIndexKey(recoveryKeys[i]))
	}
	view, err := repository.ReadFixedKeys(ctx, keys, initial.ReadRevision)
	if err != nil {
		return current, false, err
	}
	defer etcdstore.ClearValues(view.Values)
	if len(view.Values) != len(keys) || view.Values[0] == nil || view.Values[3] == nil ||
		view.Values[4] == nil || view.Values[0].ModRevision != current.Revision ||
		view.Values[3].ModRevision != initial.Values[1].ModRevision {
		return current, false, errs.New(errs.KindStateConflict, "backup orphan cleanup evidence changed")
	}
	if err := backupruntime.ValidateBackupOrphanCompanionEvidence(view.Values[:3], current.Record); err != nil {
		return current, false, err
	}
	delivery, err := backupruntime.DecodeBackupStagingDelivery(view.Values[4].Value)
	if err != nil {
		return current, false, err
	}
	if delivery.Ack == nil || delivery.AgentID != assignment.AgentID ||
		delivery.AgentGeneration != assignment.AgentGeneration ||
		view.Values[4].ModRevision <= view.Values[3].ModRevision {
		return current, false, nil
	}
	selected := -1
	for i, value := range view.Values[5:] {
		if value == nil {
			continue
		}
		index, err := backupruntime.DecodeBackupStagingIndex(value.Value)
		if err != nil || value.Version != 1 || index.TaskID != task.ID ||
			index.StepID != task.Steps[i].ID || index.PointID != current.Record.Target.ID ||
			index.AgentID != assignment.AgentID || index.AgentGeneration != assignment.AgentGeneration ||
			index.PlanSHA256 != task.PlanHash || selected >= 0 {
			return current, false, backupruntime.CorruptBackupRuntimeRecord()
		}
		selected = i
	}
	if selected < 0 {
		return current, false, errs.New(errs.KindStateConflict, "backup orphan staging index is unavailable")
	}
	disposition := "absent"
	for i, stage := range delivery.Inventory.Entries {
		if !bytes.Equal(stage.RecoveryKeySha256, recoveryKeys[selected]) {
			continue
		}
		if delivery.Plan.Dispositions[i].GetDiscardRecovered() == nil {
			return current, false, nil
		}
		disposition = "discarded"
		break
	}
	digest := sha256.Sum256(view.Values[4].Value)
	updated := current.Record
	updated.CleanupProof = backupruntime.BackupOrphanCleanupProof{
		TaskID: task.ID, TaskRevision: view.Values[3].ModRevision, StepID: task.Steps[selected].ID,
		RecoveryKeySHA256: hex.EncodeToString(recoveryKeys[selected]),
		AgentID:           assignment.AgentID, AgentGeneration: assignment.AgentGeneration,
		DeliveryRevision: view.Values[4].ModRevision, DeliverySHA256: hex.EncodeToString(digest[:]),
		Disposition: disposition,
	}
	updated.UpdatedAt = at
	if !updated.UpdatedAt.After(current.Record.UpdatedAt) {
		updated.UpdatedAt = current.Record.UpdatedAt.Add(time.Millisecond)
	}
	encoded, err := backupruntime.EncodeBackupOrphanRecord(updated)
	if err != nil {
		return current, false, err
	}
	defer clear(encoded)
	conditions := []etcdstore.Condition{
		{Key: keys[0], ModRevision: current.Revision},
		{Key: keys[1], ModRevision: current.Revision},
		{Key: keys[2], ModRevision: current.Revision},
		{Key: keys[3], ModRevision: view.Values[3].ModRevision},
		{Key: keys[4], ModRevision: view.Values[4].ModRevision},
		{Key: keys[5+selected], ModRevision: view.Values[5+selected].ModRevision},
	}
	result, err := repository.TransactRuntime(ctx, conditions, []etcdstore.Mutation{
		{Type: etcdstore.MutationPut, Key: keys[0], Value: encoded},
		{Type: etcdstore.MutationPut, Key: keys[1], Value: []byte(updated.Target.ID)},
		{Type: etcdstore.MutationPut, Key: keys[2], Value: []byte(updated.Target.ID)},
	})
	if err != nil {
		return current, false, err
	}
	if !result.Succeeded {
		etcdstore.ClearValues(result.FailureReads)
		return current, false, errs.New(errs.KindStateConflict, "backup orphan cleanup proof changed")
	}
	return etcdstore.Versioned[backupruntime.BackupOrphanRecord]{Record: updated,
		Revision: result.Revision, ReadRevision: result.Revision}, true, nil
}
