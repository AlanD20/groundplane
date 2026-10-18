package etcd

import (
	"context"

	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/internal/infra/etcd/backupruntime"
	"github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/workqueue"
	"github.com/AlanD20/groundplane/pkg/errs"
)

// Finish relies on this rotation's receipt, not the latest key era; a later
// rotation may already have replaced the Environment key by this time.
func (repository *BackupPolicyRepository) PrepareBackupKeyRotationFinish(ctx context.Context, finished workqueue.FinishedWork) (workqueue.Change, error) {
	task := finished.Task.Task
	if task.Action != workexecution.ActionRotate || task.ExpectedItems != 1 || len(finished.Items) != 1 {
		return workqueue.Change{}, errs.New(errs.KindStateConflict, "backup key rotation completion requires one work item")
	}
	item := finished.Items[0]
	if item.Item.Operation != workexecution.SecretRotation || item.Item.TaskID != task.ID || item.Item.Phase != workexecution.Settled ||
		item.Item.Input == nil || item.Item.Result == nil || item.Item.ActiveAttemptID != "" || item.Item.Cleanup != workexecution.CleanupNone {
		return workqueue.Change{}, errs.New(errs.KindStateConflict, "backup key rotation has not settled")
	}
	key := backupruntime.BackupKeyRotationKey(task.ID)
	read, err := repository.store.GetMany(ctx, keyvalue.GetManyRequest{Keys: []string{key}, Revision: finished.ReadRevision})
	if err != nil {
		return workqueue.Change{}, err
	}
	if read == nil || read.ReadRevision != finished.ReadRevision || len(read.Values) != 1 || read.Values[0] == nil {
		return workqueue.Change{}, errs.New(errs.KindStateConflict, "backup key rotation receipt is unavailable")
	}
	defer keyvalue.ClearValues(read.Values)
	record, err := backupruntime.DecodeBackupKeyRotationRecord(read.Values[0].Value)
	if err != nil {
		return workqueue.Change{}, err
	}
	defer clear(record.NextEncryptedIdentity)
	input, err := backupruntime.KeyRotationInput(record)
	if err != nil {
		return workqueue.Change{}, err
	}
	expected := backupruntime.BackupKeyRotationDiscarded
	if item.Item.Outcome == workexecution.Succeeded {
		expected = backupruntime.BackupKeyRotationApplied
	}
	if record.TaskID != task.ID || record.WorkItemID != item.Item.ID || record.EnvironmentID != task.TargetID ||
		record.State != expected || task.Intent != input || *item.Item.Input != input || *item.Item.Result != input {
		return workqueue.Change{}, errs.New(errs.KindStateConflict, "backup key rotation completion differs from its captured result")
	}
	return workqueue.Change{Conditions: []keyvalue.Condition{{Key: key, ModRevision: read.Values[0].ModRevision},
		{Key: workqueue.ItemKey(item.Item.ID), ModRevision: item.Revision}}}, nil
}
