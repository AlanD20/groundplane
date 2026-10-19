package etcd

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"time"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/internal/infra/etcd/attachremoval"
	"github.com/AlanD20/groundplane/internal/infra/etcd/hierarchydeletion"
	"github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/workqueue"
	"github.com/AlanD20/groundplane/pkg/errs"
)

func (repository *HierarchyDeletionRepository) PrepareHierarchyAttachRetention(ctx context.Context, view workqueue.View,
	at time.Time, final bool,
) (bool, workqueue.Change, error) {
	task, item := view.Task.Task, view.Item.Item
	if task.Validate() != nil || item.Validate() != nil || task.CollectionStartedAt == nil ||
		task.Action != workexecution.ActionRemove ||
		item.Operation != workexecution.Hierarchy ||
		item.Placement.Executor != workexecution.Agent ||
		item.TaskID != task.ID ||
		item.Phase != workexecution.Settled ||
		item.ActiveAttemptID != "" ||
		item.Input == nil ||
		item.Result == nil {
		return false, workqueue.Change{}, errs.New(
			errs.KindStateConflict,
			"Attach removal is not eligible for retention",
		)
	}
	guard, err := repository.HierarchyCollectionGuard(ctx, view.Task, view.ReadRevision, at)
	if err != nil {
		return false, workqueue.Change{}, err
	}
	if !guard.Present || !guard.Eligible {
		return false, workqueue.Change{}, nil
	}
	source, err := repository.LoadHierarchyAttachCapture(ctx, view)
	if err != nil {
		return false, workqueue.Change{}, err
	}
	defer source.Clear()
	if source.Revision <= 0 {
		return false, workqueue.Change{}, hierarchydeletion.CorruptHierarchyDeletion()
	}
	captured, err := repository.LoadHierarchyWork(ctx, view)
	if err != nil {
		return false, workqueue.Change{}, err
	}
	completionKey, err := hierarchydeletion.HierarchyDeletionCompletionKey(
		captured.Action.ParentOperationID,
		captured.Action.Ordinal,
	)
	if err != nil {
		return false, workqueue.Change{}, err
	}
	read, err := repository.store.GetMany(
		ctx,
		keyvalue.GetManyRequest{Keys: []string{completionKey}, Revision: view.ReadRevision},
	)
	if read != nil {
		defer keyvalue.ClearValues(read.Values)
	}
	if err != nil {
		return false, workqueue.Change{}, err
	}
	if read == nil || read.ReadRevision != view.ReadRevision || len(read.Values) != 1 || read.Values[0] == nil ||
		read.Values[0].Key != completionKey {
		return false, workqueue.Change{}, hierarchydeletion.CorruptHierarchyDeletion()
	}
	result, err := attachremoval.DecodeWorkResult(read.Values[0].Value)
	if err != nil {
		return false, workqueue.Change{}, err
	}
	raw, err := attachremoval.EncodeWorkCapture(source.Capture)
	if err != nil {
		return false, workqueue.Change{}, err
	}
	digest := sha256.Sum256(raw)
	clear(raw)
	checkpoint, err := result.Report.CheckpointAuthority(result.Attempt, result.Authority)
	if err != nil || item.Outcome != workexecution.Succeeded || result.Attempt.TaskID != task.ID ||
		result.Attempt.WorkItemID != item.ID ||
		result.Attempt.Input != *item.Input ||
		result.Attempt.Number != item.AttemptNumber ||
		result.Attempt.Executor.HostID != item.Placement.HostID ||
		checkpoint.Evidence != *item.Result ||
		result.Authority.AttachID != item.TargetID ||
		result.Authority.BackingServiceID != source.Source.Input.Attach.BackingServiceID ||
		result.Authority.Source.ID != ids.DeriveAt(ids.KindPlan, task.CreatedAt, item.ID, "attach-removal-source") ||
		result.Authority.Source.SHA256 != hex.EncodeToString(digest[:]) {
		return false, workqueue.Change{}, hierarchydeletion.CorruptHierarchyDeletion()
	}
	change := guard.Authority
	change.Conditions = append(change.Conditions, source.Conditions...)
	change.Conditions = append(
		change.Conditions,
		keyvalue.Condition{Key: workqueue.ItemKey(item.ID), ModRevision: view.Item.Revision},
		keyvalue.Condition{Key: completionKey, ModRevision: read.Values[0].ModRevision},
	)
	if !final {
		return true, change, nil
	}
	if source.Capture.Pins != nil {
		retired, pinChange, err := attachremoval.RetireCapturePins(
			ctx,
			repository.store,
			view.ReadRevision,
			source.Capture,
			change.Conditions,
		)
		if err != nil || !retired {
			return false, workqueue.Change{}, err
		}
		change.Conditions = append(change.Conditions, pinChange.Conditions...)
	}
	actionKey, err := hierarchydeletion.HierarchyDeletionActionKey(
		captured.Action.ParentOperationID,
		captured.Action.Ordinal,
	)
	if err != nil {
		return false, workqueue.Change{}, err
	}
	for _, key := range []string{actionKey, completionKey, hierarchydeletion.WorkBindingKey(item.ID), hierarchydeletion.WorkStopKey(item.ID),
		attachremoval.WorkRecoveryKey(item.ID), attachremoval.WorkCaptureKey(item.ID), hierarchyAttachPreparedKey(item.ID),
		attachremoval.FrozenKey(captured.Action.ParentOperationID, item.TargetID)} {
		change.Mutations = append(change.Mutations, keyvalue.Mutation{Type: keyvalue.MutationDelete, Key: key})
	}
	change.Mutations = append(
		change.Mutations,
		keyvalue.Mutation{
			Type:   keyvalue.MutationDelete,
			Prefix: true,
			Key:    hierarchydeletion.WorkStopEvidencePrefix(item.ID),
		},
	)
	return true, change, nil
}
