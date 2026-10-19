package attachprovision

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"time"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/internal/infra/etcd/attachments"
	"github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/recordcodec"
	"github.com/AlanD20/groundplane/internal/infra/etcd/workqueue"
)

func DirectControllerResultKey(workID string) string {
	return "/v1/attach-controller-results/" + workID
}

type directControllerResult struct {
	TaskID        string                         `json:"task_id"`
	WorkItemID    string                         `json:"work_item_id"`
	AttachID      string                         `json:"attach_id"`
	Input         workexecution.Input            `json:"input"`
	BindingSHA256 string                         `json:"binding_sha256"`
	References    []attachments.BindingReference `json:"references"`
	At            time.Time                      `json:"at"`
}

// PrepareDirectControllerSettlement commits a pending binding, applied receipt
// and visible work result together. This is a Controller decision with no host
// effects, not a synthetic Agent execution or a hidden Apply.
func (repository *InputRepository) PrepareDirectControllerSettlement(ctx context.Context, view workqueue.View,
	at time.Time,
) (_ workqueue.Change, resultErr error) {
	task, item := view.Task.Task, view.Item.Item
	if task.Action != workexecution.ActionAttach || item.Operation != workexecution.Attach || item.Input == nil ||
		item.Placement.Executor != workexecution.Controller || item.AttemptNumber != 0 || item.ActiveAttemptID != "" ||
		item.Phase != workexecution.Ready || item.ForwardRevoked || task.Stop != nil || !task.PublicationComplete ||
		task.FinishedAt != nil || task.CollectionStartedAt != nil || at.IsZero() || at.Location() != time.UTC ||
		at.Before(item.UpdatedAt) || !at.Before(task.Deadline) || !at.Before(item.Deadline) {
		return workqueue.Change{}, checkpointConflict("Attach Controller settlement requires ready direct work")
	}
	source, err := repository.LoadCaptured(ctx, view)
	if err != nil {
		return workqueue.Change{}, err
	}
	defer source.Clear()
	capture := source.Generation.Record
	if !capture.SettlesInController() {
		return workqueue.Change{}, checkpointConflict("Attach requires host effects before it can settle")
	}
	current, guards, err := repository.ReadDesiredMatch(ctx, view)
	if err != nil {
		return workqueue.Change{}, err
	}
	if !current {
		return workqueue.Change{}, checkpointConflict("Attach intent changed before Controller settlement")
	}
	change, err := repository.PrepareControllerBinding(ctx, view)
	if err != nil {
		return workqueue.Change{}, err
	}
	defer func() {
		if resultErr != nil {
			keyvalue.ClearMutationValues(change.Mutations)
		}
	}()
	change.Conditions = append(change.Conditions, guards...)
	record, err := capture.ControllerBinding(task.CreatedAt)
	if err != nil {
		return workqueue.Change{}, err
	}
	digest, err := attachments.BindingDigest(record)
	if err != nil {
		return workqueue.Change{}, err
	}
	references, referenceGuards, err := repository.CaptureControllerReferences(ctx, view)
	if err != nil {
		return workqueue.Change{}, err
	}
	receipt := directReceipt(view)
	receipt.ControllerOperationID, receipt.ControllerAttachInputRevision = capture.OperationID, source.Generation.Revision
	receipt.ControllerAttachBindingSHA256, receipt.ControllerAttachReferences = digest, references
	applied, err := repository.prepareDirectReceipt(ctx, view, receipt)
	if err != nil {
		return workqueue.Change{}, err
	}
	change.Conditions = append(change.Conditions, referenceGuards...)
	change.Conditions = append(change.Conditions, applied.Conditions...)
	change.Mutations = append(change.Mutations, applied.Mutations...)
	binding, err := repository.PrepareControllerAppliedBinding(ctx, view)
	if err != nil {
		return workqueue.Change{}, err
	}
	change.Conditions = append(change.Conditions, binding.Conditions...)
	change.Mutations = append(change.Mutations, binding.Mutations...)
	value, err := recordcodec.Encode("attach-controller-result", directControllerResult{
		task.ID, item.ID, item.TargetID, *item.Input, digest, references, at,
	})
	if err != nil {
		return workqueue.Change{}, err
	}
	resultHash := sha256.Sum256(value)
	terminal, err := workqueue.PrepareUnstartedSettlement(view.Task, view.Item, workexecution.Completion{
		Outcome: workexecution.Succeeded, EffectsSettled: true, Evidence: workexecution.Input{
			ID: ids.DeriveAt(
				ids.KindPlan,
				task.CreatedAt,
				item.ID,
				"attach-controller-result",
			), SHA256: hex.EncodeToString(resultHash[:]),
		},
	}, at)
	if err != nil {
		clear(value)
		return workqueue.Change{}, err
	}
	change.Conditions = append(change.Conditions, terminal.Conditions...)
	change.Mutations = append(change.Mutations, terminal.Mutations...)
	key := DirectControllerResultKey(item.ID)
	change.Conditions = append(change.Conditions, keyvalue.Condition{Key: key})
	change.Mutations = append(change.Mutations, keyvalue.Mutation{Type: keyvalue.MutationPut, Key: key, Value: value})
	return change, nil
}
