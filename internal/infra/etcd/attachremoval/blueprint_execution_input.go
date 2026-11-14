package attachremoval

import (
	"context"
	"time"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/internal/infra/etcd/attachments"
	"github.com/AlanD20/groundplane/internal/infra/etcd/backingchecks"
	"github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/workqueue"
	"github.com/AlanD20/groundplane/internal/infra/tasksecretpins"
)

// PrepareBlueprintInput joins consumer input, exact presealed hook custody,
// binding ownership and readiness. It does not create another Task or promote
// a desired head; those were accepted with this Apply's immutable graph.
func (repository *InputRepository) PrepareBlueprintInput(ctx context.Context, view workqueue.View,
	input DetachInput, frozen BlueprintInput, authority []keyvalue.Condition, at time.Time,
) (_ workqueue.Change, resultErr error) {
	task, item := view.Task.Task, view.Item.Item
	if task.Action != workexecution.ActionApply || !task.PublicationComplete || task.Stop != nil ||
		task.FinishedAt != nil || task.CollectionStartedAt != nil || item.TaskID != task.ID || item.Input != nil ||
		item.Operation != workexecution.Detach || item.Phase != workexecution.Waiting || item.Wait == nil ||
		item.Wait.Reason != workexecution.WaitInput || item.ForwardRevoked || item.AttemptNumber != 0 || item.ActiveAttemptID != "" ||
		view.Item.Revision <= 0 || view.Task.Revision <= 0 || len(authority) == 0 || frozen.Snapshot == nil ||
		frozen.Validate(
			ctx,
			true,
		) != nil || input.Validate(ctx) != nil || input.TaskID != task.ID || input.WorkItemID != item.ID ||
		frozen.TaskID != task.ID || frozen.WorkItemID != item.ID || frozen.AttachID != item.TargetID ||
		input.Snapshot.Source.Attach.ID != item.TargetID || input.HasHostEffects() != frozen.HasHostEffects() ||
		input.HasHostEffects() != (item.Placement.Executor == workexecution.Agent) ||
		at.IsZero() || at.Location() != time.UTC || !at.Before(task.Deadline) || !at.Before(item.Deadline) {
		return workqueue.Change{}, checkpointConflict("Blueprint Detach input requires its waiting captured work")
	}
	source := input.Snapshot.Source
	guards := append([]keyvalue.Condition(nil), authority...)
	guards = append(guards, keyvalue.Condition{Key: workqueue.TaskKey(task.ID), ModRevision: view.Task.Revision},
		keyvalue.Condition{Key: workqueue.ItemKey(item.ID), ModRevision: view.Item.Revision},
		keyvalue.Condition{Key: WorkCaptureKey(item.ID)})
	sources, err := SourceConditions(ctx, repository.store, source)
	if err != nil {
		return workqueue.Change{}, err
	}
	guards = append(guards, sources...)
	guards = append(guards, input.Snapshot.ConsumerConditions()...)
	idle, err := backingchecks.RequireAttachIdle(ctx, repository.store, source.Attach.ID, view.ReadRevision)
	if err != nil {
		return workqueue.Change{}, err
	}
	guards = append(guards, idle)
	detaching, ownership, err := PrepareDetachClaim(ctx, repository.store, source, task.ID, item.Placement.HostID)
	if err != nil {
		return workqueue.Change{}, err
	}
	defer keyvalue.ClearMutationValues(ownership.Mutations)
	guards = append(guards, ownership.Conditions...)
	if frozen.Pins != nil {
		pins, err := tasksecretpins.NewEtcdRepository(repository.store, source.Attach.BackingProjectID)
		if err != nil {
			return workqueue.Change{}, err
		}
		root, found, err := pins.LoadActive(ctx, frozen.OperationID, view.ReadRevision)
		if err != nil {
			return workqueue.Change{}, err
		}
		if !found {
			return workqueue.Change{}, checkpointConflict("Blueprint Detach lost its admission Secret pins")
		}
		condition, err := root.AdmissionCondition(*frozen.Pins)
		if err != nil {
			return workqueue.Change{}, err
		}
		guards = append(guards, keyvalue.Condition{Key: condition.Key, ModRevision: condition.ModRevision})
	}
	inputID := ids.DeriveAt(ids.KindPlan, item.CreatedAt, item.ID, "detach-input")
	reference, staged, err := repository.StageDetach(ctx, inputID, input, guards)
	if err != nil {
		return workqueue.Change{}, err
	}
	defer func() {
		if resultErr != nil {
			keyvalue.ClearMutationValues(staged.Mutations)
		}
	}()
	ready, err := workqueue.PrepareInput(view.Task, view.Item, reference, at)
	if err != nil {
		return workqueue.Change{}, err
	}
	defer func() {
		if resultErr != nil {
			keyvalue.ClearMutationValues(ready.Mutations)
		}
	}()
	preparedItem := item
	preparedItem.Input = &reference
	capture, err := NewDetachCapture(task, preparedItem, input)
	if err != nil {
		return workqueue.Change{}, err
	}
	capture.Hook, capture.Sources, capture.Pins = frozen.Hook, frozen.Sources, frozen.Pins
	if !matchesDetachHookCapture(source, capture) {
		return workqueue.Change{}, checkpointConflict("Blueprint Detach hook differs from its admission capture")
	}
	raw, err := EncodeWorkCapture(capture)
	if err != nil {
		return workqueue.Change{}, err
	}
	value, err := attachments.EncodeAttachRecord(detaching)
	if err != nil {
		clear(raw)
		return workqueue.Change{}, err
	}
	ready.Conditions = append(ready.Conditions, staged.Conditions...)
	ready.Mutations = append(ready.Mutations, staged.Mutations...)
	ready.Mutations = append(ready.Mutations, ownership.Mutations...)
	ownership.Mutations = nil
	ready.Mutations = append(ready.Mutations,
		keyvalue.Mutation{Type: keyvalue.MutationPut, Key: WorkCaptureKey(item.ID), Value: raw},
		keyvalue.Mutation{Type: keyvalue.MutationPut, Key: attachments.AttachKey(detaching.ID), Value: value})
	return ready, nil
}
