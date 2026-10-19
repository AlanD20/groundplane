package attachwork

import (
	"context"
	"slices"
	"time"

	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/internal/controller/workdispatch"
	"github.com/AlanD20/groundplane/internal/infra/etcd/attachments"
	"github.com/AlanD20/groundplane/internal/infra/etcd/blueprintunits"
	"github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/workqueue"
	"github.com/AlanD20/groundplane/pkg/errs"
)

func (owner *Reports) admitController(
	ctx context.Context,
	request workqueue.Assignment,
	at time.Time,
) (workdispatch.Decision, error) {
	task, item := request.Task.Task, request.Item.Item
	if !request.DecisionOnly || request.Direction != workexecution.Forward || item.Operation != workexecution.Attach ||
		item.Placement.Executor != workexecution.Controller || item.AttemptNumber != 0 ||
		item.ActiveAttemptID != "" || item.Phase != workexecution.Ready {
		return workdispatch.Decision{}, errs.New(
			errs.KindStateConflict,
			"Attach connection requires unstarted Controller work",
		)
	}
	view := workqueue.View{Task: request.Task, Item: request.Item, ReadRevision: request.ReadRevision}
	if task.Stop != nil || item.ForwardRevoked || !at.Before(task.Deadline) || !at.Before(item.Deadline) {
		change, err := owner.stopPrepared(ctx, view, at)
		return workdispatch.Decision{Disposition: workdispatch.Settle, Change: change}, err
	}
	current, guards, err := owner.inputs.repository.ReadDesiredMatch(ctx, view)
	if err != nil {
		return workdispatch.Decision{}, err
	}
	if !current {
		var change workqueue.Change
		if task.Action == workexecution.ActionAttach {
			change, err = owner.revokeUnstarted(ctx, view, workexecution.Superseded, at)
		} else {
			change, err = workqueue.PrepareRevocation(view.Task, view.Item, workexecution.Superseded, at)
		}
		change.Conditions = append(change.Conditions, guards...)
		return workdispatch.Decision{Disposition: workdispatch.Settle, Change: change}, err
	}
	if task.Action == workexecution.ActionAttach {
		change, err := owner.inputs.repository.PrepareDirectControllerSettlement(ctx, view, at)
		return workdispatch.Decision{Disposition: workdispatch.Settle, Change: change}, err
	}
	source, err := owner.inputs.repository.LoadCaptured(ctx, view)
	if err != nil {
		return workdispatch.Decision{}, err
	}
	defer source.Clear()
	graph, err := owner.units.LoadCapturedGraph(ctx, task, view.ReadRevision)
	if err != nil {
		return workdispatch.Decision{}, err
	}
	var unit *blueprintunits.Unit
	for index, captured := range graph.Record.Items {
		if captured.ID != item.ID {
			continue
		}
		if captured.TaskID != item.TaskID || captured.Operation != item.Operation ||
			captured.TargetID != item.TargetID ||
			captured.Placement != item.Placement ||
			captured.Input != nil ||
			!slices.Equal(captured.Dependencies, item.Dependencies) ||
			!slices.Equal(captured.Claims, item.Claims) ||
			!captured.CreatedAt.Equal(item.CreatedAt) ||
			!captured.Deadline.Equal(item.Deadline) {
			return workdispatch.Decision{}, errs.New(
				errs.KindStateConflict,
				"Attach connection differs from its captured work",
			)
		}
		unit = &graph.Record.Units[index]
		break
	}
	if unit == nil {
		return workdispatch.Decision{}, errs.New(
			errs.KindStateConflict,
			"Attach connection is absent from the accepted Apply",
		)
	}
	snapshot, err := owner.units.LoadAtRevision(ctx, task.Scope.EnvironmentID, view.ReadRevision)
	if err != nil {
		return workdispatch.Decision{}, err
	}
	binding, err := owner.inputs.repository.PrepareControllerBinding(ctx, view)
	if err != nil {
		return workdispatch.Decision{}, err
	}
	record, err := source.Generation.Record.ControllerBinding(task.CreatedAt)
	if err != nil {
		keyvalue.ZeroMutationBytes(binding.Mutations)
		return workdispatch.Decision{}, err
	}
	digest, err := attachments.BindingDigest(record)
	if err != nil {
		keyvalue.ZeroMutationBytes(binding.Mutations)
		return workdispatch.Decision{}, err
	}
	references, referenceGuards, err := owner.inputs.repository.CaptureControllerReferences(ctx, view)
	if err != nil {
		keyvalue.ZeroMutationBytes(binding.Mutations)
		return workdispatch.Decision{}, err
	}
	change, err := blueprintunits.PrepareAttachControllerSettlement(snapshot, view, *unit,
		source.Generation.Record.OperationID, source.Generation.Revision, digest, references, at)
	if err != nil {
		keyvalue.ZeroMutationBytes(binding.Mutations)
		return workdispatch.Decision{}, err
	}
	change.Conditions = append(change.Conditions, guards...)
	change.Conditions = append(change.Conditions, referenceGuards...)
	change.Conditions = append(change.Conditions, binding.Conditions...)
	change.Conditions = append(change.Conditions, keyvalue.Condition{
		Key: blueprintunits.CapturedGraphKey(task.Scope.EnvironmentID, task.ID), ModRevision: graph.Revision,
	})
	change.Mutations = append(change.Mutations, binding.Mutations...)
	applied, err := owner.inputs.repository.PrepareControllerAppliedBinding(ctx, view)
	if err != nil {
		keyvalue.ZeroMutationBytes(change.Mutations)
		return workdispatch.Decision{}, err
	}
	change.Conditions = append(change.Conditions, applied.Conditions...)
	change.Mutations = append(change.Mutations, applied.Mutations...)
	return workdispatch.Decision{Disposition: workdispatch.Settle, Change: change}, nil
}
