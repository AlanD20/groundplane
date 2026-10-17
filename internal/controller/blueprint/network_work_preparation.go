package blueprint

import (
	"context"
	"slices"
	"time"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/internal/infra/etcd/blueprints"
	"github.com/AlanD20/groundplane/internal/infra/etcd/blueprintunits"
	"github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/networkwork"
	"github.com/AlanD20/groundplane/internal/infra/etcd/workqueue"
	"github.com/AlanD20/groundplane/pkg/errs"
)

type NetworkWorkPreparation struct {
	service *Service
	units   *blueprintunits.Repository
	queue   *workqueue.Repository
	inputs  *networkwork.InputRepository
}

func NewNetworkWorkPreparation(service *Service, units *blueprintunits.Repository,
	queue *workqueue.Repository, inputs *networkwork.InputRepository,
) (*NetworkWorkPreparation, error) {
	if service == nil || units == nil || queue == nil || inputs == nil {
		return nil, errs.New(errs.KindInternal, "Blueprint Network preparation dependencies are missing")
	}
	return &NetworkWorkPreparation{service: service, units: units, queue: queue, inputs: inputs}, nil
}

func (owner *NetworkWorkPreparation) Prepare(ctx context.Context, taskID, workID string) error {
	work, err := owner.queue.ReadWork(ctx, taskID, workID, 0)
	if err != nil {
		return err
	}
	task, item := work.Task.Task, work.Item.Item
	if task.Action != workexecution.ActionApply || !task.PublicationComplete || task.Stop != nil ||
		task.FinishedAt != nil || task.CollectionStartedAt != nil || item.ForwardRevoked ||
		item.Operation != workexecution.Network || item.Placement.Executor != workexecution.Agent ||
		item.Phase != workexecution.Waiting || item.Wait == nil || item.Wait.Reason != workexecution.WaitInput ||
		item.Input != nil || item.AttemptNumber != 0 {
		return errs.New(errs.KindStateConflict, "Blueprint Network is not waiting for executable input")
	}
	snapshot, err := owner.units.LoadAtRevision(ctx, task.Scope.EnvironmentID, work.ReadRevision)
	if err != nil {
		return err
	}
	if snapshot.HeadTaskID != task.ID || snapshot.Desired == nil ||
		snapshot.Desired.Record.ParentTaskID != task.ID || !snapshot.Desired.Record.Complete {
		return errs.New(errs.KindStateConflict, "Blueprint Network no longer belongs to the current complete Apply")
	}
	graph, err := owner.units.LoadCapturedGraph(ctx, task, work.ReadRevision)
	if err != nil {
		return err
	}
	var unit *blueprintunits.Unit
	for index, captured := range graph.Record.Items {
		if captured.ID != item.ID {
			continue
		}
		if captured.TaskID != item.TaskID || captured.Operation != item.Operation || captured.TargetID != item.TargetID ||
			captured.Placement != item.Placement || !slices.Equal(captured.Dependencies, item.Dependencies) ||
			!slices.Equal(captured.Claims, item.Claims) || captured.Input != nil ||
			!captured.CreatedAt.Equal(item.CreatedAt) || !captured.Deadline.Equal(item.Deadline) {
			return errs.New(errs.KindStateConflict, "Blueprint Network work differs from its accepted graph")
		}
		unit = &graph.Record.Units[index]
		break
	}
	if unit == nil || unit.Target.Kind != ids.KindNetwork || unit.Removal {
		return errs.New(errs.KindStateConflict, "Blueprint graph has no managed Network for this work")
	}
	dependencies := make([]workqueue.ItemSnapshot, len(item.Dependencies))
	for index, id := range item.Dependencies {
		dependency, err := owner.queue.ReadWork(ctx, task.ID, id, work.ReadRevision)
		if err != nil {
			return err
		}
		dependencies[index] = dependency.Item
	}
	if _, err := workqueue.DependencyConditions(item, dependencies); err != nil {
		return err
	}
	input, guards, err := owner.service.prepareAuthoredNetworkInput(ctx, work)
	if err != nil {
		return err
	}
	guards = append(guards,
		keyvalue.Condition{Key: blueprintunits.CapturedGraphKey(task.Scope.EnvironmentID, task.ID), ModRevision: graph.Revision},
		keyvalue.Condition{Key: blueprints.EnvironmentBlueprintHeadKey(task.Scope.EnvironmentID), ModRevision: snapshot.HeadRevision},
		keyvalue.Condition{Key: blueprintunits.EpochKey(task.Scope.EnvironmentID), ModRevision: snapshot.EpochRevision},
		keyvalue.Condition{Key: blueprintunits.DesiredPlanKey(task.Scope.EnvironmentID), ModRevision: snapshot.Desired.Revision},
	)
	change, err := owner.inputs.PrepareBlueprintInput(ctx, work, input, dependencies, guards, time.Now().UTC())
	if err != nil {
		return err
	}
	defer keyvalue.ZeroMutationBytes(change.Mutations)
	if !time.Now().Before(task.Deadline) || !time.Now().Before(item.Deadline) {
		return errs.New(errs.KindStateConflict, "Blueprint Network preparation exceeded its deadline")
	}
	result, err := owner.queue.Commit(ctx, change)
	keyvalue.ClearValues(result.FailureReads)
	if err != nil {
		return err
	}
	if !result.Succeeded {
		return errs.New(errs.KindStateConflict, "Blueprint Network inputs changed during preparation")
	}
	return nil
}
