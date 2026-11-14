package blueprint

import (
	"context"
	"slices"
	"time"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/internal/infra/etcd/blueprintunits"
	"github.com/AlanD20/groundplane/internal/infra/etcd/entrywork"
	"github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/workqueue"
	"github.com/AlanD20/groundplane/pkg/errs"
)

type EntryRemovalPreparation struct {
	service *Service
	units   *blueprintunits.Repository
	queue   *workqueue.Repository
	inputs  *entrywork.InputRepository
}

func NewEntryRemovalPreparation(service *Service, units *blueprintunits.Repository,
	queue *workqueue.Repository, inputs *entrywork.InputRepository,
) (*EntryRemovalPreparation, error) {
	if service == nil || units == nil || queue == nil || inputs == nil {
		return nil, errs.New(errs.KindInternal, "Blueprint Entry cleanup requires file and resource owners")
	}
	return &EntryRemovalPreparation{service: service, units: units, queue: queue, inputs: inputs}, nil
}

func (owner *EntryRemovalPreparation) Prepare(ctx context.Context, taskID, workID string) error {
	view, err := owner.queue.ReadWork(ctx, taskID, workID, 0)
	if err != nil {
		return err
	}
	task, item := view.Task.Task, view.Item.Item
	at := time.Now().UTC()
	if task.Action != workexecution.ActionApply || !task.PublicationComplete || task.Stop != nil ||
		task.FinishedAt != nil || task.CollectionStartedAt != nil || item.Operation != workexecution.EntryConfiguration ||
		item.Placement.Executor != workexecution.Agent || item.Input != nil || item.ForwardRevoked || item.AttemptNumber != 0 ||
		item.ActiveAttemptID != "" || item.Phase != workexecution.Waiting || item.Wait == nil || item.Wait.Reason != workexecution.WaitInput ||
		!at.Before(task.Deadline) || !at.Before(item.Deadline) {
		return errs.New(errs.KindStateConflict, "Blueprint Entry removal is not waiting for captured file input")
	}
	graph, err := owner.units.LoadCapturedGraph(ctx, task, view.ReadRevision)
	if err != nil {
		return err
	}
	var unit *blueprintunits.Unit
	for index, captured := range graph.Record.Items {
		if captured.ID != item.ID {
			continue
		}
		if captured.Operation != item.Operation || captured.TargetID != item.TargetID ||
			captured.Placement != item.Placement ||
			!slices.Equal(captured.Claims, item.Claims) ||
			!slices.Equal(captured.Dependencies, item.Dependencies) ||
			!captured.CreatedAt.Equal(item.CreatedAt) ||
			!captured.Deadline.Equal(item.Deadline) {
			return errs.New(errs.KindStateConflict, "Entry cleanup differs from its captured Blueprint work")
		}
		unit = &graph.Record.Units[index]
	}
	if unit == nil || unit.Target.Kind != ids.KindEnvEntry || !unit.Removal {
		return errs.New(errs.KindStateConflict, "Entry cleanup has no captured omission unit")
	}
	snapshot, err := owner.units.LoadAtRevision(ctx, task.Scope.EnvironmentID, view.ReadRevision)
	if err != nil {
		return err
	}
	if snapshot.HeadTaskID != task.ID || snapshot.Desired == nil || snapshot.Desired.Record.ParentTaskID != task.ID {
		return errs.New(errs.KindStateConflict, "Entry cleanup no longer owns the Environment intent")
	}
	current, guards, err := owner.units.ReadWorkIntentMatch(ctx, view)
	if err != nil {
		return err
	}
	if !current {
		return errs.New(errs.KindStateConflict, "Entry cleanup was superseded before preparation")
	}
	dependencies := make([]workqueue.ItemSnapshot, 0, len(item.Dependencies))
	for _, id := range item.Dependencies {
		dependency, err := owner.queue.ReadWork(ctx, task.ID, id, view.ReadRevision)
		if err != nil {
			return err
		}
		dependencies = append(dependencies, dependency.Item)
	}
	dependencyGuards, err := workqueue.DependencyConditions(item, dependencies)
	if err != nil {
		return err
	}
	guards = append(guards, dependencyGuards...)
	guards = append(
		guards,
		keyvalue.Condition{
			Key:         blueprintunits.CapturedGraphKey(snapshot.EnvironmentID, task.ID),
			ModRevision: graph.Revision,
		},
		keyvalue.Condition{
			Key:         blueprintunits.DesiredPlanKey(snapshot.EnvironmentID),
			ModRevision: snapshot.Desired.Revision,
		},
		keyvalue.Condition{Key: blueprintunits.EpochKey(snapshot.EnvironmentID), ModRevision: snapshot.EpochRevision},
	)
	source, err := owner.service.repository.ReadBlueprintEntryRemoval(ctx, view, *unit)
	if err != nil {
		return err
	}
	guards = append(guards, source.Conditions...)
	var change workqueue.Change
	if source.Removal == nil {
		change, err = owner.service.prepareEntryOmission(ctx, view, *unit)
	} else {
		change, err = owner.prepareFileInput(ctx, view, snapshot, source, guards, at)
	}
	if err != nil {
		return err
	}
	defer keyvalue.ClearMutationValues(change.Mutations)
	change.Conditions = append(change.Conditions, guards...)
	result, err := owner.queue.Commit(ctx, change)
	keyvalue.ClearValues(result.FailureReads)
	if err != nil {
		return err
	}
	if !result.Succeeded {
		return errs.New(errs.KindStateConflict, "Entry cleanup input changed before publication; reload the Task")
	}
	return nil
}
