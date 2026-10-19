package blueprint

import (
	"context"
	"slices"
	"time"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/internal/infra/etcd/blueprints"
	"github.com/AlanD20/groundplane/internal/infra/etcd/blueprintunits"
	"github.com/AlanD20/groundplane/internal/infra/etcd/componentwork"
	"github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/workqueue"
	"github.com/AlanD20/groundplane/pkg/errs"
)

type ComponentWorkPreparation struct {
	service *Service
	units   *blueprintunits.Repository
	queue   *workqueue.Repository
	inputs  *componentwork.InputRepository
}

func NewComponentWorkPreparation(service *Service, units *blueprintunits.Repository,
	queue *workqueue.Repository, inputs *componentwork.InputRepository,
) (*ComponentWorkPreparation, error) {
	if service == nil || units == nil || queue == nil || inputs == nil {
		return nil, errs.New(errs.KindInternal, "Blueprint Component preparation dependencies are missing")
	}
	return &ComponentWorkPreparation{service: service, units: units, queue: queue, inputs: inputs}, nil
}

func (owner *ComponentWorkPreparation) Prepare(ctx context.Context, taskID, workID string) error {
	work, err := owner.queue.ReadWork(ctx, taskID, workID, 0)
	if err != nil {
		return err
	}
	task, item := work.Task.Task, work.Item.Item
	if task.Action != workexecution.ActionApply || !task.PublicationComplete || task.Stop != nil ||
		task.FinishedAt != nil || task.CollectionStartedAt != nil || item.ForwardRevoked ||
		item.Operation != workexecution.Component || item.Placement.Executor != workexecution.Agent ||
		item.Phase != workexecution.Waiting || item.Wait == nil || item.Wait.Reason != workexecution.WaitInput ||
		item.Input != nil || item.AttemptNumber != 0 {
		return errs.New(errs.KindStateConflict, "Blueprint Component is not waiting for executable input")
	}
	snapshot, err := owner.units.LoadAtRevision(ctx, task.Scope.EnvironmentID, work.ReadRevision)
	if err != nil {
		return err
	}
	if snapshot.HeadTaskID != task.ID || snapshot.Desired == nil ||
		snapshot.Desired.Record.ParentTaskID != task.ID || !snapshot.Desired.Record.Complete {
		return errs.New(errs.KindStateConflict, "Blueprint Component no longer belongs to the current complete Apply")
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
		if captured.TaskID != item.TaskID || captured.Operation != item.Operation ||
			captured.TargetID != item.TargetID ||
			captured.Placement != item.Placement ||
			!slices.Equal(captured.Dependencies, item.Dependencies) ||
			!slices.Equal(captured.Claims, item.Claims) ||
			captured.Input != nil ||
			!captured.CreatedAt.Equal(item.CreatedAt) ||
			!captured.Deadline.Equal(item.Deadline) {
			return errs.New(errs.KindStateConflict, "Blueprint Component work differs from its accepted graph")
		}
		unit = &graph.Record.Units[index]
		break
	}
	if unit == nil || unit.Target.Kind != ids.KindComponent {
		return errs.New(errs.KindStateConflict, "Blueprint graph has no Component for this work")
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
	var services []workqueue.View
	writes := make(map[string]bool, len(unit.Writes))
	for _, target := range unit.Writes {
		writes[target.ID] = true
	}
	for _, captured := range graph.Record.Items {
		if captured.Operation != workexecution.Release || !writes[captured.TargetID] {
			continue
		}
		service, err := owner.queue.ReadWork(ctx, task.ID, captured.ID, work.ReadRevision)
		if err != nil {
			return err
		}
		if service.Item.Item.TargetID != captured.TargetID || service.Item.Item.Placement != captured.Placement ||
			!slices.Equal(service.Item.Item.Dependencies, captured.Dependencies) ||
			!slices.Equal(service.Item.Item.Claims, captured.Claims) {
			return errs.New(errs.KindStateConflict, "Proxy Service work differs from the accepted graph")
		}
		services = append(services, service)
	}
	prepared, err := owner.service.prepareComponentWork(ctx, work, services, *unit, snapshot)
	if err != nil {
		return err
	}
	defer keyvalue.ClearMutationValues(prepared.Placement.Change.Mutations)
	guards, err := owner.service.repository.ComponentWorkConfigurationConditions(
		ctx,
		work,
		prepared.Capture.Configuration,
	)
	if err != nil {
		return err
	}
	if prepared.Capture.Proxy != nil {
		if err := owner.service.repository.ValidateAgentProxyPublication(ctx, work, *prepared.Capture.Proxy, prepared.Placement.Change); err != nil {
			return err
		}
	}
	guards = append(
		guards,
		keyvalue.Condition{
			Key:         blueprintunits.CapturedGraphKey(task.Scope.EnvironmentID, task.ID),
			ModRevision: graph.Revision,
		},
		keyvalue.Condition{
			Key:         blueprints.EnvironmentBlueprintHeadKey(task.Scope.EnvironmentID),
			ModRevision: snapshot.HeadRevision,
		},
		keyvalue.Condition{Key: blueprintunits.EpochKey(task.Scope.EnvironmentID), ModRevision: snapshot.EpochRevision},
		keyvalue.Condition{
			Key:         blueprintunits.DesiredPlanKey(task.Scope.EnvironmentID),
			ModRevision: snapshot.Desired.Revision,
		},
	)
	inputID := ids.DeriveAt(ids.KindPlan, item.CreatedAt, item.ID, "component-input")
	_, change, err := owner.inputs.PreparePublication(ctx, work, inputID, prepared.Input, prepared.Capture,
		prepared.Secrets, dependencies, time.Now().UTC())
	if err != nil {
		return err
	}
	change.Conditions = append(change.Conditions, guards...)
	change.Conditions = append(change.Conditions, prepared.Placement.Change.Conditions...)
	change.Mutations = append(change.Mutations, prepared.Placement.Change.Mutations...)
	defer keyvalue.ZeroMutationBytes(change.Mutations)
	if !time.Now().Before(task.Deadline) || !time.Now().Before(item.Deadline) {
		return errs.New(errs.KindStateConflict, "Blueprint Component preparation exceeded its deadline")
	}
	result, err := owner.queue.Commit(ctx, change)
	keyvalue.ClearValues(result.FailureReads)
	if err != nil {
		return err
	}
	if !result.Succeeded {
		return errs.New(errs.KindStateConflict, "Blueprint Component inputs changed during preparation")
	}
	return nil
}
