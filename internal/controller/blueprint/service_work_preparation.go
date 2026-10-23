package blueprint

import (
	"context"
	"errors"
	"slices"
	"time"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/internal/infra/etcd/blueprints"
	"github.com/AlanD20/groundplane/internal/infra/etcd/blueprintunits"
	"github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/workqueue"
	"github.com/AlanD20/groundplane/pkg/errs"
)

// ServiceWorkPreparation resolves waiting Service input without creating another
// Task. It owns preparation/publication only, never assignment or host execution.
type ServiceWorkPreparation struct {
	service *Service
	units   *blueprintunits.Repository
	queue   *workqueue.Repository
}

func NewServiceWorkPreparation(service *Service, units *blueprintunits.Repository,
	queue *workqueue.Repository,
) (*ServiceWorkPreparation, error) {
	if service == nil || service.blueprintReleases == nil || units == nil || queue == nil {
		return nil, errs.New(errs.KindInternal, "Blueprint Service preparation dependencies are missing")
	}
	return &ServiceWorkPreparation{service: service, units: units, queue: queue}, nil
}

func (owner *ServiceWorkPreparation) Prepare(ctx context.Context, taskID, workID string) error {
	work, err := owner.queue.ReadWork(ctx, taskID, workID, 0)
	if err != nil {
		return err
	}
	task, item := work.Task.Task, work.Item.Item
	if task.Action != workexecution.ActionApply || !task.PublicationComplete || task.Stop != nil ||
		task.FinishedAt != nil || task.CollectionStartedAt != nil || item.ForwardRevoked ||
		item.Operation != workexecution.Release || item.Placement.Executor != workexecution.Agent ||
		item.Phase != workexecution.Waiting || item.Wait == nil || item.Wait.Reason != workexecution.WaitInput ||
		item.Input != nil || item.AttemptNumber != 0 || !time.Now().Before(task.Deadline) || !time.Now().Before(item.Deadline) {
		return errs.New(errs.KindStateConflict, "Blueprint Service is not waiting for executable input")
	}
	snapshot, err := owner.units.LoadAtRevision(ctx, task.Scope.EnvironmentID, work.ReadRevision)
	if err != nil {
		return err
	}
	if snapshot.HeadTaskID != task.ID || snapshot.Desired == nil ||
		snapshot.Desired.Record.ParentTaskID != task.ID || !snapshot.Desired.Record.Complete {
		return errs.New(errs.KindStateConflict, "Blueprint Service no longer belongs to the current complete Apply")
	}
	ready, err := owner.service.PrepareRuntimeProjection(ctx, task, snapshot)
	if err != nil || !ready {
		return err
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
			return errs.New(errs.KindStateConflict, "Blueprint Service work differs from its accepted graph")
		}
		unit = &graph.Record.Units[index]
		break
	}
	if unit == nil || unit.Target.Kind != ids.KindService || unit.Removal {
		return errs.New(errs.KindStateConflict, "Blueprint graph has no deployable Service for this work")
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
	waiting, err := owner.waitsForServiceConfigurationTurn(ctx, work)
	if err != nil || waiting {
		return err
	}
	input, err := owner.service.prepareAuthoredServiceWorkInput(ctx, work, *unit)
	if err != nil {
		return err
	}
	prepared, err := owner.service.blueprintReleases.PrepareServiceUnit(ctx, input.Release)
	if err != nil {
		return err
	}
	defer prepared.Clear()
	publication, err := prepared.Publication.WithZoneDNSActivation(input.DNS)
	if err != nil {
		return errors.Join(err, prepared.Abandon(ctx))
	}
	prepared.Publication = publication
	change, err := prepared.Change(dependencies)
	if err != nil {
		return errors.Join(err, prepared.Abandon(ctx))
	}
	defer keyvalue.ZeroMutationBytes(change.Mutations)
	change.Conditions = append(
		change.Conditions,
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
	if !time.Now().Before(task.Deadline) || !time.Now().Before(item.Deadline) {
		return errors.Join(
			errs.New(errs.KindStateConflict, "Blueprint Service preparation exceeded its deadline"),
			prepared.Abandon(ctx),
		)
	}
	result, err := owner.queue.Commit(ctx, change)
	if err != nil {
		// A transport failure may hide a successful publication. Retain staged
		// sources; the next sweep reconciles the same Task/work identity.
		return err
	}
	keyvalue.ClearValues(result.FailureReads)
	if !result.Succeeded {
		return errors.Join(
			errs.New(errs.KindStateConflict, "Blueprint Service inputs changed during preparation"),
			prepared.Abandon(ctx),
		)
	}
	return nil
}
