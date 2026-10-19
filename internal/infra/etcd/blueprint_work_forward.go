package etcd

import (
	"context"
	"slices"

	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/internal/infra/etcd/blueprints"
	"github.com/AlanD20/groundplane/internal/infra/etcd/blueprintunits"
	"github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/releases"
	"github.com/AlanD20/groundplane/internal/infra/etcd/workqueue"
	"github.com/AlanD20/groundplane/internal/infra/serviceruntimerecord"
	"github.com/AlanD20/groundplane/pkg/errs"
)

// LoadReleaseForwardSources is first-attempt admission, not a recovery
// reader. It keeps current-head and unchanged-predecessor checks out of recovery,
// which must retain access to captured input after partial effects or supersession.
// The returned comparisons join assignment; this read never authorizes effects.
func (ledger *ReleaseLedger) LoadReleaseForwardSources(ctx context.Context,
	work workqueue.View,
) (ReleaseWorkSources, error) {
	var empty ReleaseWorkSources
	task, item := work.Task.Task, work.Item.Item
	if task.Stop != nil || task.FinishedAt != nil || task.CollectionStartedAt != nil || !task.PublicationComplete ||
		item.Phase != workexecution.Ready || item.AttemptNumber != 0 || item.ActiveAttemptID != "" ||
		item.ForwardRevoked || item.Cleanup != workexecution.CleanupNone {
		return empty, errs.New(
			errs.KindStateConflict,
			"Blueprint Release is not ready for its first forward assignment",
		)
	}
	return ledger.loadReleaseForwardSources(ctx, work)
}

// LoadReleaseStartSources repeats current intent and predecessor
// comparisons at the first-effect boundary, not just when reserving capacity.
func (ledger *ReleaseLedger) LoadReleaseStartSources(
	ctx context.Context,
	view workqueue.ResumeView,
) (ReleaseWorkSources, error) {
	task, item, attempt := view.Work.Task.Task, view.Work.Item.Item, view.Attempt.Attempt
	if !view.CurrentExecutor || !view.ActiveAttempt || attempt.Direction != workexecution.Forward ||
		attempt.Number != 1 ||
		attempt.StartedAt != nil ||
		workexecution.ValidateDelivery(item, attempt) != nil ||
		task.Stop != nil ||
		task.FinishedAt != nil ||
		task.CollectionStartedAt != nil ||
		!task.PublicationComplete ||
		item.Phase != workexecution.Assigned ||
		item.ForwardRevoked ||
		item.Cleanup != workexecution.CleanupNone {
		return ReleaseWorkSources{}, errs.New(
			errs.KindStateConflict,
			"Blueprint Release cannot authorize its first forward effect",
		)
	}
	return ledger.loadReleaseForwardSources(ctx, view.Work)
}

// LoadReleaseCheckpointSources guards a new forward effect, including
// proxy cutover. A Start receipt does not indefinitely authorize later effects
// after a newer Blueprint has replaced the accepted intent. Confirm/recovery
// readers intentionally do not use this current-head gate.
func (ledger *ReleaseLedger) LoadReleaseCheckpointSources(
	ctx context.Context,
	view workqueue.ResumeView,
) (ReleaseWorkSources, error) {
	task, item, attempt := view.Work.Task.Task, view.Work.Item.Item, view.Attempt.Attempt
	if !view.CurrentExecutor || !view.ActiveAttempt || attempt.Direction != workexecution.Forward ||
		attempt.StartedAt == nil || attempt.FinishedAt != nil || workexecution.ValidateDelivery(item, attempt) != nil ||
		task.Stop != nil || task.FinishedAt != nil || task.CollectionStartedAt != nil || !task.PublicationComplete ||
		item.Phase != workexecution.Running || item.ForwardRevoked || item.Cleanup != workexecution.CleanupNone {
		return ReleaseWorkSources{}, errs.New(
			errs.KindStateConflict,
			"Blueprint Release cannot authorize another forward effect",
		)
	}
	return ledger.loadReleaseForwardSources(ctx, view.Work)
}

func (ledger *ReleaseLedger) loadReleaseForwardSources(
	ctx context.Context,
	work workqueue.View,
) (ReleaseWorkSources, error) {
	var empty ReleaseWorkSources
	task, item := work.Task.Task, work.Item.Item
	sources, err := ledger.LoadReleaseWorkInput(ctx, work)
	if err != nil {
		return empty, err
	}
	if task.Action == workexecution.ActionDeploy || task.Action == workexecution.ActionRollback {
		request, guard, err := ledger.ReadDirectReleasePreparation(ctx, work)
		if err != nil {
			return empty, err
		}
		if err := request.Matches(task, item, sources.Rollout.Record); err != nil {
			return empty, err
		}
		conditions, err := ledger.directReleaseSourceConditions(
			ctx,
			task.Scope,
			request.Source,
			item.TargetID,
			work.ReadRevision,
		)
		if err != nil {
			return empty, err
		}
		sources.Conditions = append(sources.Conditions, conditions...)
		sources.Conditions = append(sources.Conditions, guard)
	} else if task.Action == workexecution.ActionApply {
		units, err := blueprintunits.NewRepository(ledger.store)
		if err != nil {
			return empty, err
		}
		snapshot, err := units.LoadAtRevision(ctx, task.Scope.EnvironmentID, work.ReadRevision)
		if err != nil {
			return empty, err
		}
		if snapshot.HeadTaskID != task.ID || snapshot.Desired == nil ||
			snapshot.Desired.Record.ParentTaskID != task.ID || !snapshot.Desired.Record.Complete {
			return empty, errs.New(errs.KindStateConflict, "Blueprint Release was superseded before assignment")
		}
		graph, err := units.LoadCapturedGraph(ctx, task, work.ReadRevision)
		if err != nil {
			return empty, err
		}
		index := slices.IndexFunc(graph.Record.Items, func(candidate workexecution.Item) bool { return candidate.ID == item.ID })
		if index < 0 {
			return empty, errs.New(errs.KindStateConflict, "Blueprint Release is absent from its captured graph")
		}
		captured := graph.Record.Items[index]
		if captured.Operation != item.Operation || captured.TargetID != item.TargetID || captured.Placement != item.Placement ||
			!slices.Equal(captured.Dependencies, item.Dependencies) || !slices.Equal(captured.Claims, item.Claims) ||
			!captured.CreatedAt.Equal(item.CreatedAt) || !captured.Deadline.Equal(item.Deadline) || graph.Record.Units[index].Removal {
			return empty, errs.New(errs.KindStateConflict, "Blueprint Release differs from its accepted resource work")
		}
		sources.Conditions = append(sources.Conditions,
			keyvalue.Condition{Key: blueprints.EnvironmentBlueprintHeadKey(task.Scope.EnvironmentID), ModRevision: snapshot.HeadRevision},
			keyvalue.Condition{Key: blueprintunits.EpochKey(task.Scope.EnvironmentID), ModRevision: snapshot.EpochRevision},
			keyvalue.Condition{Key: blueprintunits.DesiredPlanKey(task.Scope.EnvironmentID), ModRevision: snapshot.Desired.Revision},
			keyvalue.Condition{Key: blueprintunits.CapturedGraphKey(task.Scope.EnvironmentID, task.ID), ModRevision: graph.Revision},
		)
		prerequisites, err := ledger.BlueprintWorkPrerequisiteConditions(ctx, work, sources.Rollout.Record.Render.Projection.BlueprintRequirements)
		if err != nil {
			return empty, err
		}
		sources.Conditions = append(sources.Conditions, prerequisites...)
	} else {
		return empty, errs.New(errs.KindStateConflict, "Task action cannot authorize forward Release effects")
	}
	configuration, err := ledger.ReleaseWorkConfigurationConditions(ctx, work, sources.Input.Configuration)
	if err != nil {
		return empty, err
	}
	sources.Conditions = append(sources.Conditions, configuration...)
	for _, predecessor := range sources.Predecessors {
		keys := []string{
			releases.ReleaseProjectionKey(predecessor.ServiceID),
			serviceruntimerecord.Key(predecessor.ServiceID),
		}
		read, err := ledger.store.GetMany(ctx, keyvalue.GetManyRequest{Keys: keys, Revision: work.ReadRevision})
		if err != nil {
			return empty, err
		}
		if read == nil || read.ReadRevision != work.ReadRevision || len(read.Values) != len(keys) {
			return empty, releases.CorruptReleaseRecord()
		}
		expected := []int64{predecessor.ProjectionRevision, predecessor.RuntimeRevision}
		for index, key := range keys {
			value := read.Values[index]
			if expected[index] == 0 && value != nil || expected[index] > 0 &&
				(value == nil || value.Key != key || value.ModRevision != expected[index]) {
				keyvalue.ClearValues(read.Values)
				return empty, errs.New(
					errs.KindStateConflict,
					"Service runtime changed after Blueprint Release preparation",
				)
			}
			sources.Conditions = append(sources.Conditions, keyvalue.Condition{Key: key, ModRevision: expected[index]})
		}
		keyvalue.ClearValues(read.Values)
	}
	return sources, nil
}
