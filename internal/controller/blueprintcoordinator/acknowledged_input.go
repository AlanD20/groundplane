package blueprintcoordinator

import (
	"context"
	"slices"
	"time"

	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/internal/core/blueprintreconcile"
	"github.com/AlanD20/groundplane/internal/infra/etcd/blueprints"
	"github.com/AlanD20/groundplane/internal/infra/etcd/blueprintunits"
	"github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/workqueue"
	"github.com/AlanD20/groundplane/pkg/errs"
)

// The resource ledger can prove that this exact captured intent is already
// applied. Acknowledgement never runs an operation or manufactures a new effect
// receipt; the original receipt remains authoritative and is fenced by its epoch.
func (owner *InputPreparation) acknowledgeApplied(ctx context.Context, view workqueue.View) (bool, error) {
	task, item := view.Task.Task, view.Item.Item
	if item.Input != nil || item.Phase != workexecution.Waiting || item.AttemptNumber != 0 ||
		task.Stop != nil || task.FinishedAt != nil || !task.PublicationComplete {
		return false, nil
	}
	snapshot, err := owner.units.LoadAtRevision(ctx, task.Scope.EnvironmentID, view.ReadRevision)
	if err != nil {
		return false, err
	}
	if snapshot.Desired == nil {
		return false, nil
	}
	if snapshot.HeadTaskID != task.ID || snapshot.Desired.Record.ParentTaskID != task.ID ||
		!snapshot.Desired.Record.Complete {
		return false, errs.New(
			errs.KindStateConflict,
			"Blueprint acknowledgement requires its complete captured intent",
		)
	}
	graph, err := owner.units.LoadCapturedGraph(ctx, task, view.ReadRevision)
	if err != nil {
		return false, err
	}
	var unit *blueprintunits.Unit
	for index, captured := range graph.Record.Items {
		if captured.ID == item.ID {
			if captured.Operation != item.Operation || captured.Placement != item.Placement ||
				!slices.Equal(captured.Claims, item.Claims) || !slices.Equal(captured.Dependencies, item.Dependencies) {
				return false, errs.New(errs.KindStateConflict, "Captured Blueprint work authority changed")
			}
			unit = &graph.Record.Units[index]
			break
		}
	}
	if unit == nil || unit.Target.ID != item.TargetID {
		return false, errs.New(errs.KindStateConflict, "Captured Blueprint work membership changed")
	}
	// Removal must still retire the definition and its retained runtime authority.
	if unit.Removal {
		return false, nil
	}
	selection, err := blueprintunits.Select(snapshot)
	if err != nil {
		return false, err
	}
	if !slices.ContainsFunc(snapshot.Desired.Record.Units, func(candidate blueprintunits.Unit) bool {
		return candidate.Target == unit.Target && candidate.Fingerprint == unit.Fingerprint &&
			candidate.Removal == unit.Removal && slices.Equal(candidate.Reads, unit.Reads) &&
			slices.Equal(candidate.Writes, unit.Writes) && slices.Equal(candidate.After, unit.After)
	}) {
		return false, errs.New(errs.KindStateConflict, "Captured Blueprint resource intent changed")
	}
	if !slices.Contains(
		selection.Satisfied,
		blueprintreconcile.ResourceKey{Kind: unit.Target.Kind, ID: unit.Target.ID},
	) {
		return false, nil
	}
	dependencies := make([]workqueue.ItemSnapshot, 0, len(item.Dependencies))
	for _, id := range item.Dependencies {
		dependency, err := owner.queue.ReadWork(ctx, task.ID, id, view.ReadRevision)
		if err != nil {
			return false, err
		}
		dependencies = append(dependencies, dependency.Item)
	}
	guards, err := workqueue.DependencyConditions(item, dependencies)
	if err != nil {
		return false, err
	}
	change, err := workqueue.PrepareInputDecision(view, task.Intent, workexecution.Succeeded,
		"Configuration is already applied; no host execution is needed", time.Now().UTC())
	if err != nil {
		return false, err
	}
	defer keyvalue.ClearMutationValues(change.Mutations)
	change.Conditions = append(change.Conditions, guards...)
	change.Conditions = append(
		change.Conditions,
		keyvalue.Condition{
			Key:         blueprints.EnvironmentBlueprintHeadKey(snapshot.EnvironmentID),
			ModRevision: snapshot.HeadRevision,
		},
		keyvalue.Condition{Key: blueprintunits.EpochKey(snapshot.EnvironmentID), ModRevision: snapshot.EpochRevision},
		keyvalue.Condition{
			Key:         blueprintunits.DesiredPlanKey(snapshot.EnvironmentID),
			ModRevision: snapshot.Desired.Revision,
		},
		keyvalue.Condition{
			Key:         blueprintunits.CapturedGraphKey(snapshot.EnvironmentID, task.ID),
			ModRevision: graph.Revision,
		},
	)
	for _, claim := range item.Claims {
		change.Conditions = append(change.Conditions, keyvalue.Condition{Key: workqueue.ClaimKey(claim)})
	}
	result, err := owner.queue.Commit(ctx, change)
	keyvalue.ClearValues(result.FailureReads)
	if err != nil {
		return false, err
	}
	if !result.Succeeded {
		return false, errs.New(errs.KindStateConflict, "Applied configuration changed before acknowledgement")
	}
	return true, nil
}
