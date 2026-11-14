package detachwork

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"slices"
	"time"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/internal/controller/attachplan"
	"github.com/AlanD20/groundplane/internal/infra/etcd/attachremoval"
	"github.com/AlanD20/groundplane/internal/infra/etcd/blueprintunits"
	"github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/workqueue"
	"github.com/AlanD20/groundplane/pkg/errs"
)

// Prepare derives executable input only after captured prerequisites settle.
// Its admission snapshot owns all operator decisions and encrypted hook inputs.
func (owner *Owner) Prepare(ctx context.Context, taskID, workID string) error {
	view, err := owner.queue.ReadWork(ctx, taskID, workID, 0)
	if err != nil {
		return err
	}
	task, item := view.Task.Task, view.Item.Item
	at := time.Now().UTC()
	if task.Action != workexecution.ActionApply || !task.PublicationComplete || task.Stop != nil ||
		task.FinishedAt != nil || task.CollectionStartedAt != nil || item.Operation != workexecution.Detach ||
		item.Input != nil || item.Phase != workexecution.Waiting || item.Wait == nil || item.Wait.Reason != workexecution.WaitInput ||
		item.ForwardRevoked || item.AttemptNumber != 0 || !at.Before(task.Deadline) || !at.Before(item.Deadline) {
		return errs.New(errs.KindStateConflict, "Blueprint Detach is not waiting for input")
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
			!slices.Equal(captured.Dependencies, item.Dependencies) {
			return errs.New(errs.KindStateConflict, "Blueprint Detach changed its captured effects")
		}
		unit = &graph.Record.Units[index]
	}
	if unit == nil || !unit.Removal || unit.Target.Kind != ids.KindAttach {
		return errs.New(errs.KindStateConflict, "Blueprint Detach has no captured removal unit")
	}
	snapshot, err := owner.units.LoadAtRevision(ctx, task.Scope.EnvironmentID, view.ReadRevision)
	if err != nil {
		return err
	}
	if snapshot.HeadTaskID != task.ID || snapshot.Desired == nil || snapshot.Desired.Record.ParentTaskID != task.ID {
		return errs.New(errs.KindStateConflict, "Blueprint Detach is no longer the selected Environment intent")
	}
	current, headGuards, err := owner.units.ReadWorkIntentMatch(ctx, view)
	if err != nil {
		return err
	}
	if !current {
		return errs.New(errs.KindStateConflict, "Blueprint Detach was superseded before preparation")
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
	frozen, captureGuard, err := owner.inputs.LoadBlueprintInput(ctx, view)
	if err != nil {
		return err
	}
	defer frozen.Clear()
	guards := append(headGuards, dependencyGuards...)
	guards = append(
		guards,
		captureGuard,
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
	var change workqueue.Change

	changed, sourceGuards, err := owner.inputs.BlueprintSourceChange(ctx, view, frozen)
	if err != nil {
		return err
	}
	if changed {
		change, err = workqueue.PrepareInputRejection(
			view,
			"Connection cleanup inputs changed after Apply was accepted. Submit a new Blueprint Apply to capture the current connection and backing configuration.",
			at,
		)
		if err != nil {
			return err
		}
		change.Conditions = append(change.Conditions, sourceGuards...)
	} else if frozen.Snapshot == nil {
		change, err = owner.prepareDeclaredDetachAbsence(ctx, view, snapshot, *unit, frozen, captureGuard, at)
	} else {
		var refreshed attachremoval.DetachSnapshot
		var runtimeGuards []keyvalue.Condition
		refreshed, runtimeGuards, err = owner.inputs.RefreshBlueprintSnapshot(ctx, view, frozen, graph.Record.Items)
		if err != nil {
			return err
		}
		defer refreshed.Clear()
		consumer, consumerErr := attachplan.ConsumerTransition(ctx, refreshed, nil,
			ids.DeriveAt(ids.KindConfig, item.CreatedAt, item.ID, "detach-consumer"))
		if consumerErr != nil {
			return consumerErr
		}
		input, inputErr := attachremoval.NewDetachInput(ctx, task.ID, item.ID, &refreshed, nil, consumer)
		if consumer != nil {
			if consumer.Baseline != nil {
				clear(consumer.Baseline.CanonicalYaml)
			}
			if consumer.Candidate != nil {
				clear(consumer.Candidate.CanonicalYaml)
			}
		}
		if inputErr != nil {
			return inputErr
		}
		defer input.Clear()
		change, err = owner.inputs.PrepareBlueprintInput(ctx, view, input, frozen, append(guards, runtimeGuards...), at)
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
		return errs.New(
			errs.KindStateConflict,
			"Blueprint Detach preparation raced resource ownership; reload the Task",
		)
	}
	return nil
}

func (owner *Owner) prepareDeclaredDetachAbsence(ctx context.Context, view workqueue.View,
	snapshot blueprintunits.Snapshot, unit blueprintunits.Unit, frozen attachremoval.BlueprintInput,
	capture keyvalue.Condition, at time.Time,
) (workqueue.Change, error) {
	birth, absent, err := blueprintunits.DeclarationAbsenceConditions(
		snapshot,
		unit.Target,
		frozen.BirthRevisionID,
		view.Item.Item.ID,
	)
	if err != nil {
		return workqueue.Change{}, err
	}
	if !absent {
		return workqueue.Change{}, errs.New(
			errs.KindStateConflict,
			"Missing Attach has no proof that provisioning never ran",
		)
	}
	actual, err := owner.inputs.BlueprintAbsenceConditions(ctx, view, frozen)
	if err != nil {
		return workqueue.Change{}, err
	}
	raw, err := attachremoval.EncodeBlueprintInput(ctx, frozen)
	if err != nil {
		return workqueue.Change{}, err
	}
	digest := sha256.Sum256(raw)
	clear(raw)
	reference := workexecution.Input{ID: ids.DeriveAt(ids.KindPlan, view.Task.Task.CreatedAt, view.Item.Item.ID,
		"detach-declaration"), SHA256: hex.EncodeToString(digest[:])}
	receipt, err := blueprintunits.PrepareControllerDetachRemoval(
		snapshot,
		unit,
		frozen.OperationID,
		capture.ModRevision,
		reference,
	)
	if err != nil {
		return workqueue.Change{}, err
	}
	change, err := workqueue.PrepareInputDecision(view, view.Task.Task.Intent, workexecution.Succeeded,
		"Removed an unprovisioned connection declaration; no binding, generated facts or host effects existed", at)
	if err != nil {
		receipt.Clear()
		return workqueue.Change{}, err
	}
	change.Conditions = append(change.Conditions, birth...)
	change.Conditions = append(change.Conditions, actual...)
	change.Conditions = append(change.Conditions, receipt.Conditions()...)
	change.Mutations = append(change.Mutations, receipt.Mutations()...)
	return change, nil
}
