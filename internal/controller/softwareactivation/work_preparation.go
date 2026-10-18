package softwareactivation

import (
	"context"
	"slices"
	"time"

	upgrade "github.com/AlanD20/groundplane/internal/common/controllerupgrade"
	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/internal/infra/etcd/agentoperations"
	"github.com/AlanD20/groundplane/internal/infra/etcd/controllersoftware"
	"github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	activation "github.com/AlanD20/groundplane/internal/infra/etcd/softwareactivation"
	"github.com/AlanD20/groundplane/internal/infra/etcd/workqueue"
	"github.com/AlanD20/groundplane/pkg/errs"
)

// Prepare binds exact staged outputs to a waiting item in the same visible Task.
// It neither publishes another Task nor selects a new release or predecessor.
func (service *Service) Prepare(ctx context.Context, taskID, workID string) error {
	view, err := service.queue.ReadWork(ctx, taskID, workID, 0)
	if err != nil {
		return err
	}
	task, item := view.Task.Task, view.Item.Item
	if task.Action != workexecution.ActionActivate || task.TargetID != activation.TargetID() || !task.PublicationComplete || task.Stop != nil || task.FinishedAt != nil ||
		item.Phase != workexecution.Waiting || item.Wait == nil || item.Wait.Reason != workexecution.WaitInput || item.Input != nil || item.ForwardRevoked || item.AttemptNumber != 0 || !time.Now().Before(item.Deadline) {
		return errs.New(errs.KindStateConflict, "software activation item is not waiting for captured input")
	}
	record, err := service.records.At(ctx, taskID, view.ReadRevision)
	if err != nil {
		return err
	}
	_, graph, err := record.Record.Graph()
	if err != nil {
		return err
	}
	matched := false
	for _, expected := range graph {
		if expected.ID != item.ID {
			continue
		}
		matched = expected.Operation == item.Operation && expected.TargetID == item.TargetID && expected.Placement == item.Placement && slices.Equal(expected.Claims, item.Claims) &&
			slices.Equal(expected.Dependencies, item.Dependencies) && expected.Input == nil && expected.CreatedAt.Equal(item.CreatedAt) && expected.Deadline.Equal(item.Deadline)
	}
	if !matched {
		return errs.New(errs.KindStateConflict, "software activation work differs from its accepted graph")
	}
	dependencies := make([]workqueue.ItemSnapshot, 0, len(item.Dependencies))
	for _, id := range item.Dependencies {
		dependency, err := service.queue.ReadWork(ctx, taskID, id, view.ReadRevision)
		if err != nil {
			return err
		}
		dependencies = append(dependencies, dependency.Item)
	}
	if _, err := workqueue.DependencyConditions(item, dependencies); err != nil {
		return err
	}
	stage, err := service.queue.ReadWork(ctx, taskID, record.Record.WorkItemID, view.ReadRevision)
	if err != nil {
		return err
	}
	if stage.Item.Item.Phase != workexecution.Settled || stage.Item.Item.Outcome != workexecution.Succeeded || stage.Item.Item.Result == nil || stage.Item.Item.AttemptNumber == 0 {
		return errs.New(errs.KindStateConflict, "software activation has no successful staging proof")
	}
	_, attempt, err := service.queue.ReadAttempt(ctx, taskID, stage.Item.Item.ID, stage.Item.Item.AttemptNumber, view.ReadRevision)
	if err != nil {
		return err
	}
	saved, found, err := service.records.StagingResult(ctx, attempt.Attempt, view.ReadRevision)
	if err != nil {
		return err
	}
	if !found || attempt.Attempt.FinishedAt == nil || attempt.Attempt.Result == nil {
		return errs.New(errs.KindStateConflict, "software staging has no accepted receipt")
	}
	stageProof, completion, err := activation.PrepareStagingResult(record, saved, attempt.Attempt)
	if err != nil {
		return err
	}
	if !completion.EffectsSettled || completion.Outcome != workexecution.Succeeded || *stage.Item.Item.Result != completion.Evidence || *attempt.Attempt.Result != completion.Evidence ||
		record.Record.Progress.Phase != activation.PhaseStaged {
		return errs.New(errs.KindStateConflict, "software staging receipt does not prove the selected artifacts")
	}
	stageProof.Conditions = append(stageProof.Conditions, keyvalue.Condition{Key: workqueue.ItemKey(stage.Item.Item.ID), ModRevision: stage.Item.Revision}, keyvalue.Condition{Key: workqueue.AttemptKey(attempt.Attempt.ID), ModRevision: attempt.Revision})
	var prepared workqueue.Change
	switch item.Operation {
	case workexecution.ControllerSoftware:
		release := saved.Record.Result.Controller
		if release == nil {
			return errs.New(errs.KindStateConflict, "Controller activation has no captured staged release")
		}
		if release.Manifest.ControllerSHA256 == record.Record.Input.PreviousController {
			return service.rejectUnit(ctx, view, record, stageProof, "The Controller already runs the selected prepared binary. Prepare a different Controller build or select Agent-only.")
		}
		if err := release.Manifest.Validate(); err != nil {
			return service.rejectUnit(ctx, view, record, stageProof, "The prepared Controller has an incompatible storage or channel format. Prepare software for this installation before applying it.")
		}
		capture := controllersoftware.Capture{TaskID: taskID, WorkItemID: item.ID, PlanID: ids.DeriveAt(ids.KindPlan, record.Record.CreatedAt, item.ID, "controller-activation"), CreatedAt: record.Record.CreatedAt,
			Input: upgrade.Input{Release: release.Release, Manifest: release.Manifest, PreviousController: record.Record.Input.PreviousController, Agent: record.Record.Input.Agent}}
		prepared, err = service.records.PrepareControllerInput(ctx, view, record, capture, dependencies)
	case workexecution.AgentSoftware:
		predecessor := record.Record.Input.Agent
		if predecessor == nil || !saved.Record.Result.Agent {
			return errs.New(errs.KindStateConflict, "Agent activation has no captured staged image or predecessor")
		}
		capture := agentoperations.Capture{TaskID: taskID, WorkItemID: item.ID, PlanID: ids.DeriveAt(ids.KindPlan, record.Record.CreatedAt, item.ID, "agent-activation"), CreatedAt: record.Record.CreatedAt,
			AgentID: predecessor.ID, Action: workexecution.ActionUpdate, Image: record.Record.Input.Preparation.Progress.Result.Agent.Artifact.Reference, PreviousImage: predecessor.Image, StartingGeneration: predecessor.Generation}
		if capture.Image == capture.PreviousImage {
			return service.rejectUnit(ctx, view, record, stageProof, "The Agent already runs the selected prepared image. Prepare a different Agent build or select Controller-only.")
		}
		prepared, err = service.records.PrepareAgentInput(view, record, capture, dependencies)
	default:
		return errs.New(errs.KindStateConflict, "software activation item has no input owner")
	}
	if err != nil {
		return err
	}
	defer keyvalue.ClearMutationValues(prepared.Mutations)
	committed, err := service.queue.Commit(ctx, stageProof, prepared)
	if err != nil {
		return err
	}
	if !committed.Succeeded {
		return errs.New(errs.KindStateConflict, "software activation changed before executable input publication")
	}
	return nil
}

func (service *Service) rejectUnit(ctx context.Context, view workqueue.View, record keyvalue.Versioned[activation.Record], proof workqueue.Change, reason string) error {
	rejected, err := workqueue.PrepareInputRejection(view, reason, time.Now().UTC())
	if err != nil {
		return err
	}
	defer keyvalue.ClearMutationValues(rejected.Mutations)
	guard := activationGuard(record)
	committed, err := service.queue.Commit(ctx, guard, proof, rejected)
	if err != nil {
		return err
	}
	if !committed.Succeeded {
		return errs.New(errs.KindStateConflict, "software input changed before its rejection was recorded")
	}
	return nil
}
