package softwareactivation

import (
	"context"
	"time"

	upgrade "github.com/AlanD20/groundplane/internal/common/controllerupgrade"
	"github.com/AlanD20/groundplane/internal/common/taskprogress"
	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/internal/infra/etcd/agentoperations"
	"github.com/AlanD20/groundplane/internal/infra/etcd/controllersoftware"
	"github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	activation "github.com/AlanD20/groundplane/internal/infra/etcd/softwareactivation"
	"github.com/AlanD20/groundplane/internal/infra/etcd/workqueue"
	"github.com/AlanD20/groundplane/pkg/errs"
)

func (service *Service) PrepareFinish(ctx context.Context, finished workqueue.FinishedWork, at time.Time) (workqueue.Change, error) {
	task := finished.Task.Task
	if task.Action != workexecution.ActionActivate || task.TargetID != activation.TargetID() {
		return workqueue.Change{}, errs.New(errs.KindStateConflict, "software activation completion has a different requested action")
	}
	record, err := service.records.At(ctx, task.ID, finished.ReadRevision)
	if err != nil {
		return workqueue.Change{}, err
	}
	_, graph, err := record.Record.Graph()
	if err != nil {
		return workqueue.Change{}, err
	}
	if len(finished.Items) != len(graph) {
		return workqueue.Change{}, errs.New(errs.KindStateConflict, "software activation completion has incomplete work")
	}
	change := activationGuard(record)
	progress := record.Record.Progress
	allSucceeded := true
	for _, snapshot := range finished.Items {
		item := snapshot.Item
		matched := false
		for _, expected := range graph {
			if expected.ID == item.ID && expected.Operation == item.Operation && expected.TargetID == item.TargetID {
				matched = true
			}
		}
		if !matched || item.Phase != workexecution.Settled || item.ActiveAttemptID != "" || item.Result == nil {
			return workqueue.Change{}, errs.New(errs.KindStateConflict, "software activation completion includes unsettled or foreign work")
		}
		change.Conditions = append(change.Conditions, keyvalue.Condition{Key: workqueue.ItemKey(item.ID), ModRevision: snapshot.Revision})
		if item.Outcome != workexecution.Succeeded {
			allSucceeded = false
		}
		view := workqueue.View{Task: finished.Task, Item: snapshot, ReadRevision: finished.ReadRevision}
		if item.Input == nil {
			settlement, err := service.queue.ReadInputSettlement(ctx, view)
			if err != nil {
				return workqueue.Change{}, err
			}
			if settlement == nil {
				return workqueue.Change{}, errs.New(errs.KindStateConflict, "unprepared software work has no recorded settlement")
			}
			continue
		}
		part, controllerApplied, agentApplied, failure, err := service.finishedItem(ctx, view, record)
		if err != nil {
			return workqueue.Change{}, err
		}
		change.Conditions = append(change.Conditions, part.Conditions...)
		progress.ControllerApplied = progress.ControllerApplied || controllerApplied
		progress.AgentApplied = progress.AgentApplied || agentApplied
		if failure != nil && progress.ErrorCode == "" {
			progress.ErrorCode, progress.ErrorDetail = failure.Code, failure.Summary()
		}
	}
	progress.Phase = activation.PhaseCompleted
	if !allSucceeded {
		progress.Phase = activation.PhaseFailed
		if progress.ErrorCode == "" {
			failure := taskprogress.CaptureControllerFailure(errs.New(errs.KindRequestFailed, "software activation did not complete every selected component"))
			progress.ErrorCode, progress.ErrorDetail = failure.Code, failure.Summary()
		}
	}
	updated := record.Record
	updated.Progress, updated.UpdatedAt = progress, at
	raw, err := activation.Encode(updated)
	if err != nil {
		return workqueue.Change{}, err
	}
	change.Mutations = append(change.Mutations, keyvalue.Mutation{Type: keyvalue.MutationPut, Key: activation.Key(task.ID), Value: raw})
	return change, nil
}

func (service *Service) finishedItem(ctx context.Context, view workqueue.View, record keyvalue.Versioned[activation.Record]) (workqueue.Change, bool, bool, *taskprogress.Failure, error) {
	item := view.Item.Item
	if item.AttemptNumber == 0 {
		if item.Outcome == workexecution.Succeeded {
			return workqueue.Change{}, false, false, nil, errs.New(errs.KindStateConflict, "unstarted software work cannot claim activation")
		}
		var reference workexecution.Input
		var change workqueue.Change
		var err error
		switch item.Operation {
		case workexecution.SoftwareStaging:
			reference, err = record.Record.Reference()
			change = activationGuard(record)
		case workexecution.ControllerSoftware:
			repository := service.controllerRecords
			capture, loadErr := repository.Load(ctx, item, view.ReadRevision)
			if loadErr != nil {
				return workqueue.Change{}, false, false, nil, loadErr
			}
			reference, err = capture.Record.Reference()
			change = workqueue.Change{Conditions: []keyvalue.Condition{{Key: controllersoftware.InputKey(item.ID), ModRevision: capture.Revision}}}
		case workexecution.AgentSoftware:
			repository := service.agentRecords
			capture, loadErr := repository.Load(ctx, item, view.ReadRevision)
			if loadErr != nil {
				return workqueue.Change{}, false, false, nil, loadErr
			}
			reference, err = capture.Record.Reference()
			change = workqueue.Change{Conditions: []keyvalue.Condition{{Key: agentoperations.InputKey(item.ID), ModRevision: capture.Revision}}}
		default:
			err = errs.New(errs.KindStateConflict, "software activation has an unknown work operation")
		}
		if err != nil {
			return workqueue.Change{}, false, false, nil, err
		}
		if *item.Result != reference {
			return workqueue.Change{}, false, false, nil, errs.New(errs.KindStateConflict, "unstarted software work has a different captured rejection")
		}
		return change, false, false, nil, nil
	}
	_, attempt, err := service.queue.ReadAttempt(ctx, item.TaskID, item.ID, item.AttemptNumber, view.ReadRevision)
	if err != nil {
		return workqueue.Change{}, false, false, nil, err
	}
	var proof workqueue.Change
	var result workexecution.Completion
	var controllerApplied, agentApplied bool
	var failure *taskprogress.Failure
	switch item.Operation {
	case workexecution.SoftwareStaging:
		saved, found, readErr := service.records.StagingResult(ctx, attempt.Attempt, view.ReadRevision)
		if readErr != nil {
			return proof, false, false, nil, readErr
		}
		if !found {
			return proof, false, false, nil, errs.New(errs.KindStateConflict, "software staging result is missing")
		}
		proof, result, err = activation.PrepareStagingResult(record, saved, attempt.Attempt)
		failure = saved.Record.Result.Failure
	case workexecution.ControllerSoftware:
		repository := service.controllerRecords
		capture, readErr := repository.Load(ctx, item, view.ReadRevision)
		if readErr != nil {
			return proof, false, false, nil, readErr
		}
		saved, found, readErr := repository.ReadResult(ctx, attempt.Attempt, view.ReadRevision)
		if readErr != nil {
			return proof, false, false, nil, readErr
		}
		if !found {
			return proof, false, false, nil, errs.New(errs.KindStateConflict, "Controller activation result is missing")
		}
		proof, result, err = controllersoftware.PrepareResult(capture, saved, attempt.Attempt)
		controllerApplied = saved.Record.Result.Journal != nil && saved.Record.Result.Journal.Phase == upgrade.PhaseHealthy
		failure = saved.Record.Result.Failure
	case workexecution.AgentSoftware:
		repository := service.agentRecords
		capture, readErr := repository.Load(ctx, item, view.ReadRevision)
		if readErr != nil {
			return proof, false, false, nil, readErr
		}
		saved, found, readErr := repository.ReadResult(ctx, attempt.Attempt, view.ReadRevision)
		if readErr != nil {
			return proof, false, false, nil, readErr
		}
		if !found {
			return proof, false, false, nil, errs.New(errs.KindStateConflict, "Agent activation result is missing")
		}
		proof, result, err = agentoperations.PrepareResult(capture, saved, attempt.Attempt)
		agentApplied = saved.Record.Result.Observed == agentoperations.ReadyDesired
		failure = saved.Record.Result.Failure
	default:
		err = errs.New(errs.KindStateConflict, "software activation has an unknown work operation")
	}
	if err != nil {
		return proof, false, false, nil, err
	}
	accepted := item.Result
	if attempt.Attempt.Direction == workexecution.Recovery {
		accepted = item.CleanupResult
	}
	if !result.EffectsSettled || attempt.Attempt.FinishedAt == nil || attempt.Attempt.Result == nil || accepted == nil || *accepted != result.Evidence || *attempt.Attempt.Result != result.Evidence {
		return workqueue.Change{}, false, false, nil, errs.New(errs.KindStateConflict, "software activation still has unresolved or unaccepted effects")
	}
	proof.Conditions = append(proof.Conditions, keyvalue.Condition{Key: workqueue.AttemptKey(attempt.Attempt.ID), ModRevision: attempt.Revision})
	return proof, controllerApplied, agentApplied, failure, nil
}
