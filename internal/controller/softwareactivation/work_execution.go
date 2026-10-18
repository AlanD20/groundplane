package softwareactivation

import (
	"context"
	"time"

	"github.com/AlanD20/groundplane/internal/common/taskprogress"
	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	activation "github.com/AlanD20/groundplane/internal/infra/etcd/softwareactivation"
	"github.com/AlanD20/groundplane/internal/infra/etcd/workqueue"
	"github.com/AlanD20/groundplane/pkg/errs"
)

func (service *Service) loadDelivery(ctx context.Context, delivery workqueue.Delivery) (keyvalue.Versioned[activation.Record], error) {
	attempt := delivery.Attempt.Attempt
	view, err := service.queue.ReadResume(ctx, attempt.TaskID, attempt.WorkItemID, attempt.ID, attempt.Executor)
	if err != nil {
		return keyvalue.Versioned[activation.Record]{}, err
	}
	if !view.ActiveAttempt || view.Attempt.Attempt.FinishedAt != nil || attempt.Executor.Executor != workexecution.Controller ||
		!workexecution.SameAssignment(attempt, view.Attempt.Attempt) {
		return keyvalue.Versioned[activation.Record]{}, errs.New(errs.KindStateConflict, "software staging attempt no longer owns execution")
	}
	record, err := service.records.At(ctx, attempt.TaskID, view.Work.ReadRevision)
	if err != nil {
		return record, err
	}
	return record, record.Record.MatchStage(view.Work.Item.Item)
}

func activationGuard(record keyvalue.Versioned[activation.Record]) workqueue.Change {
	return workqueue.Change{Conditions: []keyvalue.Condition{{Key: activation.Key(record.Record.TaskID), ModRevision: record.Revision}}}
}

func (service *Service) AuthorizeStart(ctx context.Context, delivery workqueue.Delivery) (workqueue.Change, error) {
	record, err := service.loadDelivery(ctx, delivery)
	if err != nil {
		return workqueue.Change{}, err
	}
	return activationGuard(record), nil
}

func (service *Service) RejectUnstarted(ctx context.Context, delivery workqueue.Delivery) error {
	if _, err := service.loadDelivery(ctx, delivery); err != nil {
		return err
	}
	attempt := delivery.Attempt.Attempt
	if attempt.StartedAt != nil {
		return errs.New(errs.KindStateConflict, "started staging needs cleanup proof")
	}
	result := activation.StagingResult{Settled: attempt.Direction == workexecution.Forward,
		Failure: taskprogress.CaptureControllerFailure(errs.New(errs.KindRequestFailed, "software staging ended before execution"))}
	return service.records.SaveStagingResult(ctx, service.queue, attempt, result, time.Now().UTC())
}

func (service *Service) Execute(ctx context.Context, delivery workqueue.Delivery) error {
	if delivery.Attempt.Attempt.Direction == workexecution.Recovery {
		return service.Reconcile(ctx, delivery)
	}
	record, err := service.loadDelivery(ctx, delivery)
	if err != nil {
		return err
	}
	attempt := delivery.Attempt.Attempt
	if attempt.StartedAt == nil {
		return errs.New(errs.KindStateConflict, "software staging requires its durable start")
	}
	result := activation.StagingResult{}
	selection := record.Record.Input.Preparation.Source.Selection
	if selection.IncludesController() {
		err := service.observe(ctx, delivery, "software_controller_stage", "Stage verified Controller bundle", "Copy the pinned bundle into an immutable native release without starting it", func(ctx context.Context) error {
			staged, err := service.stager.Stage(ctx, record.Record.Input.Preparation, record.Record.Input.StagingAgentImage)
			if err == nil {
				result.Controller = &staged
			}
			return err
		})
		if err != nil {
			return service.failStaging(ctx, delivery, record, err)
		}
	}
	if selection.IncludesAgent() {
		if err := service.observe(ctx, delivery, "software_agent_stage", "Import verified Agent image", "Import the exact prepared image without replacing the running Agent", func(ctx context.Context) error {
			return service.stager.StageAgent(ctx, record.Record.Input.Preparation.Progress.Result.Agent.Artifact)
		}); err != nil {
			return service.failStaging(ctx, delivery, record, err)
		}
		result.Agent = true
	}
	result.Settled = true
	saving, cancel := context.WithTimeout(context.WithoutCancel(ctx), 15*time.Second)
	defer cancel()
	return service.records.SaveStagingResult(saving, service.queue, attempt, result, time.Now().UTC())
}

func (service *Service) failStaging(ctx context.Context, delivery workqueue.Delivery, record keyvalue.Versioned[activation.Record], cause error) error {
	if ctx.Err() == context.Canceled {
		return ctx.Err()
	}
	cleanup, cancel := context.WithTimeout(context.WithoutCancel(ctx), 2*time.Minute)
	defer cancel()
	err := service.observe(cleanup, delivery, "software_stage_cleanup", "Remove staging helper and scratch", "Remove only the owned stopped copy helper and private staging directory", func(ctx context.Context) error { return service.stager.Cleanup(ctx, record.Record.Input.Preparation) })
	result := activation.StagingResult{Settled: err == nil, Failure: taskprogress.CaptureControllerFailure(cause)}
	if err != nil {
		result.CleanupFailure = taskprogress.CaptureControllerFailure(err)
	}
	saving, finish := context.WithTimeout(context.WithoutCancel(ctx), 15*time.Second)
	defer finish()
	if err := service.records.SaveStagingResult(saving, service.queue, delivery.Attempt.Attempt, result, time.Now().UTC()); err != nil {
		return err
	}
	return cause
}

// A started staging import is never rerun on reconnect. Clean up only its copy
// helper/scratch. Without a saved complete receipt, this activation fails safely
// before any Controller/Agent change; a new Apply may import the artifacts again.
func (service *Service) Reconcile(ctx context.Context, delivery workqueue.Delivery) error {
	record, err := service.loadDelivery(ctx, delivery)
	if err != nil {
		return err
	}
	saved, found, err := service.records.StagingResult(ctx, delivery.Attempt.Attempt, record.ReadRevision)
	if err != nil {
		return err
	}
	if found && saved.Record.Result.Settled {
		return nil
	}
	cause := errs.New(errs.KindRequestFailed, "software staging was interrupted before its complete receipt; apply the preparation again after cleanup")
	cleanup, cancel := context.WithTimeout(context.WithoutCancel(ctx), 2*time.Minute)
	defer cancel()
	cleanupErr := service.observe(cleanup, delivery, "software_stage_reconcile", "Reconcile staging helper and scratch", "Prove staging scratch is retired without rerunning the import or replacing software", func(ctx context.Context) error { return service.stager.Cleanup(ctx, record.Record.Input.Preparation) })
	result := activation.StagingResult{Settled: cleanupErr == nil, Failure: taskprogress.CaptureControllerFailure(cause)}
	if cleanupErr != nil {
		result.CleanupFailure = taskprogress.CaptureControllerFailure(cleanupErr)
	}
	if found && saved.Record.Result.Failure != nil {
		result.Failure = saved.Record.Result.Failure
	}
	saving, finish := context.WithTimeout(context.WithoutCancel(ctx), 15*time.Second)
	defer finish()
	if err := service.records.SaveStagingResult(saving, service.queue, delivery.Attempt.Attempt, result, time.Now().UTC()); err != nil {
		return err
	}
	return cleanupErr
}

func (service *Service) Settle(ctx context.Context, queue *workqueue.Repository, delivery workqueue.Delivery, at time.Time) error {
	attempt := delivery.Attempt.Attempt
	view, err := queue.ReadResume(ctx, attempt.TaskID, attempt.WorkItemID, attempt.ID, attempt.Executor)
	if err != nil || view.Attempt.Attempt.FinishedAt != nil {
		return err
	}
	var request workqueue.Completion
	if view.CurrentExecutor {
		request, err = queue.ReadCompletionAt(ctx, view)
	} else {
		request, err = queue.ReadInterruptedSoftwareStagingAt(ctx, view)
	}
	if err != nil {
		return err
	}
	record, err := service.records.At(ctx, attempt.TaskID, view.Work.ReadRevision)
	if err != nil {
		return err
	}
	if err := record.Record.MatchStage(view.Work.Item.Item); err != nil {
		return err
	}
	saved, found, err := service.records.StagingResult(ctx, attempt, view.Work.ReadRevision)
	if err != nil {
		return err
	}
	if !found {
		return errs.New(errs.KindStateConflict, "software staging has no saved import or cleanup proof")
	}
	proof, result, err := activation.PrepareStagingResult(record, saved, attempt)
	if err != nil {
		return err
	}
	if result.EffectsSettled && attempt.Direction == workexecution.Forward && view.Work.Task.Task.Stop != nil {
		result.Outcome = view.Work.Task.Task.Stop.Outcome
	}
	request.Result, request.At = result, at
	completion, err := workqueue.PrepareCompletion(request)
	if err != nil {
		return err
	}
	committed, err := queue.Commit(ctx, proof, completion)
	if err != nil {
		return err
	}
	if !committed.Succeeded {
		return errs.New(errs.KindStateConflict, "software staging changed before settlement")
	}
	return nil
}
