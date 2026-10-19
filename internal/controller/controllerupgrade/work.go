package controllerupgrade

import (
	"context"
	"errors"
	"sync"
	"time"

	upgrade "github.com/AlanD20/groundplane/internal/common/controllerupgrade"
	"github.com/AlanD20/groundplane/internal/common/taskprogress"
	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/internal/infra/etcd/controllersoftware"
	"github.com/AlanD20/groundplane/internal/infra/etcd/executiondiagnostics"
	"github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/workqueue"
	"github.com/AlanD20/groundplane/pkg/errs"
)

type Work struct {
	queue          *workqueue.Repository
	inputs         *controllersoftware.Repository
	coordinator    *Coordinator
	diagnostics    *executiondiagnostics.Repository
	observationsMu sync.Mutex
	observations   map[string][]workexecution.Stage
}

// A nil coordinator explicitly represents a foreground process without native
// bootstrap. Such a process cannot admit or reconcile host software activation.
func NewWork(
	queue *workqueue.Repository,
	inputs *controllersoftware.Repository,
	coordinator *Coordinator,
	diagnostics *executiondiagnostics.Repository,
) (*Work, error) {
	if queue == nil || inputs == nil || diagnostics == nil {
		return nil, errs.New(errs.KindInternal, "Controller software work dependencies are incomplete")
	}
	return &Work{
		queue:        queue,
		inputs:       inputs,
		coordinator:  coordinator,
		diagnostics:  diagnostics,
		observations: make(map[string][]workexecution.Stage),
	}, nil
}

func captureGuard(capture keyvalue.Versioned[controllersoftware.Capture]) workqueue.Change {
	return workqueue.Change{
		Conditions: []keyvalue.Condition{
			{Key: controllersoftware.InputKey(capture.Record.WorkItemID), ModRevision: capture.Revision},
		},
	}
}

func (owner *Work) origin(
	ctx context.Context,
	item workexecution.Item,
	revision int64,
) (keyvalue.Versioned[controllersoftware.Capture], upgrade.Journal, error) {
	capture, err := owner.inputs.Load(ctx, item, revision)
	if err != nil {
		return capture, upgrade.Journal{}, err
	}
	_, first, err := owner.queue.ReadAttempt(ctx, item.TaskID, item.ID, 1, capture.ReadRevision)
	if err != nil {
		return capture, upgrade.Journal{}, err
	}
	if first.Attempt.Direction != workexecution.Forward || first.Attempt.Input != *item.Input ||
		first.Attempt.Executor.Executor != workexecution.Controller {
		return capture, upgrade.Journal{}, errs.New(
			errs.KindStateConflict,
			"Controller activation has no original forward assignment",
		)
	}
	expected, err := capture.Record.Journal(first.Attempt.AssignedAt, first.Attempt.Deadline)
	return capture, expected, err
}

func (owner *Work) loadDelivery(
	ctx context.Context,
	delivery workqueue.Delivery,
) (keyvalue.Versioned[controllersoftware.Capture], upgrade.Journal, error) {
	item, attempt := delivery.Item.Item, delivery.Attempt.Attempt
	if workexecution.ValidateDelivery(item, attempt) != nil || attempt.Executor.Executor != workexecution.Controller ||
		item.ActiveAttemptID != attempt.ID || attempt.FinishedAt != nil {
		return keyvalue.Versioned[controllersoftware.Capture]{}, upgrade.Journal{}, errs.New(
			errs.KindStateConflict,
			"Controller activation delivery is not its active attempt",
		)
	}
	view, err := owner.queue.ReadResume(ctx, attempt.TaskID, attempt.WorkItemID, attempt.ID, attempt.Executor)
	if err != nil {
		return keyvalue.Versioned[controllersoftware.Capture]{}, upgrade.Journal{}, err
	}
	if !view.ActiveAttempt || view.Attempt.Attempt.FinishedAt != nil ||
		!workexecution.SameAssignment(attempt, view.Attempt.Attempt) {
		return keyvalue.Versioned[controllersoftware.Capture]{}, upgrade.Journal{}, errs.New(
			errs.KindStateConflict,
			"Controller activation ownership changed",
		)
	}
	if err := owner.inputs.VerifyRecoveryClaim(ctx, view.Work.Item.Item, view.Work.ReadRevision); err != nil {
		return keyvalue.Versioned[controllersoftware.Capture]{}, upgrade.Journal{}, err
	}
	return owner.origin(ctx, view.Work.Item.Item, view.Work.ReadRevision)
}

func (owner *Work) AuthorizeStart(ctx context.Context, delivery workqueue.Delivery) (workqueue.Change, error) {
	if owner.coordinator == nil {
		return workqueue.Change{}, bootstrapUnavailable()
	}
	capture, _, err := owner.loadDelivery(ctx, delivery)
	if err != nil {
		return workqueue.Change{}, err
	}
	if err := capture.Record.Input.Validate(); err != nil {
		return workqueue.Change{}, err
	}
	change := captureGuard(capture)
	projection, err := owner.inputs.PrepareCurrent(ctx, capture, capture.ReadRevision, true, "", false)
	if err != nil {
		return workqueue.Change{}, err
	}
	change.Conditions = append(change.Conditions, projection.Conditions...)
	change.Mutations = append(change.Mutations, projection.Mutations...)
	return change, nil
}

func (owner *Work) RejectUnstarted(ctx context.Context, delivery workqueue.Delivery) error {
	_, expected, err := owner.loadDelivery(ctx, delivery)
	if err != nil {
		return err
	}
	attempt := delivery.Attempt.Attempt
	if attempt.StartedAt != nil {
		return errs.New(errs.KindStateConflict, "started Controller activation requires handoff reconciliation")
	}
	result := upgrade.ExecutionResult{
		Outcome: workexecution.Failed,
		Settled: attempt.Direction == workexecution.Forward,
		Failure: taskprogress.CaptureControllerFailure(
			errs.New(errs.KindRequestFailed, "Controller activation stopped before execution"),
		),
	}
	return owner.inputs.SaveResult(ctx, owner.queue, attempt, expected, result, time.Now().UTC())
}

func (owner *Work) Execute(ctx context.Context, delivery workqueue.Delivery) error {
	return owner.run(ctx, delivery, delivery.Attempt.Attempt.Direction == workexecution.Forward)
}

func (owner *Work) Reconcile(ctx context.Context, delivery workqueue.Delivery) error {
	return owner.run(ctx, delivery, false)
}

func (owner *Work) run(ctx context.Context, delivery workqueue.Delivery, forward bool) error {
	if owner.coordinator == nil {
		return bootstrapUnavailable()
	}
	_, expected, err := owner.loadDelivery(ctx, delivery)
	if err != nil {
		return err
	}
	attempt := delivery.Attempt.Attempt
	if attempt.StartedAt == nil {
		return errs.New(errs.KindStateConflict, "Controller activation requires its durable start")
	}
	saved, found, err := owner.inputs.ReadResult(ctx, attempt, 0)
	if err != nil {
		return err
	}
	if found && saved.Record.Result.Settled {
		return nil
	}
	if err := owner.coordinator.Restore(ctx, expected); err != nil {
		return err
	}
	view, err := owner.queue.ReadResume(ctx, attempt.TaskID, attempt.WorkItemID, attempt.ID, attempt.Executor)
	if err != nil {
		return err
	}
	if forward && (view.Work.Task.Task.Stop != nil || view.Work.Item.Item.ForwardRevoked) {
		if err := owner.coordinator.RequestAbort(ctx, attempt.TaskID); err != nil {
			return err
		}
	}
	observer := &workObserver{
		repository:   owner.diagnostics,
		attempt:      attempt,
		ctx:          ctx,
		terminalOnly: !view.CurrentExecutor,
	}
	result, executionErr := owner.coordinator.Execute(ctx, expected, forward, observer)
	if executionErr != nil {
		// Daemon replacement/shutdown leaves the original attempt alive. The
		// new process qualifies it; the outgoing one cannot report replacement.
		if errors.Is(ctx.Err(), context.Canceled) {
			return executionErr
		}
		observing, cancel := context.WithTimeout(context.WithoutCancel(ctx), 5*time.Second)
		result, err = owner.coordinator.Unresolved(observing, expected, executionErr)
		cancel()
		if err != nil {
			return err
		}
	}
	saving, cancel := context.WithTimeout(context.WithoutCancel(ctx), 15*time.Second)
	defer cancel()
	if err := owner.inputs.SaveResult(saving, owner.queue, attempt, expected, result, time.Now().UTC()); err != nil {
		return err
	}
	owner.rememberObservations(attempt.ID, observer)
	return executionErr
}

func (owner *Work) ControllerRecovery(ctx context.Context, taskID string) (upgrade.Journal, error) {
	task, revision, err := owner.queue.ReadTask(ctx, taskID, 0)
	if err != nil {
		return upgrade.Journal{}, err
	}
	if task.Task.ExpectedItems < 1 || task.Task.ExpectedItems > 3 || task.Task.CollectionStartedAt != nil ||
		task.Task.FinishedAt != nil {
		return upgrade.Journal{}, errs.New(errs.KindStateConflict, "native handoff has no unfinished Controller work")
	}
	page, err := owner.queue.ListWork(ctx, taskID, "", 3, revision)
	if err != nil {
		return upgrade.Journal{}, err
	}
	if len(page.Items) != int(task.Task.ExpectedItems) || page.More {
		return upgrade.Journal{}, errs.New(errs.KindStateConflict, "native handoff work membership is invalid")
	}
	var item workexecution.Item
	for _, candidate := range page.Items {
		if candidate.Item.Operation != workexecution.ControllerSoftware {
			continue
		}
		if item.ID != "" || candidate.Item.Phase == workexecution.Settled {
			return upgrade.Journal{}, errs.New(
				errs.KindStateConflict,
				"native handoff Controller membership is ambiguous or settled",
			)
		}
		item = candidate.Item
	}
	if item.ID == "" {
		return upgrade.Journal{}, errs.New(errs.KindStateConflict, "native handoff has no Controller activation work")
	}
	_, first, err := owner.queue.ReadAttempt(ctx, taskID, item.ID, 1, revision)
	if err != nil {
		return upgrade.Journal{}, err
	}
	if first.Attempt.StartedAt == nil {
		return upgrade.Journal{}, errs.New(
			errs.KindStateConflict,
			"native handoff journal has no durable execution start",
		)
	}
	_, expected, err := owner.origin(ctx, item, revision)
	if err != nil {
		return upgrade.Journal{}, err
	}
	if err := owner.inputs.VerifyRecoveryClaim(ctx, item, revision); err != nil {
		return upgrade.Journal{}, err
	}
	return expected, nil
}
