package softwareactivation

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"time"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/common/jcs"
	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/internal/controller/workdispatch"
	"github.com/AlanD20/groundplane/internal/infra/consolerelease"
	"github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	activation "github.com/AlanD20/groundplane/internal/infra/etcd/softwareactivation"
	"github.com/AlanD20/groundplane/internal/infra/etcd/workqueue"
	"github.com/AlanD20/groundplane/pkg/errs"
)

// ConsoleWork switches one already verified immutable asset release. It has no
// daemon handoff: the filesystem receipt is the durable effect authority.
type ConsoleWork struct {
	queue   *workqueue.Repository
	records *activation.Repository
	store   *consolerelease.Store
}

func (service *Service) ConsoleWork() *ConsoleWork {
	if service == nil || service.console == nil {
		return nil
	}
	return &ConsoleWork{queue: service.queue, records: service.records, store: service.console}
}

func (owner *ConsoleWork) assignment(
	ctx context.Context,
	task workqueue.TaskSnapshot,
	item workqueue.ItemSnapshot,
	revision int64,
) (keyvalue.Versioned[activation.Record], workexecution.Input, error) {
	if item.Item.Operation != workexecution.ConsoleSoftware ||
		item.Item.Placement.Executor != workexecution.Controller || item.Item.Input == nil {
		return keyvalue.Versioned[activation.Record]{}, workexecution.Input{},
			errs.New(errs.KindStateConflict, "Console activation assignment is invalid")
	}
	record, err := owner.records.At(ctx, task.Task.ID, revision)
	if err != nil {
		return record, workexecution.Input{}, err
	}
	reference, err := record.Record.ConsoleReference()
	if err != nil || item.Item.ID != record.Record.ConsoleWorkItemID || *item.Item.Input != reference {
		return record, workexecution.Input{},
			errs.New(errs.KindStateConflict, "Console activation assignment differs from its captured bundle")
	}
	return record, reference, nil
}

func (owner *ConsoleWork) Admit(
	ctx context.Context,
	request workqueue.Assignment,
	at time.Time,
) (workdispatch.Decision, error) {
	record, reference, err := owner.assignment(ctx, request.Task, request.Item, request.ReadRevision)
	if err != nil {
		return workdispatch.Decision{}, err
	}
	if !at.Before(request.Item.Item.Deadline) {
		if request.Direction == workexecution.Recovery {
			change, err := workqueue.PrepareCleanupRetryExpiry(request.Task, request.Item, at)
			change.Conditions = append(change.Conditions, activationGuard(record).Conditions...)
			return workdispatch.Decision{Disposition: workdispatch.NeedsAttention, Change: change}, err
		}
		change, err := workqueue.PrepareUnstartedSettlement(
			request.Task,
			request.Item,
			workexecution.Completion{Outcome: workexecution.Failed, EffectsSettled: true, Evidence: reference},
			at,
		)
		change.Conditions = append(change.Conditions, activationGuard(record).Conditions...)
		return workdispatch.Decision{Disposition: workdispatch.Settle, Change: change}, err
	}
	artifact := record.Record.Input.Preparation.Progress.Result.Console.Artifact
	if err := owner.store.Compatible(artifact); err != nil {
		return workdispatch.Decision{}, err
	}
	return workdispatch.Decision{
		Disposition: workdispatch.Dispatch,
		Deadline:    request.Item.Item.Deadline,
		Change:      activationGuard(record),
	}, nil
}

func (owner *ConsoleWork) Stop(
	ctx context.Context,
	task workqueue.TaskSnapshot,
	item workqueue.ItemSnapshot,
	at time.Time,
) (workqueue.Change, error) {
	if task.Task.Stop == nil {
		return workqueue.Change{}, errs.New(errs.KindStateConflict, "Console activation has no stop request")
	}
	record, reference, err := owner.assignment(ctx, task, item, 0)
	if err != nil {
		return workqueue.Change{}, err
	}
	if item.Item.AttemptNumber != 0 {
		return workqueue.PrepareRevocation(task, item, task.Task.Stop.Outcome, at)
	}
	change, err := workqueue.PrepareUnstartedSettlement(
		task,
		item,
		workexecution.Completion{Outcome: task.Task.Stop.Outcome, EffectsSettled: true, Evidence: reference},
		at,
	)
	change.Conditions = append(change.Conditions, activationGuard(record).Conditions...)
	return change, err
}

func (owner *ConsoleWork) load(
	ctx context.Context,
	delivery workqueue.Delivery,
) (workqueue.ResumeView, keyvalue.Versioned[activation.Record], error) {
	attempt := delivery.Attempt.Attempt
	if delivery.Item.Item.Operation != workexecution.ConsoleSoftware ||
		attempt.Executor.Executor != workexecution.Controller || attempt.FinishedAt != nil {
		return workqueue.ResumeView{}, keyvalue.Versioned[activation.Record]{},
			errs.New(errs.KindStateConflict, "Console activation delivery is invalid")
	}
	view, err := owner.queue.ReadResume(ctx, attempt.TaskID, attempt.WorkItemID, attempt.ID, attempt.Executor)
	if err != nil {
		return workqueue.ResumeView{}, keyvalue.Versioned[activation.Record]{}, err
	}
	if !view.ActiveAttempt || view.Attempt.Attempt.FinishedAt != nil ||
		!workexecution.SameAssignment(attempt, view.Attempt.Attempt) {
		return workqueue.ResumeView{}, keyvalue.Versioned[activation.Record]{},
			errs.New(errs.KindStateConflict, "Console activation no longer owns execution")
	}
	record, err := owner.records.At(ctx, attempt.TaskID, view.Work.ReadRevision)
	if err != nil {
		return workqueue.ResumeView{}, record, err
	}
	reference, err := record.Record.ConsoleReference()
	if err != nil || view.Work.Item.Item.ID != record.Record.ConsoleWorkItemID ||
		view.Work.Item.Item.Input == nil || *view.Work.Item.Item.Input != reference || attempt.Input != reference {
		return workqueue.ResumeView{}, record, errs.New(
			errs.KindStateConflict,
			"Console activation input differs from its captured bundle",
		)
	}
	return view, record, nil
}

func (owner *ConsoleWork) AuthorizeStart(ctx context.Context, delivery workqueue.Delivery) (workqueue.Change, error) {
	_, record, err := owner.load(ctx, delivery)
	if err != nil {
		return workqueue.Change{}, err
	}
	artifact := record.Record.Input.Preparation.Progress.Result.Console.Artifact
	if err := owner.store.Compatible(artifact); err != nil {
		return workqueue.Change{}, err
	}
	return activationGuard(record), nil
}

func (owner *ConsoleWork) RejectUnstarted(ctx context.Context, delivery workqueue.Delivery) error {
	_, _, err := owner.load(ctx, delivery)
	if err != nil {
		return err
	}
	if delivery.Attempt.Attempt.StartedAt != nil {
		return errs.New(errs.KindStateConflict, "started Console activation requires reconciliation")
	}
	return nil
}

func (owner *ConsoleWork) Execute(ctx context.Context, delivery workqueue.Delivery) error {
	_, record, err := owner.load(ctx, delivery)
	if err != nil {
		return err
	}
	if delivery.Attempt.Attempt.StartedAt == nil {
		return errs.New(errs.KindStateConflict, "Console activation requires its durable start")
	}
	return owner.store.Activate(record.Record.Input.Preparation.Progress.Result.Console.Artifact)
}

func (owner *ConsoleWork) Reconcile(ctx context.Context, delivery workqueue.Delivery) error {
	_, record, err := owner.load(ctx, delivery)
	if err != nil {
		return err
	}
	desired := record.Record.Input.Preparation.Progress.Result.Console.Artifact
	current, found := owner.store.Current()
	if !found || current != desired {
		return errs.New(errs.KindRequestFailed, "Console activation did not commit the selected bundle")
	}
	return nil
}

func (owner *ConsoleWork) Settle(
	ctx context.Context,
	queue *workqueue.Repository,
	delivery workqueue.Delivery,
	at time.Time,
) error {
	view, record, err := owner.load(ctx, delivery)
	if err != nil {
		return err
	}
	current, found := owner.store.Current()
	completion, err := consoleCompletion(record.Record, view.Attempt.Attempt, current, found)
	if err != nil {
		return err
	}
	var request workqueue.Completion
	if view.CurrentExecutor {
		request, err = queue.ReadCompletionAt(ctx, view)
	} else {
		request, err = queue.ReadInterruptedAt(ctx, view, workexecution.ConsoleSoftware)
	}
	if err != nil {
		return err
	}
	request.Result, request.At = completion, at
	change, err := workqueue.PrepareCompletion(request)
	if err != nil {
		return err
	}
	committed, err := queue.Commit(ctx, activationGuard(record), change)
	if err != nil {
		return err
	}
	if !committed.Succeeded {
		return errs.New(errs.KindStateConflict, "Console activation changed before settlement")
	}
	return nil
}

func consoleCompletion(
	record activation.Record,
	attempt workexecution.Attempt,
	current consolerelease.Artifact,
	found bool,
) (workexecution.Completion, error) {
	desired := record.Input.Preparation.Progress.Result.Console.Artifact
	reference, err := record.ConsoleReference()
	if err != nil || reference != attempt.Input {
		return workexecution.Completion{}, errs.New(errs.KindStateConflict, "Console completion input is invalid")
	}
	evidence := struct {
		Desired consolerelease.Artifact `json:"desired"`
		Current consolerelease.Artifact `json:"current"`
		Found   bool                    `json:"found"`
	}{Desired: desired, Current: current, Found: found}
	raw, err := json.Marshal(evidence)
	if err != nil {
		return workexecution.Completion{}, errs.Wrap(errs.KindInternal, err)
	}
	canonical, err := jcs.Canonicalize(raw)
	if err != nil {
		return workexecution.Completion{}, err
	}
	digest := sha256.Sum256(canonical)
	outcome := workexecution.Failed
	if attempt.StartedAt != nil && found && current == desired {
		outcome = workexecution.Succeeded
	}
	return workexecution.Completion{Outcome: outcome, EffectsSettled: found, Evidence: workexecution.Input{
		ID:     ids.DeriveAt(ids.KindPlan, record.CreatedAt, attempt.ID, "console-activation-result"),
		SHA256: hex.EncodeToString(digest[:]),
	}}, nil
}

func (owner *ConsoleWork) RetireWork(
	_ context.Context,
	view workqueue.View,
	at time.Time,
	_ bool,
) (bool, workqueue.Change, error) {
	task, item := view.Task.Task, view.Item.Item
	if task.CollectionStartedAt == nil || task.FinishedAt == nil || task.RetainUntil == nil ||
		at.Before(*task.RetainUntil) || item.Operation != workexecution.ConsoleSoftware ||
		item.Phase != workexecution.Settled || item.ActiveAttemptID != "" || item.Result == nil ||
		item.Cleanup != workexecution.CleanupNone && item.Cleanup != workexecution.CleanupComplete {
		return false, workqueue.Change{}, errs.New(
			errs.KindStateConflict,
			"Console software history requires expired, settled work",
		)
	}
	return true, workqueue.Change{Conditions: []keyvalue.Condition{{
		Key: workqueue.ItemKey(item.ID), ModRevision: view.Item.Revision,
	}}}, nil
}

func (owner *ConsoleWork) PrepareAttemptRetirement(
	view workqueue.View,
	snapshot workqueue.AttemptSnapshot,
) (workqueue.Change, error) {
	attempt := snapshot.Attempt
	if view.Item.Item.Operation != workexecution.ConsoleSoftware || view.Item.Item.Input == nil ||
		attempt.Input != *view.Item.Item.Input || attempt.Executor.Executor != workexecution.Controller {
		return workqueue.Change{}, errs.New(
			errs.KindStateConflict,
			"Console activation history differs from captured input",
		)
	}
	return workqueue.PrepareAttemptCollection(view.Task, view.Item, snapshot)
}
