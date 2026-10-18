package dnsresolver

import (
	"context"
	"time"

	"github.com/AlanD20/groundplane/internal/common/jcs"
	"github.com/AlanD20/groundplane/internal/common/resolverexecution"
	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/internal/infra/etcd/agentregistration"
	"github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/resolverwork"
	"github.com/AlanD20/groundplane/internal/infra/etcd/workqueue"
	"github.com/AlanD20/groundplane/pkg/errs"
)

func (owner *Work) ReconcileInterrupted(ctx context.Context, view workqueue.ResumeView, authorization agentregistration.WorkAuthorization) error {
	guards, err := authorization.Conditions()
	if err != nil {
		return err
	}
	actor, old := authorization.Identity(), view.Attempt.Attempt.Executor
	if actor.Executor != workexecution.Agent || actor.HostID != old.HostID || actor == old || view.CurrentExecutor ||
		!view.ActiveAttempt || view.Attempt.Attempt.FinishedAt != nil || view.Work.Task.Task.CollectionStartedAt != nil ||
		view.Work.Item.Item.Operation != workexecution.Resolver {
		return errs.New(errs.KindStateConflict, "CoreDNS interruption requires the replacement Agent on the same host")
	}
	at := time.Now().UTC()
	var change workqueue.Change
	if view.Attempt.Attempt.Direction == workexecution.Forward {
		change, _, err = owner.records.PrepareFailure(ctx, view, resolverexecution.Interrupted, at)
	} else {
		stored, loadErr := owner.records.Load(ctx, view.Work)
		if loadErr != nil {
			return loadErr
		}
		source, loadErr := owner.records.LoadRecoveryForAttempt(ctx, view.Work, view.Attempt.Attempt)
		if loadErr != nil {
			return loadErr
		}
		report := resolverexecution.RecoveryReport{AttemptID: view.Attempt.Attempt.ID, ForwardID: source.Recovery.Forward.ID,
			Input: view.Attempt.Attempt.Input, Failure: source.Recovery.Evidence, Status: resolverexecution.RecoveryInterrupted, ObservedAt: at}
		raw, _, encodeErr := resolverexecution.EncodeRecoveryReport(view.Attempt.Attempt, stored.Input, source.Recovery, report)
		if encodeErr != nil {
			return encodeErr
		}
		change, _, err = owner.records.PrepareRecoveryResult(ctx, view, raw, at)
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
		return errs.New(errs.KindStateConflict, "CoreDNS interruption changed before settlement")
	}
	return nil
}

func (owner *Work) PrepareFinish(ctx context.Context, finished workqueue.FinishedWork, _ time.Time) (workqueue.Change, error) {
	if len(finished.Items) != 1 || finished.Task.Task.ExpectedItems != 1 {
		return workqueue.Change{}, errs.New(errs.KindStateConflict, "CoreDNS action requires one settled work item")
	}
	item := finished.Items[0]
	if item.Item.Operation != workexecution.Resolver || item.Item.Phase != workexecution.Settled || item.Item.ActiveAttemptID != "" || item.Item.Result == nil {
		return workqueue.Change{}, errs.New(errs.KindStateConflict, "CoreDNS action still has unresolved effects")
	}
	view := workqueue.View{Task: finished.Task, Item: item, ReadRevision: finished.ReadRevision}
	stored, err := owner.records.Load(ctx, view)
	if err != nil {
		return workqueue.Change{}, err
	}
	change := workqueue.Change{Conditions: append(append([]keyvalue.Condition(nil), stored.Guards...),
		keyvalue.Condition{Key: workqueue.TaskKey(finished.Task.Task.ID), ModRevision: finished.Task.Revision},
		keyvalue.Condition{Key: workqueue.ItemKey(item.Item.ID), ModRevision: item.Revision})}
	if item.Item.AttemptNumber == 0 {
		if *item.Item.Result != stored.Reference || item.Item.Outcome == workexecution.Succeeded {
			return workqueue.Change{}, errs.New(errs.KindStateConflict, "unexecuted CoreDNS work cannot prove runtime application")
		}
		return change, nil
	}
	_, latest, err := owner.queue.ReadAttempt(ctx, finished.Task.Task.ID, item.Item.ID, item.Item.AttemptNumber, finished.ReadRevision)
	if err != nil {
		return workqueue.Change{}, err
	}
	resume, err := owner.queue.ReadResumeAt(ctx, latest.Attempt.TaskID, latest.Attempt.WorkItemID, latest.Attempt.ID, latest.Attempt.Executor, finished.ReadRevision)
	if err != nil {
		return workqueue.Change{}, err
	}
	if latest.Attempt.FinishedAt == nil || latest.Attempt.Result == nil {
		return workqueue.Change{}, errs.New(errs.KindStateConflict, "CoreDNS action lost its terminal execution receipt")
	}
	var guard keyvalue.Condition
	if latest.Attempt.Direction == workexecution.Recovery {
		if item.Item.Cleanup != workexecution.CleanupComplete || latest.Attempt.Outcome != workexecution.Succeeded ||
			item.Item.CleanupResult == nil || *item.Item.CleanupResult != *latest.Attempt.Result {
			return workqueue.Change{}, errs.New(errs.KindStateConflict, "CoreDNS cleanup is not proven complete")
		}
		raw, receipt, err := owner.records.ReadRecoveryResult(ctx, resume)
		if err != nil {
			return workqueue.Change{}, err
		}
		report, err := jcs.Decode[resolverexecution.RecoveryReport](raw)
		if err != nil || report.Status != resolverexecution.RecoveryComplete {
			return workqueue.Change{}, errs.New(errs.KindStateConflict, "CoreDNS cleanup has no successful restoration receipt")
		}
		guard = receipt
	} else {
		if *item.Item.Result != *latest.Attempt.Result {
			return workqueue.Change{}, errs.New(errs.KindStateConflict, "CoreDNS forward result differs from its settled work")
		}
		if latest.Attempt.Result.ID == latest.Attempt.Input.ID {
			_, guard, err = owner.records.ReadCompleted(ctx, resume)
		} else {
			_, _, guard, err = owner.records.ReadFailure(ctx, resume)
		}
		if err != nil {
			return workqueue.Change{}, err
		}
	}
	change.Conditions = append(change.Conditions, guard, keyvalue.Condition{Key: workqueue.AttemptKey(latest.Attempt.ID), ModRevision: latest.Revision})
	return change, nil
}

func (owner *Work) RetireWork(ctx context.Context, view workqueue.View, at time.Time, final bool) (bool, workqueue.Change, error) {
	return owner.records.PrepareRetention(ctx, view, at, final)
}

func (owner *Work) PrepareAttemptRetirement(view workqueue.View, attempt workqueue.AttemptSnapshot) (workqueue.Change, error) {
	return resolverwork.PrepareAttemptRetirement(view, attempt)
}
