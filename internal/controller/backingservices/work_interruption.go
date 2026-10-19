package backingservices

import (
	"context"
	"time"

	"github.com/AlanD20/groundplane/internal/common/backingprovision"
	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/internal/infra/etcd/agentregistration"
	"github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/workqueue"
	"github.com/AlanD20/groundplane/pkg/errs"
)

func (owner *Work) ReconcileInterrupted(
	ctx context.Context,
	view workqueue.ResumeView,
	authorization agentregistration.WorkAuthorization,
) error {
	guards, err := authorization.Conditions()
	if err != nil {
		return err
	}
	actor, previous := authorization.Identity(), view.Attempt.Attempt.Executor
	if actor.Executor != workexecution.Agent || actor.HostID != previous.HostID || actor == previous ||
		view.CurrentExecutor ||
		!view.ActiveAttempt ||
		view.Attempt.Attempt.FinishedAt != nil ||
		view.Work.Task.Task.CollectionStartedAt != nil ||
		view.Work.Item.Item.Operation != workexecution.BackingProvision {
		return errs.New(
			errs.KindStateConflict,
			"backing creation interruption requires the replacement Agent on the same host",
		)
	}
	at := time.Now().UTC()
	var change workqueue.Change
	if view.Attempt.Attempt.Direction == workexecution.Forward {
		change, _, err = owner.records.PrepareFailure(ctx, view, backingprovision.Interrupted, at)
	} else {
		input, _, loadErr := owner.records.Load(ctx, view.Work)
		if loadErr != nil {
			return loadErr
		}
		defer clear(input.Wire)
		procedure, loadErr := input.Procedure()
		if loadErr != nil {
			return loadErr
		}
		layout, loadErr := backingprovision.CaptureLayout(view.Attempt.Attempt, procedure)
		if loadErr != nil {
			return loadErr
		}
		source, loadErr := owner.records.LoadRecoveryForAttempt(ctx, view.Work, view.Attempt.Attempt)
		if loadErr != nil {
			return loadErr
		}
		report := backingprovision.RecoveryReport{AttemptID: view.Attempt.Attempt.ID, ForwardID: source.Recovery.Forward.ID,
			Failure: source.Recovery.Evidence, Input: view.Attempt.Attempt.Input, ServiceID: layout.ServiceID,
			Status: backingprovision.RecoveryInterrupted, ObservedAt: at}
		raw, _, encodeErr := backingprovision.EncodeRecoveryReport(view.Attempt.Attempt, source.Recovery, layout, report)
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
		return errs.New(errs.KindStateConflict, "backing creation interruption changed before settlement")
	}
	return nil
}
