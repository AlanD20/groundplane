package etcdconfig

import (
	"context"
	"time"

	"github.com/AlanD20/groundplane/internal/common/config"
	"github.com/AlanD20/groundplane/internal/common/etcdactivation"
	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/internal/infra/etcd/etcdconfiguration"
	"github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/workqueue"
	"github.com/AlanD20/groundplane/pkg/errs"
)

func (s *Service) loadDelivery(
	ctx context.Context,
	delivery workqueue.Delivery,
) (keyvalue.Versioned[etcdconfiguration.Input], error) {
	item, attempt := delivery.Item.Item, delivery.Attempt.Attempt
	if workexecution.ValidateDelivery(item, attempt) != nil || attempt.Executor.Executor != workexecution.Controller ||
		attempt.FinishedAt != nil || item.ActiveAttemptID != attempt.ID {
		return keyvalue.Versioned[etcdconfiguration.Input]{}, errs.New(
			errs.KindStateConflict,
			"etcd configuration delivery is not its active attempt",
		)
	}
	input, err := s.inputs.Load(ctx, item.ID, 0)
	if err != nil {
		return keyvalue.Versioned[etcdconfiguration.Input]{}, err
	}
	if err := input.Record.Match(item); err != nil {
		return keyvalue.Versioned[etcdconfiguration.Input]{}, err
	}
	return input, nil
}

func (s *Service) AuthorizeStart(ctx context.Context, delivery workqueue.Delivery) (workqueue.Change, error) {
	input, err := s.loadDelivery(ctx, delivery)
	if err != nil {
		return workqueue.Change{}, err
	}
	for _, document := range []string{input.Record.Candidate, input.Record.Previous} {
		if err := config.ValidateEtcdDocument(ctx, []byte(document)); err != nil {
			return workqueue.Change{}, err
		}
	}
	return inputGuard(input), nil
}

func (s *Service) RejectUnstarted(ctx context.Context, delivery workqueue.Delivery) error {
	if _, err := s.loadDelivery(ctx, delivery); err != nil {
		return err
	}
	attempt := delivery.Attempt.Attempt
	if attempt.StartedAt != nil {
		return errs.New(errs.KindStateConflict, "started etcd Apply requires effect reconciliation")
	}
	observed := etcdactivation.NotStarted
	if attempt.Direction == workexecution.Recovery {
		observed = etcdactivation.Unresolved
	}
	return s.inputs.SaveResult(ctx, s.queue, attempt, observed, true)
}

func (s *Service) Execute(ctx context.Context, delivery workqueue.Delivery) error {
	if delivery.Attempt.Attempt.Direction == workexecution.Recovery {
		return s.Reconcile(ctx, delivery)
	}
	input, err := s.loadDelivery(ctx, delivery)
	if err != nil {
		return err
	}
	attempt := delivery.Attempt.Attempt
	if attempt.StartedAt == nil {
		return errs.New(errs.KindStateConflict, "etcd Apply requires its durable start")
	}
	applyErr := s.runtime.Apply(ctx, input.Record.TaskID, input.Record.Candidate, input.Record.Previous)
	// Etcd may briefly be unavailable during activation. Observation and result
	// publication have their own bounded deadline, even when Apply timed out.
	observe, cancel := context.WithTimeout(context.WithoutCancel(ctx), 15*time.Second)
	defer cancel()
	outcome, observeErr := s.runtime.ObserveApply(
		observe,
		input.Record.TaskID,
		input.Record.Candidate,
		input.Record.Previous,
	)
	if observeErr != nil {
		outcome = etcdactivation.Unresolved
	}
	if err := s.inputs.SaveResult(observe, s.queue, attempt, outcome, applyErr != nil); err != nil {
		return err
	}
	if applyErr != nil {
		return applyErr
	}
	return observeErr
}

// Reconcile accounts for the captured activation or restores its predecessor.
// A recorded start can never cause another forward Apply after a restart.
func (s *Service) Reconcile(ctx context.Context, delivery workqueue.Delivery) error {
	input, err := s.loadDelivery(ctx, delivery)
	if err != nil {
		return err
	}
	attempt := delivery.Attempt.Attempt
	if attempt.StartedAt == nil {
		return errs.New(errs.KindStateConflict, "etcd reconciliation requires a started attempt")
	}
	saved, found, err := s.inputs.ReadResult(ctx, attempt, 0)
	if err != nil {
		return err
	}
	if found && saved.Record.Observed != etcdactivation.Unresolved {
		return nil
	}
	outcome, recoveryErr := s.runtime.RecoverApply(
		ctx,
		input.Record.TaskID,
		input.Record.Candidate,
		input.Record.Previous,
	)
	if recoveryErr != nil {
		outcome = etcdactivation.Unresolved
	}
	// A matching durable applied receipt can prove success after lost reporting;
	// a previous saved failure is retained by SaveResult independently of cleanup.
	if err := s.inputs.SaveResult(ctx, s.queue, attempt, outcome, outcome != etcdactivation.Applied); err != nil {
		return err
	}
	return recoveryErr
}

func (s *Service) Settle(
	ctx context.Context,
	queue *workqueue.Repository,
	delivery workqueue.Delivery,
	at time.Time,
) error {
	attempt := delivery.Attempt.Attempt
	view, err := queue.ReadResume(ctx, attempt.TaskID, attempt.WorkItemID, attempt.ID, attempt.Executor)
	if err != nil || view.Attempt.Attempt.FinishedAt != nil {
		return err
	}
	var request workqueue.Completion
	if view.CurrentExecutor {
		request, err = queue.ReadCompletionAt(ctx, view)
	} else {
		request, err = queue.ReadInterruptedEtcdConfigurationAt(ctx, view)
	}
	if err != nil {
		return err
	}
	input, err := s.inputs.Load(ctx, attempt.WorkItemID, view.Work.ReadRevision)
	if err != nil {
		return err
	}
	if err := input.Record.Match(view.Work.Item.Item); err != nil {
		return err
	}
	saved, found, err := s.inputs.ReadResult(ctx, attempt, view.Work.ReadRevision)
	if err != nil {
		return err
	}
	if !found {
		return errs.New(errs.KindStateConflict, "etcd Apply has no saved activation observation")
	}
	proof, result, err := etcdconfiguration.PrepareResult(input, saved, attempt)
	if err != nil {
		return err
	}
	if result.EffectsSettled && attempt.Direction == workexecution.Forward {
		if stop := view.Work.Task.Task.Stop; stop != nil {
			result.Outcome = stop.Outcome
		} else if view.Work.Item.Item.ForwardRevoked {
			result.Outcome = workexecution.Superseded
		}
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
		return errs.New(errs.KindStateConflict, "etcd Apply changed before result settlement")
	}
	return nil
}
