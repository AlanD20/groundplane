package etcdconfig

import (
	"context"
	"time"

	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/internal/infra/etcd/etcdconfiguration"
	"github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/workqueue"
	"github.com/AlanD20/groundplane/pkg/errs"
)

func (s *Service) PrepareFinish(
	ctx context.Context,
	finished workqueue.FinishedWork,
	_ time.Time,
) (workqueue.Change, error) {
	if len(finished.Items) != 1 || finished.Task.Task.Action != workexecution.ActionApply ||
		finished.Task.Task.TargetID != etcdconfiguration.TargetID() {
		return workqueue.Change{}, errs.New(
			errs.KindStateConflict,
			"etcd Apply completion does not match its single captured operation",
		)
	}
	item := finished.Items[0].Item
	input, err := s.inputs.Load(ctx, item.ID, finished.ReadRevision)
	if err != nil {
		return workqueue.Change{}, err
	}
	if err := input.Record.Match(item); err != nil {
		return workqueue.Change{}, err
	}
	change := inputGuard(input)
	if item.AttemptNumber == 0 {
		reference, err := input.Record.Reference()
		if err != nil {
			return workqueue.Change{}, err
		}
		if item.Result == nil || *item.Result != reference || item.Outcome == workexecution.Succeeded {
			return workqueue.Change{}, errs.New(
				errs.KindStateConflict,
				"unstarted etcd Apply has no matching rejection",
			)
		}
		return change, nil
	}
	_, previous, err := s.queue.ReadAttempt(ctx, item.TaskID, item.ID, item.AttemptNumber, finished.ReadRevision)
	if err != nil {
		return workqueue.Change{}, err
	}
	saved, found, err := s.inputs.ReadResult(ctx, previous.Attempt, finished.ReadRevision)
	if err != nil {
		return workqueue.Change{}, err
	}
	if !found || previous.Attempt.FinishedAt == nil || previous.Attempt.Result == nil {
		return workqueue.Change{}, errs.New(
			errs.KindStateConflict,
			"etcd Apply completion has no accepted activation result",
		)
	}
	proof, result, err := etcdconfiguration.PrepareResult(input, saved, previous.Attempt)
	if err != nil {
		return workqueue.Change{}, err
	}
	accepted := item.Result
	if previous.Attempt.Direction == workexecution.Recovery {
		accepted = item.CleanupResult
	}
	if !result.EffectsSettled || *previous.Attempt.Result != result.Evidence || accepted == nil ||
		*accepted != result.Evidence {
		return workqueue.Change{}, errs.New(
			errs.KindStateConflict,
			"etcd Apply still has unresolved activation effects",
		)
	}
	return proof, nil
}

func (s *Service) RetireWork(
	ctx context.Context,
	view workqueue.View,
	at time.Time,
	final bool,
) (bool, workqueue.Change, error) {
	task, item := view.Task.Task, view.Item.Item
	if task.CollectionStartedAt == nil || task.FinishedAt == nil || task.RetainUntil == nil ||
		at.Before(*task.RetainUntil) ||
		item.Phase != workexecution.Settled ||
		item.ActiveAttemptID != "" ||
		item.Result == nil ||
		item.Cleanup != workexecution.CleanupNone && item.Cleanup != workexecution.CleanupComplete {
		return false, workqueue.Change{}, errs.New(
			errs.KindStateConflict,
			"etcd history collection requires expired, settled work",
		)
	}
	input, err := s.inputs.Load(ctx, item.ID, view.ReadRevision)
	if err != nil {
		return false, workqueue.Change{}, err
	}
	if err := input.Record.Match(item); err != nil {
		return false, workqueue.Change{}, err
	}
	change := inputGuard(input)
	if final {
		// Only history is retired. Applied configuration and the independent
		// local activation receipt are not owned by diagnostic retention.
		change.Mutations = append(
			change.Mutations,
			keyvalue.Mutation{Type: keyvalue.MutationDelete, Key: etcdconfiguration.InputKey(item.ID)},
		)
	}
	return true, change, nil
}

func (s *Service) PrepareAttemptRetirement(
	view workqueue.View,
	snapshot workqueue.AttemptSnapshot,
) (workqueue.Change, error) {
	attempt := snapshot.Attempt
	if view.Item.Item.Operation != workexecution.EtcdConfiguration || view.Item.Item.Input == nil ||
		attempt.Input != *view.Item.Item.Input || attempt.Executor.Executor != workexecution.Controller {
		return workqueue.Change{}, errs.New(
			errs.KindStateConflict,
			"etcd attempt history differs from its captured activation",
		)
	}
	prepared, err := workqueue.PrepareAttemptCollection(view.Task, view.Item, snapshot)
	if err != nil {
		return workqueue.Change{}, err
	}
	return workqueue.Change{Conditions: prepared.Conditions, Mutations: []keyvalue.Mutation{
		{Type: keyvalue.MutationDelete, Key: etcdconfiguration.ResultKey(attempt.ID)},
	}}, nil
}
