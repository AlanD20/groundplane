package servicelifecyclework

import (
	"context"
	"time"

	"github.com/AlanD20/groundplane/internal/common/lifecycleexecution"
	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/workqueue"
	"github.com/AlanD20/groundplane/proto/agentpb"
)

func RecoveryResultKey(attemptID string) string {
	return "/v1/execution/service-lifecycle/recovery-results/" + attemptID
}

func (repository *Repository) PrepareRecoveryResult(ctx context.Context, view workqueue.ResumeView, input *agentpb.ServiceLifecycleWorkInput, raw []byte, at time.Time) (workqueue.Change, workexecution.Completion, error) {
	attempt := view.Attempt.Attempt
	if !view.CurrentExecutor || !view.ActiveAttempt || attempt.FinishedAt != nil || attempt.StartedAt == nil ||
		view.Work.Task.Task.CollectionStartedAt != nil || at.Before(view.Work.Item.Item.UpdatedAt) {
		return workqueue.Change{}, workexecution.Completion{}, conflict("Service lifecycle recovery no longer owns active inspection")
	}
	captured, guard, err := repository.Load(ctx, view.Work)
	if err != nil {
		return workqueue.Change{}, workexecution.Completion{}, err
	}
	if !sameCapture(captured, captured, input) {
		return workqueue.Change{}, workexecution.Completion{}, conflict("Service lifecycle recovery input changed")
	}
	saved, err := repository.LoadRecovery(ctx, view.Work, input)
	if err != nil {
		return workqueue.Change{}, workexecution.Completion{}, err
	}
	report, evidence, err := lifecycleexecution.DecodeRecoveryReport(attempt, input, saved.Source, raw)
	if err != nil {
		return workqueue.Change{}, workexecution.Completion{}, err
	}
	if report.ObservedAt.After(at) {
		return workqueue.Change{}, workexecution.Completion{}, conflict("Service lifecycle recovery observation is newer than settlement")
	}
	completed := report.BoundaryCompleted()
	outcome := workexecution.Failed
	if completed {
		outcome = workexecution.Succeeded
	}
	conditions := []keyvalue.Condition{guard,
		{Key: RecoveryKey(attempt.WorkItemID), ModRevision: saved.Revision},
		{Key: RecoveryResultKey(attempt.ID)},
		{Key: workqueue.TaskKey(attempt.TaskID), ModRevision: view.Work.Task.Revision},
		{Key: workqueue.ItemKey(attempt.WorkItemID), ModRevision: view.Work.Item.Revision},
		{Key: workqueue.AttemptKey(attempt.ID), ModRevision: view.Attempt.Revision},
		{Key: workqueue.ExecutorKey(attempt.Executor), ModRevision: view.ExecutorRevision},
	}
	return workqueue.Change{Conditions: conditions, Mutations: []keyvalue.Mutation{
		{Type: keyvalue.MutationPut, Key: RecoveryResultKey(attempt.ID), Value: append([]byte(nil), raw...)},
	}}, workexecution.Completion{Outcome: outcome, Evidence: evidence, EffectsSettled: completed}, nil
}

func (repository *Repository) LoadRecoveryResult(ctx context.Context, view workqueue.ResumeView, input *agentpb.ServiceLifecycleWorkInput) ([]byte, bool, error) {
	attempt := view.Attempt.Attempt
	if attempt.FinishedAt == nil || attempt.Result == nil {
		return nil, false, conflict("Service lifecycle recovery has no terminal result")
	}
	saved, err := repository.LoadRecovery(ctx, view.Work, input)
	if err != nil {
		return nil, false, err
	}
	read, err := repository.read(ctx, []string{RecoveryResultKey(attempt.ID)}, view.Work.ReadRevision)
	if err != nil {
		return nil, false, err
	}
	defer keyvalue.ClearValues(read.Values)
	if read.Values[0] == nil {
		return nil, false, conflict("Service lifecycle recovery result is missing")
	}
	raw := read.Values[0].Value
	report, evidence, err := lifecycleexecution.DecodeRecoveryReport(attempt, input, saved.Source, raw)
	if err != nil {
		return nil, false, err
	}
	completed := report.BoundaryCompleted()
	if evidence != *attempt.Result || completed != (attempt.Outcome == workexecution.Succeeded) || report.ObservedAt.After(*attempt.FinishedAt) {
		return nil, false, conflict("Service lifecycle recovery result differs from its terminal evidence")
	}
	return append([]byte(nil), raw...), completed, nil
}
