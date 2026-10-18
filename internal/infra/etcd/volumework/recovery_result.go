package volumework

import (
	"context"
	"time"

	"github.com/AlanD20/groundplane/internal/common/volumeexecution"
	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/workqueue"
)

func RecoveryResultKey(attemptID string) string {
	return "/v1/volume-work-recovery-result/" + attemptID
}

func (repository *InputRepository) PrepareRecoveryResult(ctx context.Context, view workqueue.ResumeView, raw []byte, at time.Time) (workqueue.Change, workexecution.Completion, error) {
	attempt := view.Attempt.Attempt
	if !view.CanSettleResult() || !view.ActiveAttempt || attempt.FinishedAt != nil || attempt.StartedAt == nil ||
		view.Work.Task.Task.CollectionStartedAt != nil || at.Before(view.Work.Item.Item.UpdatedAt) {
		return workqueue.Change{}, workexecution.Completion{}, conflict("Volume recovery no longer owns active inspection")
	}
	captured, err := repository.LoadRecoveryInput(ctx, view.Work, attempt)
	if err != nil {
		return workqueue.Change{}, workexecution.Completion{}, err
	}
	report, evidence, err := volumeexecution.DecodeRecoveryReport(attempt, captured.Source, captured.Input.Action, captured.Input.VolumeId, captured.Boundary, raw)
	if err != nil {
		return workqueue.Change{}, workexecution.Completion{}, err
	}
	if report.Observation.At.After(at) {
		return workqueue.Change{}, workexecution.Completion{}, conflict("Volume recovery observation is newer than settlement")
	}
	completed := report.BoundaryCompleted(captured.Boundary)
	outcome := workexecution.Failed
	if completed {
		outcome = workexecution.Succeeded
	}
	guards := append(captured.Conditions,
		keyvalue.Condition{Key: RecoveryResultKey(attempt.ID)},
		keyvalue.Condition{Key: workqueue.TaskKey(attempt.TaskID), ModRevision: view.Work.Task.Revision},
		keyvalue.Condition{Key: workqueue.ItemKey(attempt.WorkItemID), ModRevision: view.Work.Item.Revision},
		keyvalue.Condition{Key: workqueue.AttemptKey(attempt.ID), ModRevision: view.Attempt.Revision},
		keyvalue.Condition{Key: workqueue.ExecutorKey(attempt.Executor), ModRevision: view.ExecutorRevision})
	return workqueue.Change{Conditions: guards, Mutations: []keyvalue.Mutation{
		{Type: keyvalue.MutationPut, Key: RecoveryResultKey(attempt.ID), Value: append([]byte(nil), raw...)},
	}}, workexecution.Completion{Outcome: outcome, Evidence: evidence, EffectsSettled: completed}, nil
}

func (repository *InputRepository) LoadRecoveryResult(ctx context.Context, view workqueue.ResumeView) ([]byte, bool, error) {
	attempt := view.Attempt.Attempt
	if attempt.FinishedAt == nil || attempt.Result == nil {
		return nil, false, conflict("Volume recovery has no saved terminal result")
	}
	captured, err := repository.LoadRecoveryInput(ctx, view.Work, attempt)
	if err != nil {
		return nil, false, err
	}
	read, err := repository.read(ctx, []string{RecoveryResultKey(attempt.ID)}, view.Work.ReadRevision)
	if err != nil {
		return nil, false, err
	}
	defer keyvalue.ClearValues(read.Values)
	if read.Values[0] == nil {
		return nil, false, conflict("Volume recovery result is unavailable")
	}
	raw := read.Values[0].Value
	report, evidence, err := volumeexecution.DecodeRecoveryReport(attempt, captured.Source, captured.Input.Action, captured.Input.VolumeId, captured.Boundary, raw)
	if err != nil {
		return nil, false, err
	}
	completed := report.BoundaryCompleted(captured.Boundary)
	if evidence != *attempt.Result || completed != (attempt.Outcome == workexecution.Succeeded) || report.Observation.At.After(*attempt.FinishedAt) {
		return nil, false, conflict("Volume recovery result differs from its terminal evidence")
	}
	return append([]byte(nil), raw...), completed, nil
}
