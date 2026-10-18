package componentwork

import (
	"context"
	"time"

	"github.com/AlanD20/groundplane/internal/common/componentexecution"
	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/workqueue"
)

func RecoveryResultKey(attemptID string) string {
	return "/v1/component-work-recovery-result/" + attemptID
}

// PrepareRecoveryProof supplies restoration proof and captured resource state.
// The operation owner must join predecessor settlement, reservation cleanup and
// queue completion in the same transaction. This fragment alone settles nothing.
func (repository *InputRepository) PrepareRecoveryProof(ctx context.Context, view workqueue.ResumeView,
	raw []byte, at time.Time,
) (Capture, workqueue.Change, workexecution.Completion, error) {
	attempt := view.Attempt.Attempt
	if !view.CanSettleResult() || !view.ActiveAttempt || attempt.FinishedAt != nil || attempt.StartedAt == nil ||
		view.Work.Task.Task.CollectionStartedAt != nil || at.Before(view.Work.Item.Item.UpdatedAt) {
		return Capture{}, workqueue.Change{}, workexecution.Completion{}, conflict("Component recovery no longer owns active execution")
	}
	captured, err := repository.LoadRecoveryInput(ctx, view.Work, attempt)
	if err != nil {
		return Capture{}, workqueue.Change{}, workexecution.Completion{}, err
	}
	report, evidence, err := componentexecution.DecodeRecoveryReport(attempt, captured.Source, captured.Input, captured.Boundary, raw)
	if err != nil {
		return Capture{}, workqueue.Change{}, workexecution.Completion{}, err
	}
	if report.ObservedAt.After(at) {
		return Capture{}, workqueue.Change{}, workexecution.Completion{}, conflict("Component restoration observation is newer than settlement")
	}
	_, capture, guards, err := repository.Load(ctx, view.Work)
	if err != nil {
		return Capture{}, workqueue.Change{}, workexecution.Completion{}, err
	}
	guards = append(guards, captured.Conditions...)
	guards = append(guards,
		keyvalue.Condition{Key: RecoveryResultKey(attempt.ID)},
		keyvalue.Condition{Key: workqueue.TaskKey(attempt.TaskID), ModRevision: view.Work.Task.Revision},
		keyvalue.Condition{Key: workqueue.ItemKey(attempt.WorkItemID), ModRevision: view.Work.Item.Revision},
		keyvalue.Condition{Key: workqueue.AttemptKey(attempt.ID), ModRevision: view.Attempt.Revision},
		keyvalue.Condition{Key: workqueue.ExecutorKey(attempt.Executor), ModRevision: view.ExecutorRevision})
	change := workqueue.Change{Conditions: guards, Mutations: []keyvalue.Mutation{
		{Type: keyvalue.MutationPut, Key: RecoveryResultKey(attempt.ID), Value: append([]byte(nil), raw...)},
	}}
	return capture, change, workexecution.Completion{Outcome: workexecution.Succeeded, Evidence: evidence, EffectsSettled: true}, nil
}

func (repository *InputRepository) LoadRecoveryResult(ctx context.Context, view workqueue.ResumeView) ([]byte, error) {
	attempt := view.Attempt.Attempt
	if attempt.FinishedAt == nil || attempt.Result == nil || attempt.Outcome != workexecution.Succeeded {
		return nil, conflict("Component recovery has no successful retained result")
	}
	captured, err := repository.LoadRecoveryInput(ctx, view.Work, attempt)
	if err != nil {
		return nil, err
	}
	read, err := repository.read(ctx, []string{RecoveryResultKey(attempt.ID)}, view.Work.ReadRevision)
	if err != nil {
		return nil, err
	}
	defer keyvalue.ClearValues(read.Values)
	if read.Values[0] == nil {
		return nil, conflict("Component recovery evidence is unavailable")
	}
	raw := read.Values[0].Value
	report, evidence, err := componentexecution.DecodeRecoveryReport(attempt, captured.Source, captured.Input, captured.Boundary, raw)
	if err != nil {
		return nil, err
	}
	if evidence != *attempt.Result || report.ObservedAt.After(*attempt.FinishedAt) {
		return nil, conflict("Component recovery evidence differs from its recorded outcome")
	}
	return append([]byte(nil), raw...), nil
}
