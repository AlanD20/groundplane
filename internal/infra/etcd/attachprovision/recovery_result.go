package attachprovision

import (
	"context"
	"time"

	"github.com/AlanD20/groundplane/internal/common/attachexecution"
	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/workqueue"
)

func RecoveryResultKey(attemptID string) string {
	return "/v1/attach-provision-inspections/" + attemptID
}

// PrepareRecoveryResult retains observed execution state independently from the
// original failure. Stopped execution leaves its failed binding and facts intact.
func (repository *InputRepository) PrepareRecoveryResult(ctx context.Context, view workqueue.ResumeView,
	raw []byte, evidence workexecution.Input, at time.Time, interrupted bool,
) (workqueue.Change, workexecution.Completion, error) {
	attempt := view.Attempt.Attempt
	if !view.ActiveAttempt || view.CanSettleResult() == interrupted || attempt.FinishedAt != nil ||
		attempt.Direction != workexecution.Recovery || view.Work.Task.Task.CollectionStartedAt != nil ||
		at.IsZero() || at.Location() != time.UTC || at.Before(view.Work.Item.Item.UpdatedAt) {
		return workqueue.Change{}, workexecution.Completion{}, checkpointConflict(
			"Attach inspection no longer owns active execution",
		)
	}
	saved, err := repository.LoadRecovery(ctx, view.Work)
	if err != nil {
		return workqueue.Change{}, workexecution.Completion{}, err
	}
	input := attachexecution.ProvisionRecoveryInput{Attempt: attempt, Source: saved.Source}
	report, err := attachexecution.DecodeProvisionInspection(input, raw, evidence)
	if err != nil {
		return workqueue.Change{}, workexecution.Completion{}, err
	}
	if report.ObservedAt.After(at) || interrupted && report.State != attachexecution.InspectionInterrupted {
		return workqueue.Change{}, workexecution.Completion{}, checkpointConflict(
			"Attach inspection cannot claim unobserved execution state",
		)
	}
	source, err := repository.LoadCaptured(ctx, view.Work)
	if err != nil {
		return workqueue.Change{}, workexecution.Completion{}, err
	}
	defer source.Clear()
	expected, err := source.Generation.Record.ProvisionAuthority()
	if err != nil {
		return workqueue.Change{}, workexecution.Completion{}, err
	}
	if expected != input.Source.Failure.Authority || source.Generation.Record.SettlesInController() {
		return workqueue.Change{}, workexecution.Completion{}, checkpointConflict(
			"Attach inspection changed its captured binding",
		)
	}
	guards := append(source.Conditions,
		keyvalue.Condition{Key: RecoveryKey(attempt.WorkItemID), ModRevision: saved.Revision},
		keyvalue.Condition{Key: RecoveryResultKey(attempt.ID)},
		keyvalue.Condition{Key: workqueue.TaskKey(attempt.TaskID), ModRevision: view.Work.Task.Revision},
		keyvalue.Condition{Key: workqueue.ItemKey(attempt.WorkItemID), ModRevision: view.Work.Item.Revision},
		keyvalue.Condition{Key: workqueue.AttemptKey(attempt.ID), ModRevision: view.Attempt.Revision},
		keyvalue.Condition{Key: workqueue.ExecutorKey(attempt.Executor), ModRevision: view.ExecutorRevision})
	outcome := workexecution.Failed
	if report.ExecutionStopped() {
		outcome = workexecution.Succeeded
	}
	settlement, err := repository.prepareConsumerRecoverySettlement(
		ctx,
		view,
		source.Generation.Record,
		report,
		evidence,
	)
	if err != nil {
		return workqueue.Change{}, workexecution.Completion{}, err
	}
	settlement.Conditions = append(settlement.Conditions, guards...)
	settlement.Mutations = append(
		settlement.Mutations,
		keyvalue.Mutation{
			Type:  keyvalue.MutationPut,
			Key:   RecoveryResultKey(attempt.ID),
			Value: append([]byte(nil), raw...),
		},
	)
	return settlement, workexecution.Completion{
		Outcome:        outcome,
		Evidence:       evidence,
		EffectsSettled: report.ExecutionStopped(),
	}, nil
}

func (repository *InputRepository) LoadRecoveryResult(ctx context.Context, view workqueue.ResumeView) ([]byte, error) {
	attempt := view.Attempt.Attempt
	if attempt.FinishedAt == nil || attempt.Result == nil || attempt.Direction != workexecution.Recovery {
		return nil, checkpointConflict("Attach inspection has no retained terminal result")
	}
	saved, err := repository.LoadRecovery(ctx, view.Work)
	if err != nil {
		return nil, err
	}
	key := RecoveryResultKey(attempt.ID)
	read, err := repository.store.GetMany(
		ctx,
		keyvalue.GetManyRequest{Keys: []string{key}, Revision: view.Work.ReadRevision},
	)
	if read != nil {
		defer keyvalue.ClearValues(read.Values)
	}
	if err != nil {
		return nil, err
	}
	if read == nil || read.ReadRevision != view.Work.ReadRevision || len(read.Values) != 1 || read.Values[0] == nil ||
		read.Values[0].Key != key || read.Values[0].ModRevision <= 0 || read.Values[0].ModRevision > view.Work.ReadRevision {
		return nil, checkpointConflict("Attach inspection result is unavailable")
	}
	raw := read.Values[0].Value
	report, err := attachexecution.DecodeProvisionInspection(
		attachexecution.ProvisionRecoveryInput{Attempt: attempt, Source: saved.Source},
		raw,
		*attempt.Result,
	)
	if err != nil {
		return nil, err
	}
	if report.ObservedAt.After(*attempt.FinishedAt) ||
		(attempt.Outcome == workexecution.Succeeded) != report.ExecutionStopped() {
		return nil, checkpointConflict("Attach inspection result differs from its terminal outcome")
	}
	return append([]byte(nil), raw...), nil
}
