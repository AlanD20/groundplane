package attachprovision

import (
	"context"
	"time"

	"github.com/AlanD20/groundplane/internal/common/attachexecution"
	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/workqueue"
)

func FailureKey(attemptID string) string { return "/v1/attach-provision-failures/" + attemptID }

// PrepareFailure retains the original failure separately from recovery. A
// provisioning command may have created credentials or emitted facts even when
// its process exited. Any authorized command therefore requires effect
// settlement, including failures after the final command but before fact commit.
// The caller atomically joins this evidence with attempt/resource settlement.
func (repository *InputRepository) PrepareFailure(ctx context.Context, view workqueue.ResumeView,
	input attachexecution.ProvisionInput, reason attachexecution.FailureReason, at time.Time, interrupted bool,
) (workqueue.Change, workexecution.Completion, error) {
	attempt := view.Attempt.Attempt
	if !view.ActiveAttempt || view.CanSettleResult() == interrupted || view.ExecutorRevision <= 0 ||
		attempt.FinishedAt != nil || view.Work.Item.Item.Operation != workexecution.Attach ||
		!workexecution.SameAssignment(attempt, input.Attempt) {
		return workqueue.Change{}, workexecution.Completion{}, checkpointConflict(
			"Attach failure no longer owns its active attempt",
		)
	}
	source, err := repository.LoadCaptured(ctx, view.Work)
	if err != nil {
		return workqueue.Change{}, workexecution.Completion{}, err
	}
	defer source.Clear()
	authority, err := input.Authority()
	if err != nil {
		return workqueue.Change{}, workexecution.Completion{}, err
	}
	if source.Generation.Record.SettlesInController() || authority.AttachID != source.Generation.Record.AttachID ||
		authority.BackingServiceID != source.Generation.Record.BackingServiceID {
		return workqueue.Change{}, workexecution.Completion{}, checkpointConflict(
			"Attach failure differs from its captured provisioning target",
		)
	}
	progress, guards, err := repository.LoadProgress(ctx, view, input)
	if err != nil {
		return workqueue.Change{}, workexecution.Completion{}, err
	}
	failure := attachexecution.ProvisionFailure{AttemptID: attempt.ID, Input: attempt.Input, Authority: authority,
		Reason: reason, Checkpoint: view.Checkpoint.Record, RecordedAt: at}
	raw, evidence, err := attachexecution.EncodeProvisionFailure(attempt, failure)
	if err != nil {
		return workqueue.Change{}, workexecution.Completion{}, err
	}
	change := workqueue.Change{Conditions: append(source.Conditions, guards...),
		Mutations: []keyvalue.Mutation{{Type: keyvalue.MutationPut, Key: FailureKey(attempt.ID), Value: raw}}}
	change.Conditions = append(change.Conditions,
		keyvalue.Condition{Key: FailureKey(attempt.ID)},
		keyvalue.Condition{Key: workqueue.AttemptKey(attempt.ID), ModRevision: view.Attempt.Revision},
		keyvalue.Condition{Key: workqueue.ExecutorKey(attempt.Executor), ModRevision: view.ExecutorRevision},
	)
	outcome := workexecution.Failed
	if view.Work.Task.Task.Stop != nil {
		outcome = view.Work.Task.Task.Stop.Outcome
	} else if view.Work.Item.Item.ForwardRevoked {
		outcome = workexecution.Superseded
	}
	result := workexecution.Completion{Outcome: outcome, Evidence: evidence, EffectsSettled: progress == nil}
	if !result.EffectsSettled {
		recovery, err := repository.prepareRecoverySource(ctx, view, failure, result, at)
		if err != nil {
			keyvalue.ZeroMutationBytes(change.Mutations)
			return workqueue.Change{}, workexecution.Completion{}, err
		}
		change.Conditions = append(change.Conditions, recovery.Conditions...)
		change.Mutations = append(change.Mutations, recovery.Mutations...)
	}
	return change, result, nil
}

func (repository *InputRepository) LoadFailure(ctx context.Context, view workqueue.ResumeView) ([]byte, error) {
	attempt := view.Attempt.Attempt
	if view.Work.Item.Item.Operation != workexecution.Attach || attempt.Direction != workexecution.Forward ||
		attempt.FinishedAt == nil || attempt.Result == nil || attempt.Outcome == workexecution.Succeeded || view.Work.ReadRevision <= 0 {
		return nil, checkpointConflict("Attach failure has no retained terminal attempt")
	}
	key := FailureKey(attempt.ID)
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
		return nil, checkpointConflict("Attach failure evidence is unavailable")
	}
	failure, err := attachexecution.DecodeProvisionFailure(attempt, read.Values[0].Value, *attempt.Result)
	if err != nil {
		return nil, err
	}
	if !failure.RecordedAt.Equal(*attempt.FinishedAt) {
		return nil, checkpointConflict("Attach failure time differs from its terminal attempt")
	}
	return append([]byte(nil), read.Values[0].Value...), nil
}
