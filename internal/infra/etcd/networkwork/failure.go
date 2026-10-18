package networkwork

import (
	"context"
	"time"

	"github.com/AlanD20/groundplane/internal/common/networkexecution"
	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/workqueue"
)

func FailureKey(attemptID string) string { return "/v1/network-work-failure/" + attemptID }

// PrepareFailure keeps unknown effects protected. A command error, lease loss or
// deadline does not prove that a host effect stopped. Only an acknowledged
// between-effects boundary permits settlement without inspection.
func (repository *InputRepository) PrepareFailure(ctx context.Context, view workqueue.ResumeView,
	reason networkexecution.FailureReason, at time.Time, interrupted bool,
) (workqueue.Change, workexecution.Completion, error) {
	attempt, item, task := view.Attempt.Attempt, view.Work.Item.Item, view.Work.Task.Task
	if !view.ActiveAttempt || view.CanSettleResult() == interrupted || view.ExecutorRevision <= 0 ||
		attempt.FinishedAt != nil ||
		item.Operation != workexecution.Network || !task.PublicationComplete || task.CollectionStartedAt != nil ||
		interrupted && reason != networkexecution.ExecutionInterrupted {
		return workqueue.Change{}, workexecution.Completion{}, conflict("Network failure no longer owns the active execution")
	}
	input, guards, err := repository.Load(ctx, view.Work)
	if err != nil {
		return workqueue.Change{}, workexecution.Completion{}, err
	}
	checkpoint := view.Checkpoint.Record
	settled := checkpoint == nil
	if attempt.Direction == workexecution.Recovery {
		if checkpoint != nil {
			return workqueue.Change{}, workexecution.Completion{}, conflict("Network inspection failure cannot contain a forward checkpoint")
		}
		captured, err := repository.LoadRecoveryInput(ctx, view.Work, attempt)
		if err != nil {
			return workqueue.Change{}, workexecution.Completion{}, err
		}
		guards = append(guards, captured.Conditions...)
		settled = false
	}
	if checkpoint != nil {
		key := ReportKey(attempt.ID, checkpoint.Sequence)
		read, err := repository.read(ctx, []string{key}, view.Work.ReadRevision)
		if err != nil {
			return workqueue.Change{}, workexecution.Completion{}, err
		}
		defer keyvalue.ClearValues(read.Values)
		if read.Values[0] == nil {
			return workqueue.Change{}, workexecution.Completion{}, conflict("Network failure is missing its acknowledged progress")
		}
		progress, err := networkexecution.DecodeProgress(attempt, input.Action, input.NetworkId, read.Values[0].Value)
		if err != nil {
			return workqueue.Change{}, workexecution.Completion{}, err
		}
		expected, err := progress.Checkpoint(attempt, input.Action, input.NetworkId)
		if err != nil || expected != *checkpoint || progress.Stage == networkexecution.Complete {
			return workqueue.Change{}, workexecution.Completion{}, conflict("Network failure differs from acknowledged progress; completed work must report completion")
		}
		settled = !progress.InFlight
		guards = append(guards, keyvalue.Condition{Key: key, ModRevision: read.Values[0].ModRevision})
	}
	raw, evidence, err := networkexecution.EncodeFailure(attempt, networkexecution.Failure{
		AttemptID: attempt.ID, Input: attempt.Input, Reason: reason, Checkpoint: checkpoint, RecordedAt: at,
	})
	if err != nil {
		return workqueue.Change{}, workexecution.Completion{}, err
	}
	guards = append(guards,
		keyvalue.Condition{Key: FailureKey(attempt.ID)},
		keyvalue.Condition{Key: workqueue.TaskKey(task.ID), ModRevision: view.Work.Task.Revision},
		keyvalue.Condition{Key: workqueue.ItemKey(item.ID), ModRevision: view.Work.Item.Revision},
		keyvalue.Condition{Key: workqueue.AttemptKey(attempt.ID), ModRevision: view.Attempt.Revision},
		keyvalue.Condition{Key: workqueue.CheckpointKey(attempt.ID), ModRevision: view.Checkpoint.Revision},
		keyvalue.Condition{Key: workqueue.ExecutorKey(attempt.Executor), ModRevision: view.ExecutorRevision})
	outcome := workexecution.Failed
	if task.Stop != nil {
		outcome = task.Stop.Outcome
	} else if item.ForwardRevoked {
		outcome = workexecution.Superseded
	}
	change := workqueue.Change{Conditions: guards, Mutations: []keyvalue.Mutation{
		{Type: keyvalue.MutationPut, Key: FailureKey(attempt.ID), Value: raw},
	}}
	if settled && (task.Stop != nil || item.ForwardRevoked) {
		fence, err := repository.prepareRemovalFenceRelease(ctx, view.Work)
		if err != nil {
			keyvalue.ZeroMutationBytes(change.Mutations)
			return workqueue.Change{}, workexecution.Completion{}, err
		}
		change.Conditions = append(change.Conditions, fence.Conditions...)
		change.Mutations = append(change.Mutations, fence.Mutations...)
	}
	return change, workexecution.Completion{Outcome: outcome, Evidence: evidence, EffectsSettled: settled}, nil
}

func (repository *InputRepository) LoadFailure(ctx context.Context, view workqueue.ResumeView) ([]byte, error) {
	attempt := view.Attempt.Attempt
	if view.Work.Item.Item.Operation != workexecution.Network || attempt.FinishedAt == nil || attempt.Result == nil {
		return nil, conflict("Network failure has no retained result")
	}
	read, err := repository.read(ctx, []string{FailureKey(attempt.ID)}, view.Work.ReadRevision)
	if err != nil {
		return nil, err
	}
	defer keyvalue.ClearValues(read.Values)
	if read.Values[0] == nil {
		return nil, conflict("Network failure evidence is unavailable")
	}
	if _, err := networkexecution.DecodeFailure(attempt, read.Values[0].Value, *attempt.Result); err != nil {
		return nil, err
	}
	return append([]byte(nil), read.Values[0].Value...), nil
}
