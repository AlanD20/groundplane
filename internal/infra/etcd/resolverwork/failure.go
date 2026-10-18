package resolverwork

import (
	"bytes"
	"context"
	"time"

	"github.com/AlanD20/groundplane/internal/common/resolverexecution"
	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/workqueue"
)

func FailureKey(evidenceID string) string { return "/v1/execution/resolver/failures/" + evidenceID }

func (repository *Repository) PrepareFailure(ctx context.Context, view workqueue.ResumeView,
	reason resolverexecution.FailureReason, at time.Time,
) (workqueue.Change, workexecution.Input, error) {
	if !view.ActiveAttempt || view.Attempt.Attempt.Direction != workexecution.Forward || view.Attempt.Attempt.FinishedAt != nil {
		return workqueue.Change{}, workexecution.Input{}, invalid("DNS resolver failure requires its active forward attempt")
	}
	stored, err := repository.Load(ctx, view.Work)
	if err != nil {
		return workqueue.Change{}, workexecution.Input{}, err
	}
	attempt := view.Attempt.Attempt
	layout, err := resolverexecution.CaptureLayout(attempt, stored.Input)
	if err != nil {
		return workqueue.Change{}, workexecution.Input{}, err
	}
	if cursor := view.Checkpoint.Record; cursor != nil {
		if _, err := repository.ReadProgress(ctx, view, stored.Input, cursor.Sequence); err != nil {
			return workqueue.Change{}, workexecution.Input{}, err
		}
	}
	failure := resolverexecution.Failure{AttemptID: attempt.ID, Input: attempt.Input, Reason: reason, Checkpoint: view.Checkpoint.Record, RecordedAt: at.UTC()}
	raw, evidence, err := resolverexecution.EncodeFailure(attempt, layout, failure)
	if err != nil {
		return workqueue.Change{}, workexecution.Input{}, err
	}
	queue, err := workqueue.NewRepository(repository.store)
	if err != nil {
		return workqueue.Change{}, workexecution.Input{}, err
	}
	completion, err := queue.ReadCompletionAt(ctx, view)
	if !view.CurrentExecutor && reason == resolverexecution.Interrupted {
		completion, err = queue.ReadInterruptedAt(ctx, view, workexecution.Resolver)
	}
	if err != nil {
		return workqueue.Change{}, workexecution.Input{}, err
	}
	outcome := forwardOutcome(view)
	if outcome == workexecution.Succeeded {
		outcome = workexecution.Failed
	}
	completion.At, completion.Result = at.UTC(), workexecution.Completion{Outcome: outcome, Evidence: evidence, EffectsSettled: attempt.StartedAt == nil}
	var change workqueue.Change
	if completion.Result.EffectsSettled {
		change, err = workqueue.PrepareCompletion(completion)
	} else {
		change, err = workqueue.PrepareCompletionWithRecovery(completion, CleanupDeadline(at))
	}
	if err != nil {
		return workqueue.Change{}, workexecution.Input{}, err
	}
	change.Conditions = append(change.Conditions, stored.Guards...)
	change.Conditions = append(change.Conditions, keyvalue.Condition{Key: FailureKey(evidence.ID)},
		keyvalue.Condition{Key: workqueue.CheckpointKey(attempt.ID), ModRevision: view.Checkpoint.Revision},
		keyvalue.Condition{Key: workqueue.ExecutorKey(attempt.Executor), ModRevision: view.ExecutorRevision})
	change.Mutations = append(change.Mutations, keyvalue.Mutation{Type: keyvalue.MutationPut, Key: FailureKey(evidence.ID), Value: raw})
	request, err := repository.prepareRequestResult(ctx, view.Work, outcome)
	if err != nil {
		keyvalue.ClearMutationValues(change.Mutations)
		return workqueue.Change{}, workexecution.Input{}, err
	}
	change.Conditions = append(change.Conditions, request.Conditions...)
	change.Mutations = append(change.Mutations, request.Mutations...)
	return change, evidence, nil
}

func (repository *Repository) ReadFailure(ctx context.Context, view workqueue.ResumeView) (resolverexecution.Failure, []byte, keyvalue.Condition, error) {
	attempt := view.Attempt.Attempt
	if attempt.FinishedAt == nil || attempt.Result == nil || attempt.Direction != workexecution.Forward || attempt.Outcome == workexecution.Succeeded {
		return resolverexecution.Failure{}, nil, keyvalue.Condition{}, invalid("DNS resolver attempt has no retained forward failure")
	}
	stored, err := repository.Load(ctx, view.Work)
	if err != nil {
		return resolverexecution.Failure{}, nil, keyvalue.Condition{}, err
	}
	layout, err := resolverexecution.CaptureLayout(attempt, stored.Input)
	if err != nil {
		return resolverexecution.Failure{}, nil, keyvalue.Condition{}, err
	}
	key := FailureKey(attempt.Result.ID)
	read, err := repository.read(ctx, []string{key}, view.Work.ReadRevision)
	if err != nil {
		return resolverexecution.Failure{}, nil, keyvalue.Condition{}, err
	}
	defer keyvalue.ClearValues(read.Values)
	if read.Values[0] == nil {
		return resolverexecution.Failure{}, nil, keyvalue.Condition{}, invalid("DNS resolver failed attempt lost its failure receipt")
	}
	failure, err := resolverexecution.DecodeFailure(attempt, layout, read.Values[0].Value, *attempt.Result)
	if err != nil {
		return resolverexecution.Failure{}, nil, keyvalue.Condition{}, err
	}
	return failure, bytes.Clone(read.Values[0].Value), keyvalue.Condition{Key: key, ModRevision: read.Values[0].ModRevision}, nil
}
