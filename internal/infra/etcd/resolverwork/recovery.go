package resolverwork

import (
	"context"

	"github.com/AlanD20/groundplane/internal/common/jcs"
	"github.com/AlanD20/groundplane/internal/common/resolverexecution"
	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/workqueue"
)

type RecoverySource struct {
	Recovery resolverexecution.Recovery
	Guards   []keyvalue.Condition
}

func (repository *Repository) LoadRecovery(ctx context.Context, view workqueue.View) (RecoverySource, error) {
	number := view.Item.Item.AttemptNumber
	if view.Item.Item.ActiveAttemptID == "" {
		if number == ^uint64(0) {
			return RecoverySource{}, invalid("DNS resolver attempt sequence is exhausted")
		}
		number++
	}
	return repository.loadRecovery(ctx, view, number, nil)
}

func (repository *Repository) LoadRecoveryForAttempt(ctx context.Context, view workqueue.View, attempt workexecution.Attempt) (RecoverySource, error) {
	if attempt.Validate() != nil || attempt.Direction != workexecution.Recovery || attempt.TaskID != view.Task.Task.ID ||
		attempt.WorkItemID != view.Item.Item.ID || attempt.Number > view.Item.Item.AttemptNumber {
		return RecoverySource{}, invalid("DNS resolver cleanup requires its actual retained assignment")
	}
	var failure *workexecution.Input
	if attempt.FinishedAt != nil {
		read, err := repository.read(ctx, []string{RecoveryResultKey(attempt.ID)}, view.ReadRevision)
		if err != nil {
			return RecoverySource{}, err
		}
		defer keyvalue.ClearValues(read.Values)
		if read.Values[0] == nil || len(read.Values[0].Value) > resolverexecution.MaximumReportBytes {
			return RecoverySource{}, invalid("DNS resolver terminal cleanup lost its own captured source receipt")
		}
		report, err := jcs.Decode[resolverexecution.RecoveryReport](read.Values[0].Value)
		if err != nil {
			return RecoverySource{}, err
		}
		failure = &report.Failure
	}
	source, err := repository.loadRecovery(ctx, view, attempt.Number, failure)
	if err != nil {
		return RecoverySource{}, err
	}
	stored, err := repository.Load(ctx, view)
	if err != nil {
		return RecoverySource{}, err
	}
	layout, err := resolverexecution.CaptureLayout(attempt, stored.Input)
	if err != nil || source.Recovery.ValidateFor(attempt, layout) != nil {
		return RecoverySource{}, invalid("DNS resolver cleanup assignment differs from its retained source")
	}
	return source, nil
}

func (repository *Repository) loadRecovery(ctx context.Context, view workqueue.View, number uint64, evidence *workexecution.Input) (RecoverySource, error) {
	stored, err := repository.Load(ctx, view)
	if err != nil {
		return RecoverySource{}, err
	}
	if evidence == nil {
		evidence = view.Item.Item.Result
	}
	if evidence == nil || evidence.Validate() != nil {
		return RecoverySource{}, invalid("DNS resolver cleanup has no current forward failure")
	}
	key := FailureKey(evidence.ID)
	read, err := repository.read(ctx, []string{key}, view.ReadRevision)
	if err != nil {
		return RecoverySource{}, err
	}
	defer keyvalue.ClearValues(read.Values)
	if read.Values[0] == nil || len(read.Values[0].Value) > resolverexecution.MaximumReportBytes {
		return RecoverySource{}, invalid("DNS resolver cleanup lost its original failure")
	}
	failure, err := jcs.Decode[resolverexecution.Failure](read.Values[0].Value)
	if err != nil {
		return RecoverySource{}, err
	}
	attemptRead, err := repository.read(ctx, []string{workqueue.AttemptKey(failure.AttemptID)}, view.ReadRevision)
	if err != nil {
		return RecoverySource{}, err
	}
	defer keyvalue.ClearValues(attemptRead.Values)
	if attemptRead.Values[0] == nil {
		return RecoverySource{}, invalid("DNS resolver cleanup lost its original forward attempt")
	}
	forward, err := workqueue.DecodeAttempt(attemptRead.Values[0].Value)
	if err != nil {
		return RecoverySource{}, err
	}
	if forward.TaskID != view.Task.Task.ID || forward.WorkItemID != view.Item.Item.ID ||
		forward.Executor.HostID != stored.Input.HostID || forward.Input != stored.Reference || number <= forward.Number {
		return RecoverySource{}, invalid("DNS resolver cleanup source belongs to another action, input or host")
	}
	layout, err := resolverexecution.CaptureLayout(forward, stored.Input)
	if err != nil {
		return RecoverySource{}, err
	}
	failure, err = resolverexecution.DecodeFailure(forward, layout, read.Values[0].Value, *evidence)
	if err != nil {
		return RecoverySource{}, err
	}
	queue, err := workqueue.NewRepository(repository.store)
	if err != nil {
		return RecoverySource{}, err
	}
	forwardView, err := queue.ReadResumeAt(ctx, forward.TaskID, forward.WorkItemID, forward.ID, forward.Executor, view.ReadRevision)
	if err != nil {
		return RecoverySource{}, err
	}
	if (failure.Checkpoint == nil) != (forwardView.Checkpoint.Record == nil) ||
		failure.Checkpoint != nil && *failure.Checkpoint != *forwardView.Checkpoint.Record {
		return RecoverySource{}, invalid("DNS resolver cleanup failure differs from the saved forward checkpoint")
	}
	if cursor := forwardView.Checkpoint.Record; cursor != nil {
		if _, err := repository.ReadProgress(ctx, forwardView, stored.Input, cursor.Sequence); err != nil {
			return RecoverySource{}, err
		}
	}
	source := RecoverySource{Recovery: resolverexecution.Recovery{Forward: forward, Failure: failure, Evidence: *evidence},
		Guards: append(append([]keyvalue.Condition(nil), stored.Guards...),
			keyvalue.Condition{Key: key, ModRevision: read.Values[0].ModRevision},
			keyvalue.Condition{Key: workqueue.AttemptKey(forward.ID), ModRevision: attemptRead.Values[0].ModRevision},
			keyvalue.Condition{Key: workqueue.CheckpointKey(forward.ID), ModRevision: forwardView.Checkpoint.Revision},
			keyvalue.Condition{Key: workqueue.TaskKey(view.Task.Task.ID), ModRevision: view.Task.Revision},
			keyvalue.Condition{Key: workqueue.ItemKey(view.Item.Item.ID), ModRevision: view.Item.Revision})}
	if number > forward.Number+1 {
		_, prior, err := queue.ReadAttempt(ctx, forward.TaskID, forward.WorkItemID, number-1, view.ReadRevision)
		if err != nil {
			return RecoverySource{}, err
		}
		result, err := repository.read(ctx, []string{RecoveryResultKey(prior.Attempt.ID)}, view.ReadRevision)
		if err != nil {
			return RecoverySource{}, err
		}
		defer keyvalue.ClearValues(result.Values)
		if result.Values[0] == nil || len(result.Values[0].Value) > resolverexecution.MaximumReportBytes {
			return RecoverySource{}, invalid("DNS resolver cleanup Retry lost the immediately preceding result")
		}
		report, err := jcs.Decode[resolverexecution.RecoveryReport](result.Values[0].Value)
		if err != nil || prior.Attempt.Result == nil {
			return RecoverySource{}, invalid("DNS resolver cleanup Retry has an invalid preceding result")
		}
		source.Recovery.Previous = &resolverexecution.RecoveryInterruption{Attempt: prior.Attempt, Report: report, Evidence: *prior.Attempt.Result}
		source.Guards = append(source.Guards, keyvalue.Condition{Key: workqueue.AttemptKey(prior.Attempt.ID), ModRevision: prior.Revision},
			keyvalue.Condition{Key: result.Values[0].Key, ModRevision: result.Values[0].ModRevision})
	}
	if err := source.Recovery.Validate(layout); err != nil {
		return RecoverySource{}, err
	}
	return source, nil
}
