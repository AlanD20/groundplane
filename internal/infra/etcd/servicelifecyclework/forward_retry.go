package servicelifecyclework

import (
	"context"
	"strconv"
	"time"

	"github.com/AlanD20/groundplane/internal/common/lifecycleexecution"
	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/workqueue"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/AlanD20/groundplane/proto/agentpb"
)

func ContinuationPrefix(workID string) string {
	return "/v1/execution/service-lifecycle/continuations/" + workID + "/"
}
func ContinuationKey(workID string, number uint64) string {
	return ContinuationPrefix(workID) + strconv.FormatUint(number, 10)
}

// PrepareForwardRetry captures proof of completed stages with the queued retry.
// Unknown effects cannot enter the forward queue; they require recovery first.
func (repository *Repository) PrepareForwardRetry(
	ctx context.Context,
	view workqueue.View,
	previous workqueue.AttemptSnapshot,
	input *agentpb.ServiceLifecycleWorkInput,
	at, deadline time.Time,
) (workqueue.Change, error) {
	task, item, attempt := view.Task.Task, view.Item.Item, previous.Attempt
	if item.Operation != workexecution.ServiceLifecycle || task.Stop != nil || task.CollectionStartedAt != nil ||
		item.ForwardRevoked ||
		item.Phase != workexecution.Settled ||
		item.Outcome != workexecution.Failed ||
		item.Input == nil ||
		item.Result == nil ||
		attempt.FinishedAt == nil ||
		attempt.Result == nil ||
		attempt.Input != *item.Input ||
		attempt.Number != item.AttemptNumber ||
		attempt.Executor.HostID != item.Placement.HostID ||
		!deadline.After(at) {
		return workqueue.Change{}, errs.New(
			errs.KindTaskNotRetryable,
			"Service lifecycle Retry requires a failed action with settled effects",
		)
	}
	captured, change, err := repository.PrepareAssignment(ctx, view)
	if err != nil {
		return workqueue.Change{}, err
	}
	if !sameCapture(captured, captured, input) {
		return workqueue.Change{}, conflict("Service lifecycle Retry input changed")
	}
	var continuation lifecycleexecution.Continuation
	if attempt.Direction == workexecution.Recovery {
		if item.Cleanup != workexecution.CleanupComplete || item.CleanupResult == nil ||
			*item.CleanupResult != *attempt.Result {
			return workqueue.Change{}, errs.New(
				errs.KindTaskNotRetryable,
				"Service lifecycle Retry requires completed recovery",
			)
		}
		saved, err := repository.LoadRecovery(ctx, view, input)
		if err != nil {
			return workqueue.Change{}, err
		}
		raw, completed, err := repository.LoadRecoveryResult(
			ctx,
			workqueue.ResumeView{Work: view, Attempt: previous},
			input,
		)
		if err != nil {
			return workqueue.Change{}, err
		}
		if !completed || saved.Source.Evidence != *item.Result {
			return workqueue.Change{}, conflict("Service lifecycle recovery did not prove the failed boundary")
		}
		report, _, err := lifecycleexecution.DecodeRecoveryReport(attempt, input, saved.Source, raw)
		if err != nil {
			return workqueue.Change{}, err
		}
		continuation = lifecycleexecution.Continuation{Source: saved.Source, Inspector: &attempt, Inspection: &report}
		change.Conditions = append(
			change.Conditions,
			keyvalue.Condition{Key: RecoveryKey(item.ID), ModRevision: saved.Revision},
		)
	} else {
		if attempt.Direction != workexecution.Forward || item.Cleanup != workexecution.CleanupNone || *item.Result != *attempt.Result {
			return workqueue.Change{}, errs.New(errs.KindTaskNotRetryable, "Service lifecycle Retry has unresolved effects")
		}
		source, guards, err := repository.LoadRecoverySource(ctx, view, previous, input)
		if err != nil {
			return workqueue.Change{}, err
		}
		continuation.Source = source
		change.Conditions = append(change.Conditions, guards...)
		// A retry can fail before acknowledging its inherited prefix. Keep
		// the original continuation rather than treating that as zero progress.
		if source.Progress == nil && attempt.Number > 1 {
			prior, guard, err := repository.LoadContinuation(ctx, view, attempt, input)
			if err != nil {
				return workqueue.Change{}, err
			}
			if prior == nil {
				return workqueue.Change{}, conflict("Service lifecycle Retry lost its completed-stage evidence")
			}
			continuation = *prior
			change.Conditions = append(change.Conditions, guard)
		}
	}
	raw, err := lifecycleexecution.EncodeContinuation(input, continuation)
	if err != nil {
		return workqueue.Change{}, err
	}
	retry, err := workqueue.PrepareRetry(view.Task, view.Item, previous, workexecution.RetryDecision{
		Direction: workexecution.Forward, Result: *attempt.Result, NotBefore: at, Deadline: deadline,
	}, at)
	if err != nil {
		clear(raw)
		return workqueue.Change{}, err
	}
	key := ContinuationKey(item.ID, item.AttemptNumber+1)
	retry.Conditions = append(retry.Conditions, change.Conditions...)
	retry.Conditions = append(retry.Conditions, keyvalue.Condition{Key: key})
	retry.Mutations = append(retry.Mutations, keyvalue.Mutation{Type: keyvalue.MutationPut, Key: key, Value: raw})
	return retry, nil
}

func (repository *Repository) LoadContinuation(ctx context.Context, view workqueue.View, attempt workexecution.Attempt,
	input *agentpb.ServiceLifecycleWorkInput,
) (*lifecycleexecution.Continuation, keyvalue.Condition, error) {
	return LoadContinuation(ctx, repository.store, view, attempt, input)
}

// LoadContinuation is shared by operation owners executing the same captured
// member procedure. It reads only exact retry authority, never latest intent.
func LoadContinuation(ctx context.Context, reader ReportReader, view workqueue.View, attempt workexecution.Attempt,
	input *agentpb.ServiceLifecycleWorkInput,
) (*lifecycleexecution.Continuation, keyvalue.Condition, error) {
	if attempt.TaskID != view.Task.Task.ID || attempt.WorkItemID != view.Item.Item.ID ||
		attempt.Direction != workexecution.Forward ||
		attempt.Number == 0 {
		return nil, keyvalue.Condition{}, conflict("Service lifecycle continuation requires its forward assignment")
	}
	key := ContinuationKey(attempt.WorkItemID, attempt.Number)
	if reader == nil || view.ReadRevision <= 0 {
		return nil, keyvalue.Condition{}, conflict("Service runtime continuation has no consistent snapshot")
	}
	read, err := reader.GetMany(ctx, keyvalue.GetManyRequest{Keys: []string{key}, Revision: view.ReadRevision})
	if err != nil {
		return nil, keyvalue.Condition{}, err
	}
	if read == nil {
		return nil, keyvalue.Condition{}, conflict("Service runtime continuation is unavailable")
	}
	defer keyvalue.ClearValues(read.Values)
	if read.ReadRevision != view.ReadRevision || len(read.Values) != 1 ||
		read.Values[0] != nil &&
			(read.Values[0].Key != key || read.Values[0].ModRevision <= 0 || read.Values[0].ModRevision > view.ReadRevision) {
		return nil, keyvalue.Condition{}, conflict("Service runtime continuation snapshot is invalid")
	}
	if read.Values[0] == nil {
		if attempt.Number != 1 {
			return nil, keyvalue.Condition{}, conflict("Service lifecycle Retry continuation is missing")
		}
		return nil, keyvalue.Condition{Key: key}, nil
	}
	continuation, err := lifecycleexecution.DecodeContinuation(input, read.Values[0].Value)
	if err != nil {
		return nil, keyvalue.Condition{}, err
	}
	if err := continuation.ValidateFor(attempt, input); err != nil {
		return nil, keyvalue.Condition{}, err
	}
	return &continuation, keyvalue.Condition{Key: key, ModRevision: read.Values[0].ModRevision}, nil
}
