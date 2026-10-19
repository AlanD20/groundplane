package attachremoval

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"time"

	"github.com/AlanD20/groundplane/internal/common/attachexecution"
	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/common/workexecution"
	attachrecord "github.com/AlanD20/groundplane/internal/infra/etcd/attachments"
	"github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/recordcodec"
	"github.com/AlanD20/groundplane/internal/infra/etcd/workqueue"
	"github.com/AlanD20/groundplane/pkg/errs"
)

func (repository *InputRepository) PrepareDetachRetry(ctx context.Context, view workqueue.View,
	previous workqueue.AttemptSnapshot, at time.Time,
) (workqueue.Change, error) {
	item, task := view.Item.Item, view.Task.Task
	if task.Action == workexecution.ActionRemove && item.AttemptNumber == 0 && item.Result != nil &&
		item.Result.ID == ids.DeriveAt(ids.KindPlan, task.CreatedAt, task.ID, "cascade-publication-rejection") {
		return workqueue.Change{}, errs.New(
			errs.KindTaskNotRetryable,
			"Zone removal was rejected before execution; submit a new removal to capture current consumers and approval",
		)
	}
	if item.Operation != workexecution.Detach || item.Input == nil || item.ActiveAttemptID != "" ||
		task.CollectionStartedAt != nil ||
		item.TaskID != task.ID ||
		at.IsZero() ||
		at.Before(item.UpdatedAt) {
		return workqueue.Change{}, errs.New(errs.KindTaskNotRetryable, "Detach has no retained idle work to retry")
	}
	input, conditions, err := repository.LoadDetach(ctx, view)
	if err != nil {
		return workqueue.Change{}, err
	}
	defer input.Clear()
	if item.Phase == workexecution.Attention {
		return repository.prepareDetachRecoveryRetry(ctx, view, previous, at)
	}
	if task.Stop != nil || item.ForwardRevoked {
		return workqueue.Change{}, errs.New(
			errs.KindTaskNotRetryable,
			"stopped or replaced Detach requires a new action",
		)
	}
	budget := attachexecution.RemovalBudget(0)
	source := input.Snapshot.Source
	if source.Attach.HookBundle && source.Backing.Desired.Hooks != nil && source.Backing.Desired.Hooks.Detach != nil {
		budget = attachexecution.RemovalBudget(int64(source.Backing.Desired.Hooks.Detach.TimeoutSeconds))
	}
	var change workqueue.Change
	var retryErr error
	if item.AttemptNumber == 0 {
		if item.Result == nil || item.Outcome != workexecution.Failed {
			return workqueue.Change{}, errs.New(errs.KindTaskNotRetryable, "Detach has no failed unassigned decision")
		}
		key := DetachDecisionKey(item.ID, item.Result.ID)
		read, err := repository.read(ctx, []string{key}, view.ReadRevision)
		if err != nil {
			return workqueue.Change{}, err
		}
		defer keyvalue.ClearValues(read.Values)
		if read.Values[0] == nil {
			return workqueue.Change{}, recordcodec.CorruptRecord()
		}
		decision, err := recordcodec.Decode[detachDecision](read.Values[0].Value, "detach-decision")
		digest := sha256.Sum256(read.Values[0].Value)
		if err != nil || decision.ID != item.Result.ID || decision.TaskID != task.ID ||
			decision.WorkItemID != item.ID ||
			decision.AttachID != item.TargetID ||
			decision.Input != *item.Input ||
			decision.Outcome != workexecution.Failed ||
			!decision.At.Equal(item.UpdatedAt) ||
			hex.EncodeToString(digest[:]) != item.Result.SHA256 {
			return workqueue.Change{}, recordcodec.CorruptRecord()
		}
		conditions = append(conditions, keyvalue.Condition{Key: key, ModRevision: read.Values[0].ModRevision})
		change, retryErr = workqueue.PrepareUnstartedRetry(view.Task, view.Item, at.Add(budget), at)
	} else {
		attempt := previous.Attempt
		if previous.Revision <= 0 || attempt.FinishedAt == nil || attempt.Result == nil || attempt.Input != *item.Input ||
			attempt.TaskID != task.ID || attempt.WorkItemID != item.ID || attempt.Number != item.AttemptNumber ||
			attempt.Executor.HostID != item.Placement.HostID || item.Phase != workexecution.Settled || item.Outcome != workexecution.Failed {
			return workqueue.Change{}, errs.New(errs.KindTaskNotRetryable, "Detach has no matching settled failed attempt")
		}
		proof, err := repository.detachForwardRetryBasis(ctx, view, previous)
		if err != nil {
			return workqueue.Change{}, err
		}
		conditions = append(conditions, proof...)
		capture, err := repository.DetachCaptureAdmission(ctx, view)
		if err != nil {
			return workqueue.Change{}, err
		}
		conditions = append(conditions, capture.Conditions...)
		change, retryErr = workqueue.PrepareRetry(view.Task, view.Item, previous, workexecution.RetryDecision{
			Direction: workexecution.Forward, Result: *attempt.Result, NotBefore: at, Deadline: at.Add(budget)}, at)
	}
	if retryErr != nil {
		return workqueue.Change{}, retryErr
	}
	resource, err := repository.prepareDetachRetryBinding(ctx, view, input)
	if err != nil {
		keyvalue.ClearMutationValues(change.Mutations)
		return workqueue.Change{}, err
	}
	if len(input.Consumer) != 0 {
		consumer, err := repository.DetachConsumerAdmission(ctx, view)
		if err != nil {
			keyvalue.ClearMutationValues(change.Mutations)
			keyvalue.ClearMutationValues(resource.Mutations)
			return workqueue.Change{}, err
		}
		conditions = append(conditions, consumer.Conditions...)
	}
	change.Conditions = append(change.Conditions, conditions...)
	change.Conditions = append(change.Conditions, resource.Conditions...)
	change.Mutations = append(change.Mutations, resource.Mutations...)
	return change, nil
}

func (repository *InputRepository) prepareDetachRetryBinding(ctx context.Context, view workqueue.View,
	input DetachInput,
) (workqueue.Change, error) {
	current, conditions, err := repository.readOwnedDetach(ctx, view, input)
	if err != nil {
		return workqueue.Change{}, err
	}
	if current.Record.Status != "failed" {
		return workqueue.Change{}, errs.New(errs.KindTaskNotRetryable, "Detach does not own a failed binding")
	}
	retrying, err := attachrecord.RetryAttachOperation(current.Record, view.Task.Task.ID)
	if err != nil {
		return workqueue.Change{}, err
	}
	raw, err := attachrecord.EncodeAttachRecord(retrying)
	if err != nil {
		return workqueue.Change{}, err
	}
	return workqueue.Change{Conditions: conditions,
		Mutations: []keyvalue.Mutation{
			{Type: keyvalue.MutationPut, Key: attachrecord.AttachKey(retrying.ID), Value: raw},
		}}, nil
}

func (repository *InputRepository) detachForwardRetryBasis(ctx context.Context, view workqueue.View,
	previous workqueue.AttemptSnapshot,
) ([]keyvalue.Condition, error) {
	attempt := previous.Attempt
	if attempt.Direction == workexecution.Forward {
		key := WorkFailureKey(attempt.ID)
		read, err := repository.read(ctx, []string{key}, view.ReadRevision)
		if err != nil {
			return nil, err
		}
		defer keyvalue.ClearValues(read.Values)
		if read.Values[0] == nil {
			return nil, recordcodec.CorruptRecord()
		}
		failure, err := attachexecution.DecodeRemovalFailure(attempt, read.Values[0].Value, *attempt.Result)
		if err != nil {
			return nil, err
		}
		if failure.Checkpoint != nil && failure.Checkpoint.Sequence%2 != 0 {
			return nil, errs.New(errs.KindTaskNotRetryable, "inspect the interrupted Detach command before retrying it")
		}
		return []keyvalue.Condition{{Key: key, ModRevision: read.Values[0].ModRevision}}, nil
	}
	if attempt.Direction != workexecution.Recovery || attempt.Outcome != workexecution.Succeeded ||
		view.Item.Item.Cleanup != workexecution.CleanupComplete {
		return nil, errs.New(errs.KindTaskNotRetryable, "Detach execution is not proven stopped")
	}
	input, conditions, err := repository.DetachRecoveryInput(ctx, view, attempt)
	if err != nil {
		return nil, err
	}
	defer attachexecution.ClearRecoveryInput(&input)
	key := WorkInspectionKey(attempt.ID)
	read, err := repository.read(ctx, []string{key}, view.ReadRevision)
	if err != nil {
		return nil, err
	}
	defer keyvalue.ClearValues(read.Values)
	if read.Values[0] == nil {
		return nil, recordcodec.CorruptRecord()
	}
	report, err := attachexecution.DecodeRemovalInspection(input, read.Values[0].Value, *attempt.Result)
	if err != nil || !report.ExecutionStopped() || report.ObservedAt.After(*attempt.FinishedAt) {
		return nil, errs.New(errs.KindTaskNotRetryable, "Detach command has no retained proof that execution stopped")
	}
	return append(conditions, keyvalue.Condition{Key: key, ModRevision: read.Values[0].ModRevision}), nil
}

func (repository *InputRepository) prepareDetachRecoveryRetry(ctx context.Context, view workqueue.View,
	previous workqueue.AttemptSnapshot, at time.Time,
) (workqueue.Change, error) {
	item, attempt := view.Item.Item, previous.Attempt
	if previous.Revision <= 0 || attempt.FinishedAt == nil || attempt.Result == nil ||
		attempt.Number != item.AttemptNumber ||
		attempt.TaskID != item.TaskID ||
		attempt.WorkItemID != item.ID ||
		attempt.Input != *item.Input ||
		attempt.Executor.HostID != item.Placement.HostID ||
		(item.Cleanup != workexecution.CleanupRequired && item.Cleanup != workexecution.CleanupStopped) {
		return workqueue.Change{}, errs.New(errs.KindTaskNotRetryable, "Detach has no stopped inspection to retry")
	}
	source, err := repository.LoadDetachRecovery(ctx, view)
	if err != nil {
		return workqueue.Change{}, err
	}
	if attempt.Direction == workexecution.Forward &&
		(source.Source.Forward.ID != attempt.ID || source.Source.Evidence != *attempt.Result) ||
		attempt.Direction == workexecution.Recovery &&
			(item.CleanupResult == nil || *item.CleanupResult != *attempt.Result || source.Source.Forward.Number >= attempt.Number) {
		return workqueue.Change{}, recordcodec.CorruptRecord()
	}
	authority, err := repository.DetachCaptureAdmission(ctx, view)
	if err != nil {
		return workqueue.Change{}, err
	}
	resource, err := repository.PrepareDetachFailed(ctx, view)
	if err != nil {
		return workqueue.Change{}, err
	}
	if len(resource.Mutations) != 0 {
		keyvalue.ClearMutationValues(resource.Mutations)
		return workqueue.Change{}, recordcodec.CorruptRecord()
	}
	change, err := workqueue.PrepareRetry(view.Task, view.Item, previous, workexecution.RetryDecision{
		Direction: workexecution.Recovery, Result: *attempt.Result, NotBefore: at, Deadline: at.Add(attachexecution.InspectionBudget)}, at)
	if err != nil {
		return workqueue.Change{}, err
	}
	change.Conditions = append(change.Conditions, authority.Conditions...)
	change.Conditions = append(change.Conditions, resource.Conditions...)
	change.Conditions = append(
		change.Conditions,
		keyvalue.Condition{Key: WorkRecoveryKey(item.ID), ModRevision: source.Revision},
	)
	return change, nil
}
