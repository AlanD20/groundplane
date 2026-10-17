package attachremoval

import (
	"context"
	"time"

	"github.com/AlanD20/groundplane/internal/common/attachexecution"
	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/recordcodec"
	"github.com/AlanD20/groundplane/internal/infra/etcd/workqueue"
)

// prepareRemovalRecoverySource joins original failure and its inspection authority
// before the shared queue may release the forward slot and dispatch recovery.
func prepareRemovalRecoverySource(ctx context.Context, reader Reader, view workqueue.ResumeView,
	input attachexecution.RemovalInput, reason attachexecution.FailureReason, result workexecution.Completion, at time.Time,
) (workqueue.Change, error) {
	if result.EffectsSettled || input.Attempt.Direction != workexecution.Forward || view.Checkpoint.Record == nil ||
		view.Checkpoint.Record.Sequence%2 != 1 {
		return workqueue.Change{}, recordcodec.CorruptRecord()
	}
	_, forward, err := workexecution.Finish(view.Work.Item.Item, view.Attempt.Attempt, result, at)
	if err != nil {
		return workqueue.Change{}, err
	}
	authority, err := input.Authority()
	if err != nil {
		return workqueue.Change{}, err
	}
	source := attachexecution.RemovalRecovery{Forward: forward, Evidence: result.Evidence,
		Failure: attachexecution.RemovalFailure{AttemptID: forward.ID, Input: forward.Input, Authority: authority,
			Reason: reason, Checkpoint: view.Checkpoint.Record, RecordedAt: at}}
	raw, err := attachexecution.EncodeRemovalRecovery(source)
	if err != nil {
		return workqueue.Change{}, err
	}
	key := WorkRecoveryKey(forward.WorkItemID)
	read, err := reader.GetMany(ctx, keyvalue.GetManyRequest{Keys: []string{key}, Revision: view.Work.ReadRevision})
	if err != nil {
		clear(raw)
		return workqueue.Change{}, err
	}
	if read != nil {
		defer keyvalue.ClearValues(read.Values)
	}
	if read == nil || read.ReadRevision != view.Work.ReadRevision || len(read.Values) != 1 ||
		read.Values[0] != nil && (read.Values[0].Key != key || read.Values[0].ModRevision <= 0) {
		clear(raw)
		return workqueue.Change{}, recordcodec.CorruptRecord()
	}
	if value := read.Values[0]; value != nil {
		previous, err := attachexecution.DecodeRemovalRecovery(value.Value)
		if err != nil || previous.Forward.TaskID != forward.TaskID || previous.Forward.WorkItemID != forward.WorkItemID ||
			previous.Forward.Input != forward.Input || previous.Forward.Executor.HostID != forward.Executor.HostID ||
			previous.Forward.Number >= forward.Number {
			clear(raw)
			return workqueue.Change{}, recordcodec.CorruptRecord()
		}
	}
	return workqueue.Change{Conditions: []keyvalue.Condition{{Key: key, ModRevision: keyvalue.RevisionOf(read.Values[0])}},
		Mutations: []keyvalue.Mutation{{Type: keyvalue.MutationPut, Key: key, Value: raw}}}, nil
}

func (repository *InputRepository) LoadDetachRecovery(ctx context.Context, view workqueue.View) (RecoverySnapshot, error) {
	if view.Item.Item.Operation != workexecution.Detach {
		return RecoverySnapshot{}, recordcodec.CorruptRecord()
	}
	return LoadRecovery(ctx, repository.store, view)
}

// DetachRecoveryInput delivers the captured network descriptor only when that
// exact command was interrupted. Adapter recovery receives no consumer authority.
func (repository *InputRepository) DetachRecoveryInput(ctx context.Context, view workqueue.View,
	attempt workexecution.Attempt,
) (attachexecution.RecoveryInput, []keyvalue.Condition, error) {
	source, err := repository.LoadDetachRecovery(ctx, view)
	if err != nil {
		return attachexecution.RecoveryInput{}, nil, err
	}
	capture, err := repository.LoadDetachCapture(ctx, view)
	if err != nil {
		return attachexecution.RecoveryInput{}, nil, err
	}
	defer capture.Clear()
	input := attachexecution.RecoveryInput{Attempt: attempt, Source: source.Source}
	if input.RequiresConsumer() {
		input.Consumer, err = capture.Input.ConsumerTransition(ctx)
		if err != nil {
			return attachexecution.RecoveryInput{}, nil, err
		}
	}
	if err := input.Validate(); err != nil {
		attachexecution.ClearRecoveryInput(&input)
		return attachexecution.RecoveryInput{}, nil, err
	}
	conditions := append(capture.Conditions, keyvalue.Condition{Key: WorkRecoveryKey(attempt.WorkItemID), ModRevision: source.Revision})
	return input, conditions, nil
}
