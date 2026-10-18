package releasework

import (
	"context"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/common/jcs"
	"github.com/AlanD20/groundplane/internal/common/releaseexecution"
	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/workqueue"
)

type RecoveryOrigin struct {
	Forward    workexecution.Attempt
	Failure    releaseexecution.Failure
	Conditions []keyvalue.Condition
}

// LoadRecoveryOrigin follows the work's current forward result directly. It
// never scans diagnostics or substitutes FirstFailure after a later retry failed.
func LoadRecoveryOrigin(ctx context.Context, store Reader, view workqueue.ResumeView) (RecoveryOrigin, error) {
	item, recovery := view.Work.Item.Item, view.Attempt.Attempt
	if keyvalue.ValidateContext(ctx) != nil || store == nil || view.Work.ReadRevision <= 0 || item.Validate() != nil ||
		item.Operation != workexecution.Release || item.Result == nil || recovery.Validate() != nil || recovery.Direction != workexecution.Recovery ||
		!view.ActiveAttempt || view.ExecutorRevision <= 0 || recovery.TaskID != item.TaskID || recovery.WorkItemID != item.ID {
		return RecoveryOrigin{}, invalid()
	}
	origin, err := LoadRecoverySource(ctx, store, view.Work)
	if err != nil {
		return RecoveryOrigin{}, err
	}
	binding := releaseexecution.RecoveryInput{Input: releaseexecution.Input{Attempt: recovery}, Forward: origin.Forward, Failure: origin.Failure}
	if err := binding.ValidateIdentity(); err != nil {
		return RecoveryOrigin{}, err
	}
	return origin, nil
}

// LoadRecoverySource resolves failed forward authority before an attempt exists.
// Assignment must compare these sources; it must not invent a recovery identity
// merely to read the captured failure.
func LoadRecoverySource(ctx context.Context, store Reader, work workqueue.View) (RecoveryOrigin, error) {
	item := work.Item.Item
	if keyvalue.ValidateContext(ctx) != nil || store == nil || work.ReadRevision <= 0 || work.Item.Revision <= 0 ||
		item.Validate() != nil || item.Operation != workexecution.Release || item.Result == nil || item.Input == nil ||
		(item.Cleanup != workexecution.CleanupRequired && item.Cleanup != workexecution.CleanupStopped && item.Cleanup != workexecution.CleanupRunning) {
		return RecoveryOrigin{}, invalid()
	}
	failureValue, err := readRecoveryValue(ctx, store, FailureKey(item.Result.ID), work.ReadRevision, releaseexecution.MaximumCheckpointBytes)
	if err != nil {
		return RecoveryOrigin{}, err
	}
	defer clear(failureValue.Value)
	hint, err := jcs.Decode[releaseexecution.Failure](failureValue.Value)
	if err != nil || ids.Validate(ids.KindAttempt, hint.AttemptID) != nil {
		return RecoveryOrigin{}, invalid()
	}
	attemptValue, err := readRecoveryValue(ctx, store, workqueue.AttemptKey(hint.AttemptID), work.ReadRevision, maximumProgressBytes)
	if err != nil {
		return RecoveryOrigin{}, err
	}
	defer clear(attemptValue.Value)
	forward, err := workqueue.DecodeAttempt(attemptValue.Value)
	if err != nil {
		return RecoveryOrigin{}, err
	}
	if forward.ID != hint.AttemptID || forward.Result == nil || *forward.Result != *item.Result ||
		forward.Direction != workexecution.Forward || forward.FinishedAt == nil || forward.Outcome == workexecution.Succeeded ||
		forward.TaskID != item.TaskID || forward.WorkItemID != item.ID || forward.Input != *item.Input ||
		forward.Executor.HostID != item.Placement.HostID || forward.Number > item.AttemptNumber {
		return RecoveryOrigin{}, invalid()
	}
	failure, err := releaseexecution.DecodeFailure(forward, failureValue.Value, *item.Result)
	if err != nil {
		return RecoveryOrigin{}, err
	}
	if !failure.RecordedAt.Equal(*forward.FinishedAt) {
		return RecoveryOrigin{}, invalid()
	}
	return RecoveryOrigin{Forward: forward, Failure: failure, Conditions: []keyvalue.Condition{
		{Key: failureValue.Key, ModRevision: failureValue.ModRevision},
		{Key: attemptValue.Key, ModRevision: attemptValue.ModRevision},
		{Key: workqueue.ItemKey(item.ID), ModRevision: work.Item.Revision},
	}}, nil
}

func readRecoveryValue(ctx context.Context, store Reader, key string, revision int64, limit int) (*keyvalue.KeyValue, error) {
	read, err := store.GetMany(ctx, keyvalue.GetManyRequest{Keys: []string{key}, Revision: revision})
	if err != nil {
		return nil, err
	}
	if read == nil {
		return nil, invalid()
	}
	defer keyvalue.ClearValues(read.Values)
	if read.ReadRevision != revision || len(read.Values) != 1 || read.Values[0] == nil {
		return nil, invalid()
	}
	value := read.Values[0]
	if value.Key != key || value.ModRevision <= 0 || value.ModRevision > revision || len(value.Value) == 0 || len(value.Value) > limit {
		return nil, invalid()
	}
	owned := *value
	owned.Value = append([]byte(nil), value.Value...)
	return &owned, nil
}
