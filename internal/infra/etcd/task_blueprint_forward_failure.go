package etcd

import (
	"context"
	"time"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/infra/etcd/blueprints"
	"github.com/AlanD20/groundplane/internal/infra/etcd/blueprintunits"
	"github.com/AlanD20/groundplane/internal/infra/etcd/idempotency"
	"github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/taskjournal"
	"github.com/AlanD20/groundplane/pkg/errs"
)

// RequestBlueprintForwardFailure closes publication before the coordinator
// settles already-started children. It never invents an Agent execution or proof
// of restoration for a unit that could not be prepared.
func (repository *TaskRepository) RequestBlueprintForwardFailure(
	ctx context.Context,
	taskID string,
	expected blueprintunits.Snapshot,
	phase string,
	code errs.Code,
	detail string,
	at time.Time,
) error {
	if err := keyvalue.ValidateContext(ctx); err != nil {
		return err
	}
	if expected.HeadTaskID != taskID || expected.HeadRevision <= 0 || expected.ReadRevision <= 0 ||
		expected.EpochRevision < 0 || ids.Validate(ids.KindEnvironment, expected.EnvironmentID) != nil {
		return errs.New(errs.KindValidationFailed, "Blueprint failure decision snapshot is invalid")
	}
	record := blueprintParentFailureRecord{
		Schema: 2, ParentTaskID: taskID, CoordinatorPhase: phase, CoordinatorCode: code, CoordinatorDetail: detail, FailedAt: at,
	}
	failureValue, err := encodeBlueprintParentFailureRecord(record)
	if err != nil {
		return err
	}
	defer clear(failureValue)
	stopValue, err := encodeBlueprintParentStopRequest(blueprintParentStopRequest{
		TaskID: taskID, RequestedAt: at, Outcome: taskjournal.TaskStatusFailed,
	})
	if err != nil {
		return err
	}
	defer clear(stopValue)
	taskKey := taskjournal.TaskStorageKey(taskID)
	claimKey := taskjournal.BlueprintParentClaimKey(taskID)
	stopKey := taskjournal.BlueprintParentAbortKey(taskID)
	failureKey := blueprintParentFailureKey(taskID)
	read, err := repository.store.GetMany(ctx, keyvalue.GetManyRequest{
		Keys: []string{taskKey, claimKey, stopKey, failureKey},
	})
	if err != nil {
		return err
	}
	if read == nil || len(read.Values) != 4 || read.Values[0] == nil || read.Values[1] == nil {
		return errs.New(errs.KindStateConflict, "Blueprint preparation claim is unavailable")
	}
	defer keyvalue.ClearValues(read.Values)
	task, err := DecodeTaskRecord(read.Values[0].Value)
	if err != nil || task.ID != taskID || validateBlueprintParentClaimTask(task) != nil ||
		task.Status != taskjournal.TaskStatusRunning || task.Owner.EnvironmentID != expected.EnvironmentID {
		return errs.New(errs.KindStateConflict, "Blueprint preparation parent is no longer running")
	}
	claimID, err := idempotency.DecodeTaskReference(read.Values[1].Value)
	if err != nil || claimID != taskID {
		return errs.New(errs.KindInternal, "Blueprint preparation claim is inconsistent")
	}
	if read.Values[2] != nil {
		stop, decodeErr := decodeBlueprintParentStopRequest(read.Values[2].Value)
		if decodeErr != nil || stop.TaskID != taskID {
			return errs.New(errs.KindInternal, "Blueprint preparation stop gate is corrupt")
		}
		// Preserve the first durable Abort, timeout or child-failure outcome.
		return nil
	}
	if read.Values[3] != nil {
		return errs.New(errs.KindInternal, "Blueprint preparation failure lacks its stop gate")
	}
	headKey := blueprints.EnvironmentBlueprintHeadKey(task.Owner.EnvironmentID)
	epochKey := blueprintunits.EpochKey(task.Owner.EnvironmentID)
	head, err := repository.store.GetMany(ctx, keyvalue.GetManyRequest{
		Keys: []string{headKey, epochKey}, Revision: read.ReadRevision,
	})
	if err != nil {
		return err
	}
	if head == nil || head.ReadRevision != read.ReadRevision || len(head.Values) != 2 || head.Values[0] == nil {
		return errs.New(errs.KindStateConflict, "Blueprint preparation desired head is unavailable")
	}
	defer keyvalue.ClearValues(head.Values)
	if head.Values[0].ModRevision != expected.HeadRevision ||
		keyvalue.RevisionOf(head.Values[1]) != expected.EpochRevision {
		return errs.New(errs.KindStateConflict, "Blueprint failure decision state changed")
	}
	headID, err := idempotency.DecodeTaskReference(head.Values[0].Value)
	if err != nil || headID != taskID {
		return errs.New(errs.KindStateConflict, "Blueprint preparation was superseded")
	}
	result, err := repository.store.Transact(ctx, []keyvalue.Condition{
		{Key: taskKey, ModRevision: read.Values[0].ModRevision},
		{Key: claimKey, ModRevision: read.Values[1].ModRevision},
		{Key: headKey, ModRevision: head.Values[0].ModRevision},
		{Key: epochKey, ModRevision: expected.EpochRevision},
		{Key: stopKey}, {Key: failureKey},
	}, []keyvalue.Mutation{
		{Type: keyvalue.MutationPut, Key: stopKey, Value: stopValue},
		{Type: keyvalue.MutationPut, Key: failureKey, Value: failureValue},
	})
	keyvalue.ClearValues(result.FailureReads)
	if err != nil {
		return err
	}
	if !result.Succeeded {
		return errs.New(errs.KindStateConflict, "Blueprint preparation failure publication changed")
	}
	return nil
}
