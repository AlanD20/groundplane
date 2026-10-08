package etcd

import (
	"context"
	"time"

	idempotencyrecord "github.com/AlanD20/groundplane/internal/infra/etcd/idempotency"
	etcdstore "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/recordcodec"
	activationrecord "github.com/AlanD20/groundplane/internal/infra/etcd/softwareactivation"
	"github.com/AlanD20/groundplane/internal/infra/etcd/taskjournal"
	"github.com/AlanD20/groundplane/pkg/errs"
)

// CheckpointSoftwareActivation publishes one monotonic coordinator checkpoint
// while fencing the running parent Task and its exclusive activation claim.
func (repository *TaskRepository) CheckpointSoftwareActivation(
	ctx context.Context,
	taskID, operationID, inputSHA256 string,
	progress activationrecord.Progress,
	at time.Time,
) (etcdstore.Versioned[activationrecord.Record], error) {
	if err := etcdstore.ValidateContext(ctx); err != nil {
		return etcdstore.Versioned[activationrecord.Record]{}, err
	}
	if err := recordcodec.ValidateTimestamp("software activation checkpoint", at); err != nil {
		return etcdstore.Versioned[activationrecord.Record]{}, err
	}
	keys := []string{
		taskjournal.TaskStorageKey(taskID), activationrecord.ClaimKey(taskID), activationrecord.Key(taskID),
	}
	read, err := repository.store.GetMany(ctx, etcdstore.GetManyRequest{Keys: keys})
	if err != nil {
		return etcdstore.Versioned[activationrecord.Record]{}, err
	}
	if read == nil || len(read.Values) != 3 || read.Values[0] == nil || read.Values[1] == nil || read.Values[2] == nil {
		return etcdstore.Versioned[activationrecord.Record]{}, errs.New(
			errs.KindStateConflict,
			"running software activation claim is unavailable",
		)
	}
	task, err := DecodeTaskRecord(read.Values[0].Value)
	if err != nil || validateSoftwareActivationTask(task) != nil || task.ID != taskID ||
		task.OperationID != operationID || task.PlanHash != inputSHA256 ||
		task.Status != taskjournal.TaskStatusRunning || task.idempotencyMarker == nil {
		return etcdstore.Versioned[activationrecord.Record]{}, errs.New(
			errs.KindInternal,
			"software activation Task is inconsistent",
		)
	}
	claimedTaskID, err := idempotencyrecord.DecodeTaskReference(read.Values[1].Value)
	if err != nil || claimedTaskID != taskID {
		return etcdstore.Versioned[activationrecord.Record]{}, errs.New(
			errs.KindInternal,
			"software activation claim is inconsistent",
		)
	}
	current, err := activationrecord.Decode(read.Values[2].Value)
	if err != nil || current.TaskID != taskID || current.OperationID != operationID ||
		current.InputSHA256 != inputSHA256 {
		return etcdstore.Versioned[activationrecord.Record]{}, errs.New(
			errs.KindInternal,
			"software activation progress authority is inconsistent",
		)
	}
	if err := activationrecord.ValidateTransition(current.Progress, progress, current.Input); err != nil {
		return etcdstore.Versioned[activationrecord.Record]{}, err
	}
	if current.Progress.Equal(progress) {
		return etcdstore.Versioned[activationrecord.Record]{
			Record:       current,
			Revision:     read.Values[2].ModRevision,
			ReadRevision: read.ReadRevision,
		}, nil
	}
	previous := current.UpdatedAt
	if task.UpdatedAt.After(previous) {
		previous = task.UpdatedAt
	}
	at, err = nextTaskControllerTimestamp(previous, at)
	if err != nil {
		return etcdstore.Versioned[activationrecord.Record]{}, err
	}
	next := current
	next.Progress, next.UpdatedAt = progress, at.UTC()
	value, err := activationrecord.Encode(next)
	if err != nil {
		return etcdstore.Versioned[activationrecord.Record]{}, err
	}
	transaction, err := repository.store.Transact(ctx,
		[]etcdstore.Condition{
			{Key: keys[0], ModRevision: read.Values[0].ModRevision},
			{Key: keys[1], ModRevision: read.Values[1].ModRevision},
			{Key: keys[2], ModRevision: read.Values[2].ModRevision},
		},
		[]etcdstore.Mutation{{Type: etcdstore.MutationPut, Key: keys[2], Value: value}},
	)
	clear(value)
	etcdstore.ClearValues(transaction.FailureReads)
	if err != nil {
		return etcdstore.Versioned[activationrecord.Record]{}, err
	}
	if !transaction.Succeeded {
		return etcdstore.Versioned[activationrecord.Record]{}, errs.New(
			errs.KindStateConflict,
			"software activation checkpoint raced",
		)
	}
	return etcdstore.Versioned[activationrecord.Record]{
		Record:       next,
		Revision:     transaction.Revision,
		ReadRevision: transaction.Revision,
	}, nil
}
