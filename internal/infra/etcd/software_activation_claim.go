package etcd

import (
	"context"
	"strings"
	"time"

	"github.com/AlanD20/groundplane/internal/common/ids"
	idempotencyrecord "github.com/AlanD20/groundplane/internal/infra/etcd/idempotency"
	etcdstore "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/recordcodec"
	activationrecord "github.com/AlanD20/groundplane/internal/infra/etcd/softwareactivation"
	"github.com/AlanD20/groundplane/internal/infra/etcd/taskjournal"
	"github.com/AlanD20/groundplane/pkg/errs"
)

// ClaimNextSoftwareActivation serializes activation parents independently of
// the native Controller executor. The claim is only coordination authority;
// child update Tasks still own every activation side effect.
func (repository *TaskRepository) ClaimNextSoftwareActivation(
	ctx context.Context,
	at time.Time,
) (etcdstore.Versioned[TaskRecord], bool, error) {
	if err := etcdstore.ValidateContext(ctx); err != nil {
		return etcdstore.Versioned[TaskRecord]{}, false, err
	}
	if err := recordcodec.ValidateTimestamp("software activation claim", at); err != nil {
		return etcdstore.Versioned[TaskRecord]{}, false, err
	}
	conflicts := 0
	for {
		candidate, found, err := repository.nextTaskClaimCandidate(ctx, taskjournal.TaskExecutorSoftware)
		if err != nil || !found {
			return etcdstore.Versioned[TaskRecord]{}, found, err
		}
		if err := validateSoftwareActivationTask(candidate.task); err != nil {
			return etcdstore.Versioned[TaskRecord]{}, false, err
		}
		activeKey := taskjournal.TaskActiveOperationKey(candidate.task.OperationID)
		claimKey := activationrecord.ClaimKey(candidate.task.ID)
		progressKey := activationrecord.Key(candidate.task.ID)
		companions, err := repository.store.GetMany(ctx, etcdstore.GetManyRequest{
			Keys: []string{activeKey, claimKey, progressKey}, Revision: candidate.readRevision,
		})
		if err != nil {
			return etcdstore.Versioned[TaskRecord]{}, false, err
		}
		if companions == nil || len(companions.Values) != 3 || companions.Values[0] == nil ||
			companions.Values[1] != nil || companions.Values[2] == nil {
			return etcdstore.Versioned[TaskRecord]{}, false, errs.New(
				errs.KindInternal,
				"software activation claim authority is inconsistent",
			)
		}
		activeTaskID, err := idempotencyrecord.DecodeTaskReference(companions.Values[0].Value)
		if err != nil || activeTaskID != candidate.task.ID {
			return etcdstore.Versioned[TaskRecord]{}, false, errs.New(
				errs.KindInternal,
				"software activation active operation changed",
			)
		}
		activation, err := activationrecord.Decode(companions.Values[2].Value)
		if err != nil || activation.TaskID != candidate.task.ID ||
			activation.OperationID != candidate.task.OperationID ||
			activation.InputSHA256 != candidate.task.PlanHash {
			return etcdstore.Versioned[TaskRecord]{}, false, errs.New(
				errs.KindInternal,
				"software activation progress differs from its Task",
			)
		}
		claimAt, err := nextTaskControllerTimestamp(candidate.task.UpdatedAt, at)
		if err != nil {
			return etcdstore.Versioned[TaskRecord]{}, false, err
		}
		running, err := TransitionTaskStatus(
			candidate.task,
			taskjournal.TaskStatusPending,
			taskjournal.TaskStatusRunning,
			claimAt,
		)
		if err != nil {
			return etcdstore.Versioned[TaskRecord]{}, false, err
		}
		runningValue, err := EncodeTaskRecord(running)
		if err != nil {
			return etcdstore.Versioned[TaskRecord]{}, false, err
		}
		reference, err := idempotencyrecord.EncodeTaskReference(running.ID)
		if err != nil {
			clear(runningValue)
			return etcdstore.Versioned[TaskRecord]{}, false, err
		}
		conditions := []etcdstore.Condition{
			{Key: candidate.queued.Key, ModRevision: candidate.queued.ModRevision},
			{Key: taskjournal.TaskStorageKey(running.ID), ModRevision: candidate.taskValue.ModRevision},
			{Key: activeKey, ModRevision: companions.Values[0].ModRevision},
			{Key: claimKey}, {Key: activationrecord.ClaimPrefix, Prefix: true},
			{Key: progressKey, ModRevision: companions.Values[2].ModRevision},
		}
		mutations := []etcdstore.Mutation{
			{Type: etcdstore.MutationPut, Key: taskjournal.TaskStorageKey(running.ID), Value: runningValue},
			{Type: etcdstore.MutationDelete, Key: candidate.queued.Key},
			{Type: etcdstore.MutationPut, Key: claimKey, Value: reference},
		}
		transaction, err := repository.store.Transact(ctx, conditions, mutations)
		etcdstore.ClearMutationValues(mutations)
		etcdstore.ClearValues(transaction.FailureReads)
		if err != nil {
			return etcdstore.Versioned[TaskRecord]{}, false, err
		}
		if !transaction.Succeeded {
			conflicts++
			if err := repository.retryPolicy.waitAfterConflict(ctx, conflicts); err != nil {
				return etcdstore.Versioned[TaskRecord]{}, false, err
			}
			continue
		}
		return etcdstore.Versioned[TaskRecord]{
			Record:       running,
			Revision:     transaction.Revision,
			ReadRevision: transaction.Revision,
		}, true, nil
	}
}

func (repository *TaskRepository) ListSoftwareActivationClaims(
	ctx context.Context,
) ([]etcdstore.Versioned[TaskRecord], error) {
	if err := etcdstore.ValidateContext(ctx); err != nil {
		return nil, err
	}
	page, err := repository.store.Range(ctx, etcdstore.RangeRequest{Prefix: activationrecord.ClaimPrefix, Limit: 2})
	if err != nil {
		return nil, err
	}
	if page == nil || page.More || len(page.Values) > 1 {
		return nil, errs.New(errs.KindInternal, "software activation claim serialization is corrupt")
	}
	result := make([]etcdstore.Versioned[TaskRecord], 0, len(page.Values))
	for _, claim := range page.Values {
		taskID := strings.TrimPrefix(claim.Key, activationrecord.ClaimPrefix)
		if claim.Key != activationrecord.ClaimKey(taskID) || ids.Validate(ids.KindTask, taskID) != nil {
			return nil, errs.New(errs.KindInternal, "software activation claim key is invalid")
		}
		referenced, err := idempotencyrecord.DecodeTaskReference(claim.Value)
		if err != nil || referenced != taskID {
			return nil, errs.New(errs.KindInternal, "software activation claim reference is invalid")
		}
		read, err := repository.store.GetMany(ctx, etcdstore.GetManyRequest{
			Keys: []string{
				taskjournal.TaskStorageKey(taskID),
				activationrecord.Key(taskID),
			}, Revision: page.ReadRevision,
		})
		if err != nil {
			return nil, err
		}
		if read == nil || read.ReadRevision != page.ReadRevision || len(read.Values) != 2 ||
			read.Values[0] == nil || read.Values[1] == nil {
			return nil, errs.New(errs.KindInternal, "claimed software activation is missing")
		}
		task, err := DecodeTaskRecord(read.Values[0].Value)
		activation, activationErr := activationrecord.Decode(read.Values[1].Value)
		if err != nil || activationErr != nil || validateSoftwareActivationTask(task) != nil ||
			task.ID != taskID || task.Status != taskjournal.TaskStatusRunning || task.StartedAt == nil ||
			task.idempotencyMarker == nil || activation.TaskID != taskID || activation.OperationID != task.OperationID ||
			activation.InputSHA256 != task.PlanHash || read.Values[0].ModRevision < claim.ModRevision {
			return nil, errs.New(errs.KindInternal, "claimed software activation is inconsistent")
		}
		active, err := repository.store.GetMany(ctx, etcdstore.GetManyRequest{
			Keys: []string{taskjournal.TaskActiveOperationKey(task.OperationID)}, Revision: page.ReadRevision,
		})
		if err != nil {
			return nil, err
		}
		if active == nil || len(active.Values) != 1 || active.Values[0] == nil {
			return nil, errs.New(errs.KindInternal, "software activation active operation is missing")
		}
		activeTaskID, err := idempotencyrecord.DecodeTaskReference(active.Values[0].Value)
		if err != nil || activeTaskID != taskID {
			return nil, errs.New(errs.KindInternal, "software activation active operation is inconsistent")
		}
		result = append(
			result,
			etcdstore.Versioned[TaskRecord]{
				Record:       task,
				Revision:     read.Values[0].ModRevision,
				ReadRevision: page.ReadRevision,
			},
		)
	}
	return result, nil
}
