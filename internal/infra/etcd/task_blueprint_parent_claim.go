package etcd

import (
	"context"
	"encoding/json"
	"strings"
	"time"

	"github.com/AlanD20/groundplane/internal/common/ids"
	blueprints "github.com/AlanD20/groundplane/internal/infra/etcd/blueprints"
	idempotencyrecord "github.com/AlanD20/groundplane/internal/infra/etcd/idempotency"
	etcdstore "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/recordcodec"
	taskjournal "github.com/AlanD20/groundplane/internal/infra/etcd/taskjournal"
	"github.com/AlanD20/groundplane/pkg/errs"
)

// ClaimNextBlueprintParent starts a visible reconciliation Task without
// creating an Agent or serial Controller assignment. The durable claim is
// resumable after Controller restart; it does not authorize a child effect.
func (repository *TaskRepository) ClaimNextBlueprintParent(
	ctx context.Context, assignedAt time.Time,
) (etcdstore.Versioned[TaskRecord], bool, error) {
	if err := etcdstore.ValidateContext(ctx); err != nil {
		return etcdstore.Versioned[TaskRecord]{}, false, err
	}
	if err := recordcodec.ValidateTimestamp("Blueprint parent claim", assignedAt); err != nil {
		return etcdstore.Versioned[TaskRecord]{}, false, err
	}
	conflicts := 0
	for {
		candidate, found, err := repository.nextTaskClaimCandidate(ctx, taskjournal.TaskExecutorBlueprint)
		if err != nil || !found {
			return etcdstore.Versioned[TaskRecord]{}, found, err
		}
		if err := validateBlueprintParentClaimTask(candidate.task); err != nil {
			return etcdstore.Versioned[TaskRecord]{}, false, err
		}
		activeKey := taskjournal.TaskActiveOperationKey(candidate.task.OperationID)
		claimKey := taskjournal.BlueprintParentClaimKey(candidate.task.ID)
		headKey := blueprints.EnvironmentBlueprintHeadKey(candidate.task.Owner.EnvironmentID)
		companions, err := repository.store.GetMany(ctx, etcdstore.GetManyRequest{
			Keys: []string{activeKey, claimKey, headKey}, Revision: candidate.readRevision,
		})
		if err != nil {
			return etcdstore.Versioned[TaskRecord]{}, false, err
		}
		if companions == nil || len(companions.Values) != 3 || companions.Values[0] == nil ||
			companions.Values[1] != nil || companions.Values[2] == nil {
			return etcdstore.Versioned[TaskRecord]{}, false, errs.New(
				errs.KindInternal, "queued Blueprint parent claim authority is inconsistent",
			)
		}
		activeTaskID, err := idempotencyrecord.DecodeTaskReference(companions.Values[0].Value)
		if err != nil || activeTaskID != candidate.task.ID {
			return etcdstore.Versioned[TaskRecord]{}, false, errs.New(
				errs.KindInternal, "Blueprint parent active operation changed",
			)
		}
		headTaskID, err := idempotencyrecord.DecodeTaskReference(companions.Values[2].Value)
		if err != nil {
			return etcdstore.Versioned[TaskRecord]{}, false, err
		}
		if headTaskID != candidate.task.ID {
			retired, retireErr := repository.retireSupersededBlueprintParent(
				ctx, candidate, companions.Values[0], companions.Values[2], assignedAt,
			)
			if retireErr != nil {
				return etcdstore.Versioned[TaskRecord]{}, false, retireErr
			}
			if !retired {
				conflicts++
				if err := repository.retryPolicy.waitAfterConflict(ctx, conflicts); err != nil {
					return etcdstore.Versioned[TaskRecord]{}, false, err
				}
			}
			continue
		}
		running, err := TransitionTaskStatus(
			candidate.task, taskjournal.TaskStatusPending, taskjournal.TaskStatusRunning, assignedAt,
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
			{Key: claimKey},
			{Key: headKey, ModRevision: companions.Values[2].ModRevision},
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
			Record: running, Revision: transaction.Revision, ReadRevision: transaction.Revision,
		}, true, nil
	}
}

// A pending parent has never owned an Agent effect. Retiring it clears the
// queue and active-operation claim while preserving its Task and replay record.
func (repository *TaskRepository) retireSupersededBlueprintParent(
	ctx context.Context,
	candidate taskClaimCandidate,
	active, head *etcdstore.KeyValue,
	at time.Time,
) (bool, error) {
	task := candidate.task
	if task.idempotencyMarker == nil {
		return false, errs.New(errs.KindInternal, "Blueprint parent marker locator is missing")
	}
	terminal, err := TransitionTaskStatus(task, taskjournal.TaskStatusPending, taskjournal.TaskStatusAborted, at)
	if err != nil {
		return false, err
	}
	prepared, markerKey, retentionKey, err := prepareTerminalTaskMarker(terminal, terminal.Status, *terminal.FinishedAt)
	if err != nil {
		return false, err
	}
	read, err := repository.store.GetMany(ctx, etcdstore.GetManyRequest{
		Keys: []string{markerKey, retentionKey}, Revision: candidate.readRevision,
	})
	if err != nil {
		return false, err
	}
	if read == nil || len(read.Values) != 2 || read.Values[0] == nil || read.Values[1] != nil {
		return false, errs.New(errs.KindInternal, "pending Blueprint parent marker is inconsistent")
	}
	defer etcdstore.ClearValues(read.Values)
	if err := validateTaskLifecycleCompanions(task, active, read.Values[0]); err != nil {
		return false, err
	}
	prepared, err = hydrateTerminalTaskMarker(prepared, read.Values[0].Value)
	if err != nil {
		return false, err
	}
	defer clear(prepared.Intent.Ciphertext)
	defer clear(prepared.Response.Body)
	terminalValue, err := EncodeTaskRecord(terminal)
	if err != nil {
		return false, err
	}
	defer clear(terminalValue)
	markerValue, err := idempotencyrecord.EncodeIdempotencyMarker(prepared)
	if err != nil {
		return false, err
	}
	defer clear(markerValue)
	retentionValue, err := json.Marshal(idempotencyrecord.RetentionReferenceJSON{Schema: 1, MarkerKey: markerKey})
	if err != nil {
		return false, errs.Wrap(errs.KindInternal, err)
	}
	defer clear(retentionValue)
	taskRetentionKey, taskRetentionValue, err := prepareTaskRetentionIndex(terminal)
	if err != nil {
		return false, err
	}
	defer clear(taskRetentionValue)
	conditions := []etcdstore.Condition{
		{Key: candidate.queued.Key, ModRevision: candidate.queued.ModRevision},
		{Key: taskjournal.TaskStorageKey(task.ID), ModRevision: candidate.taskValue.ModRevision},
		{Key: taskjournal.TaskActiveOperationKey(task.OperationID), ModRevision: active.ModRevision},
		{Key: taskjournal.BlueprintParentClaimKey(task.ID)},
		{Key: blueprints.EnvironmentBlueprintHeadKey(task.Owner.EnvironmentID), ModRevision: head.ModRevision},
		{Key: markerKey, ModRevision: read.Values[0].ModRevision},
		{Key: retentionKey},
		{Key: taskRetentionKey},
	}
	mutations := []etcdstore.Mutation{
		{Type: etcdstore.MutationPut, Key: taskjournal.TaskStorageKey(task.ID), Value: terminalValue},
		{Type: etcdstore.MutationDelete, Key: candidate.queued.Key},
		{Type: etcdstore.MutationDelete, Key: taskjournal.TaskActiveOperationKey(task.OperationID)},
		{Type: etcdstore.MutationPut, Key: markerKey, Value: markerValue},
		{Type: etcdstore.MutationPut, Key: retentionKey, Value: retentionValue},
		{Type: etcdstore.MutationPut, Key: taskRetentionKey, Value: taskRetentionValue},
	}
	transaction, err := repository.store.Transact(ctx, conditions, mutations)
	etcdstore.ClearValues(transaction.FailureReads)
	if err != nil {
		return false, err
	}
	return transaction.Succeeded, nil
}

// ListBlueprintParentClaims returns every nonterminal coordinator claim at one
// MVCC revision. It never treats a missing claim as proof of completed effects.
func (repository *TaskRepository) ListBlueprintParentClaims(
	ctx context.Context,
) ([]etcdstore.Versioned[TaskRecord], error) {
	if err := etcdstore.ValidateContext(ctx); err != nil {
		return nil, err
	}
	result := make([]etcdstore.Versioned[TaskRecord], 0)
	start := ""
	var revision int64
	for {
		page, err := repository.store.Range(ctx, etcdstore.RangeRequest{
			Prefix:         taskjournal.BlueprintParentClaimPrefix,
			StartExclusive: start, Limit: taskClaimQueuePageSize, Revision: revision,
		})
		if err != nil {
			return nil, err
		}
		if revision == 0 {
			revision = page.ReadRevision
		}
		if page.ReadRevision != revision || page.More && len(page.Values) == 0 {
			return nil, errs.New(errs.KindInternal, "Blueprint parent claim scan is incomplete")
		}
		for _, claim := range page.Values {
			taskID := strings.TrimPrefix(claim.Key, taskjournal.BlueprintParentClaimPrefix)
			if claim.Key != taskjournal.BlueprintParentClaimKey(taskID) ||
				ids.Validate(ids.KindTask, taskID) != nil {
				return nil, errs.New(errs.KindInternal, "Blueprint parent claim key is invalid")
			}
			referenced, err := idempotencyrecord.DecodeTaskReference(claim.Value)
			if err != nil || referenced != taskID {
				return nil, errs.New(errs.KindInternal, "Blueprint parent claim reference is invalid")
			}
			read, err := repository.store.GetMany(ctx, etcdstore.GetManyRequest{
				Keys: []string{taskjournal.TaskStorageKey(taskID)}, Revision: revision,
			})
			if err != nil {
				return nil, err
			}
			if read == nil || read.ReadRevision != revision || len(read.Values) != 1 || read.Values[0] == nil {
				return nil, errs.New(errs.KindInternal, "claimed Blueprint parent Task is missing")
			}
			task, err := DecodeTaskRecord(read.Values[0].Value)
			if err != nil || validateBlueprintParentClaimTask(task) != nil ||
				task.ID != taskID || task.Status != taskjournal.TaskStatusRunning ||
				task.StartedAt == nil || task.idempotencyMarker == nil ||
				read.Values[0].ModRevision < claim.ModRevision {
				return nil, errs.New(errs.KindInternal, "claimed Blueprint parent Task is inconsistent")
			}
			active, err := repository.store.GetMany(ctx, etcdstore.GetManyRequest{
				Keys: []string{taskjournal.TaskActiveOperationKey(task.OperationID)}, Revision: revision,
			})
			if err != nil {
				return nil, err
			}
			if active == nil || active.ReadRevision != revision || len(active.Values) != 1 || active.Values[0] == nil {
				return nil, errs.New(errs.KindInternal, "Blueprint parent active operation is missing")
			}
			activeTaskID, err := idempotencyrecord.DecodeTaskReference(active.Values[0].Value)
			if err != nil || activeTaskID != taskID {
				return nil, errs.New(errs.KindInternal, "Blueprint parent active operation is inconsistent")
			}
			result = append(result, etcdstore.Versioned[TaskRecord]{
				Record: task, Revision: read.Values[0].ModRevision, ReadRevision: revision,
			})
		}
		if !page.More {
			return result, nil
		}
		start = page.Values[len(page.Values)-1].Key
	}
}

func validateBlueprintParentClaimTask(task TaskRecord) error {
	if err := ValidateTaskRecord(task); err != nil {
		return err
	}
	if task.Executor != taskjournal.TaskExecutorBlueprint {
		return errs.New(errs.KindStateConflict, "Task is not a Blueprint parent")
	}
	return nil
}
