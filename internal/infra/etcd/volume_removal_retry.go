package etcd

import (
	"bytes"
	"context"
	"crypto/sha256"
	blueprints "github.com/AlanD20/groundplane/internal/infra/etcd/blueprints"
	hierarchydeletion "github.com/AlanD20/groundplane/internal/infra/etcd/hierarchydeletion"
	idempotencyrecord "github.com/AlanD20/groundplane/internal/infra/etcd/idempotency"
	etcdstore "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	taskjournal "github.com/AlanD20/groundplane/internal/infra/etcd/taskjournal"
	"net/http"
	"slices"
	"strconv"

	removalrecord "github.com/AlanD20/groundplane/internal/infra/volumeremovalrecord"
	"github.com/AlanD20/groundplane/pkg/errs"
)

// Retry publishes the successor and its operation ownership together. The
// accepted DELETE response and all completed traversal work remain unchanged.
func (repository *TaskRepository) retryVolumeRemovalTask(
	ctx context.Context, source etcdstore.Versioned[TaskRecord], retryID string, actor taskjournal.TaskActor, marker idempotencyrecord.IdempotencyMarker,
) (IdempotencyTransactionResult, error) {
	if actor != taskjournal.TaskActorOperator ||
		(source.Record.Status != taskjournal.TaskStatusFailed && source.Record.Status != taskjournal.TaskStatusTimedOut) {
		return IdempotencyTransactionResult{}, errs.New(
			errs.KindTaskNotRetryable,
			"Volume removal attempt is not retryable",
		)
	}
	retry, err := cloneRetryTask(source.Record, retryID, actor, marker.CreatedAt)
	if err != nil {
		return IdempotencyTransactionResult{}, err
	}
	if marker.Kind != idempotencyrecord.IdempotencyMarkerTask || marker.State != idempotencyrecord.IdempotencyMarkerPending || marker.TaskID != retry.ID ||
		marker.Locator.Method != http.MethodPost || marker.Locator.Route != "/tasks/{id}/retry" ||
		marker.Locator.ScopeKind != idempotencyrecord.IdempotencyScopeEnvironment || marker.Locator.ScopeID != source.Record.Owner.EnvironmentID ||
		marker.ReplayTarget != nil || !marker.CreatedAt.Equal(marker.UpdatedAt) || idempotencyrecord.ValidateIdempotencyMarker(marker) != nil {
		return IdempotencyTransactionResult{}, errs.New(
			errs.KindValidationFailed,
			"Volume removal retry marker is invalid",
		)
	}
	if existing, found, err := existingIdempotencyTransaction(ctx, repository.store, marker); err != nil || found {
		return existing, err
	}
	runtimeKey := removalrecord.RuntimeKey(source.Record.OperationID)
	runtimeRead, err := repository.store.GetMany(
		ctx,
		etcdstore.GetManyRequest{Keys: []string{runtimeKey}, Revision: source.ReadRevision},
	)
	if err != nil {
		return IdempotencyTransactionResult{}, err
	}
	if runtimeRead == nil || runtimeRead.ReadRevision != source.ReadRevision || len(runtimeRead.Values) != 1 ||
		runtimeRead.Values[0] == nil || runtimeRead.Values[0].Key != runtimeKey || runtimeRead.Values[0].ModRevision <= 0 {
		return IdempotencyTransactionResult{}, volumeRemovalTerminalConflict()
	}
	defer clearKeyValues(runtimeRead.Values)
	runtime, err := removalrecord.DecodeRuntime(runtimeRead.Values[0].Value)
	if err != nil || !volumeRemovalTaskMatchesRuntime(source.Record, runtime) ||
		runtime.Checkpoint < removalrecord.DesiredPublished || runtime.Checkpoint > removalrecord.DirectoryAbsent ||
		retry.CreatedAt.Before(*source.Record.FinishedAt) {
		return IdempotencyTransactionResult{}, volumeRemovalTerminalConflict()
	}
	locator := idempotencyrecord.IdempotencyLocator{ScopeKind: idempotencyrecord.IdempotencyScopeKind(runtime.RootLocator.ScopeKind),
		ScopeID: runtime.RootLocator.ScopeID, Method: runtime.RootLocator.Method,
		Route: runtime.RootLocator.Route, Key: runtime.RootLocator.Key}
	rootKey, err := idempotencyrecord.IdempotencyMarkerKey(locator)
	if err != nil {
		return IdempotencyTransactionResult{}, err
	}
	replayKey, err := idempotencyrecord.IdempotencyReplayTargetKey(idempotencyrecord.IdempotencyReplayTarget{Kind: idempotencyrecord.IdempotencyReplayTargetVolume,
		ID: runtime.VolumeID}, locator.Method, locator.Route, locator.Key)
	if err != nil {
		return IdempotencyTransactionResult{}, err
	}
	keys := []string{removalrecord.ProgressKey(runtime.OperationID), removalrecord.PendingPathKey(runtime.OperationID),
		removalrecord.AttemptKey(runtime.OperationID, runtime.AttemptOrdinal), removalrecord.OwnerKey(runtime.VolumeID),
		removalrecord.EnvironmentLockKey(
			runtime.EnvironmentID,
		), rootKey, replayKey, blueprints.EnvironmentBlueprintHeadKey(runtime.EnvironmentID),
		hierarchydeletion.HierarchyDeletionTombstoneKey(string(hierarchydeletion.HierarchyDeletionTargetEnvironment), runtime.EnvironmentID),
		hierarchydeletion.HierarchyDeletionTombstoneKey(string(hierarchydeletion.HierarchyDeletionTargetProject), source.Record.Owner.ProjectID)}
	if source.Record.Owner.TenantID != "" {
		keys = append(
			keys,
			hierarchydeletion.HierarchyDeletionTombstoneKey(string(hierarchydeletion.HierarchyDeletionTargetTenant), source.Record.Owner.TenantID),
		)
	}
	read, err := repository.store.GetMany(ctx, etcdstore.GetManyRequest{Keys: keys, Revision: source.ReadRevision})
	if err != nil {
		return IdempotencyTransactionResult{}, err
	}
	if read == nil || read.ReadRevision != source.ReadRevision || len(read.Values) != len(keys) {
		return IdempotencyTransactionResult{}, volumeRemovalTerminalConflict()
	}
	defer clearKeyValues(read.Values)
	conditions := []etcdstore.Condition{{Key: taskjournal.TaskStorageKey(source.Record.ID), ModRevision: source.Revision},
		{Key: taskjournal.TaskStorageKey(retry.ID)}, {Key: taskjournal.TaskOperationIndexKey(retry.OperationID, retry.ID)},
		{Key: taskjournal.TaskActiveOperationKey(retry.OperationID)}, {Key: taskjournal.TaskQueueKey(retry.Executor, retry.ID)},
		{Key: runtimeKey, ModRevision: runtimeRead.Values[0].ModRevision}}
	for index, value := range read.Values {
		absent := index >= 8
		if absent && value != nil || !absent && index != 1 && value == nil ||
			value != nil && (value.Key != keys[index] || value.ModRevision <= 0) {
			return IdempotencyTransactionResult{}, volumeRemovalTerminalConflict()
		}
		// Reserved idempotency keys are bound by the closed commit below, not
		// exposed as writable keys in a generic Task mutation plan.
		if index != 5 && index != 6 {
			conditions = append(conditions, etcdstore.Condition{Key: keys[index], ModRevision: keyValueRevision(value)})
		}
	}
	progress, err := removalrecord.DecodeProgress(read.Values[0].Value)
	if err != nil || progress.OperationID != runtime.OperationID || retry.CreatedAt.Before(progress.UpdatedAt) ||
		progress.DirectoryAbsent != (runtime.Checkpoint == removalrecord.DirectoryAbsent) {
		return IdempotencyTransactionResult{}, volumeRemovalTerminalConflict()
	}
	if read.Values[1] != nil {
		pending, err := removalrecord.DecodePendingPath(read.Values[1].Value)
		if err != nil {
			return IdempotencyTransactionResult{}, volumeRemovalTerminalConflict()
		}
		origin := source.Record
		if pending.TaskID != source.Record.ID {
			key := taskjournal.TaskStorageKey(pending.TaskID)
			prior, err := repository.store.GetMany(
				ctx,
				etcdstore.GetManyRequest{Keys: []string{key}, Revision: source.ReadRevision},
			)
			if err != nil {
				return IdempotencyTransactionResult{}, err
			}
			if prior == nil || prior.ReadRevision != source.ReadRevision || len(prior.Values) != 1 ||
				prior.Values[0] == nil || prior.Values[0].Key != key || prior.Values[0].ModRevision <= 0 {
				return IdempotencyTransactionResult{}, volumeRemovalTerminalConflict()
			}
			defer clearKeyValues(prior.Values)
			origin, err = decodeTaskRecord(prior.Values[0].Value)
			if err != nil {
				return IdempotencyTransactionResult{}, volumeRemovalTerminalConflict()
			}
			conditions = append(conditions, etcdstore.Condition{Key: key, ModRevision: prior.Values[0].ModRevision})
		}
		if ValidateEnvironmentVolumeRemovalPendingRecovery(runtime, progress, pending, origin) != nil {
			return IdempotencyTransactionResult{}, volumeRemovalTerminalConflict()
		}
	}
	attempt, err := removalrecord.DecodeAttempt(read.Values[2].Value)
	if err != nil || attempt.OperationID != runtime.OperationID || attempt.TaskID != source.Record.ID ||
		attempt.OriginTaskID != runtime.OriginTaskID || attempt.Ordinal != runtime.AttemptOrdinal ||
		attempt.PredecessorTaskID != runtime.PredecessorTaskID || attempt.Ordinal+1 == 0 {
		return IdempotencyTransactionResult{}, volumeRemovalTerminalConflict()
	}
	for _, value := range read.Values[3:5] {
		owner, err := removalrecord.DecodeOwner(value.Value)
		if err != nil || owner.OperationID != runtime.OperationID || owner.VolumeID != runtime.VolumeID ||
			owner.EnvironmentID != runtime.EnvironmentID {
			return IdempotencyTransactionResult{}, volumeRemovalTerminalConflict()
		}
	}
	root, err := idempotencyrecord.DecodeIdempotencyMarker(read.Values[5].Value, locator)
	if err != nil {
		return IdempotencyTransactionResult{}, err
	}
	defer clear(root.Intent.Ciphertext)
	defer clear(root.Response.Body)
	if root.Kind != idempotencyrecord.IdempotencyMarkerTask || root.State != idempotencyrecord.IdempotencyMarkerPending ||
		root.TaskID != runtime.OriginTaskID ||
		root.Response.Status != http.StatusAccepted ||
		root.ReplayTarget == nil ||
		root.ReplayTarget.Kind != idempotencyrecord.IdempotencyReplayTargetVolume ||
		root.ReplayTarget.ID != runtime.VolumeID ||
		sha256.Sum256(root.Intent.Ciphertext) != runtime.IntentSHA256 ||
		sha256.Sum256(root.Response.Body) != runtime.RootResponseSHA256 ||
		idempotencyrecord.DecodeReplayTargetReference(read.Values[6].Value, rootKey) != nil {
		return IdempotencyTransactionResult{}, volumeRemovalTerminalConflict()
	}
	head, err := idempotencyrecord.DecodeTaskReference(read.Values[7].Value)
	if err != nil || head != runtime.DesiredRevisionID {
		return IdempotencyTransactionResult{}, volumeRemovalTerminalConflict()
	}
	attempt.Ordinal++
	attempt.TaskID, attempt.PredecessorTaskID, attempt.CreatedAt = retry.ID, source.Record.ID, retry.CreatedAt
	runtime.CurrentTaskID, runtime.PredecessorTaskID = retry.ID, source.Record.ID
	runtime.AttemptOrdinal, runtime.UpdatedAt = attempt.Ordinal, retry.CreatedAt
	retry.Params = EnvironmentVolumeRemovalTaskParams(runtime, attempt.Ordinal)
	retry.idempotencyMarker = cloneIdempotencyLocator(&marker.Locator)
	taskValue, err := encodeTaskRecord(retry)
	if err != nil {
		return IdempotencyTransactionResult{}, err
	}
	defer clear(taskValue)
	reference, err := idempotencyrecord.EncodeTaskReference(retry.ID)
	if err != nil {
		return IdempotencyTransactionResult{}, err
	}
	defer clear(reference)
	runtimeValue, err := removalrecord.EncodeRuntime(runtime)
	if err != nil {
		return IdempotencyTransactionResult{}, err
	}
	defer clear(runtimeValue)
	attemptValue, err := removalrecord.EncodeAttempt(attempt)
	if err != nil {
		return IdempotencyTransactionResult{}, err
	}
	defer clear(attemptValue)
	conditions = append(conditions, etcdstore.Condition{Key: removalrecord.AttemptKey(runtime.OperationID, attempt.Ordinal)})
	mutations := []etcdstore.Mutation{{Type: etcdstore.MutationPut, Key: taskjournal.TaskStorageKey(retry.ID), Value: taskValue},
		{Type: etcdstore.MutationPut, Key: taskjournal.TaskOperationIndexKey(retry.OperationID, retry.ID), Value: reference},
		{Type: etcdstore.MutationPut, Key: taskjournal.TaskActiveOperationKey(retry.OperationID), Value: reference},
		{Type: etcdstore.MutationPut, Key: taskjournal.TaskQueueKey(retry.Executor, retry.ID), Value: reference},
		{Type: etcdstore.MutationPut, Key: runtimeKey, Value: runtimeValue},
		{Type: etcdstore.MutationPut, Key: removalrecord.AttemptKey(runtime.OperationID, attempt.Ordinal), Value: attemptValue}}
	ancestry, err := bindHierarchyMutation(
		ctx,
		repository.store,
		source.ReadRevision,
		HierarchyMutationScope{
			TenantID:  source.Record.Owner.TenantID,
			ProjectID: source.Record.Owner.ProjectID,
		},
		conditions,
		mutations,
	)
	if err != nil {
		return IdempotencyTransactionResult{}, err
	}
	defer ancestry.clear()
	initiation, err := newInheritedTaskInitiation(source, actor)
	if err != nil {
		return IdempotencyTransactionResult{}, err
	}
	plan, err := newTaskIdempotencyMutationPlan(retry, initiation, ancestry.conditions, ancestry.mutations,
		func(_ int64, _ []*etcdstore.KeyValue) error { return volumeRemovalTerminalConflict() })
	if err != nil {
		return IdempotencyTransactionResult{}, err
	}
	idempotency, err := newIdempotencyRepository(repository.store)
	if err != nil {
		return IdempotencyTransactionResult{}, err
	}
	return idempotency.apply(
		ctx,
		marker,
		plan,
		func(ctx context.Context, compares []etcdstore.Condition, writes []etcdstore.Mutation) (etcdstore.TransactionResult, error) {
			all := append(append([]etcdstore.Condition(nil), compares...),
				etcdstore.Condition{Key: rootKey, ModRevision: read.Values[5].ModRevision},
				etcdstore.Condition{Key: replayKey, ModRevision: read.Values[6].ModRevision})
			if len(all) > 24 || len(writes) > 24 {
				return etcdstore.TransactionResult{}, errs.New(
					errs.KindInternal,
					"Volume removal retry exceeds its operation budget",
				)
			}
			if err := ValidateBlueprintTransaction(repository.store, all, writes, 48, 900*1024); err != nil {
				return etcdstore.TransactionResult{}, err
			}
			result, err := repository.store.Transact(ctx, all, writes)
			if err != nil || result.Succeeded {
				return result, err
			}
			if len(result.FailureReads) != len(all) {
				clearKeyValues(result.FailureReads)
				return etcdstore.TransactionResult{}, volumeRemovalTerminalConflict()
			}
			// The plan classifies every losing operation fence as StateConflict.
			// Its caller owns only the plan and new-marker failure reads.
			clearKeyValues(result.FailureReads[len(compares):])
			result.FailureReads = result.FailureReads[:len(compares)]
			return result, nil
		},
	)
}

// ValidateEnvironmentVolumeRemovalPendingRecovery binds an unchanged helper
// request to its terminal originating attempt, independently of the current
// execution assignment. Callers fence both Task primaries at their read revision.
func ValidateEnvironmentVolumeRemovalPendingRecovery(
	runtime removalrecord.Runtime,
	progress removalrecord.Progress,
	pending removalrecord.PendingPath,
	origin TaskRecord,
) error {
	ordinal, err := strconv.ParseUint(origin.Params[removalrecord.AttemptParam], 10, 32)
	if err != nil || ordinal == 0 || ordinal > uint64(runtime.AttemptOrdinal) ||
		removalrecord.ValidatePendingPath(pending) != nil || origin.ID != pending.TaskID ||
		origin.Type != taskjournal.TaskRemove || (ordinal == 1) != (origin.ID == runtime.OriginTaskID) ||
		(ordinal == 1 && origin.RetryOf != "") ||
		(origin.Status != taskjournal.TaskStatusFailed && origin.Status != taskjournal.TaskStatusTimedOut) ||
		origin.CreatedAt.After(pending.CreatedAt) ||
		(uint32(ordinal) == runtime.AttemptOrdinal) != (origin.ID == runtime.CurrentTaskID) ||
		runtime.Checkpoint != removalrecord.ConsumersDetached || progress.DirectoryAbsent ||
		pending.OperationID != runtime.OperationID || pending.VolumeID != runtime.VolumeID || pending.Key != runtime.Key ||
		pending.IntentSHA256 != runtime.IntentSHA256 || pending.RequestOrdinal != progress.NextRequestOrdinal ||
		pending.CreatedAt.Before(
			progress.UpdatedAt,
		) || !slices.Equal(pending.ComponentStack, progress.ComponentStack) ||
		!bytes.Equal(pending.Cursor, progress.Cursor) {
		return volumeRemovalTerminalConflict()
	}
	runtime.CurrentTaskID, runtime.PredecessorTaskID = origin.ID, origin.RetryOf
	runtime.AttemptOrdinal, runtime.UpdatedAt = uint32(ordinal), pending.CreatedAt
	if !volumeRemovalTaskMatchesRuntime(origin, runtime) {
		return volumeRemovalTerminalConflict()
	}
	return nil
}
