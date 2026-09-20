package etcd

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	idempotencyrecord "github.com/AlanD20/groundplane/internal/infra/etcd/idempotency"
	etcdstore "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"net/http"

	removalrecord "github.com/AlanD20/groundplane/internal/infra/volumeremovalrecord"
	"github.com/AlanD20/groundplane/pkg/errs"
)

// This is the successful Task transaction, not a separate runtime checkpoint.
// The caller supplies its Task/assignment/retention CAS; removal contributes its
// exact checkpoint and ownership authority before any terminal write executes.
func (repository *TaskRepository) transactVolumeRemovalTerminal(
	ctx context.Context, task TaskRecord, conditions []etcdstore.Condition, mutations []etcdstore.Mutation,
) (etcdstore.TransactionResult, error) {
	runtimeRead, err := repository.store.Get(ctx, removalrecord.RuntimeKey(task.OperationID))
	if err != nil {
		return etcdstore.TransactionResult{}, err
	}
	if runtimeRead == nil || runtimeRead.Entry == nil || runtimeRead.ReadRevision <= 0 ||
		runtimeRead.Entry.ModRevision <= 0 || runtimeRead.Entry.Key != removalrecord.RuntimeKey(task.OperationID) {
		return etcdstore.TransactionResult{}, volumeRemovalTerminalConflict()
	}
	defer clear(runtimeRead.Entry.Value)
	runtime, err := removalrecord.DecodeRuntime(runtimeRead.Entry.Value)
	if err != nil || !volumeRemovalTerminalTaskMatches(task, runtime) {
		return etcdstore.TransactionResult{}, volumeRemovalTerminalConflict()
	}
	locator := idempotencyrecord.IdempotencyLocator{ScopeKind: idempotencyrecord.IdempotencyScopeKind(runtime.RootLocator.ScopeKind),
		ScopeID: runtime.RootLocator.ScopeID, Method: runtime.RootLocator.Method,
		Route: runtime.RootLocator.Route, Key: runtime.RootLocator.Key}
	markerKey, err := idempotencyrecord.IdempotencyMarkerKey(locator)
	if err != nil {
		return etcdstore.TransactionResult{}, err
	}
	replayKey, err := idempotencyrecord.IdempotencyReplayTargetKey(idempotencyrecord.IdempotencyReplayTarget{Kind: idempotencyrecord.IdempotencyReplayTargetVolume,
		ID: runtime.VolumeID}, locator.Method, locator.Route, locator.Key)
	if err != nil {
		return etcdstore.TransactionResult{}, err
	}
	keys := []string{removalrecord.ProgressKey(runtime.OperationID), removalrecord.PendingPathKey(runtime.OperationID),
		removalrecord.AttemptKey(runtime.OperationID, runtime.AttemptOrdinal), removalrecord.OwnerKey(runtime.VolumeID),
		removalrecord.EnvironmentLockKey(
			runtime.EnvironmentID,
		), markerKey, replayKey, environmentBlueprintHeadKey(runtime.EnvironmentID)}
	read, err := repository.store.GetMany(ctx, etcdstore.GetManyRequest{Keys: keys, Revision: runtimeRead.ReadRevision})
	if err != nil {
		return etcdstore.TransactionResult{}, err
	}
	if read == nil || read.ReadRevision != runtimeRead.ReadRevision || len(read.Values) != len(keys) {
		return etcdstore.TransactionResult{}, volumeRemovalTerminalConflict()
	}
	defer clearKeyValues(read.Values)
	for index, value := range read.Values {
		if index == 1 {
			if value != nil {
				return etcdstore.TransactionResult{}, volumeRemovalTerminalConflict()
			}
			continue
		}
		if value == nil || value.Key != keys[index] || value.ModRevision <= 0 {
			return etcdstore.TransactionResult{}, volumeRemovalTerminalConflict()
		}
	}
	progress, err := removalrecord.DecodeProgress(read.Values[0].Value)
	if err != nil || progress.OperationID != runtime.OperationID || !progress.DirectoryAbsent ||
		progress.NextRequestOrdinal <= 1 || task.FinishedAt.Before(progress.UpdatedAt) {
		return etcdstore.TransactionResult{}, volumeRemovalTerminalConflict()
	}
	attempt, err := removalrecord.DecodeAttempt(read.Values[2].Value)
	if err != nil || attempt.OperationID != runtime.OperationID || attempt.TaskID != task.ID ||
		attempt.OriginTaskID != runtime.OriginTaskID || attempt.PredecessorTaskID != task.RetryOf ||
		attempt.Ordinal != runtime.AttemptOrdinal {
		return etcdstore.TransactionResult{}, volumeRemovalTerminalConflict()
	}
	for _, value := range read.Values[3:5] {
		owner, err := removalrecord.DecodeOwner(value.Value)
		if err != nil || owner.OperationID != runtime.OperationID || owner.VolumeID != runtime.VolumeID ||
			owner.EnvironmentID != runtime.EnvironmentID {
			return etcdstore.TransactionResult{}, volumeRemovalTerminalConflict()
		}
	}
	head, err := idempotencyrecord.DecodeTaskReference(read.Values[7].Value)
	if err != nil || head != runtime.DesiredRevisionID {
		return etcdstore.TransactionResult{}, volumeRemovalTerminalConflict()
	}
	marker, err := idempotencyrecord.DecodeIdempotencyMarker(read.Values[5].Value, locator)
	if err != nil {
		return etcdstore.TransactionResult{}, err
	}
	defer clear(marker.Intent.Ciphertext)
	defer clear(marker.Response.Body)
	if marker.State != idempotencyrecord.IdempotencyMarkerPending || marker.Kind != idempotencyrecord.IdempotencyMarkerTask ||
		marker.TaskID != runtime.OriginTaskID || marker.Response.Status != http.StatusAccepted ||
		marker.ReplayTarget == nil || marker.ReplayTarget.Kind != idempotencyrecord.IdempotencyReplayTargetVolume ||
		marker.ReplayTarget.ID != runtime.VolumeID || sha256.Sum256(marker.Intent.Ciphertext) != runtime.IntentSHA256 ||
		sha256.Sum256(marker.Response.Body) != runtime.RootResponseSHA256 ||
		idempotencyrecord.DecodeReplayTargetReference(read.Values[6].Value, markerKey) != nil {
		return etcdstore.TransactionResult{}, volumeRemovalTerminalConflict()
	}
	completionKey := removalrecord.CompletionKey(runtime.OperationID, progress.NextRequestOrdinal-1)
	completionRead, err := repository.store.GetMany(
		ctx,
		etcdstore.GetManyRequest{Keys: []string{completionKey}, Revision: read.ReadRevision},
	)
	if err != nil {
		return etcdstore.TransactionResult{}, err
	}
	if completionRead == nil || completionRead.ReadRevision != read.ReadRevision || len(completionRead.Values) != 1 ||
		completionRead.Values[0] == nil || completionRead.Values[0].Key != completionKey || completionRead.Values[0].ModRevision <= 0 {
		return etcdstore.TransactionResult{}, volumeRemovalTerminalConflict()
	}
	defer clearKeyValues(completionRead.Values)
	completion, err := removalrecord.DecodeCompletion(completionRead.Values[0].Value)
	if err != nil || completion.OperationID != runtime.OperationID || !completion.DirectoryAbsent ||
		completion.RequestOrdinal != progress.NextRequestOrdinal-1 || !completion.CompletedAt.Equal(progress.UpdatedAt) ||
		completionRead.Values[0].ModRevision != read.Values[0].ModRevision ||
		read.Values[0].ModRevision > runtimeRead.Entry.ModRevision ||
		(runtime.AttemptOrdinal == 1 && read.Values[0].ModRevision != runtimeRead.Entry.ModRevision) {
		return etcdstore.TransactionResult{}, volumeRemovalTerminalConflict()
	}
	baseConditions := conditions
	conditions = make([]etcdstore.Condition, 0, len(baseConditions)+len(keys)+2)
	if task.RetainUntil == nil {
		return etcdstore.TransactionResult{}, volumeRemovalTerminalConflict()
	}
	queueKey := taskQueueKey(task.Executor, task.ID)
	taskRetentionKey := taskRetentionIndexKey(task.ID, *task.RetainUntil)
	for _, condition := range baseConditions {
		// These are derived indexes owned by the exact Task primary CAS.
		// Rebuild its retention index and remove its queue entry at commit;
		// neither index grants assignment or removal authority.
		if condition.Key == queueKey || condition.Key == taskRetentionKey {
			if condition.ModRevision != 0 || condition.Prefix {
				return etcdstore.TransactionResult{}, volumeRemovalTerminalConflict()
			}
			continue
		}
		conditions, err = appendVolumeRemovalTerminalCondition(conditions, condition)
		if err != nil {
			return etcdstore.TransactionResult{}, err
		}
	}
	for index, key := range keys {
		conditions, err = appendVolumeRemovalTerminalCondition(
			conditions,
			etcdstore.Condition{Key: key, ModRevision: keyValueRevision(read.Values[index])},
		)
		if err != nil {
			return etcdstore.TransactionResult{}, err
		}
	}
	conditions = append(conditions, etcdstore.Condition{Key: runtimeRead.Entry.Key, ModRevision: runtimeRead.Entry.ModRevision},
		etcdstore.Condition{Key: completionKey, ModRevision: completionRead.Values[0].ModRevision})
	ancestry, err := bindHierarchyMutation(ctx, repository.store, read.ReadRevision,
		HierarchyMutationScope{TenantID: task.Owner.TenantID, ProjectID: task.Owner.ProjectID}, nil, nil)
	if err != nil {
		return etcdstore.TransactionResult{}, err
	}
	defer ancestry.clear()
	conditions = append(conditions, ancestry.conditions...)
	conditions = append(
		conditions,
		etcdstore.Condition{
			Key: HierarchyDeletionTombstoneKey(string(HierarchyDeletionTargetEnvironment), runtime.EnvironmentID),
		},
		etcdstore.Condition{Key: HierarchyDeletionTombstoneKey(string(HierarchyDeletionTargetProject), task.Owner.ProjectID)},
	)
	if task.Owner.TenantID != "" {
		conditions = append(
			conditions,
			etcdstore.Condition{Key: HierarchyDeletionTombstoneKey(string(HierarchyDeletionTargetTenant), task.Owner.TenantID)},
		)
	}
	marker.State, marker.UpdatedAt, marker.TerminalAt = idempotencyrecord.IdempotencyMarkerCompleted, *task.FinishedAt, *task.FinishedAt
	marker.RetainUntil = task.FinishedAt.Add(idempotencyrecord.MarkerRetention)
	markerValue, err := idempotencyrecord.EncodeIdempotencyMarker(marker)
	if err != nil {
		return etcdstore.TransactionResult{}, err
	}
	defer clear(markerValue)
	retentionKey, err := idempotencyrecord.IdempotencyRetentionKey(markerKey, marker.RetainUntil)
	if err != nil {
		return etcdstore.TransactionResult{}, err
	}
	retentionValue, err := json.Marshal(idempotencyrecord.RetentionReferenceJSON{Schema: 1, MarkerKey: markerKey})
	if err != nil {
		return etcdstore.TransactionResult{}, errs.Wrap(errs.KindInternal, err)
	}
	defer clear(retentionValue)
	conditions, err = appendVolumeRemovalTerminalCondition(conditions, etcdstore.Condition{Key: retentionKey})
	if err != nil {
		return etcdstore.TransactionResult{}, err
	}
	mutations = append([]etcdstore.Mutation(nil), mutations...)
	mutations = append(mutations, ancestry.mutations...)
	mutations = replaceVolumeRemovalTerminalPut(mutations, markerKey, markerValue)
	mutations = replaceVolumeRemovalTerminalPut(mutations, retentionKey, retentionValue)
	mutations = append(
		mutations,
		etcdstore.Mutation{Type: etcdstore.MutationDelete, Key: removalrecord.Root(runtime.OperationID), Prefix: true},
		etcdstore.Mutation{Type: etcdstore.MutationDelete, Key: keys[3]},
		etcdstore.Mutation{Type: etcdstore.MutationDelete, Key: keys[4]},
		etcdstore.Mutation{Type: etcdstore.MutationDelete, Key: queueKey},
	)
	if len(conditions) > 26 || len(mutations) > 26 {
		return etcdstore.TransactionResult{}, errs.Newf(errs.KindInternal,
			"Volume removal terminal transaction exceeds its operation budget: %d compares, %d mutations",
			len(conditions), len(mutations))
	}
	if err := validateBlueprintTransaction(repository.store, conditions, mutations, 52, 900*1024); err != nil {
		return etcdstore.TransactionResult{}, err
	}
	return repository.store.Transact(ctx, conditions, mutations)
}

func isVolumeRemovalTerminalTask(task TaskRecord) bool {
	return task.Type == TaskRemove && task.Params[TaskResourceKindParam] == TaskResourceVolume &&
		task.Status == TaskStatusCompleted
}

func volumeRemovalTerminalTaskMatches(task TaskRecord, runtime removalrecord.Runtime) bool {
	return task.Status == TaskStatusCompleted && runtime.Checkpoint == removalrecord.DirectoryAbsent &&
		volumeRemovalTaskMatchesRuntime(task, runtime) && task.Result != nil &&
		task.Result.Kind == TaskResultEnvironmentDirectory && task.Result.Diagnostic == TaskResultDiagnosticNone &&
		!task.Result.ReconciliationRequired && len(task.Result.Projects) == 0 && task.Result.ExitCode == 0
}

func volumeRemovalTaskMatchesRuntime(task TaskRecord, runtime removalrecord.Runtime) bool {
	if task.Actor != TaskActorOperator || task.Executor != TaskExecutorAgent || task.FinishedAt == nil ||
		task.FinishedAt.Before(
			runtime.UpdatedAt,
		) || task.ID != runtime.CurrentTaskID || task.OperationID != runtime.OperationID ||
		task.Target != runtime.VolumeID || task.Owner.EnvironmentID != runtime.EnvironmentID ||
		task.RetryOf != runtime.PredecessorTaskID || !EnvironmentVolumeRemovalStepMatches(task.Steps, runtime.StepID) ||
		task.RenderGeneration != int32(
			runtime.DesiredGeneration,
		) || task.TimeoutSeconds != removalrecord.TimeoutSeconds ||
		task.IdempotencyKey != runtime.RootLocator.Key {
		return false
	}
	want := EnvironmentVolumeRemovalTaskParams(runtime, runtime.AttemptOrdinal)
	if len(task.Params) != len(want) {
		return false
	}
	for key, value := range want {
		if task.Params[key] != value {
			return false
		}
	}
	return true
}

// Failure releases the attempt's assignment, never the removal operation. The
// root marker remains byte-for-byte pending with no replay expiry index.
func (repository *TaskRepository) transactVolumeRemovalAttemptTerminal(
	ctx context.Context, task TaskRecord, conditions []etcdstore.Condition, mutations []etcdstore.Mutation,
) (etcdstore.TransactionResult, error) {
	if (task.Status != TaskStatusFailed && task.Status != TaskStatusTimedOut) ||
		task.Result == nil || task.Result.Kind != TaskResultEnvironmentDirectory ||
		task.Result.Diagnostic != TaskResultDiagnosticNone || task.Result.ReconciliationRequired ||
		len(task.Result.Projects) != 0 {
		return etcdstore.TransactionResult{}, volumeRemovalTerminalConflict()
	}
	runtimeKey := removalrecord.RuntimeKey(task.OperationID)
	current, err := repository.store.Get(ctx, runtimeKey)
	if err != nil {
		return etcdstore.TransactionResult{}, err
	}
	if current == nil || current.ReadRevision <= 0 || current.Entry == nil || current.Entry.Key != runtimeKey ||
		current.Entry.ModRevision <= 0 {
		return etcdstore.TransactionResult{}, volumeRemovalTerminalConflict()
	}
	defer clear(current.Entry.Value)
	runtime, err := removalrecord.DecodeRuntime(current.Entry.Value)
	if err != nil || !volumeRemovalTaskMatchesRuntime(task, runtime) ||
		runtime.Checkpoint < removalrecord.DesiredPublished || runtime.Checkpoint > removalrecord.DirectoryAbsent {
		return etcdstore.TransactionResult{}, volumeRemovalTerminalConflict()
	}
	locator := idempotencyrecord.IdempotencyLocator{ScopeKind: idempotencyrecord.IdempotencyScopeKind(runtime.RootLocator.ScopeKind),
		ScopeID: runtime.RootLocator.ScopeID, Method: runtime.RootLocator.Method,
		Route: runtime.RootLocator.Route, Key: runtime.RootLocator.Key}
	markerKey, err := idempotencyrecord.IdempotencyMarkerKey(locator)
	if err != nil {
		return etcdstore.TransactionResult{}, err
	}
	keys := []string{removalrecord.OwnerKey(runtime.VolumeID), removalrecord.EnvironmentLockKey(runtime.EnvironmentID),
		removalrecord.ProgressKey(runtime.OperationID), removalrecord.PendingPathKey(runtime.OperationID),
		removalrecord.AttemptKey(runtime.OperationID, runtime.AttemptOrdinal), markerKey}
	replayKey, err := idempotencyrecord.IdempotencyReplayTargetKey(idempotencyrecord.IdempotencyReplayTarget{Kind: idempotencyrecord.IdempotencyReplayTargetVolume,
		ID: runtime.VolumeID}, locator.Method, locator.Route, locator.Key)
	if err != nil {
		return etcdstore.TransactionResult{}, err
	}
	keys = append(keys, environmentBlueprintHeadKey(runtime.EnvironmentID), replayKey,
		HierarchyDeletionTombstoneKey(string(HierarchyDeletionTargetEnvironment), runtime.EnvironmentID),
		HierarchyDeletionTombstoneKey(string(HierarchyDeletionTargetProject), task.Owner.ProjectID))
	if task.Owner.TenantID != "" {
		keys = append(keys, HierarchyDeletionTombstoneKey(string(HierarchyDeletionTargetTenant), task.Owner.TenantID))
	}
	read, err := repository.store.GetMany(ctx, etcdstore.GetManyRequest{Keys: keys, Revision: current.ReadRevision})
	if err != nil {
		return etcdstore.TransactionResult{}, err
	}
	if read == nil || read.ReadRevision != current.ReadRevision || len(read.Values) != len(keys) {
		return etcdstore.TransactionResult{}, volumeRemovalTerminalConflict()
	}
	defer clearKeyValues(read.Values)
	conditions = append([]etcdstore.Condition(nil), conditions...)
	conditions = append(conditions, etcdstore.Condition{Key: runtimeKey, ModRevision: current.Entry.ModRevision})
	for index, value := range read.Values {
		if index < 8 && index != 3 && value == nil || index >= 8 && value != nil ||
			value != nil && (value.Key != keys[index] || value.ModRevision <= 0) {
			return etcdstore.TransactionResult{}, volumeRemovalTerminalConflict()
		}
		conditions, err = appendVolumeRemovalTerminalCondition(
			conditions,
			etcdstore.Condition{Key: keys[index], ModRevision: keyValueRevision(value)},
		)
		if err != nil {
			return etcdstore.TransactionResult{}, err
		}
	}
	for _, value := range read.Values[:2] {
		owner, err := removalrecord.DecodeOwner(value.Value)
		if err != nil || owner.OperationID != runtime.OperationID || owner.EnvironmentID != runtime.EnvironmentID ||
			owner.VolumeID != runtime.VolumeID {
			return etcdstore.TransactionResult{}, volumeRemovalTerminalConflict()
		}
	}
	progress, err := removalrecord.DecodeProgress(read.Values[2].Value)
	if err != nil || progress.OperationID != runtime.OperationID || task.FinishedAt.Before(progress.UpdatedAt) ||
		progress.DirectoryAbsent != (runtime.Checkpoint == removalrecord.DirectoryAbsent) {
		return etcdstore.TransactionResult{}, volumeRemovalTerminalConflict()
	}
	if read.Values[3] != nil {
		pending, err := removalrecord.DecodePendingPath(read.Values[3].Value)
		// Failure retains the pending request; it does not execute or reassign
		// it. A successor may fail while the original attempt's call remains.
		if err != nil || pending.OperationID != runtime.OperationID ||
			(runtime.AttemptOrdinal == 1 && pending.TaskID != task.ID) ||
			pending.VolumeID != runtime.VolumeID || pending.Key != runtime.Key || pending.IntentSHA256 != runtime.IntentSHA256 ||
			pending.RequestOrdinal != progress.NextRequestOrdinal || runtime.Checkpoint != removalrecord.ConsumersDetached {
			return etcdstore.TransactionResult{}, volumeRemovalTerminalConflict()
		}
	}
	attempt, err := removalrecord.DecodeAttempt(read.Values[4].Value)
	if err != nil || attempt.OperationID != runtime.OperationID || attempt.TaskID != task.ID ||
		attempt.OriginTaskID != runtime.OriginTaskID || attempt.Ordinal != runtime.AttemptOrdinal ||
		attempt.PredecessorTaskID != task.RetryOf {
		return etcdstore.TransactionResult{}, volumeRemovalTerminalConflict()
	}
	marker, err := idempotencyrecord.DecodeIdempotencyMarker(read.Values[5].Value, locator)
	if err != nil {
		return etcdstore.TransactionResult{}, err
	}
	defer clear(marker.Intent.Ciphertext)
	defer clear(marker.Response.Body)
	if marker.Kind != idempotencyrecord.IdempotencyMarkerTask || marker.State != idempotencyrecord.IdempotencyMarkerPending ||
		marker.TaskID != runtime.OriginTaskID || marker.Response.Status != http.StatusAccepted || marker.ReplayTarget == nil ||
		marker.ReplayTarget.Kind != idempotencyrecord.IdempotencyReplayTargetVolume || marker.ReplayTarget.ID != runtime.VolumeID ||
		sha256.Sum256(marker.Intent.Ciphertext) != runtime.IntentSHA256 ||
		sha256.Sum256(marker.Response.Body) != runtime.RootResponseSHA256 ||
		idempotencyrecord.DecodeReplayTargetReference(read.Values[7].Value, markerKey) != nil {
		return etcdstore.TransactionResult{}, volumeRemovalTerminalConflict()
	}
	head, err := idempotencyrecord.DecodeTaskReference(read.Values[6].Value)
	if err != nil || head != runtime.DesiredRevisionID {
		return etcdstore.TransactionResult{}, volumeRemovalTerminalConflict()
	}
	retentionKey, err := idempotencyrecord.IdempotencyRetentionKey(markerKey, task.FinishedAt.Add(idempotencyrecord.MarkerRetention))
	if err != nil {
		return etcdstore.TransactionResult{}, err
	}
	filtered := make([]etcdstore.Mutation, 0, len(mutations))
	for _, mutation := range mutations {
		if mutation.Key != markerKey && mutation.Key != retentionKey {
			filtered = append(filtered, mutation)
		}
	}
	ancestry, err := bindHierarchyMutation(ctx, repository.store, read.ReadRevision,
		HierarchyMutationScope{TenantID: task.Owner.TenantID, ProjectID: task.Owner.ProjectID}, conditions, filtered)
	if err != nil {
		return etcdstore.TransactionResult{}, err
	}
	defer ancestry.clear()
	if len(ancestry.conditions) > 24 || len(ancestry.mutations) > 24 {
		return etcdstore.TransactionResult{}, errs.New(
			errs.KindInternal,
			"Volume removal attempt terminal transaction exceeds its operation budget",
		)
	}
	if err := validateBlueprintTransaction(repository.store, ancestry.conditions, ancestry.mutations, 48, 900*1024); err != nil {
		return etcdstore.TransactionResult{}, err
	}
	return repository.store.Transact(ctx, ancestry.conditions, ancestry.mutations)
}

func appendVolumeRemovalTerminalCondition(conditions []etcdstore.Condition, candidate etcdstore.Condition) ([]etcdstore.Condition, error) {
	for _, condition := range conditions {
		if condition.Key == candidate.Key {
			if condition != candidate {
				return nil, volumeRemovalTerminalConflict()
			}
			return conditions, nil
		}
	}
	return append(conditions, candidate), nil
}

func replaceVolumeRemovalTerminalPut(mutations []etcdstore.Mutation, key string, value []byte) []etcdstore.Mutation {
	for index, mutation := range mutations {
		if mutation.Key == key {
			mutations[index] = etcdstore.Mutation{Type: etcdstore.MutationPut, Key: key, Value: value}
			return mutations
		}
	}
	return append(mutations, etcdstore.Mutation{Type: etcdstore.MutationPut, Key: key, Value: value})
}

func volumeRemovalTerminalConflict() error {
	return errs.New(errs.KindStateConflict, "Volume removal terminal authority is incomplete or changed")
}
