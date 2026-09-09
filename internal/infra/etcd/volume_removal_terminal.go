package etcd

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"net/http"

	removalrecord "github.com/AlanD20/groundplane/internal/infra/volumeremovalrecord"
	"github.com/AlanD20/groundplane/pkg/errs"
)

// This is the successful Task transaction, not a separate runtime checkpoint.
// The caller supplies its Task/assignment/retention CAS; removal contributes its
// exact checkpoint and ownership authority before any terminal write executes.
func (repository *TaskRepository) transactVolumeRemovalTerminal(
	ctx context.Context, task TaskRecord, conditions []Condition, mutations []Mutation,
) (TransactionResult, error) {
	runtimeRead, err := repository.store.Get(ctx, removalrecord.RuntimeKey(task.OperationID))
	if err != nil {
		return TransactionResult{}, err
	}
	if runtimeRead == nil || runtimeRead.Entry == nil || runtimeRead.ReadRevision <= 0 ||
		runtimeRead.Entry.ModRevision <= 0 || runtimeRead.Entry.Key != removalrecord.RuntimeKey(task.OperationID) {
		return TransactionResult{}, volumeRemovalTerminalConflict()
	}
	defer clear(runtimeRead.Entry.Value)
	runtime, err := removalrecord.DecodeRuntime(runtimeRead.Entry.Value)
	if err != nil || !volumeRemovalTerminalTaskMatches(task, runtime) {
		return TransactionResult{}, volumeRemovalTerminalConflict()
	}
	locator := IdempotencyLocator{ScopeKind: IdempotencyScopeKind(runtime.RootLocator.ScopeKind),
		ScopeID: runtime.RootLocator.ScopeID, Method: runtime.RootLocator.Method,
		Route: runtime.RootLocator.Route, Key: runtime.RootLocator.Key}
	markerKey, err := idempotencyMarkerKey(locator)
	if err != nil {
		return TransactionResult{}, err
	}
	replayKey, err := idempotencyReplayTargetKey(IdempotencyReplayTarget{Kind: IdempotencyReplayTargetVolume,
		ID: runtime.VolumeID}, locator.Method, locator.Route, locator.Key)
	if err != nil {
		return TransactionResult{}, err
	}
	keys := []string{removalrecord.ProgressKey(runtime.OperationID), removalrecord.PendingPathKey(runtime.OperationID),
		removalrecord.AttemptKey(runtime.OperationID, runtime.AttemptOrdinal), removalrecord.OwnerKey(runtime.VolumeID),
		removalrecord.EnvironmentLockKey(
			runtime.EnvironmentID,
		), markerKey, replayKey, environmentBlueprintHeadKey(runtime.EnvironmentID)}
	read, err := repository.store.GetMany(ctx, GetManyRequest{Keys: keys, Revision: runtimeRead.ReadRevision})
	if err != nil {
		return TransactionResult{}, err
	}
	if read == nil || read.ReadRevision != runtimeRead.ReadRevision || len(read.Values) != len(keys) {
		return TransactionResult{}, volumeRemovalTerminalConflict()
	}
	defer clearKeyValues(read.Values)
	for index, value := range read.Values {
		if index == 1 {
			if value != nil {
				return TransactionResult{}, volumeRemovalTerminalConflict()
			}
			continue
		}
		if value == nil || value.Key != keys[index] || value.ModRevision <= 0 {
			return TransactionResult{}, volumeRemovalTerminalConflict()
		}
	}
	progress, err := removalrecord.DecodeProgress(read.Values[0].Value)
	if err != nil || progress.OperationID != runtime.OperationID || !progress.DirectoryAbsent ||
		progress.NextRequestOrdinal <= 1 || task.FinishedAt.Before(progress.UpdatedAt) {
		return TransactionResult{}, volumeRemovalTerminalConflict()
	}
	attempt, err := removalrecord.DecodeAttempt(read.Values[2].Value)
	if err != nil || attempt.OperationID != runtime.OperationID || attempt.TaskID != task.ID ||
		attempt.OriginTaskID != runtime.OriginTaskID || attempt.PredecessorTaskID != task.RetryOf ||
		attempt.Ordinal != runtime.AttemptOrdinal {
		return TransactionResult{}, volumeRemovalTerminalConflict()
	}
	for _, value := range read.Values[3:5] {
		owner, err := removalrecord.DecodeOwner(value.Value)
		if err != nil || owner.OperationID != runtime.OperationID || owner.VolumeID != runtime.VolumeID ||
			owner.EnvironmentID != runtime.EnvironmentID {
			return TransactionResult{}, volumeRemovalTerminalConflict()
		}
	}
	head, err := decodeTaskReference(read.Values[7].Value)
	if err != nil || head != runtime.DesiredRevisionID {
		return TransactionResult{}, volumeRemovalTerminalConflict()
	}
	marker, err := decodeIdempotencyMarker(read.Values[5].Value, locator)
	if err != nil {
		return TransactionResult{}, err
	}
	defer clear(marker.Intent.Ciphertext)
	defer clear(marker.Response.Body)
	if marker.State != IdempotencyMarkerPending || marker.Kind != IdempotencyMarkerTask ||
		marker.TaskID != runtime.OriginTaskID || marker.Response.Status != http.StatusAccepted ||
		marker.ReplayTarget == nil || marker.ReplayTarget.Kind != IdempotencyReplayTargetVolume ||
		marker.ReplayTarget.ID != runtime.VolumeID || sha256.Sum256(marker.Intent.Ciphertext) != runtime.IntentSHA256 ||
		sha256.Sum256(marker.Response.Body) != runtime.RootResponseSHA256 ||
		decodeReplayTargetReference(read.Values[6].Value, markerKey) != nil {
		return TransactionResult{}, volumeRemovalTerminalConflict()
	}
	completionKey := removalrecord.CompletionKey(runtime.OperationID, progress.NextRequestOrdinal-1)
	completionRead, err := repository.store.GetMany(
		ctx,
		GetManyRequest{Keys: []string{completionKey}, Revision: read.ReadRevision},
	)
	if err != nil {
		return TransactionResult{}, err
	}
	if completionRead == nil || completionRead.ReadRevision != read.ReadRevision || len(completionRead.Values) != 1 ||
		completionRead.Values[0] == nil || completionRead.Values[0].Key != completionKey || completionRead.Values[0].ModRevision <= 0 {
		return TransactionResult{}, volumeRemovalTerminalConflict()
	}
	defer clearKeyValues(completionRead.Values)
	completion, err := removalrecord.DecodeCompletion(completionRead.Values[0].Value)
	if err != nil || completion.OperationID != runtime.OperationID || !completion.DirectoryAbsent ||
		completion.RequestOrdinal != progress.NextRequestOrdinal-1 || !completion.CompletedAt.Equal(progress.UpdatedAt) ||
		completionRead.Values[0].ModRevision != read.Values[0].ModRevision ||
		read.Values[0].ModRevision != runtimeRead.Entry.ModRevision {
		return TransactionResult{}, volumeRemovalTerminalConflict()
	}
	baseConditions := conditions
	conditions = make([]Condition, 0, len(baseConditions)+len(keys)+2)
	for _, condition := range baseConditions {
		conditions, err = appendVolumeRemovalTerminalCondition(conditions, condition)
		if err != nil {
			return TransactionResult{}, err
		}
	}
	for index, key := range keys {
		conditions, err = appendVolumeRemovalTerminalCondition(
			conditions,
			Condition{Key: key, ModRevision: keyValueRevision(read.Values[index])},
		)
		if err != nil {
			return TransactionResult{}, err
		}
	}
	conditions = append(conditions, Condition{Key: runtimeRead.Entry.Key, ModRevision: runtimeRead.Entry.ModRevision},
		Condition{Key: completionKey, ModRevision: completionRead.Values[0].ModRevision})
	ancestry, err := bindHierarchyMutation(ctx, repository.store, read.ReadRevision,
		HierarchyMutationScope{TenantID: task.Owner.TenantID, ProjectID: task.Owner.ProjectID}, nil, nil)
	if err != nil {
		return TransactionResult{}, err
	}
	defer ancestry.clear()
	conditions = append(conditions, ancestry.conditions...)
	conditions = append(
		conditions,
		Condition{
			Key: HierarchyDeletionTombstoneKey(string(HierarchyDeletionTargetEnvironment), runtime.EnvironmentID),
		},
		Condition{Key: HierarchyDeletionTombstoneKey(string(HierarchyDeletionTargetProject), task.Owner.ProjectID)},
	)
	if task.Owner.TenantID != "" {
		conditions = append(
			conditions,
			Condition{Key: HierarchyDeletionTombstoneKey(string(HierarchyDeletionTargetTenant), task.Owner.TenantID)},
		)
	}
	marker.State, marker.UpdatedAt, marker.TerminalAt = IdempotencyMarkerCompleted, *task.FinishedAt, *task.FinishedAt
	marker.RetainUntil = task.FinishedAt.Add(markerRetention)
	markerValue, err := encodeIdempotencyMarker(marker)
	if err != nil {
		return TransactionResult{}, err
	}
	defer clear(markerValue)
	retentionKey, err := idempotencyRetentionKey(markerKey, marker.RetainUntil)
	if err != nil {
		return TransactionResult{}, err
	}
	retentionValue, err := json.Marshal(retentionReferenceJSON{Schema: 1, MarkerKey: markerKey})
	if err != nil {
		return TransactionResult{}, errs.Wrap(errs.KindInternal, err)
	}
	defer clear(retentionValue)
	conditions, err = appendVolumeRemovalTerminalCondition(conditions, Condition{Key: retentionKey})
	if err != nil {
		return TransactionResult{}, err
	}
	mutations = append([]Mutation(nil), mutations...)
	mutations = append(mutations, ancestry.mutations...)
	mutations = replaceVolumeRemovalTerminalPut(mutations, markerKey, markerValue)
	mutations = replaceVolumeRemovalTerminalPut(mutations, retentionKey, retentionValue)
	mutations = append(
		mutations,
		Mutation{Type: MutationDelete, Key: removalrecord.Root(runtime.OperationID), Prefix: true},
		Mutation{Type: MutationDelete, Key: keys[3]},
		Mutation{Type: MutationDelete, Key: keys[4]},
	)
	if len(conditions) > 26 || len(mutations) > 26 {
		return TransactionResult{}, errs.Newf(errs.KindInternal,
			"Volume removal terminal transaction exceeds its operation budget: %d compares, %d mutations",
			len(conditions), len(mutations))
	}
	if err := validateBlueprintTransaction(repository.store, conditions, mutations, 52, 900*1024); err != nil {
		return TransactionResult{}, err
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
		task.RetryOf != runtime.PredecessorTaskID || len(task.Steps) != 1 || task.Steps[0].ID != runtime.StepID ||
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
	ctx context.Context, task TaskRecord, conditions []Condition, mutations []Mutation,
) (TransactionResult, error) {
	if (task.Status != TaskStatusFailed && task.Status != TaskStatusTimedOut) ||
		task.Result == nil || task.Result.Kind != TaskResultEnvironmentDirectory ||
		task.Result.Diagnostic != TaskResultDiagnosticNone || task.Result.ReconciliationRequired ||
		len(task.Result.Projects) != 0 {
		return TransactionResult{}, volumeRemovalTerminalConflict()
	}
	runtimeKey := removalrecord.RuntimeKey(task.OperationID)
	current, err := repository.store.Get(ctx, runtimeKey)
	if err != nil {
		return TransactionResult{}, err
	}
	if current == nil || current.ReadRevision <= 0 || current.Entry == nil || current.Entry.Key != runtimeKey ||
		current.Entry.ModRevision <= 0 {
		return TransactionResult{}, volumeRemovalTerminalConflict()
	}
	defer clear(current.Entry.Value)
	runtime, err := removalrecord.DecodeRuntime(current.Entry.Value)
	if err != nil || !volumeRemovalTaskMatchesRuntime(task, runtime) ||
		runtime.Checkpoint < removalrecord.DesiredPublished || runtime.Checkpoint > removalrecord.DirectoryAbsent {
		return TransactionResult{}, volumeRemovalTerminalConflict()
	}
	locator := IdempotencyLocator{ScopeKind: IdempotencyScopeKind(runtime.RootLocator.ScopeKind),
		ScopeID: runtime.RootLocator.ScopeID, Method: runtime.RootLocator.Method,
		Route: runtime.RootLocator.Route, Key: runtime.RootLocator.Key}
	markerKey, err := idempotencyMarkerKey(locator)
	if err != nil {
		return TransactionResult{}, err
	}
	keys := []string{removalrecord.OwnerKey(runtime.VolumeID), removalrecord.EnvironmentLockKey(runtime.EnvironmentID),
		removalrecord.ProgressKey(runtime.OperationID), removalrecord.PendingPathKey(runtime.OperationID),
		removalrecord.AttemptKey(runtime.OperationID, runtime.AttemptOrdinal), markerKey}
	replayKey, err := idempotencyReplayTargetKey(IdempotencyReplayTarget{Kind: IdempotencyReplayTargetVolume,
		ID: runtime.VolumeID}, locator.Method, locator.Route, locator.Key)
	if err != nil {
		return TransactionResult{}, err
	}
	keys = append(keys, environmentBlueprintHeadKey(runtime.EnvironmentID), replayKey,
		HierarchyDeletionTombstoneKey(string(HierarchyDeletionTargetEnvironment), runtime.EnvironmentID),
		HierarchyDeletionTombstoneKey(string(HierarchyDeletionTargetProject), task.Owner.ProjectID))
	if task.Owner.TenantID != "" {
		keys = append(keys, HierarchyDeletionTombstoneKey(string(HierarchyDeletionTargetTenant), task.Owner.TenantID))
	}
	read, err := repository.store.GetMany(ctx, GetManyRequest{Keys: keys, Revision: current.ReadRevision})
	if err != nil {
		return TransactionResult{}, err
	}
	if read == nil || read.ReadRevision != current.ReadRevision || len(read.Values) != len(keys) {
		return TransactionResult{}, volumeRemovalTerminalConflict()
	}
	defer clearKeyValues(read.Values)
	conditions = append([]Condition(nil), conditions...)
	conditions = append(conditions, Condition{Key: runtimeKey, ModRevision: current.Entry.ModRevision})
	for index, value := range read.Values {
		if index < 8 && index != 3 && value == nil || index >= 8 && value != nil ||
			value != nil && (value.Key != keys[index] || value.ModRevision <= 0) {
			return TransactionResult{}, volumeRemovalTerminalConflict()
		}
		conditions, err = appendVolumeRemovalTerminalCondition(
			conditions,
			Condition{Key: keys[index], ModRevision: keyValueRevision(value)},
		)
		if err != nil {
			return TransactionResult{}, err
		}
	}
	for _, value := range read.Values[:2] {
		owner, err := removalrecord.DecodeOwner(value.Value)
		if err != nil || owner.OperationID != runtime.OperationID || owner.EnvironmentID != runtime.EnvironmentID ||
			owner.VolumeID != runtime.VolumeID {
			return TransactionResult{}, volumeRemovalTerminalConflict()
		}
	}
	progress, err := removalrecord.DecodeProgress(read.Values[2].Value)
	if err != nil || progress.OperationID != runtime.OperationID || task.FinishedAt.Before(progress.UpdatedAt) ||
		progress.DirectoryAbsent != (runtime.Checkpoint == removalrecord.DirectoryAbsent) {
		return TransactionResult{}, volumeRemovalTerminalConflict()
	}
	if read.Values[3] != nil {
		pending, err := removalrecord.DecodePendingPath(read.Values[3].Value)
		if err != nil || pending.OperationID != runtime.OperationID || pending.TaskID != task.ID ||
			pending.VolumeID != runtime.VolumeID || pending.Key != runtime.Key || pending.IntentSHA256 != runtime.IntentSHA256 ||
			pending.RequestOrdinal != progress.NextRequestOrdinal || runtime.Checkpoint != removalrecord.ConsumersDetached {
			return TransactionResult{}, volumeRemovalTerminalConflict()
		}
	}
	attempt, err := removalrecord.DecodeAttempt(read.Values[4].Value)
	if err != nil || attempt.OperationID != runtime.OperationID || attempt.TaskID != task.ID ||
		attempt.OriginTaskID != runtime.OriginTaskID || attempt.Ordinal != runtime.AttemptOrdinal ||
		attempt.PredecessorTaskID != task.RetryOf {
		return TransactionResult{}, volumeRemovalTerminalConflict()
	}
	marker, err := decodeIdempotencyMarker(read.Values[5].Value, locator)
	if err != nil {
		return TransactionResult{}, err
	}
	defer clear(marker.Intent.Ciphertext)
	defer clear(marker.Response.Body)
	if marker.Kind != IdempotencyMarkerTask || marker.State != IdempotencyMarkerPending ||
		marker.TaskID != runtime.OriginTaskID || marker.Response.Status != http.StatusAccepted || marker.ReplayTarget == nil ||
		marker.ReplayTarget.Kind != IdempotencyReplayTargetVolume || marker.ReplayTarget.ID != runtime.VolumeID ||
		sha256.Sum256(marker.Intent.Ciphertext) != runtime.IntentSHA256 ||
		sha256.Sum256(marker.Response.Body) != runtime.RootResponseSHA256 ||
		decodeReplayTargetReference(read.Values[7].Value, markerKey) != nil {
		return TransactionResult{}, volumeRemovalTerminalConflict()
	}
	head, err := decodeTaskReference(read.Values[6].Value)
	if err != nil || head != runtime.DesiredRevisionID {
		return TransactionResult{}, volumeRemovalTerminalConflict()
	}
	retentionKey, err := idempotencyRetentionKey(markerKey, task.FinishedAt.Add(markerRetention))
	if err != nil {
		return TransactionResult{}, err
	}
	filtered := make([]Mutation, 0, len(mutations))
	for _, mutation := range mutations {
		if mutation.Key != markerKey && mutation.Key != retentionKey {
			filtered = append(filtered, mutation)
		}
	}
	ancestry, err := bindHierarchyMutation(ctx, repository.store, read.ReadRevision,
		HierarchyMutationScope{TenantID: task.Owner.TenantID, ProjectID: task.Owner.ProjectID}, conditions, filtered)
	if err != nil {
		return TransactionResult{}, err
	}
	defer ancestry.clear()
	if len(ancestry.conditions) > 26 || len(ancestry.mutations) > 26 {
		return TransactionResult{}, errs.New(
			errs.KindInternal,
			"Volume removal attempt terminal transaction exceeds its operation budget",
		)
	}
	if err := validateBlueprintTransaction(repository.store, ancestry.conditions, ancestry.mutations, 52, 900*1024); err != nil {
		return TransactionResult{}, err
	}
	return repository.store.Transact(ctx, ancestry.conditions, ancestry.mutations)
}

func appendVolumeRemovalTerminalCondition(conditions []Condition, candidate Condition) ([]Condition, error) {
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

func replaceVolumeRemovalTerminalPut(mutations []Mutation, key string, value []byte) []Mutation {
	for index, mutation := range mutations {
		if mutation.Key == key {
			mutations[index] = Mutation{Type: MutationPut, Key: key, Value: value}
			return mutations
		}
	}
	return append(mutations, Mutation{Type: MutationPut, Key: key, Value: value})
}

func volumeRemovalTerminalConflict() error {
	return errs.New(errs.KindStateConflict, "Volume removal terminal authority is incomplete or changed")
}
