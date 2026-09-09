package volumeremoval

import (
	"bytes"
	"context"
	"crypto/sha256"
	"net/http"

	removalrecord "github.com/AlanD20/groundplane/internal/infra/volumeremovalrecord"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/infra/etcd"
	"github.com/AlanD20/groundplane/pkg/errs"
)

func (repository *EnvironmentVolumeRemovalRuntimeRepository) PublishSuccessorAttempt(
	ctx context.Context,
	operationID string,
	successor etcd.TaskRecord,
) (EnvironmentVolumeRemovalResumeState, error) {
	state, err := repository.Resume(ctx, operationID)
	if err != nil {
		return EnvironmentVolumeRemovalResumeState{}, err
	}
	if state.Runtime.Record.Checkpoint == removalrecord.RuntimeFinalized || state.Pending != nil {
		return EnvironmentVolumeRemovalResumeState{}, errs.New(
			errs.KindStateConflict,
			"Environment Volume removal attempt cannot advance",
		)
	}
	ownerFence, err := repository.loadOwnerFence(ctx, state.Runtime.Record, state.Runtime.ReadRevision)
	if err != nil {
		return EnvironmentVolumeRemovalResumeState{}, err
	}
	rootMarkerKey, err := etcd.CapabilityIdempotencyMarkerKey(volumeRemovalRootLocator(state.Runtime.Record))
	if err != nil {
		return EnvironmentVolumeRemovalResumeState{}, removalrecord.Corrupt()
	}
	read, err := repository.store.GetMany(ctx, etcd.GetManyRequest{Keys: []string{
		etcd.CapabilityTaskKey(state.Runtime.Record.CurrentTaskID),
		rootMarkerKey,
	}, Revision: state.Runtime.ReadRevision})
	if err != nil {
		return EnvironmentVolumeRemovalResumeState{}, err
	}
	if read == nil || len(read.Values) != 2 || read.Values[0] == nil || read.Values[1] == nil {
		return EnvironmentVolumeRemovalResumeState{}, errs.New(
			errs.KindStateConflict,
			"Environment Volume removal retry evidence is incomplete",
		)
	}
	defer clearKeyValues(read.Values)
	source, err := etcd.DecodeCapabilityTaskRecord(read.Values[0].Value)
	if err != nil || source.ID != state.Runtime.Record.CurrentTaskID ||
		!etcd.IsCapabilityTerminalTaskStatus(source.Status) || source.FinishedAt == nil {
		return EnvironmentVolumeRemovalResumeState{}, errs.New(
			errs.KindStateConflict,
			"Environment Volume removal current attempt is not terminal",
		)
	}
	if err := validateEnvironmentVolumeRemovalTask(source, state.Runtime.Record, state.Attempt); err != nil {
		return EnvironmentVolumeRemovalResumeState{}, err
	}
	if err := validateEnvironmentVolumeRemovalRootMarker(
		read.Values[1].Value, state.Runtime.Record,
	); err != nil {
		return EnvironmentVolumeRemovalResumeState{}, err
	}
	attempt := removalrecord.Attempt{
		OperationID:       operationID,
		OriginTaskID:      state.Runtime.Record.OriginTaskID,
		TaskID:            successor.ID,
		PredecessorTaskID: source.ID,
		Ordinal:           state.Runtime.Record.AttemptOrdinal + 1,
		CreatedAt:         successor.CreatedAt,
	}
	if attempt.Ordinal == 0 {
		return EnvironmentVolumeRemovalResumeState{}, errs.New(
			errs.KindValidationFailed,
			"Environment Volume removal attempt ordinal overflowed",
		)
	}
	updated := state.Runtime.Record
	updated.CurrentTaskID = successor.ID
	updated.PredecessorTaskID = source.ID
	updated.AttemptOrdinal = attempt.Ordinal
	updated.UpdatedAt = successor.CreatedAt
	if successor.Status != etcd.TaskStatusPending || successor.Result != nil ||
		validateEnvironmentVolumeRemovalTask(successor, updated, attempt) != nil {
		return EnvironmentVolumeRemovalResumeState{}, errs.New(
			errs.KindValidationFailed,
			"Environment Volume removal successor Task is invalid",
		)
	}
	taskValue, err := etcd.EncodeCapabilityTaskRecord(successor)
	if err != nil {
		return EnvironmentVolumeRemovalResumeState{}, err
	}
	reference, err := etcd.EncodeCapabilityTaskReference(successor.ID)
	if err != nil {
		clear(taskValue)
		return EnvironmentVolumeRemovalResumeState{}, err
	}
	attemptValue, err := removalrecord.EncodeAttempt(attempt)
	if err != nil {
		clear(taskValue)
		clear(reference)
		return EnvironmentVolumeRemovalResumeState{}, err
	}
	runtimeValue, err := removalrecord.EncodeRuntime(updated)
	if err != nil {
		clear(taskValue)
		clear(reference)
		clear(attemptValue)
		return EnvironmentVolumeRemovalResumeState{}, err
	}
	defer clear(taskValue)
	defer clear(reference)
	defer clear(attemptValue)
	defer clear(runtimeValue)
	conditions := []etcd.Condition{
		ownerFence,
		{Key: removalrecord.RuntimeKey(operationID), ModRevision: state.Runtime.Revision},
		{Key: etcd.CapabilityTaskKey(source.ID), ModRevision: read.Values[0].ModRevision},
		{Key: rootMarkerKey, ModRevision: read.Values[1].ModRevision},
		{Key: removalrecord.AttemptKey(operationID, attempt.Ordinal)},
		{Key: etcd.CapabilityTaskKey(successor.ID)},
		{Key: etcd.CapabilityTaskOperationIndexKey(operationID, successor.ID)},
		{Key: etcd.CapabilityTaskActiveOperationKey(operationID)},
		{Key: etcd.CapabilityTaskQueueKey(successor.Executor, successor.ID)},
	}
	mutations := []etcd.Mutation{
		{Type: etcd.MutationPut, Key: etcd.CapabilityTaskKey(successor.ID), Value: taskValue},
		{
			Type:  etcd.MutationPut,
			Key:   etcd.CapabilityTaskOperationIndexKey(operationID, successor.ID),
			Value: reference,
		},
		{Type: etcd.MutationPut, Key: etcd.CapabilityTaskActiveOperationKey(operationID), Value: reference},
		{Type: etcd.MutationPut, Key: etcd.CapabilityTaskQueueKey(successor.Executor, successor.ID), Value: reference},
		{
			Type:  etcd.MutationPut,
			Key:   removalrecord.AttemptKey(operationID, attempt.Ordinal),
			Value: attemptValue,
		},
		{Type: etcd.MutationPut, Key: removalrecord.RuntimeKey(operationID), Value: runtimeValue},
	}
	if err := validateEnvironmentVolumeRemovalTransaction(repository.store, conditions, mutations, 48); err != nil {
		return EnvironmentVolumeRemovalResumeState{}, err
	}
	transaction, err := repository.store.Transact(ctx, conditions, mutations)
	if err != nil {
		return EnvironmentVolumeRemovalResumeState{}, err
	}
	if !transaction.Succeeded {
		return EnvironmentVolumeRemovalResumeState{}, errs.New(
			errs.KindStateConflict,
			"Environment Volume removal successor publication raced",
		)
	}
	return repository.Resume(ctx, operationID)
}

func (repository *EnvironmentVolumeRemovalRuntimeRepository) ReplayRootResponse(
	ctx context.Context,
	operationID string,
	locator etcd.IdempotencyLocator,
	intentSHA256 [sha256.Size]byte,
) (string, bool, error) {
	state, err := repository.Resume(ctx, operationID)
	if err != nil {
		return "", false, err
	}
	if volumeRemovalRootLocator(state.Runtime.Record) != locator || state.Runtime.Record.IntentSHA256 != intentSHA256 {
		return "", false, errs.New(
			errs.KindIdempotencyMismatch,
			"Environment Volume removal idempotency intent changed",
		)
	}
	markerKey, err := etcd.CapabilityIdempotencyMarkerKey(locator)
	if err != nil {
		return "", false, removalrecord.Corrupt()
	}
	read, err := repository.store.GetMany(ctx, etcd.GetManyRequest{
		Keys: []string{markerKey}, Revision: state.Runtime.ReadRevision,
	})
	if err != nil {
		return "", false, err
	}
	if read == nil || len(read.Values) != 1 || read.Values[0] == nil {
		return "", false, errs.New(
			errs.KindStateConflict,
			"Environment Volume removal root replay marker is missing",
		)
	}
	defer clearKeyValues(read.Values)
	if err := validateEnvironmentVolumeRemovalRootMarker(
		read.Values[0].Value, state.Runtime.Record,
	); err != nil {
		return "", false, err
	}
	return state.Runtime.Record.OriginTaskID, true, nil
}

func (repository *EnvironmentVolumeRemovalRuntimeRepository) loadAssignmentFence(
	ctx context.Context,
	input EnvironmentVolumeRemovalAssignment,
	revision int64,
	runtime removalrecord.Runtime,
	attempt removalrecord.Attempt,
) ([]etcd.Condition, error) {
	_, conditions, err := repository.loadAssignedTask(ctx, input, revision, runtime, attempt)
	return conditions, err
}

func (repository *EnvironmentVolumeRemovalRuntimeRepository) loadAssignedTask(
	ctx context.Context,
	input EnvironmentVolumeRemovalAssignment,
	revision int64,
	runtime removalrecord.Runtime,
	attempt removalrecord.Attempt,
) (etcd.TaskRecord, []etcd.Condition, error) {
	if input.OperationID != runtime.OperationID || input.TaskID != runtime.CurrentTaskID ||
		ids.Validate(ids.KindAssignment, input.AssignmentID) != nil ||
		ids.Validate(ids.KindAgent, input.AgentID) != nil || input.AgentGeneration == 0 || revision <= 0 {
		return etcd.TaskRecord{}, nil, errs.New(
			errs.KindValidationFailed,
			"Environment Volume removal assignment is invalid",
		)
	}
	primary, err := repository.store.GetMany(ctx, etcd.GetManyRequest{
		Keys:     []string{etcd.CapabilityTaskKey(input.TaskID), etcd.CapabilityTaskAssignmentIndexKey(input.TaskID)},
		Revision: revision,
	})
	if err != nil {
		return etcd.TaskRecord{}, nil, err
	}
	if primary == nil || primary.ReadRevision != revision || len(primary.Values) != 2 ||
		primary.Values[0] == nil || primary.Values[1] == nil {
		return etcd.TaskRecord{}, nil, errs.New(
			errs.KindStateConflict,
			"Environment Volume removal assignment changed",
		)
	}
	defer clearKeyValues(primary.Values)
	task, err := etcd.DecodeCapabilityTaskRecord(primary.Values[0].Value)
	if err != nil || task.ID != input.TaskID || task.Status != etcd.TaskStatusRunning ||
		validateEnvironmentVolumeRemovalTask(task, runtime, attempt) != nil {
		return etcd.TaskRecord{}, nil, errs.New(
			errs.KindStateConflict,
			"Environment Volume removal Task changed",
		)
	}
	assignment, err := etcd.DecodeCapabilityTaskAssignment(primary.Values[1].Value)
	if err != nil || assignment.AssignmentID != input.AssignmentID ||
		assignment.TaskID != input.TaskID || assignment.Executor != etcd.TaskExecutorAgent ||
		assignment.AgentID != input.AgentID || assignment.AgentGeneration != input.AgentGeneration {
		return etcd.TaskRecord{}, nil, errs.New(
			errs.KindStateConflict,
			"Environment Volume removal assignment changed",
		)
	}
	claimKey := etcd.CapabilityTaskExecutionClaimKey(etcd.TaskExecutorAgent, input.AgentID, input.TaskID)
	claim, err := repository.store.GetMany(ctx, etcd.GetManyRequest{
		Keys:     []string{claimKey, etcd.CapabilityTaskTimeoutIndexKey(input.TaskID, assignment.Deadline)},
		Revision: revision,
	})
	if err != nil {
		return etcd.TaskRecord{}, nil, err
	}
	if claim == nil || claim.ReadRevision != revision || len(claim.Values) != 2 ||
		claim.Values[0] == nil || claim.Values[1] == nil ||
		claim.Values[0].ModRevision != primary.Values[1].ModRevision ||
		claim.Values[1].ModRevision != primary.Values[1].ModRevision ||
		!bytes.Equal(claim.Values[0].Value, primary.Values[1].Value) ||
		!bytes.Equal(claim.Values[1].Value, primary.Values[1].Value) {
		return etcd.TaskRecord{}, nil, errs.New(
			errs.KindStateConflict,
			"Environment Volume removal assignment copies changed",
		)
	}
	defer clearKeyValues(claim.Values)
	ownerFence, err := repository.loadOwnerFence(ctx, runtime, revision)
	if err != nil {
		return etcd.TaskRecord{}, nil, err
	}
	return task, []etcd.Condition{
		ownerFence,
		{Key: etcd.CapabilityTaskKey(input.TaskID), ModRevision: primary.Values[0].ModRevision},
		{Key: etcd.CapabilityTaskAssignmentIndexKey(input.TaskID), ModRevision: primary.Values[1].ModRevision},
		{Key: claimKey, ModRevision: claim.Values[0].ModRevision},
		{
			Key:         etcd.CapabilityTaskTimeoutIndexKey(input.TaskID, assignment.Deadline),
			ModRevision: claim.Values[1].ModRevision,
		},
	}, nil
}

func (repository *EnvironmentVolumeRemovalRuntimeRepository) loadOwnerFence(
	ctx context.Context,
	runtime removalrecord.Runtime,
	revision int64,
) (etcd.Condition, error) {
	key := removalrecord.OwnerKey(runtime.VolumeID)
	read, err := repository.store.GetMany(ctx, etcd.GetManyRequest{Keys: []string{key}, Revision: revision})
	if err != nil {
		return etcd.Condition{}, err
	}
	if read == nil || read.ReadRevision != revision || len(read.Values) != 1 ||
		read.Values[0] == nil || read.Values[0].ModRevision <= 0 {
		return etcd.Condition{}, errs.New(errs.KindStateConflict, "volume removal owner is missing")
	}
	defer clearKeyValues(read.Values)
	owner, err := removalrecord.DecodeOwner(read.Values[0].Value)
	if err != nil || owner.VolumeID != runtime.VolumeID || owner.EnvironmentID != runtime.EnvironmentID ||
		owner.OperationID != runtime.OperationID {
		return etcd.Condition{}, errs.New(errs.KindStateConflict, "volume removal owner changed")
	}
	return etcd.Condition{Key: key, ModRevision: read.Values[0].ModRevision}, nil
}

func validateEnvironmentVolumeRemovalRootMarker(
	value []byte,
	runtime removalrecord.Runtime,
) error {
	marker, err := etcd.DecodeCapabilityIdempotencyMarker(value, volumeRemovalRootLocator(runtime))
	if err != nil || marker.Kind != etcd.IdempotencyMarkerTask || marker.State != etcd.IdempotencyMarkerPending ||
		marker.TaskID != runtime.OriginTaskID || marker.Locator != volumeRemovalRootLocator(runtime) ||
		marker.Response.Status != http.StatusAccepted ||
		sha256.Sum256(marker.Response.Body) != runtime.RootResponseSHA256 {
		return errs.New(
			errs.KindStateConflict,
			"Environment Volume removal root replay evidence changed",
		)
	}
	return nil
}

func validateEnvironmentVolumeRemovalTransaction(
	store store,
	conditions []etcd.Condition,
	mutations []etcd.Mutation,
	maximumOperations int,
) error {
	operations := len(conditions) + len(mutations)
	if operations == 0 || operations > maximumOperations {
		return errs.New(errs.KindInternal, "Environment Volume removal transaction operation budget exceeded")
	}
	valueBytes := 0
	for _, condition := range conditions {
		if len(condition.Key) == 0 || len(condition.Key) > maximumKeyBytes {
			return errs.New(errs.KindInternal, "Environment Volume removal transaction key is invalid")
		}
	}
	for _, mutation := range mutations {
		if len(mutation.Key) == 0 || len(mutation.Key) > maximumKeyBytes {
			return errs.New(errs.KindInternal, "Environment Volume removal transaction key is invalid")
		}
		if mutation.Type == etcd.MutationPut {
			valueBytes += len(mutation.Value)
		}
	}
	const maximumBytes = 900 * 1024
	if valueBytes+operations*maximumKeyBytes+operations*64+128 > maximumBytes {
		return errs.New(errs.KindInternal, "Environment Volume removal transaction byte budget exceeded")
	}
	if sizer, ok := store.(transactionSizer); ok {
		actual, err := sizer.TransactionSize(conditions, mutations)
		if err != nil {
			return err
		}
		if actual > maximumBytes {
			return errs.New(errs.KindInternal, "Environment Volume removal encoded transaction is oversized")
		}
	}
	return nil
}

func sameEnvironmentVolumeRemovalRuntime(
	left removalrecord.Runtime,
	right removalrecord.Runtime,
) bool {
	return left.OperationID == right.OperationID && left.EnvironmentID == right.EnvironmentID &&
		left.VolumeID == right.VolumeID && left.Key == right.Key &&
		left.DesiredRevisionID == right.DesiredRevisionID && left.DesiredGeneration == right.DesiredGeneration &&
		left.ImpactSHA256 == right.ImpactSHA256 && left.EvidenceManifestSHA256 == right.EvidenceManifestSHA256 &&
		left.IntentSHA256 == right.IntentSHA256 && left.RootLocator == right.RootLocator &&
		left.RootResponseSHA256 == right.RootResponseSHA256 && left.OriginTaskID == right.OriginTaskID &&
		left.CurrentTaskID == right.CurrentTaskID && left.PredecessorTaskID == right.PredecessorTaskID &&
		left.AttemptOrdinal == right.AttemptOrdinal && left.StepID == right.StepID &&
		left.Checkpoint == right.Checkpoint && left.CreatedAt.Equal(right.CreatedAt) &&
		left.UpdatedAt.Equal(right.UpdatedAt)
}

func sameEnvironmentVolumeRemovalProgress(
	left removalrecord.Progress,
	right removalrecord.Progress,
) bool {
	return left.OperationID == right.OperationID && left.NextRequestOrdinal == right.NextRequestOrdinal &&
		left.DirectoryAbsent == right.DirectoryAbsent && left.UpdatedAt.Equal(right.UpdatedAt) &&
		equalVolumeRemovalStrings(left.ComponentStack, right.ComponentStack) && bytes.Equal(left.Cursor, right.Cursor)
}

func sameEnvironmentVolumeRemovalPendingPath(
	left removalrecord.PendingPath,
	right removalrecord.PendingPath,
) bool {
	return left.OperationID == right.OperationID && left.VolumeID == right.VolumeID && left.Key == right.Key &&
		left.IntentSHA256 == right.IntentSHA256 && left.RequestOrdinal == right.RequestOrdinal &&
		left.MutationBudget == right.MutationBudget && left.RequestSHA256 == right.RequestSHA256 &&
		left.TaskID == right.TaskID && left.AssignmentID == right.AssignmentID && left.AgentID == right.AgentID &&
		left.AgentGeneration == right.AgentGeneration && left.CreatedAt.Equal(right.CreatedAt) &&
		equalVolumeRemovalStrings(left.ComponentStack, right.ComponentStack) && bytes.Equal(left.Cursor, right.Cursor)
}

func sameEnvironmentVolumeRemovalCompletion(
	left removalrecord.Completion,
	right removalrecord.Completion,
) bool {
	return left.OperationID == right.OperationID && left.RequestOrdinal == right.RequestOrdinal &&
		left.RequestSHA256 == right.RequestSHA256 && left.ResponseSHA256 == right.ResponseSHA256 &&
		left.ResponseBytes == right.ResponseBytes &&
		left.MutationCount == right.MutationCount && left.DirectoryAbsent == right.DirectoryAbsent &&
		left.CompletedAt.Equal(right.CompletedAt) &&
		equalVolumeRemovalStrings(left.NextComponentStack, right.NextComponentStack) &&
		bytes.Equal(left.NextCursor, right.NextCursor)
}

func equalVolumeRemovalStrings(left, right []string) bool {
	if len(left) != len(right) {
		return false
	}
	for index := range left {
		if left[index] != right[index] {
			return false
		}
	}
	return true
}
